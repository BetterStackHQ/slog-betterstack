package slogbetterstack

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"time"

	"log/slog"

	slogcommon "github.com/samber/slog-common"
)

const BetterstackEndpoint = "https://in.logs.betterstack.com/"

type Option struct {
	// log level (default: debug)
	Level slog.Leveler

	// source token; without it the handler reports the omission through OnError once and
	// drops every record instead of sending anything
	Token string
	// optional: endpoint
	Endpoint string
	// optional: how long one upload attempt may take (default: 10s)
	Timeout time.Duration

	// optional: customize record builder
	Converter Converter
	// optional: custom marshaler, called with the []map[string]any of a batch's records
	Marshaler func(v any) ([]byte, error)
	// optional: fetch attributes from context
	AttrFromContext []func(ctx context.Context) []slog.Attr

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr

	// Delivery. Records are queued and uploaded in batches by a background goroutine, so
	// logging never waits for the network. Call Close before the program exits to deliver
	// what is still queued.

	// optional: records per upload (default: 1000)
	BatchSize int
	// optional: how long a partial batch waits before it is uploaded (default: 1s)
	BatchInterval time.Duration
	// optional: records the queue holds while uploads are behind; further records are
	// dropped and counted rather than blocking the application (default: 100000)
	MaxQueueSize int
	// optional: concurrent uploads (default: 5)
	MaxInFlight int
	// optional: retries after a failed attempt, for 408, 429, 5xx and network errors;
	// negative disables retries (default: 5)
	MaxRetries int
	// optional: base delay before a retry, doubled on every attempt with jitter; a
	// Retry-After header is honoured instead (default: 300ms)
	RetryBackoff time.Duration
	// optional: how long Close waits for queued and in-flight records (default: 15s)
	ShutdownTimeout time.Duration
	// optional: send the JSON uncompressed instead of gzip-compressed
	DisableCompression bool
	// optional: receives every delivery failure and drop summary, from a background
	// goroutine; it must not log through this handler (default: one line on stderr)
	OnError func(err error)
	// optional: the HTTP client to upload with; Timeout still applies to every request
	HTTPClient *http.Client
}

// NewBetterstackHandler returns a handler that sends records to Better Stack. The handler is
// also a [slog.Handler]; keep the returned value to call Close before the program exits.
func (o Option) NewBetterstackHandler() *BetterstackHandler {
	if o.Level == nil {
		o.Level = slog.LevelDebug
	}

	if o.Endpoint == "" {
		o.Endpoint = BetterstackEndpoint
	}

	if o.Timeout == 0 {
		o.Timeout = defaultTimeout
	}

	if o.Converter == nil {
		o.Converter = DefaultConverter
	}

	if o.Marshaler == nil {
		o.Marshaler = json.Marshal
	}

	if o.AttrFromContext == nil {
		o.AttrFromContext = []func(ctx context.Context) []slog.Attr{}
	}

	if o.BatchSize <= 0 {
		o.BatchSize = defaultBatchSize
	}
	if o.BatchInterval <= 0 {
		o.BatchInterval = defaultBatchInterval
	}
	if o.MaxQueueSize <= 0 {
		o.MaxQueueSize = defaultMaxQueueSize
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = defaultMaxInFlight
	}
	switch {
	case o.MaxRetries == 0:
		o.MaxRetries = defaultMaxRetries
	case o.MaxRetries < 0:
		o.MaxRetries = 0
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = defaultRetryBackoff
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = defaultShutdownTimeout
	}
	if o.OnError == nil {
		o.OnError = defaultOnError
	}

	return &BetterstackHandler{
		option:    o,
		attrs:     []slog.Attr{},
		groups:    []string{},
		transport: newTransport(o),
	}
}

var _ slog.Handler = (*BetterstackHandler)(nil)

// BetterstackHandler is a [slog.Handler] that sends records to Better Stack. Handlers derived
// with WithAttrs and WithGroup share the queue and the uploads of the handler they came from,
// and Close on any of them closes all of them.
type BetterstackHandler struct {
	option    Option
	attrs     []slog.Attr
	groups    []string
	transport *transport
}

func (h *BetterstackHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.option.Level.Level()
}

// Handle converts the record and queues it for upload. It never waits for the network: when
// the queue is full the record is dropped and counted. After Close it returns ErrClosed.
func (h *BetterstackHandler) Handle(ctx context.Context, record slog.Record) error {
	fromContext := slogcommon.ContextExtractor(ctx, h.option.AttrFromContext)
	// Every goroutine logging through this handler shares h.attrs, so appending must never
	// write into its spare capacity.
	attrs := append(slices.Clip(h.attrs), fromContext...)
	payload := h.option.Converter(h.option.AddSource, h.option.ReplaceAttr, attrs, h.groups, &record)

	return h.transport.enqueue(payload)
}

func (h *BetterstackHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &BetterstackHandler{
		option:    h.option,
		attrs:     slogcommon.AppendAttrsToGroup(h.groups, h.attrs, attrs...),
		groups:    h.groups,
		transport: h.transport,
	}
}

func (h *BetterstackHandler) WithGroup(name string) slog.Handler {
	// https://cs.opensource.google/go/x/exp/+/46b07846:slog/handler.go;l=247
	if name == "" {
		return h
	}

	return &BetterstackHandler{
		option:    h.option,
		attrs:     h.attrs,
		groups:    append(h.groups, name),
		transport: h.transport,
	}
}

// Flush uploads every record queued so far and returns once Better Stack has acknowledged
// them, a delivery failed for good, or ctx is done. Failures are reported through OnError.
func (h *BetterstackHandler) Flush(ctx context.Context) error {
	return h.transport.flush(ctx)
}

// Close delivers what is still queued, waits for the uploads in flight up to ShutdownTimeout
// and stops the background goroutine. It must run before the program exits: records are
// batched, so without it the last ones are lost. Note that os.Exit and log.Fatal skip deferred
// calls. Close is safe to call more than once; later calls return the first result.
func (h *BetterstackHandler) Close() error {
	return h.transport.close()
}

// Stats reports what happened to the records handed to this handler and the ones derived
// from it.
func (h *BetterstackHandler) Stats() Stats {
	return h.transport.stats.snapshot()
}
