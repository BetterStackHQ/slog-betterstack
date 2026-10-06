package slogbetterstack

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by Handle and Flush once the handler has been closed.
var ErrClosed = errors.New("slog-betterstack: the handler is closed")

// ErrMissingToken is returned by Handle, and reported through OnError once, when the handler
// was built without a source token. Nothing is sent and every record is counted as dropped.
var ErrMissingToken = errors.New("slog-betterstack: no source token configured, records are dropped")

const (
	defaultBatchSize       = 1000
	defaultBatchInterval   = time.Second
	defaultMaxQueueSize    = 100_000
	defaultMaxInFlight     = 5
	defaultMaxRetries      = 5
	defaultRetryBackoff    = 300 * time.Millisecond
	defaultTimeout         = 10 * time.Second
	defaultShutdownTimeout = 15 * time.Second

	maxBackoff         = 30 * time.Second
	maxRetryAfter      = time.Minute
	dropReportInterval = 5 * time.Second
)

// Stats counts what happened to the records handed to a handler. Once Close has returned,
// Enqueued equals Sent plus all the Dropped counters.
type Stats struct {
	Enqueued uint64 // records handed to Handle
	Sent     uint64 // records acknowledged by Better Stack
	Retries  uint64 // upload attempts after the first

	DroppedQueueFull uint64 // the application logged faster than records could be delivered
	DroppedRejected  uint64 // Better Stack refused them, the retries ran out, or they could not be encoded
	DroppedOversize  uint64 // a single record larger than Better Stack accepts
	DroppedClosed    uint64 // logged after Close, or not delivered before the shutdown timeout
}

type counters struct {
	enqueued, sent, retries                                           atomic.Uint64
	droppedQueueFull, droppedRejected, droppedOversize, droppedClosed atomic.Uint64
}

func (c *counters) snapshot() Stats {
	return Stats{
		Enqueued:         c.enqueued.Load(),
		Sent:             c.sent.Load(),
		Retries:          c.retries.Load(),
		DroppedQueueFull: c.droppedQueueFull.Load(),
		DroppedRejected:  c.droppedRejected.Load(),
		DroppedOversize:  c.droppedOversize.Load(),
		DroppedClosed:    c.droppedClosed.Load(),
	}
}

func defaultOnError(err error) {
	fmt.Fprintln(os.Stderr, err)
}

// transport moves records from Handle to Better Stack: a bounded queue, one sender goroutine
// that assembles batches, and up to MaxInFlight upload goroutines that retry. Handlers derived
// from one another share a transport.
type transport struct {
	option Option
	client *http.Client
	owned  *http.Transport // nil when the caller supplied the HTTP client

	queue   chan map[string]any
	flushes chan flushRequest // unbuffered: a request is only ever taken by a running sender
	closing chan struct{}     // closed by close: the sender drains, uploads and exits
	exited  chan struct{}     // closed by the sender on its way out
	slots   chan struct{}     // one token per upload in flight

	ctx    context.Context // cancelled when the shutdown timeout runs out
	cancel context.CancelFunc

	mu      sync.Mutex // guards started, closed and the queue send in enqueue
	started bool
	closed  bool

	closeOnce sync.Once
	closeErr  error
	inFlight  sync.WaitGroup
	tokenOnce sync.Once // reports the missing token a single time

	stats             counters
	reportedQueueFull uint64 // queue-full drops already summarised through OnError
}

type flushRequest struct {
	done chan struct{} // closed once every upload dispatched before the request has finished
}

func newTransport(option Option) *transport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &transport{
		option:  option,
		queue:   make(chan map[string]any, option.MaxQueueSize),
		flushes: make(chan flushRequest),
		closing: make(chan struct{}),
		exited:  make(chan struct{}),
		slots:   make(chan struct{}, option.MaxInFlight),
		ctx:     ctx,
		cancel:  cancel,
	}
	if option.HTTPClient != nil {
		t.client = option.HTTPClient
	} else {
		t.owned = http.DefaultTransport.(*http.Transport).Clone()
		t.client = &http.Client{Transport: t.owned}
	}
	return t
}

// enqueue hands a record to the sender without ever blocking: when the queue is full the
// record is dropped and counted. The sender starts with the first record.
func (t *transport) enqueue(record map[string]any) error {
	t.stats.enqueued.Add(1)

	if t.option.Token == "" {
		t.stats.droppedRejected.Add(1)
		t.tokenOnce.Do(func() { t.report(ErrMissingToken) })
		return ErrMissingToken
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		t.stats.droppedClosed.Add(1)
		return ErrClosed
	}
	if !t.started {
		t.started = true
		go t.run()
	}
	select {
	case t.queue <- record:
	default:
		t.stats.droppedQueueFull.Add(1)
	}
	return nil
}

// run is the sender: it takes records off the queue, uploads a batch when it is full or has
// waited BatchInterval, and serves Flush and Close.
func (t *transport) run() {
	defer close(t.exited)

	var (
		batch   []map[string]any
		pending []chan struct{} // uploads dispatched since the last Flush
		timer   = time.NewTimer(time.Hour)
		timerC  <-chan time.Time // nil while no partial batch is waiting
	)
	timer.Stop()
	reports := time.NewTicker(dropReportInterval)
	defer reports.Stop()

	dispatch := func() {
		if timerC != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerC = nil
		}
		if len(batch) == 0 {
			return
		}
		records := batch
		batch = nil
		pending = append(settled(pending), t.dispatch(records))
	}
	add := func(record map[string]any) {
		batch = append(batch, record)
		if len(batch) == 1 {
			timer.Reset(t.option.BatchInterval)
			timerC = timer.C
		}
		if len(batch) >= t.option.BatchSize {
			dispatch()
		}
	}
	drain := func() {
		for {
			select {
			case record := <-t.queue:
				add(record)
			default:
				return
			}
		}
	}

	for {
		select {
		case record := <-t.queue:
			add(record)
		case <-timerC:
			timerC = nil
			dispatch()
		case request := <-t.flushes:
			drain()
			dispatch()
			uploads := pending
			pending = nil
			go func() {
				for _, done := range uploads {
					<-done
				}
				close(request.done)
			}()
		case <-reports.C:
			t.reportQueueFullDrops()
		case <-t.closing:
			drain()
			dispatch()
			return
		}
	}
}

// settled drops the uploads that have finished, so pending only ever holds what is in flight.
func settled(uploads []chan struct{}) []chan struct{} {
	kept := uploads[:0]
	for _, done := range uploads {
		select {
		case <-done:
		default:
			kept = append(kept, done)
		}
	}
	return kept
}

// dispatch starts the upload of a batch once an upload slot is free, which is the only point
// where the sender waits: while every slot is taken the queue fills, and then enqueue drops.
func (t *transport) dispatch(records []map[string]any) chan struct{} {
	done := make(chan struct{})
	t.slots <- struct{}{}
	t.inFlight.Add(1)
	go func() {
		defer func() {
			<-t.slots
			t.inFlight.Done()
			close(done)
		}()
		t.upload(records)
	}()
	return done
}

// upload sends a batch, retrying what is worth retrying, splitting what is too large and
// reporting what cannot be delivered.
func (t *transport) upload(records []map[string]any) {
	body, err := t.option.Marshaler(records)
	if err != nil {
		t.drop(&t.stats.droppedRejected, len(records), fmt.Errorf("slog-betterstack: dropped %s that could not be encoded: %w", plural(len(records), "record"), err))
		return
	}
	if !t.option.DisableCompression {
		if body, err = compress(body); err != nil {
			t.drop(&t.stats.droppedRejected, len(records), fmt.Errorf("slog-betterstack: dropped %s that could not be compressed: %w", plural(len(records), "record"), err))
			return
		}
	}

	for attempt := 0; ; attempt++ {
		if t.ctx.Err() != nil {
			t.drop(&t.stats.droppedClosed, len(records), nil)
			return
		}

		code, status, header, err := t.post(body)
		var retryAfter time.Duration
		if err == nil {
			switch {
			case code >= 200 && code < 300:
				t.stats.sent.Add(uint64(len(records)))
				return
			case code == http.StatusRequestEntityTooLarge:
				if len(records) > 1 {
					half := len(records) / 2
					t.upload(records[:half])
					t.upload(records[half:])
					return
				}
				t.drop(&t.stats.droppedOversize, 1, fmt.Errorf("slog-betterstack: dropped a record that is larger than Better Stack accepts (%s)", status))
				return
			case !retryable(code):
				t.drop(&t.stats.droppedRejected, len(records), fmt.Errorf("slog-betterstack: Better Stack rejected %s: %s", plural(len(records), "record"), status))
				return
			}
			err = fmt.Errorf("the server answered %s", status)
			retryAfter = parseRetryAfter(header.Get("Retry-After"))
		}

		if t.ctx.Err() != nil {
			t.drop(&t.stats.droppedClosed, len(records), nil)
			return
		}
		if attempt >= t.option.MaxRetries {
			t.drop(&t.stats.droppedRejected, len(records), fmt.Errorf("slog-betterstack: dropped %s after %s: %w", plural(len(records), "record"), plural(attempt+1, "attempt"), err))
			return
		}
		t.stats.retries.Add(1)
		if !t.wait(backoff(t.option.RetryBackoff, attempt, retryAfter)) {
			t.drop(&t.stats.droppedClosed, len(records), nil)
			return
		}
	}
}

// post makes one upload attempt. It returns the status code and line and the response headers,
// or the error of an attempt that got no response.
func (t *transport) post(body []byte) (int, string, http.Header, error) {
	ctx, cancel := context.WithTimeout(t.ctx, t.option.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.option.Endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, "", nil, err
	}
	req.Header.Set("Authorization", "Bearer "+t.option.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	if !t.option.DisableCompression {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	// Reading the body to its end lets the connection be reused for the next upload.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	return resp.StatusCode, resp.Status, resp.Header, nil
}

// retryable reports whether a status is worth another attempt: a timeout, a throttle or a
// server-side failure. Anything else 4xx is a verdict on the request itself.
func retryable(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= 500
}

// parseRetryAfter reads a Retry-After header given as seconds or as an HTTP date, capped so
// that a throttle cannot park an upload for longer than a minute.
func parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	var wait time.Duration
	if seconds, err := strconv.Atoi(value); err == nil {
		wait = time.Duration(seconds) * time.Second
	} else if at, err := http.ParseTime(value); err == nil {
		wait = time.Until(at)
	}
	if wait < 0 {
		return 0
	}
	if wait > maxRetryAfter {
		return maxRetryAfter
	}
	return wait
}

// backoff is the delay before the retry after the given attempt: what the server asked for,
// or base doubled per attempt with jitter, capped at maxBackoff.
func backoff(base time.Duration, attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	delay := base
	for i := 0; i < attempt && delay < maxBackoff; i++ {
		delay *= 2
	}
	if delay > maxBackoff {
		delay = maxBackoff
	}
	if delay <= 1 {
		return delay
	}
	return delay/2 + time.Duration(rand.Int63n(int64(delay/2)))
}

// wait sleeps for d unless the transport is shutting down first.
func (t *transport) wait(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.ctx.Done():
		return false
	}
}

func (t *transport) drop(counter *atomic.Uint64, n int, err error) {
	counter.Add(uint64(n))
	if err != nil {
		t.report(err)
	}
}

// report hands an error to OnError. A panic in the callback is contained: the host application
// must not go down because its error reporter has a bug.
func (t *transport) report(err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "slog-betterstack: OnError panicked: %v\n", r)
		}
	}()
	t.option.OnError(err)
}

// reportQueueFullDrops summarises the records dropped at the queue since the last summary.
// It runs on the sender goroutine and, after the sender has exited, from close.
func (t *transport) reportQueueFullDrops() {
	total := t.stats.droppedQueueFull.Load()
	if n := total - t.reportedQueueFull; n > 0 {
		t.reportedQueueFull = total
		t.report(fmt.Errorf("slog-betterstack: dropped %s because the queue is full: the application logs faster than records can be delivered", plural(n, "record")))
	}
}

// flush asks the sender to upload everything queued so far and waits for those uploads.
func (t *transport) flush(ctx context.Context) error {
	t.mu.Lock()
	started, closed := t.started, t.closed
	t.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if !started {
		return nil
	}

	request := flushRequest{done: make(chan struct{})}
	select {
	case t.flushes <- request:
	case <-t.exited:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-request.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// close stops accepting records, delivers what is queued, waits for the uploads in flight up
// to ShutdownTimeout and reports what was left behind.
func (t *transport) close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		started := t.started
		t.mu.Unlock()

		deadline := time.NewTimer(t.option.ShutdownTimeout)
		defer deadline.Stop()
		timedOut := false

		if started {
			close(t.closing)
			select {
			case <-t.exited:
			case <-deadline.C:
				timedOut = true
				t.cancel()
				<-t.exited
			}

			// The sender has exited, so nothing adds to inFlight any more.
			finished := make(chan struct{})
			go func() {
				t.inFlight.Wait()
				close(finished)
			}()
			if timedOut {
				<-finished
			} else {
				select {
				case <-finished:
				case <-deadline.C:
					timedOut = true
					t.cancel()
					<-finished
				}
			}
		}
		t.cancel()
		if t.owned != nil {
			t.owned.CloseIdleConnections()
		}

		t.reportQueueFullDrops()
		if timedOut {
			undelivered := t.stats.droppedClosed.Load()
			t.closeErr = fmt.Errorf("slog-betterstack: the shutdown timeout of %s ran out with %s undelivered", t.option.ShutdownTimeout, plural(undelivered, "record"))
			t.report(t.closeErr)
		}
	})
	return t.closeErr
}

func compress(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(body); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func plural[N int | uint64](n N, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
