package slogbetterstack

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests pin the behaviour applications rely on: the shape of the records that reach
// Better Stack, the request headers, and what every option does. They only ever look at what
// arrives at the server, never at how it got there.

type request struct {
	header  http.Header
	records []map[string]any
}

// responder writes the status Better Stack would answer with. The records are the decoded body.
type responder func(w http.ResponseWriter, r *http.Request, records []map[string]any)

func accepted(w http.ResponseWriter, _ *http.Request, _ []map[string]any) {
	w.WriteHeader(http.StatusAccepted)
}

func status(code int) responder {
	return func(w http.ResponseWriter, _ *http.Request, _ []map[string]any) { w.WriteHeader(code) }
}

// newServer stands in for Better Stack: it decodes every request's JSON array body, gzip-compressed
// or not, lets respond answer it and reports the request once it is answered.
func newServer(t *testing.T, respond responder) (*httptest.Server, <-chan request) {
	t.Helper()
	requests := make(chan request, 1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			unzipped, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("Content-Encoding is gzip but the body is not: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer func() { _ = unzipped.Close() }()
			body = unzipped
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var records []map[string]any
		if err := json.Unmarshal(raw, &records); err != nil {
			t.Errorf("body is not a JSON array of records: %v\n%s", err, raw)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		respond(w, r, records)
		requests <- request{header: r.Header.Clone(), records: records}
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func receive(t *testing.T, requests <-chan request) request {
	t.Helper()
	select {
	case r := <-requests:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no request reached the server within 5s")
		return request{}
	}
}

// receiveN waits for n requests.
func receiveN(t *testing.T, requests <-chan request, n int) []request {
	t.Helper()
	var got []request
	for len(got) < n {
		got = append(got, receive(t, requests))
	}
	return got
}

func nothingWithin(t *testing.T, requests <-chan request, d time.Duration) {
	t.Helper()
	select {
	case r := <-requests:
		t.Fatalf("a request with %d records reached the server, want none", len(r.records))
	case <-time.After(d):
	}
}

func oneRecord(t *testing.T, requests <-chan request) map[string]any {
	t.Helper()
	r := receive(t, requests)
	if len(r.records) != 1 {
		t.Fatalf("got %d records in one request, want 1: %v", len(r.records), r.records)
	}
	return r.records[0]
}

func messages(records []map[string]any) []string {
	var out []string
	for _, r := range records {
		out = append(out, r["message"].(string))
	}
	return out
}

func extraOf(t *testing.T, record map[string]any) map[string]any {
	t.Helper()
	extra, ok := record["extra"].(map[string]any)
	if !ok {
		t.Fatalf(`record has no "extra" object: %v`, record)
	}
	return extra
}

// errorList collects what the handler reports through OnError.
type errorList struct {
	mu   sync.Mutex
	errs []error
}

func (l *errorList) add(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, err)
}

func (l *errorList) all() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.errs...)
}

func (l *errorList) joined() string {
	var parts []string
	for _, err := range l.all() {
		parts = append(parts, err.Error())
	}
	return strings.Join(parts, "\n")
}

// newHandler builds a handler that sends to server and is closed when the test ends. Batches wait
// 10ms instead of a second and retries back off for a millisecond, unless the test says otherwise.
func newHandler(t *testing.T, server *httptest.Server, option Option) *BetterstackHandler {
	t.Helper()
	option.Token = "test-token"
	option.Endpoint = server.URL
	if option.BatchInterval == 0 {
		option.BatchInterval = 10 * time.Millisecond
	}
	if option.RetryBackoff == 0 {
		option.RetryBackoff = time.Millisecond
	}
	handler := option.NewBetterstackHandler()
	t.Cleanup(func() { _ = handler.Close() })
	return handler
}

func newLogger(t *testing.T, server *httptest.Server, option Option) *slog.Logger {
	t.Helper()
	return slog.New(newHandler(t, server, option))
}

func TestRecordShape(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{}).With("release", "v1.0.0")

	before := time.Now()
	logger.
		With(
			slog.Group("user",
				slog.String("id", "user-123"),
				slog.Time("created_at", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
			),
		).
		With("error", errors.New("an error")).
		Error("a message", slog.Int("count", 1))

	record := oneRecord(t, requests)

	var keys []string
	for key := range record {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	wantKeys := []string{"dt", "extra", "level", "logger.name", "logger.version", "message"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("record keys = %v, want %v", keys, wantKeys)
	}

	if record["level"] != "ERROR" {
		t.Errorf(`level = %v, want "ERROR"`, record["level"])
	}
	if record["message"] != "a message" {
		t.Errorf(`message = %v, want "a message"`, record["message"])
	}
	if record["logger.name"] != "BetterStackHQ/slog-betterstack" {
		t.Errorf(`logger.name = %v, want "BetterStackHQ/slog-betterstack"`, record["logger.name"])
	}
	if record["logger.version"] != version {
		t.Errorf("logger.version = %v, want %q", record["logger.version"], version)
	}

	dt, ok := record["dt"].(string)
	if !ok || !strings.HasSuffix(dt, "Z") {
		t.Fatalf("dt = %v, want an RFC 3339 UTC timestamp", record["dt"])
	}
	parsed, err := time.Parse(time.RFC3339Nano, dt)
	if err != nil {
		t.Fatalf("dt %q does not parse as RFC 3339: %v", dt, err)
	}
	if parsed.Before(before.Add(-time.Second)) || parsed.After(time.Now().Add(time.Second)) {
		t.Errorf("dt = %s, want the time of the log call", dt)
	}

	wantExtra := map[string]any{
		"release": "v1.0.0",
		"count":   float64(1),
		"user": map[string]any{
			"id":         "user-123",
			"created_at": "2026-01-02T03:04:05Z",
		},
		"error": map[string]any{
			"kind":  "*errors.errorString",
			"error": "an error",
			"stack": nil,
		},
	}
	if extra := extraOf(t, record); !reflect.DeepEqual(extra, wantExtra) {
		t.Errorf("extra = %#v, want %#v", extra, wantExtra)
	}
}

func TestRequestHeaders(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{})

	logger.Info("hello")

	got := receive(t, requests)
	for header, want := range map[string]string{
		"Authorization":    "Bearer test-token",
		"Content-Type":     "application/json",
		"Content-Encoding": "gzip",
		"User-Agent":       "BetterStackHQ/slog-betterstack/" + version,
	} {
		if value := got.header.Get(header); value != want {
			t.Errorf("%s = %q, want %q", header, value, want)
		}
	}
	if len(got.records) != 1 {
		t.Fatalf("got %d records, want 1", len(got.records))
	}
	if extra := extraOf(t, got.records[0]); len(extra) != 0 {
		t.Errorf("extra = %v, want an empty object for a record without attributes", extra)
	}
}

func TestDisableCompression(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{DisableCompression: true})

	logger.Info("plain")

	got := receive(t, requests)
	if encoding := got.header.Get("Content-Encoding"); encoding != "" {
		t.Errorf("Content-Encoding = %q, want none with DisableCompression", encoding)
	}
	if want := []string{"plain"}; !reflect.DeepEqual(messages(got.records), want) {
		t.Errorf("messages = %v, want %v", messages(got.records), want)
	}
}

func TestDefaults(t *testing.T) {
	handler := Option{Token: "x"}.NewBetterstackHandler()
	option := handler.option
	for name, got := range map[string]any{
		"Endpoint":        option.Endpoint,
		"Timeout":         option.Timeout,
		"Level":           option.Level.Level(),
		"BatchSize":       option.BatchSize,
		"BatchInterval":   option.BatchInterval,
		"MaxQueueSize":    option.MaxQueueSize,
		"MaxRetries":      option.MaxRetries,
		"RetryBackoff":    option.RetryBackoff,
		"MaxInFlight":     option.MaxInFlight,
		"ShutdownTimeout": option.ShutdownTimeout,
	} {
		want := map[string]any{
			"Endpoint":        "https://in.logs.betterstack.com/",
			"Timeout":         10 * time.Second,
			"Level":           slog.LevelDebug,
			"BatchSize":       1000,
			"BatchInterval":   time.Second,
			"MaxQueueSize":    100_000,
			"MaxRetries":      5,
			"RetryBackoff":    300 * time.Millisecond,
			"MaxInFlight":     5,
			"ShutdownTimeout": 15 * time.Second,
		}[name]
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if option.OnError == nil {
		t.Error("OnError = nil, want the default reporter")
	}
}

func TestMissingTokenPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("NewBetterstackHandler without a token did not panic")
		}
	}()
	Option{}.NewBetterstackHandler()
}

func TestLevel(t *testing.T) {
	ctx := context.Background()

	if !(Option{Token: "x"}).NewBetterstackHandler().Enabled(ctx, slog.LevelDebug) {
		t.Error("debug records are disabled by default, want enabled")
	}

	handler := Option{Token: "x", Level: slog.LevelWarn}.NewBetterstackHandler()
	if handler.Enabled(ctx, slog.LevelInfo) {
		t.Error("info is enabled with Level: WARN")
	}
	if !handler.Enabled(ctx, slog.LevelWarn) {
		t.Error("warn is disabled with Level: WARN")
	}
}

func TestWithGroupNestsAttributes(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{}).WithGroup("request")

	logger.Info("handled", "id", "r-1", slog.Group("response", "status", 200))

	want := map[string]any{
		"request": map[string]any{
			"id":       "r-1",
			"response": map[string]any{"status": float64(200)},
		},
	}
	if extra := extraOf(t, oneRecord(t, requests)); !reflect.DeepEqual(extra, want) {
		t.Errorf("extra = %#v, want %#v", extra, want)
	}
}

func TestWithGroupEmptyNameIsANoOp(t *testing.T) {
	handler := Option{Token: "x"}.NewBetterstackHandler()
	if handler.WithGroup("") != handler {
		t.Error(`WithGroup("") returned a new handler, want the same one`)
	}
}

func TestAddSource(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{AddSource: true})

	logger.Info("where am I")

	runtime, ok := extraOf(t, oneRecord(t, requests))["runtime"].(map[string]any)
	if !ok {
		t.Fatal(`AddSource did not add an "extra.runtime" object`)
	}
	if function, _ := runtime["function"].(string); !strings.HasSuffix(function, ".TestAddSource") {
		t.Errorf("runtime.function = %v, want the calling test function", runtime["function"])
	}
	if file, _ := runtime["file"].(string); !strings.HasSuffix(file, "handler_test.go") {
		t.Errorf("runtime.file = %v, want this file", runtime["file"])
	}
	if line, _ := runtime["line"].(float64); line <= 0 {
		t.Errorf("runtime.line = %v, want a line number", runtime["line"])
	}
}

func TestReplaceAttr(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == "password" {
				return slog.String("password", "[redacted]")
			}
			return a
		},
	})

	logger.Info("login", "user", "alice", "password", "hunter2")

	want := map[string]any{"user": "alice", "password": "[redacted]"}
	if extra := extraOf(t, oneRecord(t, requests)); !reflect.DeepEqual(extra, want) {
		t.Errorf("extra = %#v, want %#v", extra, want)
	}
}

func TestAttrFromContext(t *testing.T) {
	type key struct{}
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{
		AttrFromContext: []func(ctx context.Context) []slog.Attr{
			func(ctx context.Context) []slog.Attr {
				return []slog.Attr{slog.String("request_id", ctx.Value(key{}).(string))}
			},
		},
	})

	logger.InfoContext(context.WithValue(context.Background(), key{}, "req-42"), "handled")

	want := map[string]any{"request_id": "req-42"}
	if extra := extraOf(t, oneRecord(t, requests)); !reflect.DeepEqual(extra, want) {
		t.Errorf("extra = %#v, want %#v", extra, want)
	}
}

func TestContextKey(t *testing.T) {
	previous := ContextKey
	ContextKey = "context"
	t.Cleanup(func() { ContextKey = previous })

	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{})

	logger.Info("hello", "a", 1)

	record := oneRecord(t, requests)
	if _, present := record["extra"]; present {
		t.Error(`record still has "extra" after ContextKey was changed`)
	}
	want := map[string]any{"a": float64(1)}
	if got := record["context"]; !reflect.DeepEqual(got, want) {
		t.Errorf("context = %#v, want %#v", got, want)
	}
}

func TestConverterAndMarshaler(t *testing.T) {
	server, requests := newServer(t, accepted)
	marshaled := make(chan any, 1)
	logger := newLogger(t, server, Option{
		Converter: func(addSource bool, replaceAttr func(groups []string, a slog.Attr) slog.Attr, loggerAttr []slog.Attr, groups []string, record *slog.Record) map[string]any {
			return map[string]any{"custom": record.Message}
		},
		Marshaler: func(v any) ([]byte, error) {
			marshaled <- v
			return json.Marshal(v)
		},
	})

	logger.Info("shaped elsewhere")

	want := map[string]any{"custom": "shaped elsewhere"}
	if record := oneRecord(t, requests); !reflect.DeepEqual(record, want) {
		t.Errorf("record = %#v, want the Converter's payload %#v", record, want)
	}
	select {
	case v := <-marshaled:
		if _, ok := v.([]map[string]any); !ok {
			t.Errorf("Marshaler received %T, want the []map[string]any of records", v)
		}
	default:
		t.Error("Marshaler was not called")
	}
}
