package slogbetterstack

import (
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
	"testing"
	"time"
)

// These tests pin the behaviour applications rely on: the shape of the records that reach
// Better Stack, the request headers, and what every option does. The transport may change
// underneath them, so they only ever look at what arrives at the server, never at how.

type request struct {
	header  http.Header
	records []map[string]any
}

// newServer stands in for Better Stack. It reports every request it receives with the records
// decoded from its JSON array body.
func newServer(t *testing.T) (*httptest.Server, <-chan request) {
	t.Helper()
	requests := make(chan request, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var records []map[string]any
		if err := json.Unmarshal(body, &records); err != nil {
			t.Errorf("body is not a JSON array of records: %v\n%s", err, body)
			return
		}
		requests <- request{header: r.Header.Clone(), records: records}
	}))
	t.Cleanup(func() {
		server.Close()
		// The handler sends through http.DefaultTransport; drop its idle connections so their
		// goroutines are gone before goleak looks.
		http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	})
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

func oneRecord(t *testing.T, requests <-chan request) map[string]any {
	t.Helper()
	r := receive(t, requests)
	if len(r.records) != 1 {
		t.Fatalf("got %d records in one request, want 1: %v", len(r.records), r.records)
	}
	return r.records[0]
}

func extraOf(t *testing.T, record map[string]any) map[string]any {
	t.Helper()
	extra, ok := record["extra"].(map[string]any)
	if !ok {
		t.Fatalf(`record has no "extra" object: %v`, record)
	}
	return extra
}

func newLogger(server *httptest.Server, option Option) *slog.Logger {
	option.Token = "test-token"
	option.Endpoint = server.URL
	return slog.New(option.NewBetterstackHandler())
}

func TestRecordShape(t *testing.T) {
	server, requests := newServer(t)
	logger := newLogger(server, Option{}).With("release", "v1.0.0")

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
	server, requests := newServer(t)
	logger := newLogger(server, Option{})

	logger.Info("hello")

	got := receive(t, requests)
	for header, want := range map[string]string{
		"Authorization": "Bearer test-token",
		"Content-Type":  "application/json",
		"User-Agent":    "BetterStackHQ/slog-betterstack",
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

func TestDefaults(t *testing.T) {
	handler, ok := Option{Token: "x"}.NewBetterstackHandler().(*BetterstackHandler)
	if !ok {
		t.Fatal("NewBetterstackHandler does not return a *BetterstackHandler")
	}
	if handler.option.Endpoint != "https://in.logs.betterstack.com/" {
		t.Errorf("Endpoint = %q, want the Better Stack ingesting endpoint", handler.option.Endpoint)
	}
	if handler.option.Timeout != 10*time.Second {
		t.Errorf("Timeout = %s, want 10s", handler.option.Timeout)
	}
	if handler.option.Level.Level() != slog.LevelDebug {
		t.Errorf("Level = %s, want DEBUG", handler.option.Level.Level())
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
	server, requests := newServer(t)
	logger := newLogger(server, Option{}).WithGroup("request")

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

func TestAddSource(t *testing.T) {
	server, requests := newServer(t)
	logger := newLogger(server, Option{AddSource: true})

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
	server, requests := newServer(t)
	logger := newLogger(server, Option{
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
	server, requests := newServer(t)
	logger := newLogger(server, Option{
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

	server, requests := newServer(t)
	logger := newLogger(server, Option{})

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
	server, requests := newServer(t)
	marshaled := make(chan any, 1)
	logger := newLogger(server, Option{
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

func TestWithGroupEmptyNameIsANoOp(t *testing.T) {
	handler := Option{Token: "x"}.NewBetterstackHandler()
	if handler.WithGroup("") != handler {
		t.Error(`WithGroup("") returned a new handler, want the same one`)
	}
}

func TestSendErrors(t *testing.T) {
	payload := []map[string]any{{"message": "m"}}

	t.Run("marshaling", func(t *testing.T) {
		failing := func(any) ([]byte, error) { return nil, errors.New("cannot marshal") }
		if err := send("http://127.0.0.1:0/", "x", time.Second, failing, payload); err == nil || err.Error() != "cannot marshal" {
			t.Errorf("err = %v, want the Marshaler's error", err)
		}
	})

	t.Run("invalid endpoint", func(t *testing.T) {
		if err := send("://not-a-url", "x", time.Second, json.Marshal, payload); err == nil {
			t.Error("err = nil, want a request error for an invalid endpoint")
		}
	})

	t.Run("unreachable endpoint", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		server.Close() // nothing listens on this URL any more
		if err := send(server.URL, "x", time.Second, json.Marshal, payload); err == nil {
			t.Error("err = nil, want a connection error for a closed endpoint")
		}
	})
}
