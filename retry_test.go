package slogbetterstack

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	for name, tc := range map[string]struct {
		value string
		min   time.Duration
		max   time.Duration
	}{
		"absent":    {"", 0, 0},
		"seconds":   {"7", 7 * time.Second, 7 * time.Second},
		"http date": {time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat), 25 * time.Second, 30 * time.Second},
		"past date": {time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat), 0, 0},
		"garbage":   {"soon", 0, 0},
		"capped":    {"3600", maxRetryAfter, maxRetryAfter},
	} {
		t.Run(name, func(t *testing.T) {
			if got := parseRetryAfter(tc.value); got < tc.min || got > tc.max {
				t.Errorf("parseRetryAfter(%q) = %s, want between %s and %s", tc.value, got, tc.min, tc.max)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	base := 100 * time.Millisecond

	for attempt, want := range []time.Duration{base, 2 * base, 4 * base, 8 * base} {
		for i := 0; i < 20; i++ {
			if got := backoff(base, attempt, 0); got < want/2 || got > want {
				t.Fatalf("backoff(attempt %d) = %s, want jitter between %s and %s", attempt, got, want/2, want)
			}
		}
	}

	if got := backoff(base, 60, 0); got > maxBackoff || got < maxBackoff/2 {
		t.Errorf("backoff(attempt 60) = %s, want it capped around %s", got, maxBackoff)
	}
	if got := backoff(base, 3, 9*time.Second); got != 9*time.Second {
		t.Errorf("backoff with Retry-After = %s, want the server's 9s", got)
	}
	if got := backoff(1, 0, 0); got != 1 {
		t.Errorf("backoff(1ns) = %s, want 1ns without jitter", got)
	}
}

func TestFlushBeforeAnyRecordIsANoOp(t *testing.T) {
	server, _ := newServer(t, accepted)
	handler := newHandler(t, server, Option{})

	if err := handler.Flush(context.Background()); err != nil {
		t.Errorf("Flush on an idle handler = %v, want nil", err)
	}
	if err := handler.Close(); err != nil {
		t.Errorf("Close on an idle handler = %v, want nil", err)
	}
}

func TestFlushGivesUpWhenItsContextEnds(t *testing.T) {
	gate := make(chan struct{})
	server, _ := newServer(t, func(w http.ResponseWriter, r *http.Request, _ []map[string]any) {
		select {
		case <-gate:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusAccepted)
	})
	t.Cleanup(func() { close(gate) })
	handler := newHandler(t, server, Option{BatchInterval: time.Hour, ShutdownTimeout: 100 * time.Millisecond})

	slog.New(handler).Info("stuck")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := handler.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Flush = %v, want context.DeadlineExceeded while the upload is stuck", err)
	}
}
