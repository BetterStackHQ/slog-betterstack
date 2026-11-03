package slogbetterstack

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestFlushWaitsForUploadsFromEarlierFlush(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		name := "overlapping"
		if cancelFirst {
			name = "after cancellation"
		}
		t.Run(name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			server, _ := newServer(t, func(w http.ResponseWriter, r *http.Request, _ []map[string]any) {
				close(started)
				select {
				case <-release:
				case <-r.Context().Done():
				}
				w.WriteHeader(http.StatusAccepted)
			})
			handler := newHandler(t, server, Option{BatchInterval: time.Hour})
			slog.New(handler).Info("pending")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := make(chan error, 1)
			go func() { first <- handler.Flush(ctx) }()
			// Only Flush can dispatch this partial batch before its hour-long interval.
			waitForFlushSignal(t, started)
			if cancelFirst {
				cancel()
				if err := waitForFlushResult(t, first); !errors.Is(err, context.Canceled) {
					t.Fatalf("first Flush = %v, want context.Canceled", err)
				}
			}

			ctx, cancelSecond := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancelSecond()
			if err := handler.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("second Flush = %v, want context.DeadlineExceeded while the earlier upload is blocked", err)
			}

			unblock()
			if !cancelFirst {
				if err := waitForFlushResult(t, first); err != nil {
					t.Errorf("first Flush = %v, want nil after delivery", err)
				}
			}
			if err := handler.Flush(context.Background()); err != nil {
				t.Errorf("Flush after release = %v, want nil", err)
			}
			if err := handler.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := handler.Stats(); stats.Sent != 1 || stats.Enqueued != 1 {
				t.Errorf("stats = %+v, want exactly one record delivered", stats)
			}
		})
	}
}

func TestFlushDoesNotWaitForLaterUploads(t *testing.T) {
	started := map[string]chan struct{}{"earlier": make(chan struct{}), "later": make(chan struct{})}
	releaseEarlier, releaseLater := make(chan struct{}), make(chan struct{})
	var earlierOnce sync.Once
	unblockEarlier := func() { earlierOnce.Do(func() { close(releaseEarlier) }) }
	defer unblockEarlier()
	defer close(releaseLater)
	server, _ := newServer(t, func(w http.ResponseWriter, r *http.Request, records []map[string]any) {
		message := records[0]["message"].(string)
		close(started[message])
		release := releaseEarlier
		if message == "later" {
			release = releaseLater
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusAccepted)
	})
	handler := newHandler(t, server, Option{BatchInterval: time.Hour})
	logger := slog.New(handler)
	logger.Info("earlier")
	first := make(chan error, 1)
	go func() { first <- handler.Flush(context.Background()) }()
	waitForFlushSignal(t, started["earlier"])

	logger.Info("later")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- handler.Flush(ctx) }()
	waitForFlushSignal(t, started["later"])

	unblockEarlier()
	if err := waitForFlushResult(t, first); err != nil {
		t.Errorf("first Flush = %v, want nil while the later upload is still blocked", err)
	}
	cancel()
	if err := waitForFlushResult(t, second); !errors.Is(err, context.Canceled) {
		t.Errorf("second Flush = %v, want context.Canceled while the later upload is blocked", err)
	}
}

// Test helpers: bound synchronization waits so a broken barrier fails without hanging the suite.

func waitForFlushSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not reach the server within 5s")
	}
}

func waitForFlushResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not return within 5s")
		return nil
	}
}

// End test helpers.
