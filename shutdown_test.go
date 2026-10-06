package slogbetterstack

import (
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Found by an end-to-end run against an unreachable endpoint: the batch was abandoned at the
// shutdown timeout and the only report was the timeout itself, so the connection error that
// caused it never reached the operator.
func TestAbandonedUploadReportsItsLastError(t *testing.T) {
	gate := make(chan struct{})
	server, _ := newServer(t, func(w http.ResponseWriter, r *http.Request, _ []map[string]any) {
		select {
		case <-gate:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusAccepted)
	})
	t.Cleanup(func() { close(gate) })
	errs := &errorList{}
	handler := newHandler(t, server, Option{
		Timeout:         50 * time.Millisecond, // the first attempt times out
		RetryBackoff:    10 * time.Second,      // the retry is still waiting when Close gives up
		ShutdownTimeout: 100 * time.Millisecond,
		OnError:         errs.add,
	})

	slog.New(handler).Info("stuck")
	if err := handler.Close(); err == nil {
		t.Fatal("Close = nil, want the shutdown timeout error")
	}

	got := errs.joined()
	if !strings.Contains(got, "at shutdown after 1 attempt") || !strings.Contains(got, "deadline") {
		t.Errorf("OnError got %q, want the abandoned batch's last error", got)
	}
	if stats := handler.Stats(); stats.DroppedClosed != 1 || stats.Retries != 1 {
		t.Errorf("stats = %+v, want DroppedClosed 1 and Retries 1", stats)
	}
}

// slog.Logger discards Handle's error, so a program that keeps logging after Close would
// never learn that those records go nowhere.
func TestRecordsAfterCloseAreReportedOnce(t *testing.T) {
	server, _ := newServer(t, accepted)
	errs := &errorList{}
	handler := newHandler(t, server, Option{OnError: errs.add})
	logger := slog.New(handler)

	logger.Info("before")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	logger.Info("after")
	logger.Info("after again")

	var reports int
	for _, err := range errs.all() {
		if strings.Contains(err.Error(), "after Close") {
			reports++
		}
	}
	if reports != 1 {
		t.Errorf("got %d reports about records logged after Close, want exactly 1: %v", reports, errs.all())
	}
	if stats := handler.Stats(); stats.DroppedClosed != 2 {
		t.Errorf("stats = %+v, want DroppedClosed 2", stats)
	}
}

// Found by sending a 12 MiB record to the real endpoint: it answered 2xx and the record never
// appeared, because the per-record limit is enforced after the request is accepted.
func TestRecordOverTheSizeLimitIsDroppedBeforeSending(t *testing.T) {
	server, requests := newServer(t, accepted)
	errs := &errorList{}
	handler := newHandler(t, server, Option{BatchInterval: time.Hour, OnError: errs.add})
	logger := slog.New(handler)

	logger.Info("small before")
	logger.Info("huge", "blob", strings.Repeat("x", 10<<20)) // over 10 MiB once encoded
	logger.Info("small after")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	var delivered []string
	for len(delivered) < 2 {
		delivered = append(delivered, messages(receive(t, requests).records)...)
	}
	if want := []string{"small before", "small after"}; strings.Join(delivered, ",") != strings.Join(want, ",") {
		t.Errorf("delivered %v, want %v", delivered, want)
	}
	if got := errs.joined(); !strings.Contains(got, "larger than") {
		t.Errorf("OnError got %q, want a report about the oversized record", got)
	}
	if stats := handler.Stats(); stats.Sent != 2 || stats.DroppedOversize != 1 {
		t.Errorf("stats = %+v, want Sent 2 and DroppedOversize 1", stats)
	}
}

func TestNegativeTimeoutMeansTheDefault(t *testing.T) {
	handler := Option{Token: "x", Timeout: -1}.NewBetterstackHandler()
	if handler.option.Timeout != 10*time.Second {
		t.Errorf("Timeout = %s, want the 10s default for a negative value", handler.option.Timeout)
	}
	if err := handler.Close(); err != nil {
		t.Error(err)
	}
}
