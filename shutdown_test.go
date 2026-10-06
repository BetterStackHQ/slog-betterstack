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

func TestNegativeTimeoutMeansTheDefault(t *testing.T) {
	handler := Option{Token: "x", Timeout: -1}.NewBetterstackHandler()
	if handler.option.Timeout != 10*time.Second {
		t.Errorf("Timeout = %s, want the 10s default for a negative value", handler.option.Timeout)
	}
	if err := handler.Close(); err != nil {
		t.Error(err)
	}
}
