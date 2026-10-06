package slogbetterstack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests pin how records travel: batching, flushing on Close, retries, what gets reported
// and what gets dropped. Every record handed to the handler ends up either acknowledged by the
// server or counted as dropped for a reason, and nothing here blocks the application.

func TestCloseDeliversPendingRecords(t *testing.T) {
	server, requests := newServer(t, accepted)
	handler := newHandler(t, server, Option{BatchInterval: time.Hour})
	logger := slog.New(handler)

	for i := 0; i < 3; i++ {
		logger.Info("pending", "i", i)
	}
	nothingWithin(t, requests, 50*time.Millisecond) // the batch waits for its interval or for Close

	if err := handler.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}

	got := receive(t, requests)
	if want := []string{"pending", "pending", "pending"}; !reflect.DeepEqual(messages(got.records), want) {
		t.Errorf("messages = %v, want %v in one request", messages(got.records), want)
	}
	if stats := handler.Stats(); stats.Enqueued != 3 || stats.Sent != 3 {
		t.Errorf("stats = %+v, want Enqueued 3 and Sent 3", stats)
	}
}

func TestBatchSizeTriggersASend(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{BatchSize: 2, BatchInterval: time.Hour})

	logger.Info("one")
	logger.Info("two")
	logger.Info("three")

	got := receive(t, requests)
	if want := []string{"one", "two"}; !reflect.DeepEqual(messages(got.records), want) {
		t.Errorf("messages = %v, want %v", messages(got.records), want)
	}
	nothingWithin(t, requests, 50*time.Millisecond) // "three" waits for the next full batch, the interval or Close
}

func TestBatchIntervalTriggersASend(t *testing.T) {
	server, requests := newServer(t, accepted)
	logger := newLogger(t, server, Option{BatchInterval: 20 * time.Millisecond})

	logger.Info("alone")

	got := receive(t, requests) // without Flush or Close
	if want := []string{"alone"}; !reflect.DeepEqual(messages(got.records), want) {
		t.Errorf("messages = %v, want %v", messages(got.records), want)
	}
}

func TestFlushWaitsForDelivery(t *testing.T) {
	server, requests := newServer(t, accepted)
	handler := newHandler(t, server, Option{BatchInterval: time.Hour})
	logger := slog.New(handler)

	logger.Info("a")
	logger.Info("b")
	if err := handler.Flush(context.Background()); err != nil {
		t.Fatalf("Flush = %v, want nil", err)
	}

	select {
	case got := <-requests:
		if want := []string{"a", "b"}; !reflect.DeepEqual(messages(got.records), want) {
			t.Errorf("messages = %v, want %v", messages(got.records), want)
		}
	default:
		t.Fatal("Flush returned before the records reached the server")
	}
}

func TestDerivedHandlersShareOneQueue(t *testing.T) {
	server, requests := newServer(t, accepted)
	handler := newHandler(t, server, Option{BatchInterval: time.Hour})
	base := slog.New(handler)

	base.Info("base")
	base.With("x", 1).Info("with")
	base.WithGroup("g").Info("group", "k", "v")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	got := receive(t, requests)
	if want := []string{"base", "with", "group"}; !reflect.DeepEqual(messages(got.records), want) {
		t.Errorf("messages = %v, want %v in one request", messages(got.records), want)
	}
}

func TestRejectedBatchIsReportedAndNotRetried(t *testing.T) {
	errs := &errorList{}
	server, requests := newServer(t, status(http.StatusUnauthorized))
	handler := newHandler(t, server, Option{OnError: errs.add})

	slog.New(handler).Info("secret")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	receive(t, requests)
	nothingWithin(t, requests, 50*time.Millisecond) // a bad token is not retried
	if got := errs.joined(); !strings.Contains(got, "401") {
		t.Errorf("OnError got %q, want the 401 status", got)
	}
	if stats := handler.Stats(); stats.DroppedRejected != 1 || stats.Sent != 0 {
		t.Errorf("stats = %+v, want DroppedRejected 1 and Sent 0", stats)
	}
}

func TestTransientFailuresAreRetried(t *testing.T) {
	var calls atomic.Int32
	server, requests := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ []map[string]any) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	errs := &errorList{}
	handler := newHandler(t, server, Option{OnError: errs.add})

	slog.New(handler).Info("eventually")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	got := receiveN(t, requests, 3)
	if want := []string{"eventually"}; !reflect.DeepEqual(messages(got[2].records), want) {
		t.Errorf("third request carried %v, want %v", messages(got[2].records), want)
	}
	if stats := handler.Stats(); stats.Sent != 1 || stats.Retries != 2 {
		t.Errorf("stats = %+v, want Sent 1 and Retries 2", stats)
	}
	if got := errs.all(); len(got) != 0 {
		t.Errorf("OnError got %v, want nothing for retries that succeed", got)
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	server, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ []map[string]any) {
		mu.Lock()
		times = append(times, time.Now())
		first := len(times) == 1
		mu.Unlock()
		if first {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	handler := newHandler(t, server, Option{RetryBackoff: time.Millisecond})

	slog.New(handler).Info("throttled")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("got %d requests, want 2", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < 900*time.Millisecond {
		t.Errorf("retried after %s, want at least the Retry-After second", gap)
	}
}

func TestRetriesRunOut(t *testing.T) {
	errs := &errorList{}
	server, requests := newServer(t, status(http.StatusServiceUnavailable))
	handler := newHandler(t, server, Option{MaxRetries: 2, OnError: errs.add})

	slog.New(handler).Info("doomed")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	receiveN(t, requests, 3) // the first attempt and two retries
	nothingWithin(t, requests, 50*time.Millisecond)
	if got := errs.joined(); !strings.Contains(got, "3 attempts") || !strings.Contains(got, "503") {
		t.Errorf("OnError got %q, want the attempt count and the last status", got)
	}
	if stats := handler.Stats(); stats.DroppedRejected != 1 || stats.Retries != 2 {
		t.Errorf("stats = %+v, want DroppedRejected 1 and Retries 2", stats)
	}
}

func TestOversizedBatchIsSplit(t *testing.T) {
	server, requests := newServer(t, func(w http.ResponseWriter, _ *http.Request, records []map[string]any) {
		if len(records) > 1 {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	errs := &errorList{}
	handler := newHandler(t, server, Option{BatchInterval: time.Hour, OnError: errs.add})
	logger := slog.New(handler)

	for i := 0; i < 4; i++ {
		logger.Info(fmt.Sprintf("record %d", i))
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	var delivered []string
	for _, r := range receiveN(t, requests, 7) { // 4, then 2+2, then 1+1+1+1
		if len(r.records) == 1 {
			delivered = append(delivered, messages(r.records)...)
		}
	}
	if want := []string{"record 0", "record 1", "record 2", "record 3"}; !reflect.DeepEqual(delivered, want) {
		t.Errorf("delivered = %v, want %v", delivered, want)
	}
	if stats := handler.Stats(); stats.Sent != 4 || stats.DroppedOversize != 0 {
		t.Errorf("stats = %+v, want Sent 4 and nothing dropped", stats)
	}
	if got := errs.all(); len(got) != 0 {
		t.Errorf("OnError got %v, want nothing for a batch that was split and delivered", got)
	}
}

func TestSingleOversizedRecordIsDropped(t *testing.T) {
	errs := &errorList{}
	server, requests := newServer(t, status(http.StatusRequestEntityTooLarge))
	handler := newHandler(t, server, Option{OnError: errs.add})

	slog.New(handler).Info("huge")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	receive(t, requests)
	nothingWithin(t, requests, 50*time.Millisecond) // nothing to split, nothing to retry
	if got := errs.joined(); !strings.Contains(got, "larger") {
		t.Errorf("OnError got %q, want a message about the record being too large", got)
	}
	if stats := handler.Stats(); stats.DroppedOversize != 1 {
		t.Errorf("stats = %+v, want DroppedOversize 1", stats)
	}
}

func TestFullQueueDropsInsteadOfBlocking(t *testing.T) {
	gate := make(chan struct{})
	server, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ []map[string]any) {
		<-gate
		w.WriteHeader(http.StatusAccepted)
	})
	errs := &errorList{}
	handler := newHandler(t, server, Option{
		BatchSize:     1,
		BatchInterval: time.Hour,
		MaxQueueSize:  2,
		MaxInFlight:   1,
		OnError:       errs.add,
	})
	logger := slog.New(handler)

	start := time.Now()
	for i := 0; i < 10; i++ {
		logger.Info("burst", "i", i)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("10 log calls took %s while delivery was stalled, want them not to block", elapsed)
	}

	close(gate)
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	stats := handler.Stats()
	if stats.DroppedQueueFull == 0 {
		t.Error("nothing was dropped although the queue holds 2 records and delivery was stalled")
	}
	if stats.Sent+stats.DroppedQueueFull != 10 {
		t.Errorf("stats = %+v, want Sent + DroppedQueueFull = 10", stats)
	}
	if got := errs.joined(); !strings.Contains(got, "queue is full") {
		t.Errorf("OnError got %q, want a summary of the queue-full drops", got)
	}
}

func TestConcurrentHandlesDoNotRace(t *testing.T) {
	type key struct{}
	server, _ := newServer(t, accepted)
	handler := newHandler(t, server, Option{
		BatchInterval: time.Hour,
		AttrFromContext: []func(ctx context.Context) []slog.Attr{
			func(ctx context.Context) []slog.Attr {
				return []slog.Attr{slog.String("worker", ctx.Value(key{}).(string))}
			},
		},
	})
	// Repeated keys leave spare capacity in the attribute slice the derived handler shares with
	// every goroutine that logs through it.
	logger := slog.New(handler).With("a", 1).With("b", 2).With("a", 3)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.WithValue(context.Background(), key{}, fmt.Sprint(i))
			for j := 0; j < 200; j++ {
				logger.InfoContext(ctx, "concurrent")
			}
		}(i)
	}
	wg.Wait()
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := handler.Stats(); stats.Sent != 1600 {
		t.Errorf("stats = %+v, want Sent 1600", stats)
	}
}

func TestCloseIsFinal(t *testing.T) {
	server, requests := newServer(t, accepted)
	handler := newHandler(t, server, Option{BatchInterval: time.Hour})
	ctx := context.Background()

	slog.New(handler).Info("before")
	if err := handler.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	if err := handler.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}

	if err := handler.Handle(ctx, slog.NewRecord(time.Now(), slog.LevelInfo, "after", 0)); !errors.Is(err, ErrClosed) {
		t.Errorf("Handle after Close = %v, want ErrClosed", err)
	}
	if err := handler.Flush(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Flush after Close = %v, want ErrClosed", err)
	}

	if want := []string{"before"}; !reflect.DeepEqual(messages(receive(t, requests).records), want) {
		t.Errorf("delivered %v, want %v", messages(receive(t, requests).records), want)
	}
	nothingWithin(t, requests, 50*time.Millisecond)
	if stats := handler.Stats(); stats.Sent != 1 || stats.DroppedClosed != 1 {
		t.Errorf("stats = %+v, want Sent 1 and DroppedClosed 1", stats)
	}
}

func TestShutdownTimeoutBoundsClose(t *testing.T) {
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
	handler := newHandler(t, server, Option{ShutdownTimeout: 100 * time.Millisecond, OnError: errs.add})

	slog.New(handler).Info("stuck")
	start := time.Now()
	err := handler.Close()
	if err == nil || !strings.Contains(err.Error(), "shutdown timeout") {
		t.Errorf("Close = %v, want an error naming the shutdown timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Close took %s, want about the 100ms shutdown timeout", elapsed)
	}
	if stats := handler.Stats(); stats.DroppedClosed != 1 || stats.Sent != 0 {
		t.Errorf("stats = %+v, want DroppedClosed 1 and Sent 0", stats)
	}
}

func TestTimeoutIsHonoured(t *testing.T) {
	server, _ := newServer(t, func(w http.ResponseWriter, r *http.Request, _ []map[string]any) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusAccepted)
	})
	errs := &errorList{}
	handler := newHandler(t, server, Option{Timeout: 50 * time.Millisecond, MaxRetries: -1, OnError: errs.add})

	slog.New(handler).Info("slow")
	start := time.Now()
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Close took %s, want the 50ms Timeout to cut the request short", elapsed)
	}
	if got := errs.joined(); !strings.Contains(strings.ToLower(got), "timeout") && !strings.Contains(got, "deadline") {
		t.Errorf("OnError got %q, want a timeout error", got)
	}
	if stats := handler.Stats(); stats.DroppedRejected != 1 || stats.Retries != 0 {
		t.Errorf("stats = %+v, want DroppedRejected 1 and no retries with MaxRetries -1", stats)
	}
}

func TestUnreachableEndpointIsReported(t *testing.T) {
	server, _ := newServer(t, accepted)
	server.Close() // nothing listens on this URL any more
	errs := &errorList{}
	handler := newHandler(t, server, Option{MaxRetries: 1, OnError: errs.add})

	slog.New(handler).Info("nowhere")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	if got := errs.joined(); !strings.Contains(got, "2 attempts") {
		t.Errorf("OnError got %q, want the connection error after 2 attempts", got)
	}
	if stats := handler.Stats(); stats.DroppedRejected != 1 || stats.Retries != 1 {
		t.Errorf("stats = %+v, want DroppedRejected 1 and Retries 1", stats)
	}
}

func TestEncodingFailureIsReported(t *testing.T) {
	server, requests := newServer(t, accepted)
	errs := &errorList{}
	handler := newHandler(t, server, Option{
		Marshaler: func(any) ([]byte, error) { return nil, errors.New("cannot marshal") },
		OnError:   errs.add,
	})

	slog.New(handler).Info("unencodable")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	nothingWithin(t, requests, 50*time.Millisecond)
	if got := errs.joined(); !strings.Contains(got, "cannot marshal") {
		t.Errorf("OnError got %q, want the Marshaler's error", got)
	}
	if stats := handler.Stats(); stats.DroppedRejected != 1 {
		t.Errorf("stats = %+v, want DroppedRejected 1", stats)
	}
}

func TestOnErrorPanicsAreContained(t *testing.T) {
	server, requests := newServer(t, status(http.StatusUnauthorized))
	handler := newHandler(t, server, Option{OnError: func(error) { panic("reporter bug") }})

	slog.New(handler).Info("secret")
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	receive(t, requests)
	if stats := handler.Stats(); stats.DroppedRejected != 1 {
		t.Errorf("stats = %+v, want DroppedRejected 1 with the reporter's panic contained", stats)
	}
}
