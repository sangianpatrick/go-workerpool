package workerpool_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	workerpool "github.com/sangianpatrick/go-workerpool"
)

type countHandler struct {
	count atomic.Int64
}

func (h *countHandler) Handle(_ context.Context, _ workerpool.Job) error {
	h.count.Add(1)
	return nil
}

type failHandler struct{}

func (h *failHandler) Handle(_ context.Context, _ workerpool.Job) error {
	return errors.New("process failed")
}

type testLogger struct {
	called atomic.Bool
}

func (l *testLogger) Printf(string, ...any) {
	l.called.Store(true)
}

type testMetrics struct {
	processed atomic.Int64
	failed    atomic.Int64
}

func (m *testMetrics) JobProcessed(_ workerpool.Job) {
	m.processed.Add(1)
}

func (m *testMetrics) JobFailed(_ workerpool.Job, _ error) {
	m.failed.Add(1)
}

func TestWorkerPoolProcessesAllJobs(t *testing.T) {
	h := &countHandler{}
	wp := workerpool.NewWorkerPool(
		workerpool.NumWorkers(4),
		workerpool.JobQueueSize(10),
		workerpool.SetHandler(h),
	)
	wp.Start()

	n := 100
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := wp.Submit(ctx, workerpool.Job{ID: "job"}); err != nil {
			t.Fatalf("unexpected submit error: %v", err)
		}
		cancel()
	}

	wp.Stop()

	if got := h.count.Load(); got != int64(n) {
		t.Fatalf("expected %d jobs processed, got %d", n, got)
	}
}

func TestSubmitAfterStopReturnsError(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(1))
	wp.Start()
	wp.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := wp.Submit(ctx, workerpool.Job{ID: "late"})
	if err != workerpool.ErrWorkerPoolClosed {
		t.Fatalf("expected ErrWorkerPoolClosed, got %v", err)
	}
}

func TestSubmitTimesOut(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(1))
	_ = wp.Submit(context.Background(), workerpool.Job{ID: "fill"})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := wp.Submit(ctx, workerpool.Job{ID: "timeout"})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	wp.Start()
	wp.Stop()
}

func TestStopIsIdempotent(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(2), workerpool.JobQueueSize(1))
	wp.Start()
	wp.Stop()
	wp.Stop()
}

func TestContextCancellationStopsPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := &countHandler{}
	wp := workerpool.NewWorkerPool(
		workerpool.WithContext(ctx),
		workerpool.NumWorkers(2),
		workerpool.JobQueueSize(1),
		workerpool.SetHandler(h),
	)
	wp.Start()

	cancel()
	wp.Stop()

	submitCtx, submitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer submitCancel()

	err := wp.Submit(submitCtx, workerpool.Job{ID: "after-cancel"})
	if err != workerpool.ErrWorkerPoolClosed {
		t.Fatalf("expected ErrWorkerPoolClosed, got %v", err)
	}
}

func TestWithLoggerIsUsed(t *testing.T) {
	l := &testLogger{}
	wp := workerpool.NewWorkerPool(
		workerpool.NumWorkers(1),
		workerpool.JobQueueSize(1),
		workerpool.WithLogger(l),
	)
	wp.Start()
	wp.Stop()

	if !l.called.Load() {
		t.Fatal("expected logger to be called on worker stop")
	}
}

func TestDefaultLoggerDoesNotPanic(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(1))
	wp.Start()
	wp.Stop()
}

func TestSubmitRecoverOnClosedChannel(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(1))
	wp.Start()

	wp.ForceCloseChannel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := wp.Submit(ctx, workerpool.Job{ID: "panic-recover"})
	if err != workerpool.ErrWorkerPoolClosed {
		t.Fatalf("expected ErrWorkerPoolClosed from recover, got %v", err)
	}
}

func TestOptionsWithNilValues(t *testing.T) {
	wp := workerpool.NewWorkerPool(
		workerpool.WithContext(nil),
		workerpool.SetHandler(nil),
		workerpool.WithLogger(nil),
		workerpool.OnError(nil),
		workerpool.WithMetrics(nil),
		workerpool.NumWorkers(0),
		workerpool.JobQueueSize(0),
	)
	wp.Start()
	wp.Stop()
}

func TestUnimplementedHandler(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(1))
	wp.Start()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wp.Submit(ctx, workerpool.Job{ID: "unimplemented", Data: "test"})

	wp.Stop()
}

func TestLen(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(5))

	_ = wp.Submit(context.Background(), workerpool.Job{ID: "1"})
	_ = wp.Submit(context.Background(), workerpool.Job{ID: "2"})

	if got := wp.Len(); got != 2 {
		t.Fatalf("expected Len() = 2, got %d", got)
	}

	wp.Start()
	wp.Stop()

	if got := wp.Len(); got != 0 {
		t.Fatalf("expected Len() = 0 after stop, got %d", got)
	}
}

func TestOnErrorCallback(t *testing.T) {
	var captured atomic.Int64
	wp := workerpool.NewWorkerPool(
		workerpool.NumWorkers(1),
		workerpool.JobQueueSize(5),
		workerpool.SetHandler(&failHandler{}),
		workerpool.OnError(func(_ workerpool.Job, _ error) {
			captured.Add(1)
		}),
	)
	wp.Start()

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		wp.Submit(ctx, workerpool.Job{ID: "fail"})
		cancel()
	}

	wp.Stop()

	if got := captured.Load(); got != 3 {
		t.Fatalf("expected OnError called 3 times, got %d", got)
	}
}

func TestMetricsHook(t *testing.T) {
	m := &testMetrics{}
	h := &countHandler{}
	wp := workerpool.NewWorkerPool(
		workerpool.NumWorkers(2),
		workerpool.JobQueueSize(5),
		workerpool.SetHandler(h),
		workerpool.WithMetrics(m),
	)
	wp.Start()

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		wp.Submit(ctx, workerpool.Job{ID: "ok"})
		cancel()
	}

	wp.Stop()

	if got := m.processed.Load(); got != 5 {
		t.Fatalf("expected 5 processed, got %d", got)
	}
	if got := m.failed.Load(); got != 0 {
		t.Fatalf("expected 0 failed, got %d", got)
	}
}

func TestMetricsHookOnFailure(t *testing.T) {
	m := &testMetrics{}
	wp := workerpool.NewWorkerPool(
		workerpool.NumWorkers(1),
		workerpool.JobQueueSize(5),
		workerpool.SetHandler(&failHandler{}),
		workerpool.WithMetrics(m),
	)
	wp.Start()

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		wp.Submit(ctx, workerpool.Job{ID: "fail"})
		cancel()
	}

	wp.Stop()

	if got := m.failed.Load(); got != 3 {
		t.Fatalf("expected 3 failed, got %d", got)
	}
	if got := m.processed.Load(); got != 0 {
		t.Fatalf("expected 0 processed, got %d", got)
	}
}
