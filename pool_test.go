package workerpool_test

import (
	"context"
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

type testLogger struct {
	called atomic.Bool
}

func (l *testLogger) Printf(string, ...any) {
	l.called.Store(true)
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
	// Don't start workers — channel will fill up
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
	wp.Stop() // should not panic
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
	// Default logger uses log.Printf — just ensure no panic on Start/Stop
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(1))
	wp.Start()
	wp.Stop()
}

func TestSubmitRecoverOnClosedChannel(t *testing.T) {
	wp := workerpool.NewWorkerPool(workerpool.NumWorkers(1), workerpool.JobQueueSize(1))
	wp.Start()

	// Force close the channel without setting closed flag to trigger recover
	wp.ForceCloseChannel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := wp.Submit(ctx, workerpool.Job{ID: "panic-recover"})
	if err != workerpool.ErrWorkerPoolClosed {
		t.Fatalf("expected ErrWorkerPoolClosed from recover, got %v", err)
	}
}

func TestOptionsWithNilValues(t *testing.T) {
	// Should not panic; nil values are ignored
	wp := workerpool.NewWorkerPool(
		workerpool.WithContext(nil),
		workerpool.SetHandler(nil),
		workerpool.WithLogger(nil),
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
