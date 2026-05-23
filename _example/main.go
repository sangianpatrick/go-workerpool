package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	workerpool "github.com/sangianpatrick/go-workerpool"
)

type PrintHandler struct{}

func (h *PrintHandler) Handle(ctx context.Context, job workerpool.Job) error {
	fmt.Printf("processing job: %s, data: %v\n", job.ID, job.Data)
	time.Sleep(100 * time.Millisecond) // simulate work
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wp := workerpool.NewWorkerPool(
		workerpool.WithContext(ctx),
		workerpool.NumWorkers(3),
		workerpool.JobQueueSize(5),
		workerpool.SetHandler(&PrintHandler{}),
		workerpool.OnError(func(job workerpool.Job, err error) {
			fmt.Printf("job %s failed: %v\n", job.ID, err)
		}),
	)

	wp.Start()

	for i := 0; i < 10; i++ {
		submitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := wp.Submit(submitCtx, workerpool.Job{
			ID:   fmt.Sprintf("job-%d", i),
			Data: map[string]any{"index": i},
		})
		cancel()

		if err != nil {
			fmt.Printf("failed to submit job-%d: %v\n", i, err)
		}

		fmt.Printf("queue depth: %d\n", wp.Len())
	}

	wp.Stop()
	fmt.Println("all jobs done, pool context canceled")
}
