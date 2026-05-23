package workerpool

import (
	"context"
	"log"
)

// Handler processes a job and returns an error if processing fails.
type Handler interface {
	Handle(ctx context.Context, job Job) error
}

// UnimplementedHandler is a default handler that logs and skips jobs.
type UnimplementedHandler struct{}

func (h *UnimplementedHandler) Handle(_ context.Context, job Job) error {
	log.Printf("[workerpool.UnimplementedHandler] skipping job: %s", job.ID)
	return nil
}
