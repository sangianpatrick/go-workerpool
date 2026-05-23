package workerpool

import "errors"

var (
	// ErrWorkerPoolClosed is returned when submitting a job to a closed worker pool.
	ErrWorkerPoolClosed = errors.New("[workerpool] worker pool is closed")
)
