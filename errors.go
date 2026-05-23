package workerpool

import "errors"

var (
	ErrWorkerPoolClosed = errors.New("[workerpool] worker pool is closed")
)
