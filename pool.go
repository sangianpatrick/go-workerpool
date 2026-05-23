package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
)

// ErrorHandler is called when a job returns an error.
type ErrorHandler func(job Job, err error)

// MetricsHook provides observability into the worker pool.
type MetricsHook interface {
	JobProcessed(job Job)
	JobFailed(job Job, err error)
}

type config struct {
	ctx          context.Context
	cancel       context.CancelFunc
	jobChan      chan Job
	jobQueueSize int
	wg           sync.WaitGroup
	closed       atomic.Bool
	numWorkers   int
	handler      Handler
	logger       Logger
	onError      ErrorHandler
	metrics      MetricsHook
}

type WorkerPool struct {
	config *config
}

type Option func(*config)

// WithContext sets a custom context for the worker pool.
func WithContext(ctx context.Context) Option {
	return func(c *config) {
		if ctx == nil {
			return
		}
		c.ctx, c.cancel = context.WithCancel(ctx)
	}
}

// JobQueueSize sets the size of the job queue. Defaults to 1.
func JobQueueSize(size int) Option {
	return func(c *config) {
		if size < 1 {
			return
		}
		c.jobQueueSize = size
	}
}

// NumWorkers sets the number of worker goroutines. Defaults to 1.
func NumWorkers(num int) Option {
	return func(c *config) {
		if num < 1 {
			return
		}
		c.numWorkers = num
	}
}

// SetHandler sets the Handler for processing jobs.
func SetHandler(handler Handler) Option {
	return func(c *config) {
		if handler == nil {
			return
		}
		c.handler = handler
	}
}

// WithLogger sets a custom logger. Defaults to log.Printf.
func WithLogger(logger Logger) Option {
	return func(c *config) {
		if logger == nil {
			return
		}
		c.logger = logger
	}
}

// OnError sets a callback that is invoked when a handler returns an error.
func OnError(fn ErrorHandler) Option {
	return func(c *config) {
		if fn == nil {
			return
		}
		c.onError = fn
	}
}

// WithMetrics sets a metrics hook for observability.
func WithMetrics(m MetricsHook) Option {
	return func(c *config) {
		if m == nil {
			return
		}
		c.metrics = m
	}
}

// NewWorkerPool creates a new worker pool with the provided options.
func NewWorkerPool(options ...Option) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &config{
		ctx:          ctx,
		cancel:       cancel,
		jobQueueSize: 1,
		numWorkers:   1,
		handler:      &UnimplementedHandler{},
		logger:       &defaultLogger{},
	}

	for _, option := range options {
		option(cfg)
	}

	cfg.jobChan = make(chan Job, cfg.jobQueueSize)

	return &WorkerPool{config: cfg}
}

// Submit adds a job to the worker pool. It waits until the queue has capacity,
// the provided context expires, or the pool is closed.
func (wp *WorkerPool) Submit(ctx context.Context, job Job) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrWorkerPoolClosed
		}
	}()

	if wp.config.closed.Load() {
		return ErrWorkerPoolClosed
	}

	select {
	case wp.config.jobChan <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Len returns the current number of jobs in the queue.
func (wp *WorkerPool) Len() int {
	return len(wp.config.jobChan)
}

// Stop gracefully shuts down the worker pool. Workers drain remaining jobs before exiting.
// The pool's context is canceled only after all jobs have been processed.
func (wp *WorkerPool) Stop() {
	if !wp.config.closed.CompareAndSwap(false, true) {
		return
	}
	close(wp.config.jobChan)
	wp.config.wg.Wait()
	wp.config.cancel()
}

// Start launches the worker goroutines.
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.config.numWorkers; i++ {
		wp.config.wg.Add(1)
		go wp.workerRun(i)
	}
}

// workerRun processes jobs from the channel until it is closed, draining all remaining jobs.
func (wp *WorkerPool) workerRun(id int) {
	defer wp.config.wg.Done()
	for job := range wp.config.jobChan {
		err := wp.config.handler.Handle(wp.config.ctx, job)
		if err != nil {
			if wp.config.onError != nil {
				wp.config.onError(job, err)
			}
			if wp.config.metrics != nil {
				wp.config.metrics.JobFailed(job, err)
			}
		} else if wp.config.metrics != nil {
			wp.config.metrics.JobProcessed(job)
		}
	}
	wp.config.logger.Printf("[workerpool] worker %d stopped", id)
}
