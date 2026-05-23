package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
)

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

// WithLogger sets a custom logger. Defaults to a no-op logger.
func WithLogger(logger Logger) Option {
	return func(c *config) {
		if logger == nil {
			return
		}
		c.logger = logger
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
		wp.config.handler.Handle(wp.config.ctx, job)
	}
	wp.config.logger.Printf("[workerpool] worker %d stopped", id)
}
