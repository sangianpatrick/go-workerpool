# Spec

## Public API

### Types

```go
type Job struct {
    ID   string
    Data any
}

type Handler interface {
    Handle(ctx context.Context, job Job) error
}

type Logger interface {
    Printf(format string, args ...any)
}
```

### WorkerPool

```go
func NewWorkerPool(options ...Option) *WorkerPool
func (wp *WorkerPool) Start()
func (wp *WorkerPool) Submit(ctx context.Context, job Job) error
func (wp *WorkerPool) Stop()
func (wp *WorkerPool) Len() int
```

### Options

```go
func NumWorkers(num int) Option       // default: 1
func JobQueueSize(size int) Option    // default: 1
func SetHandler(handler Handler) Option
func WithContext(ctx context.Context) Option
func WithLogger(logger Logger) Option
func OnError(fn ErrorHandler) Option  // default: nil (disabled)
func WithMetrics(m MetricsHook) Option // default: nil (disabled)
```

### Additional Types

```go
type ErrorHandler func(job Job, err error)

type MetricsHook interface {
    JobProcessed(job Job)
    JobFailed(job Job, err error)
}
```

### Errors

- `ErrWorkerPoolClosed` — returned when submitting to a stopped pool

### Behavior

- `Submit` sends immediately if channel has capacity
- `Submit` blocks waiting for capacity, respecting the caller's context deadline
- `Submit` returns `context.DeadlineExceeded` or `context.Canceled` on timeout
- `Submit` returns `ErrWorkerPoolClosed` if pool is stopped (detected via atomic flag)
- Close race (submit during stop) is handled via `recover` — no mutex on hot path
- `export_test.go` exposes `ForceCloseChannel()` to test the recover path from external test package
- `Stop` is idempotent — safe to call multiple times (`CompareAndSwap`)
- `Stop` closes the job channel, waits for workers to drain, then cancels the context
- Workers pass the pool's live context to handlers during drain
- The pool's context is only canceled AFTER all workers have finished

### Shutdown Sequence

1. `closed` atomic flag set → new submits rejected
2. Job channel closed → workers drain remaining jobs with live context
3. `wg.Wait()` → all workers finish processing
4. `cancel()` → pool context canceled

### Kafka Integration Notes

When used with Kafka consumer groups:
- Submit is non-blocking when queue has capacity (immediate send to buffered channel)
- Offset commit happens after submit, before processing (at-most-once)
- DLQ pattern: handler produces failed messages to `<topic>.dlq` with error headers
- Graceful shutdown drains all submitted messages before exiting
