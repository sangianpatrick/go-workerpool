# go-workerpool

A simple, lightweight worker pool for Go with non-blocking job submission, configurable timeout, and graceful shutdown.

## Installation

```bash
go get github.com/sangianpatrick/go-workerpool
```

## Features

- Configurable number of workers and job queue size
- Non-blocking submit with caller-controlled timeout via `context`
- Graceful shutdown — workers drain remaining jobs before context is canceled
- Lock-free close detection using `atomic`
- Pluggable `Handler` interface with live context
- Optional logger integration

## Usage

### 1. Implement the Handler interface

```go
type Handler interface {
    Handle(ctx context.Context, job Job) error
}
```

Example:

```go
type MyHandler struct{}

func (h *MyHandler) Handle(ctx context.Context, job workerpool.Job) error {
    fmt.Printf("processing: %s\n", job.ID)
    return nil
}
```

The `ctx` passed to `Handle` is the pool's context — it remains live during normal operation and is only canceled after all jobs have been drained on shutdown.

### 2. Create and start the pool

```go
wp := workerpool.NewWorkerPool(
    workerpool.NumWorkers(4),
    workerpool.JobQueueSize(10),
    workerpool.SetHandler(&MyHandler{}),
)

wp.Start()
```

### 3. Submit jobs

The caller controls the timeout. If the queue is full, `Submit` waits until capacity is available or the context expires.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := wp.Submit(ctx, workerpool.Job{
    ID:   "order-123",
    Data: map[string]any{"amount": 100},
})
if err != nil {
    // context.DeadlineExceeded — timed out waiting for queue space
    // context.Canceled — caller canceled the submit context
    // workerpool.ErrWorkerPoolClosed — pool has been stopped
    log.Printf("submit failed: %v", err)
}
```

### 4. Stop the pool

```go
wp.Stop()
```

`Stop` is safe to call multiple times. It stops accepting new jobs, closes the queue, waits for all workers to finish processing remaining jobs, and then cancels the pool's context.

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `NumWorkers(n)` | Number of worker goroutines | `1` |
| `JobQueueSize(n)` | Buffered channel capacity | `1` |
| `SetHandler(h)` | Job processing handler | `UnimplementedHandler` (logs and skips) |
| `WithContext(ctx)` | Parent context for the pool | `context.Background()` |
| `WithLogger(l)` | Logger implementing `Logger` interface | `log.Printf` (standard library) |
| `OnError(fn)` | Callback invoked when handler returns error | `nil` (disabled) |
| `WithMetrics(m)` | Metrics hook for observability | `nil` (disabled) |

## Methods

| Method | Description |
|--------|-------------|
| `Start()` | Launch worker goroutines |
| `Submit(ctx, job)` | Submit a job with caller-controlled timeout |
| `Stop()` | Graceful shutdown (drain + cancel) |
| `Len()` | Current number of jobs in the queue |

## Errors

| Error | When |
|-------|------|
| `ErrWorkerPoolClosed` | Submitting to a stopped pool |
| `context.DeadlineExceeded` | Submit timeout expired while waiting for queue space |
| `context.Canceled` | Caller canceled the submit context |

## Shutdown Behavior

1. `Stop()` sets the closed flag (new submits are rejected immediately)
2. The job channel is closed (workers finish remaining jobs)
3. Workers drain all queued jobs with a **live context**
4. After all workers finish, the pool's context is canceled

This ensures handlers can use the context for downstream calls (HTTP, DB, tracing) throughout the entire drain phase.

## Examples

### Basic Usage

A simple example that submits 10 jobs to a pool of 3 workers with a queue capacity of 5. Includes `OnError` callback and `Len()` for queue monitoring.

See [`_example/main.go`](./_example/main.go)

```go
wp := workerpool.NewWorkerPool(
    workerpool.WithContext(ctx),
    workerpool.NumWorkers(3),
    workerpool.JobQueueSize(5),
    workerpool.SetHandler(&PrintHandler{}),
    workerpool.OnError(func(job workerpool.Job, err error) {
        log.Printf("job %s failed: %v", job.ID, err)
    }),
)
wp.Start()

for i := 0; i < 10; i++ {
    submitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    wp.Submit(submitCtx, workerpool.Job{ID: fmt.Sprintf("job-%d", i), Data: i})
    cancel()

    fmt.Printf("queue depth: %d\n", wp.Len())
}

wp.Stop()
```

### Kafka Consumer Group with DLQ

A production-style example using `confluent-kafka-go` with a consumer group, multiple topics, and a Dead Letter Queue for failed messages.

See [`_example/kafka/main.go`](./_example/kafka/main.go)

#### Why use a worker pool with Kafka?

In a standard Kafka consumer loop, messages are processed sequentially. If one message takes 5 seconds (e.g., calling a slow external API), the consumer is blocked — no new messages are polled, and the consumer may be kicked out of the group due to `max.poll.interval.ms` timeout.

By decoupling consumption from processing:

```
[Kafka] → poll → [Consumer Loop] → submit → [Worker Pool (10 workers)] → Handle()
                        ↓
                   commit offset
```

- The consumer loop stays fast (poll → submit → commit)
- Slow messages don't block other messages
- 10 workers process messages concurrently

#### Tradeoff: At-Most-Once Delivery

The offset is committed immediately after submitting to the pool, **not** after processing completes:

```go
// Submit to pool
err := wp.Submit(submitCtx, workerpool.Job{ID: string(msg.Key), Data: msg})

// Commit immediately — before Handle() finishes
consumer.CommitMessage(msg)
```

This means:
- If the app crashes while jobs are in-flight → messages are **lost** (already committed)
- This is **at-most-once** semantics
- Acceptable for: analytics, notifications, logging
- Not suitable for: financial transactions, data that cannot be lost

#### DLQ Mitigation

To reduce data loss from processing failures, the handler sends failed messages to a DLQ topic:

```go
func (h *MessageHandler) Handle(ctx context.Context, job workerpool.Job) error {
    msg := job.Data.(*kafka.Message)
    if err := processMessage(ctx, msg); err != nil {
        h.sendToDLQ(msg, err) // Produce to "<topic>.dlq"
        return err
    }
    return nil
}
```

DLQ messages include headers with the error reason and original topic for reprocessing.

| Scenario | Outcome |
|----------|---------|
| Processing succeeds | ✅ Normal flow |
| Processing fails (bad payload, timeout) | ✅ Message preserved in DLQ |
| App crashes before Handle() runs | ❌ Message lost (committed but never processed) |

The crash-loss window is minimized by:
- `wp.Stop()` draining all queued jobs on graceful shutdown
- Proper signal handling (`SIGINT`, `SIGTERM`)

## Contributing

This library was developed in collaboration with AI. For future contributions, please read the context documents in the `.ai/` directory before making changes:

- `.ai/plan.md` — Project goals and status
- `.ai/spec.md` — Public API specification and behavioral contracts
- `.ai/steering.md` — Code style, design principles, and file organization
- `.ai/skills.md` — Integration patterns and usage guidelines

These documents serve as context for both human and AI contributors to maintain consistency across the codebase.

## License

MIT
