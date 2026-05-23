# Skills

## What this library does
- Manages a pool of goroutines that process jobs from a buffered channel
- Provides backpressure via bounded queue with caller-controlled timeout
- Handles graceful shutdown ensuring all queued jobs are processed with a live context

## When to use
- Background task processing (email sending, notifications, event handling)
- Rate-limited concurrent work (API calls, database writes)
- Fan-out patterns where you want bounded parallelism
- Decoupling message consumption from processing in Kafka/queue consumers

## When NOT to use
- If you need job persistence or retry across restarts (use a message queue)
- If you need distributed processing across multiple machines
- If every job must return a result to the caller synchronously

## Integration patterns

### HTTP handler offloading
```go
func (s *Server) handleOrder(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
    defer cancel()

    err := s.pool.Submit(ctx, workerpool.Job{
        ID:   orderID,
        Data: orderPayload,
    })
    if err != nil {
        http.Error(w, "service busy", http.StatusServiceUnavailable)
        return
    }
    w.WriteHeader(http.StatusAccepted)
}
```

### Graceful shutdown with signal handling
```go
ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer cancel()

wp := workerpool.NewWorkerPool(
    workerpool.WithContext(ctx),
    workerpool.NumWorkers(4),
    workerpool.JobQueueSize(10),
    workerpool.SetHandler(&MyHandler{}),
)
wp.Start()

<-ctx.Done()
wp.Stop() // drains remaining jobs with live context, then cancels
```

### Using context in handlers
```go
func (h *MyHandler) Handle(ctx context.Context, job workerpool.Job) error {
    // ctx is the pool's context — live during drain, canceled after all jobs finish
    req, _ := http.NewRequestWithContext(ctx, "POST", "https://api.example.com", nil)
    resp, err := http.DefaultClient.Do(req)
    // ...
}
```

### Kafka consumer group with worker pool

Decouple Kafka consumption from processing to avoid slow message blocking:

```go
// Consumer loop stays fast
msg := consumer.Poll(100)
wp.Submit(submitCtx, workerpool.Job{ID: string(msg.Key), Data: msg})
consumer.CommitMessage(msg) // commit before processing completes

// 10 workers process concurrently
// Failed messages → DLQ topic for later reprocessing
```

**Tradeoff**: At-most-once delivery. Offset is committed on submit, not after processing. Mitigate with DLQ for processing failures and graceful shutdown for in-flight jobs.

See `_example/kafka/main.go` for full implementation.
