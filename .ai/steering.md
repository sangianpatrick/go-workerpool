# Steering

## Code Style
- Standard Go conventions (gofmt, go vet)
- Functional options pattern for configuration
- Keep the public API surface minimal
- No external dependencies

## Design Principles
- **Lock-free hot path**: Use `atomic` over `sync.Mutex` for the submit path
- **Caller owns timeout**: Never set default timeouts internally; let the consumer decide via context
- **Graceful drain**: On shutdown, process all remaining queued jobs with a live context before canceling
- **Cancel after drain**: The pool's context is canceled only after `wg.Wait()` completes
- **Idempotent Stop**: Calling `Stop()` multiple times must be safe (uses `CompareAndSwap`)
- **Handler gets pool context**: Handlers receive the pool's context so they can use it for downstream calls
- **Crash safety via recover**: Rare race between Submit and Stop is handled with `recover`, not mutex

## Testing
- Always run with `-race` flag
- Test concurrent submit + stop scenarios
- Test timeout behavior explicitly
- Keep tests deterministic (avoid `time.Sleep` where possible)
- Tests are in external package (`workerpool_test`) to validate the public API
- `export_test.go` exposes test-only helpers for triggering internal edge cases (e.g., recover path)
- 100% statement coverage required

## File Organization
- `pool.go` — WorkerPool struct, options, Start/Submit/Stop
- `handler.go` — Handler interface and UnimplementedHandler
- `job.go` — Job struct
- `errors.go` — Sentinel errors
- `logger.go` — Logger interface and default implementation (log.Printf)
- `export_test.go` — Test-only helpers (exposes internals for external test package)
- `pool_test.go` — All tests (external package `workerpool_test`)
- `_example/main.go` — Basic usage example
- `_example/kafka/main.go` — Kafka consumer group + DLQ example

## CI/CD
- GitHub Actions runs on every push/PR to `main`
- Test job: `go test -race -coverprofile` with 100% coverage gate
- Security job: `govulncheck` + `gosec`
- Codecov uploads coverage for dynamic badge
- Minimum Go version: 1.23
- Commit offset after submit, not after processing (at-most-once)
- Use DLQ for processing failures: produce to `<topic>.dlq` with error headers
- Use `signal.NotifyContext` + `wp.Stop()` for graceful shutdown
- Size the queue (e.g., 100) to absorb bursts without blocking the consumer loop
- Set worker count based on downstream concurrency capacity (e.g., DB connection pool size)
