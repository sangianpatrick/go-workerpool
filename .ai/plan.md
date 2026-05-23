# Project Plan

## Overview
`go-workerpool` is a lightweight, concurrent worker pool library for Go applications that need to process jobs asynchronously with controlled parallelism.

## Goals
- Provide a simple API for submitting and processing jobs concurrently
- Support graceful shutdown with full queue draining under a live context
- Allow caller-controlled timeouts on job submission
- Be lock-free on the hot path (submit)
- Keep the library minimal with zero external dependencies

## Architecture
- **WorkerPool**: Orchestrates workers and manages the job channel
- **Handler**: User-implemented interface for job processing logic (receives pool context)
- **Job**: Simple struct carrying an ID and arbitrary data
- **Options**: Functional options pattern for configuration

## Shutdown Sequence
1. `closed` atomic flag set → rejects new submits
2. Job channel closed → workers drain remaining jobs
3. `wg.Wait()` → all workers finish
4. Context canceled → cleanup signal for any external observers

## Examples
- **Basic** (`_example/main.go`): Simple job submission with signal-based shutdown
- **Kafka** (`_example/kafka/main.go`): Consumer group with multi-topic consumption, worker pool for concurrent processing, and DLQ for failed messages

## Status
- [x] Core worker pool with configurable workers and queue size
- [x] Non-blocking submit with context-based timeout
- [x] Graceful shutdown with job draining (context canceled after drain)
- [x] Lock-free close detection (atomic + recover)
- [x] Pluggable Handler interface with live context
- [x] Optional Logger interface
- [x] OnError callback for failed jobs
- [x] MetricsHook interface for observability
- [x] Len() method for queue depth monitoring
- [x] Tests with race detector (100% coverage, external test package)
- [x] Basic usage example
- [x] Kafka consumer group + DLQ example
