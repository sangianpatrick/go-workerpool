// Kafka Consumer Group with Worker Pool
//
// WHY USE A WORKER POOL WITH KAFKA?
//
// In a standard Kafka consumer loop, messages are processed sequentially.
// If one message takes 5 seconds (e.g., calling a slow external API), the
// consumer is blocked — no new messages are polled, and the consumer may
// even be kicked out of the group due to max.poll.interval.ms timeout.
//
// By decoupling consumption from processing via a worker pool:
// - The consumer loop stays fast (poll → submit → commit → poll)
// - Slow messages don't block other messages from being processed
// - Multiple messages are processed concurrently by 10 workers
//
// TRADEOFF: AT-MOST-ONCE DELIVERY
//
// The offset is committed immediately after submitting the job to the pool,
// NOT after the job is fully processed. This means:
// - If the application crashes while a job is in-flight, that message is LOST
//   (already committed, won't be re-delivered)
// - This is "at-most-once" semantics, not "at-least-once"
// - Acceptable for non-critical workloads (analytics, notifications, logging)
// - NOT suitable for financial transactions or data that cannot be lost
//
// For at-least-once semantics, you'd need to commit AFTER processing completes,
// which requires a different architecture (e.g., commit tracking per message).
//
// MITIGATION: DEAD LETTER QUEUE (DLQ)
//
// To reduce data loss from failed processing, implement a DLQ:
// - If Handle() returns an error, the handler produces the failed message to a
//   DLQ topic (e.g., "orders.created.dlq")
// - The original message is already committed, so it won't be retried from the
//   source topic — but it's preserved in the DLQ for later reprocessing
// - A separate consumer (or manual replay tool) can read from the DLQ and retry
//
// This doesn't solve crash-during-processing (message is lost before Handle runs),
// but it covers the case where processing starts and fails (bad payload, downstream
// error, timeout). Combined with proper health checks and graceful shutdown, the
// window for true message loss becomes very small.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	workerpool "github.com/sangianpatrick/go-workerpool"
)

type MessageHandler struct {
	dlqProducer *kafka.Producer
}

func (h *MessageHandler) sendToDLQ(msg *kafka.Message, reason error) {
	dlqTopic := *msg.TopicPartition.Topic + ".dlq"
	h.dlqProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &dlqTopic, Partition: kafka.PartitionAny},
		Key:            msg.Key,
		Value:          msg.Value,
		Headers: append(msg.Headers,
			kafka.Header{Key: "dlq.reason", Value: []byte(reason.Error())},
			kafka.Header{Key: "dlq.original.topic", Value: []byte(*msg.TopicPartition.Topic)},
		),
	}, nil)
}

func (h *MessageHandler) Handle(ctx context.Context, job workerpool.Job) error {
	msg := job.Data.(*kafka.Message)
	fmt.Printf("[worker] topic=%s partition=%d offset=%d key=%s value=%s\n",
		*msg.TopicPartition.Topic,
		msg.TopicPartition.Partition,
		msg.TopicPartition.Offset,
		string(msg.Key),
		string(msg.Value),
	)

	// Simulate processing that may fail
	if err := processMessage(ctx, msg); err != nil {
		log.Printf("[worker] processing failed, sending to DLQ: %v", err)
		h.sendToDLQ(msg, err)
		return err
	}

	return nil
}

func processMessage(_ context.Context, _ *kafka.Message) error {
	// Your business logic here
	time.Sleep(200 * time.Millisecond)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// DLQ producer
	dlqProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("failed to create DLQ producer: %v", err)
	}
	defer dlqProducer.Close()

	// Worker pool: 10 workers, queue capacity 100
	wp := workerpool.NewWorkerPool(
		workerpool.WithContext(ctx),
		workerpool.NumWorkers(10),
		workerpool.JobQueueSize(100),
		workerpool.SetHandler(&MessageHandler{dlqProducer: dlqProducer}),
	)
	wp.Start()

	// Kafka consumer with consumer group subscribing to multiple topics
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "my-consumer-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // manual commit after submit
	})
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	topics := []string{"orders.created", "orders.updated", "orders.canceled"}
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	log.Printf("consuming from topics: %v", topics)

	// Consumer loop
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down consumer...")
			goto shutdown
		default:
		}

		ev := consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			// Submit to worker pool with timeout
			submitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := wp.Submit(submitCtx, workerpool.Job{
				ID:   string(e.Key),
				Data: e,
			})
			cancel()

			if err != nil {
				log.Printf("failed to submit message: %v (topic=%s offset=%d)",
					err, *e.TopicPartition.Topic, e.TopicPartition.Offset)
				continue
			}

			// Commit IMMEDIATELY after submit — before processing completes.
			// This is the tradeoff: message won't be re-delivered if processing fails.
			if _, err := consumer.CommitMessage(e); err != nil {
				log.Printf("failed to commit: %v", err)
			}

		case kafka.Error:
			log.Printf("kafka error: %v", e)
			if e.IsFatal() {
				goto shutdown
			}
		}
	}

shutdown:
	// Stop pool — drains all queued jobs with live context
	wp.Stop()
	log.Println("shutdown complete")
}

// Helper for structured job data if needed
func unmarshal[T any](msg *kafka.Message) (T, error) {
	var v T
	err := json.Unmarshal(msg.Value, &v)
	return v, err
}
