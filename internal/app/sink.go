package app

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"go.uber.org/zap"
)

// RepairMode controls how a SinkTask handles messages that fail decoding.
// The default (RepairModeOff) crashes the task on the first decode error.
type RepairMode int32

const (
	// RepairModeOff is the default: decode errors stop the task immediately.
	RepairModeOff RepairMode = 0
	// RepairModeDLQ sends bad messages to the dead-letter queue and continues.
	RepairModeDLQ RepairMode = 1
	// RepairModeSkip silently discards bad messages and continues.
	RepairModeSkip RepairMode = 2
)

// String returns the human-readable name of the repair mode.
func (m RepairMode) String() string {
	switch m {
	case RepairModeDLQ:
		return "dlq"
	case RepairModeSkip:
		return "skip"
	default:
		return ""
	}
}

// ParseRepairMode converts a string to a RepairMode value.
func ParseRepairMode(s string) (RepairMode, error) {
	switch s {
	case "dlq":
		return RepairModeDLQ, nil
	case "skip":
		return RepairModeSkip, nil
	case "", "off":
		return RepairModeOff, nil
	default:
		return RepairModeOff, fmt.Errorf("invalid repair mode %q: must be dlq, skip, or off", s)
	}
}

// SinkTask represents a single topic-to-table sink pipeline.
// Each task has its own lifecycle: when it encounters an unrecoverable error it stops
// itself without affecting other tasks running in the same process.
type SinkTask struct {
	mapping            TopicTableMapping
	dlqTopicSuffix     string
	chConn             driver.Conn
	decoder            MessageDecoder
	dlqProducer        *kafka.Producer
	consumer           *kafka.Consumer
	sugar              *zap.SugaredLogger
	stopped            atomic.Bool
	repairMode         atomic.Int32
	asyncInsert        bool
	waitForAsyncInsert bool
}

func (t *SinkTask) IsStopped() bool {
	return t.stopped.Load()
}

func (t *SinkTask) TopicName() string {
	return t.mapping.Topic
}

func (t *SinkTask) Assignment() ([]kafka.TopicPartition, error) {
	return t.consumer.Assignment()
}

func (t *SinkTask) SetRepairMode(mode RepairMode) {
	t.repairMode.Store(int32(mode))
}

func (t *SinkTask) GetRepairMode() RepairMode {
	return RepairMode(t.repairMode.Load())
}

// NewSinkTask creates and initializes a new SinkTask.
func NewSinkTask(
	mapping TopicTableMapping,
	cfg *Config,
	chConn driver.Conn,
	srClient schemaregistry.Client,
	dlqProducer *kafka.Producer,
	sugar *zap.SugaredLogger,
) (*SinkTask, error) {
	decoder, err := newMessageDecoder(mapping.Format, mapping.StringValueColumn, srClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder for topic %s: %w", mapping.Topic, err)
	}

	// Each topic gets a unique consumer group for full offset isolation.
	groupID := "kahouse-" + strings.TrimSpace(cfg.GroupID) + "-" + mapping.Topic
	kafkaConfig := buildKafkaConfig(kafka.ConfigMap{
		"bootstrap.servers":    cfg.KafkaBrokers,
		"group.id":             groupID,
		"auto.offset.reset":    cfg.AutoOffsetReset,
		"enable.auto.commit":   false,
		"session.timeout.ms":   cfg.KafkaSessionTimeoutMs,
		"max.poll.interval.ms": cfg.KafkaMaxPollIntervalMs,
	}, cfg)

	consumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer for topic %s: %w", mapping.Topic, err)
	}

	if err := consumer.SubscribeTopics([]string{mapping.Topic}, nil); err != nil {
		if closeErr := consumer.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to subscribe to topic %s: %w (consumer close failed: %v)", mapping.Topic, err, closeErr)
		}
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", mapping.Topic, err)
	}

	return &SinkTask{
		mapping:            mapping,
		dlqTopicSuffix:     cfg.DLQTopicSuffix,
		chConn:             chConn,
		decoder:            decoder,
		dlqProducer:        dlqProducer,
		consumer:           consumer,
		sugar:              sugar.With("topic", mapping.Topic),
		asyncInsert:        cfg.ClickHouseAsyncInsert,
		waitForAsyncInsert: cfg.ClickHouseWaitForAsyncInsert,
	}, nil
}

// Run reads messages from Kafka, accumulates them into batches, and flushes to ClickHouse.
// Batches are flushed when they reach batch_size or after batch_delay_ms of inactivity.
// After a successful write, offsets are committed via the consumer group protocol.
func (t *SinkTask) Run(ctx context.Context) {
	defer func() {
		if err := t.consumer.Close(); err != nil {
			t.sugar.Errorf("Failed to close Kafka consumer: %v", err)
		}
	}()
	defer t.stopped.Store(true)
	defer taskStopped.WithLabelValues(t.mapping.Topic).Set(1)

	topic := t.mapping.Topic
	table := t.mapping.Table
	batchSize := *t.mapping.BatchSize
	batchDelay := time.Duration(*t.mapping.BatchDelayMs) * time.Millisecond
	if batchDelay <= 0 {
		batchDelay = time.Millisecond
	}

	var batch []map[string]interface{}
	var firstInBatch time.Time

	for {
		select {
		case <-ctx.Done():
			t.sugar.Info("Context closed, flushing remaining batch")
			if len(batch) > 0 {
				t.flush(table, batch, firstInBatch)
			}
			return
		default:
		}

		// 100ms timeout lets the loop check ctx.Done() and batch delay without busy-waiting.
		msg, err := t.consumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok {
				switch kafkaErr.Code() {
				case kafka.ErrTimedOut, kafka.ErrPartitionEOF:
					// Check if the batch delay has expired.
					if len(batch) > 0 && time.Since(firstInBatch) >= batchDelay {
						t.sugar.Infof("Batch delay reached (%d messages), flushing", len(batch))
						if !t.flush(table, batch, firstInBatch) {
							return
						}
						batch = nil
						firstInBatch = time.Time{}
					}
					continue
				}
			}
			t.sugar.Errorf("Consumer error: %v", err)
			msgFailed.WithLabelValues(topic).Inc()
			continue
		}

		msgConsumed.WithLabelValues(topic).Inc()

		// Tombstone (null-value) message: commit offset and skip.
		if len(msg.Value) == 0 {
			t.sugar.Warn("Received tombstone message (nil value), committing offset and skipping")
			if _, commitErr := t.consumer.CommitMessage(msg); commitErr != nil {
				t.sugar.Errorf("Failed to commit offset after tombstone: %v", commitErr)
				return
			}
			continue
		}

		record, err := t.decoder.Decode(*msg.TopicPartition.Topic, msg.Value)
		if err != nil {
			t.sugar.Errorf("Failed to decode message: %v", err)
			msgFailed.WithLabelValues(topic).Inc()

			mode := t.GetRepairMode()
			switch mode {
			case RepairModeOff:
				t.sugar.Errorf("Decode error in strict mode, stopping task for topic %s", topic)
				return
			case RepairModeDLQ:
				if dlqErr := sendToDLQ(t.dlqProducer, topic, t.dlqTopicSuffix, msg.Key, msg.Value, err.Error()); dlqErr != nil {
					t.sugar.Errorf("Failed to send message to DLQ: %v", dlqErr)
					return
				}
				msgDLQ.WithLabelValues(topic).Inc()
			case RepairModeSkip:
				t.sugar.Warnf("Skipping bad message in repair mode (partition=%d offset=%d)",
					msg.TopicPartition.Partition, msg.TopicPartition.Offset)
			}

			if _, commitErr := t.consumer.CommitMessage(msg); commitErr != nil {
				t.sugar.Errorf("Failed to commit offset after decode error: %v", commitErr)
				return
			}
			continue
		}

		if len(batch) == 0 {
			firstInBatch = time.Now()
		}
		batch = append(batch, record)

		if len(batch) >= batchSize {
			t.sugar.Infof("Batch size reached (%d), flushing", len(batch))
			if !t.flush(table, batch, firstInBatch) {
				return
			}
			batch = nil
			firstInBatch = time.Time{}
		}
	}
}

// flush writes the batch to ClickHouse with retries and commits offsets.
// Returns true on success, false on failure (caller should stop).
func (t *SinkTask) flush(table string, batch []map[string]interface{}, firstInBatch time.Time) bool {
	topic := t.mapping.Topic
	flushStart := time.Now()
	t.sugar.Infow("Flushing batch",
		"batch_size", len(batch),
		"batch_age_ms", time.Since(firstInBatch).Milliseconds(),
	)

	// Use context.Background so an in-flight flush is not killed by shutdown signal.
	// The 30s timeout is the safety bound.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer flushCancel()

	writeStart := time.Now()
	attempts, timing, writeErr := t.writeWithRetries(flushCtx, table, batch)
	writeDuration := time.Since(writeStart)

	processLatency.WithLabelValues(topic).Observe(writeDuration.Seconds())
	batchSizeHist.WithLabelValues(topic).Observe(float64(len(batch)))
	batchDelayHist.WithLabelValues(topic).Observe(time.Since(firstInBatch).Seconds())
	retryCountHist.WithLabelValues(topic).Observe(float64(attempts))

	if writeErr != nil {
		msgFailed.WithLabelValues(topic).Add(float64(len(batch)))
		t.sugar.Errorw("Batch write failed",
			"error", writeErr,
			"batch_size", len(batch),
			"attempts", attempts,
			"flush_duration_ms", time.Since(flushStart).Milliseconds(),
			"prepare_ms", timing.Prepare.Milliseconds(),
			"append_ms", timing.Append.Milliseconds(),
			"send_ms", timing.Send.Milliseconds(),
		)
		return false
	}

	msgProduced.WithLabelValues(topic).Add(float64(len(batch)))

	// Commit the consumer's current position. Since no read-ahead happens,
	// this commits exactly the offsets of the messages in this batch.
	// If the commit fails (e.g. after a rebalance clears stored offsets),
	// we log a warning but do NOT stop the task: the data was already written
	// to ClickHouse, and the consumer will keep reading from its current
	// position. Stopping would guarantee duplicate inserts on restart.
	commitStart := time.Now()
	_, commitErr := t.consumer.Commit()
	commitDuration := time.Since(commitStart)

	if commitErr != nil {
		t.sugar.Warnw("Offset commit failed, data already written to ClickHouse; continuing",
			"error", commitErr,
			"batch_size", len(batch),
			"flush_duration_ms", time.Since(flushStart).Milliseconds(),
			"write_ms", writeDuration.Milliseconds(),
			"prepare_ms", timing.Prepare.Milliseconds(),
			"append_ms", timing.Append.Milliseconds(),
			"send_ms", timing.Send.Milliseconds(),
			"commit_ms", commitDuration.Milliseconds(),
		)
		return true
	}

	t.sugar.Infow("Batch flush completed",
		"batch_size", len(batch),
		"flush_duration_ms", time.Since(flushStart).Milliseconds(),
		"write_ms", writeDuration.Milliseconds(),
		"prepare_ms", timing.Prepare.Milliseconds(),
		"append_ms", timing.Append.Milliseconds(),
		"send_ms", timing.Send.Milliseconds(),
		"commit_ms", commitDuration.Milliseconds(),
	)
	return true
}

// writeWithRetries attempts to write the batch, retrying with exponential backoff+jitter.
// Returns the number of attempts made, the timing of the last write, and the final error (nil on success).
func (t *SinkTask) writeWithRetries(ctx context.Context, table string, batch []map[string]interface{}) (int, batchTiming, error) {
	maxRetries := *t.mapping.MaxRetries
	var timing batchTiming
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Duration(*t.mapping.RetryBackoffMs) * time.Millisecond
			jitter := time.Duration(float64(backoff) * (float64(rand.Intn(101)) / 100.0))
			wait := backoff + jitter
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return attempt, timing, ctx.Err()
			}
			t.sugar.Infof("Retrying batch write (attempt %d/%d) after %v", attempt+1, maxRetries+1, wait)
		}
		var writeErr error
		timing, writeErr = writeBatch(ctx, table, t.chConn, batch, t.asyncInsert, t.waitForAsyncInsert, t.sugar)
		if writeErr != nil {
			if !isRetriableClickHouseError(writeErr) {
				t.sugar.Errorf("Non-retriable ClickHouse error, skipping retries: %v", writeErr)
				return attempt + 1, timing, writeErr
			}
			if attempt == maxRetries {
				t.sugar.Errorf("Batch write failed on final attempt %d/%d: %v", attempt+1, maxRetries+1, writeErr)
				return attempt + 1, timing, writeErr
			}
			t.sugar.Warnf("Batch write failed on attempt %d/%d: %v", attempt+1, maxRetries+1, writeErr)
			continue
		}
		return attempt + 1, timing, nil
	}
	return 0, timing, fmt.Errorf("writeWithRetries: no attempts made (maxRetries=%d)", maxRetries)
}
