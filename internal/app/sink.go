package app

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
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
	mapping        TopicTableMapping
	dlqTopicSuffix string
	chConn         driver.Conn
	decoder        MessageDecoder
	dlqProducer    *kafka.Producer
	consumer       *kafka.Consumer
	sugar          *zap.SugaredLogger
	msgChan        chan map[string]interface{}
	stopped        atomic.Bool
	repairMode     atomic.Int32
}

// IsStopped reports whether the sink task has stopped running.
func (t *SinkTask) IsStopped() bool {
	return t.stopped.Load()
}

// TopicName returns the Kafka topic this task consumes.
func (t *SinkTask) TopicName() string {
	return t.mapping.Topic
}

// Assignment returns the current partition assignment for this task's consumer.
func (t *SinkTask) Assignment() ([]kafka.TopicPartition, error) {
	return t.consumer.Assignment()
}

// SetRepairMode atomically sets the repair mode for this task.
func (t *SinkTask) SetRepairMode(mode RepairMode) {
	t.repairMode.Store(int32(mode))
}

// GetRepairMode atomically reads the current repair mode.
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
		"bootstrap.servers":  cfg.KafkaBrokers,
		"group.id":           groupID,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": false,
	}, cfg)

	consumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer for topic %s: %w", mapping.Topic, err)
	}

	if err := consumer.SubscribeTopics([]string{mapping.Topic}, nil); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", mapping.Topic, err)
	}

	// Channel capacity is 2× BatchSize so the consumer can keep reading while a batch write is
	// in flight. Floor at 1000 for very small batch sizes.
	chanCap := *mapping.BatchSize * 2
	if chanCap < 1000 {
		chanCap = 1000
	}

	return &SinkTask{
		mapping:        mapping,
		dlqTopicSuffix: cfg.DLQTopicSuffix,
		chConn:         chConn,
		decoder:        decoder,
		dlqProducer:    dlqProducer,
		consumer:       consumer,
		sugar:          sugar.With("topic", mapping.Topic),
		msgChan:        make(chan map[string]interface{}, chanCap),
	}, nil
}

// Run starts the consumer loop and batch processor.
// Each task gets its own cancellable context derived from the parent, so stopping
// one task (e.g. due to an unrecoverable error) does not affect other tasks.
func (t *SinkTask) Run(ctx context.Context) {
	defer t.consumer.Close()
	defer t.stopped.Store(true)
	defer taskStopped.WithLabelValues(t.mapping.Topic).Set(1)

	taskCtx, taskCancel := context.WithCancel(ctx)
	defer taskCancel()

	var batchWg sync.WaitGroup
	batchWg.Add(1)
	go t.batchProcessor(taskCtx, taskCancel, &batchWg)

	t.consumerLoop(taskCtx, taskCancel)

	close(t.msgChan)
	batchWg.Wait()
}

// consumerLoop reads messages from Kafka and sends them to the batch processor.
func (t *SinkTask) consumerLoop(ctx context.Context, cancel context.CancelFunc) {
	topic := t.mapping.Topic
	for {
		select {
		case <-ctx.Done():
			t.sugar.Info("Context closed, stopping consumer loop")
			return
		default:
		}

		// 100ms timeout lets the loop check ctx.Done() frequently without busy-waiting,
		// and prevents blocking Kafka heartbeats during slow ClickHouse writes.
		msg, err := t.consumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok {
				switch kafkaErr.Code() {
				case kafka.ErrTimedOut:
					continue
				case kafka.ErrPartitionEOF:
					continue
				}
			}
			t.sugar.Errorf("Consumer error: %v", err)
			msgFailed.WithLabelValues(topic).Inc()
			continue
		}

		msgConsumed.WithLabelValues(topic).Inc()

		// Tombstone (null-value) message: commit offset and skip.
		// Without committing, a tombstone at the end of a partition would be replayed
		// on every restart, incrementing the failed counter indefinitely.
		if len(msg.Value) == 0 {
			t.sugar.Warn("Received tombstone message (nil value), committing offset and skipping")
			if _, commitErr := t.consumer.CommitMessage(msg); commitErr != nil {
				t.sugar.Errorf("Failed to commit offset after tombstone: %v", commitErr)
				cancel()
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
				cancel()
				return
			case RepairModeDLQ:
				if dlqErr := sendToDLQ(t.dlqProducer, topic, t.dlqTopicSuffix, msg.Key, msg.Value, err.Error(), t.sugar); dlqErr != nil {
					t.sugar.Errorf("Failed to send message to DLQ: %v", dlqErr)
					cancel()
					return
				}
				msgDLQ.WithLabelValues(topic).Inc()
			case RepairModeSkip:
				t.sugar.Warnf("Skipping bad message in repair mode (partition=%d offset=%d)",
					msg.TopicPartition.Partition, msg.TopicPartition.Offset)
			}

			if _, commitErr := t.consumer.CommitMessage(msg); commitErr != nil {
				t.sugar.Errorf("Failed to commit offset after decode error: %v", commitErr)
				cancel()
				return
			}
			continue
		}

		// Add idempotency fields used by ReplacingMergeTree deduplication.
		record["kafka_topic"] = *msg.TopicPartition.Topic
		record["kafka_partition"] = int32(msg.TopicPartition.Partition)
		record["kafka_offset"] = int64(msg.TopicPartition.Offset)

		select {
		case t.msgChan <- record:
		case <-ctx.Done():
			t.sugar.Info("Context closed while sending to batch channel")
			return
		}
	}
}

// batchProcessor accumulates messages and flushes them to ClickHouse.
func (t *SinkTask) batchProcessor(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer wg.Done()

	var batch []map[string]interface{}
	var firstInBatch time.Time

	// Guard against a zero BatchDelayMs causing a zero-duration ticker (which panics).
	tickDuration := time.Duration(*t.mapping.BatchDelayMs) * time.Millisecond
	if tickDuration <= 0 {
		tickDuration = time.Millisecond
	}
	batchTicker := time.NewTicker(tickDuration)
	defer batchTicker.Stop()

	// flush writes the accumulated batch and resets state. Uses a 30s deadline instead of the
	// parent context so a clean shutdown doesn't abort the final write, but also doesn't hang.
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		flushStart := time.Now()
		queueDepthBefore := len(t.msgChan)
		t.sugar.Infow(
			"Flushing batch",
			"batch_size", len(batch),
			"queue_depth_before", queueDepthBefore,
			"batch_age_ms", time.Since(firstInBatch).Milliseconds(),
		)
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer flushCancel()
		if err := t.writeAndCommitBatch(flushCtx, batch, firstInBatch); err != nil {
			t.sugar.Errorw(
				"Failed to write and commit batch",
				"error", err,
				"batch_size", len(batch),
				"flush_duration_ms", time.Since(flushStart).Milliseconds(),
				"queue_depth_after", len(t.msgChan),
			)
			cancel()
			return false
		}
		t.sugar.Infow(
			"Batch flush completed",
			"batch_size", len(batch),
			"flush_duration_ms", time.Since(flushStart).Milliseconds(),
			"queue_depth_after", len(t.msgChan),
		)
		batch = nil
		firstInBatch = time.Time{}
		return true
	}

	for {
		select {
		case msg, ok := <-t.msgChan:
			if !ok {
				t.sugar.Info("Message channel closed, flushing remaining messages")
				flush()
				return
			}
			if len(batch) == 0 {
				firstInBatch = time.Now()
				batchTicker.Reset(tickDuration)
			}
			batch = append(batch, msg)
			if len(batch) >= *t.mapping.BatchSize {
				t.sugar.Infof("Batch size reached (%d), flushing", len(batch))
				if !flush() {
					return
				}
			}
		case <-batchTicker.C:
			if len(batch) > 0 {
				t.sugar.Infof("Batch delay reached (%d messages), flushing", len(batch))
				if !flush() {
					return
				}
			}
		}
	}
}

// writeAndCommitBatch writes a batch to ClickHouse with retries, then commits offsets.
// On write failure it sends the batch to the DLQ and commits offsets only after DLQ delivery succeeds.
func (t *SinkTask) writeAndCommitBatch(ctx context.Context, batch []map[string]interface{}, firstInBatch time.Time) error {
	if len(batch) == 0 {
		return nil
	}

	topic := t.mapping.Topic
	table := t.mapping.Table
	offsets, err := extractOffsets(batch)
	if err != nil {
		return fmt.Errorf("failed to extract offsets: %w", err)
	}

	writeStart := time.Now()
	writeErr, attempts := t.writeWithRetries(ctx, table, batch)

	processLatency.WithLabelValues(topic).Observe(time.Since(writeStart).Seconds())
	batchSizeHist.WithLabelValues(topic).Observe(float64(len(batch)))
	batchDelayHist.WithLabelValues(topic).Observe(time.Since(firstInBatch).Seconds())
	retryCountHist.WithLabelValues(topic).Observe(float64(attempts))

	if writeErr != nil {
		msgFailed.WithLabelValues(topic).Add(float64(len(batch)))
		return fmt.Errorf("batch write failed after %d attempts: %w", attempts, writeErr)
	} else {
		msgProduced.WithLabelValues(topic).Add(float64(len(batch)))
	}

	// extractOffsets already returns max_consumed_offset+1 per partition — do not add 1 again.
	var commitOffsets []kafka.TopicPartition
	for partition, offset := range offsets {
		commitOffsets = append(commitOffsets, kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    kafka.Offset(offset),
		})
	}
	if _, err := t.consumer.CommitOffsets(commitOffsets); err != nil {
		return fmt.Errorf("failed to commit offsets: %w", err)
	}

	return nil
}

// writeWithRetries attempts to write the batch, retrying with exponential backoff+jitter.
// Returns the final error (nil on success) and the number of attempts made.
func (t *SinkTask) writeWithRetries(ctx context.Context, table string, batch []map[string]interface{}) (error, int) {
	maxRetries := *t.mapping.MaxRetries
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
				return ctx.Err(), attempt
			}
			t.sugar.Infof("Retrying batch write (attempt %d/%d) after %v", attempt+1, maxRetries+1, wait)
		}
		if err := writeBatch(ctx, table, t.chConn, batch, t.sugar); err != nil {
			if attempt == maxRetries {
				t.sugar.Errorf("Batch write failed on final attempt %d/%d: %v", attempt+1, maxRetries+1, err)
				return err, attempt + 1
			}
			t.sugar.Warnf("Batch write failed on attempt %d/%d: %v", attempt+1, maxRetries+1, err)
			continue
		}
		return nil, attempt + 1
	}
	return fmt.Errorf("writeWithRetries: no attempts made (maxRetries=%d)", maxRetries), 0
}

// extractOffsets returns the next offset to commit (max consumed offset + 1) for each partition.
func extractOffsets(batch []map[string]interface{}) (map[int32]int64, error) {
	offsets := make(map[int32]int64)
	for i, record := range batch {
		partition, err := partitionFromRecord(record["kafka_partition"])
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		offset, err := offsetFromRecord(record["kafka_offset"])
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		if current, exists := offsets[partition]; !exists || offset >= current {
			offsets[partition] = offset + 1
		}
	}
	return offsets, nil
}

func partitionFromRecord(value interface{}) (int32, error) {
	switch v := value.(type) {
	case int32:
		return v, nil
	case int64:
		return int32(v), nil
	case int:
		return int32(v), nil
	case kafka.Offset:
		return int32(v), nil
	default:
		return 0, fmt.Errorf("invalid kafka_partition type %T", value)
	}
}

func offsetFromRecord(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case kafka.Offset:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("invalid kafka_offset type %T", value)
	}
}
