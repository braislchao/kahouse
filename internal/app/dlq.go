package app

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

const dlqDeliveryTimeout = 30 * time.Second

// sendBatchToDLQ sends a batch of failed records to the DLQ topic.
// Records are produced asynchronously and then flushed in bulk.
func sendBatchToDLQ(producer *kafka.Producer, topic string, dlqSuffix string, batch []map[string]interface{}, errorMsg string, sugar *zap.SugaredLogger) error {
	dlqTopic := topic + dlqSuffix
	deliveryChan := make(chan kafka.Event, len(batch))

	for _, record := range batch {
		dlqRecord := map[string]interface{}{
			"original_topic": topic,
			"error":          errorMsg,
			"timestamp":      time.Now().UnixMilli(),
			"record":         record,
		}
		payload, err := json.Marshal(dlqRecord)
		if err != nil {
			sugar.Errorf("Failed to marshal DLQ record: %v", err)
			return fmt.Errorf("failed to marshal DLQ record: %w", err)
		}
		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &dlqTopic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, deliveryChan); err != nil {
			return fmt.Errorf("failed to enqueue DLQ message: %w", err)
		}
	}

	// Wait for all delivery confirmations with a single deadline for the entire batch.
	deadline := time.After(dlqDeliveryTimeout)
	for range len(batch) {
		select {
		case event := <-deliveryChan:
			msg, ok := event.(*kafka.Message)
			if !ok {
				return fmt.Errorf("unexpected delivery event type %T", event)
			}
			if msg.TopicPartition.Error != nil {
				return fmt.Errorf("DLQ delivery failed: %w", msg.TopicPartition.Error)
			}
		case <-deadline:
			return fmt.Errorf("timed out waiting for DLQ batch delivery after %s", dlqDeliveryTimeout)
		}
	}
	return nil
}

// sendToDLQ sends a single message to the DLQ topic (used for per-message deserialization errors).
func sendToDLQ(producer *kafka.Producer, topic string, dlqSuffix string, key, value []byte, errorMsg string, sugar *zap.SugaredLogger) error {
	dlqTopic := topic + dlqSuffix
	dlqRecord := map[string]interface{}{
		"original_topic": topic,
		"error":          errorMsg,
		"timestamp":      time.Now().UnixMilli(),
		"key":            string(key),
		"value":          string(value),
	}
	payload, err := json.Marshal(dlqRecord)
	if err != nil {
		sugar.Errorf("Failed to marshal DLQ record: %v", err)
		return fmt.Errorf("failed to marshal DLQ record: %w", err)
	}
	return produceAndWait(producer, &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &dlqTopic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          payload,
	})
}

func produceAndWait(producer *kafka.Producer, msg *kafka.Message) error {
	deliveryChan := make(chan kafka.Event, 1)

	if err := producer.Produce(msg, deliveryChan); err != nil {
		return err
	}

	select {
	case event := <-deliveryChan:
		deliveredMsg, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected delivery event type %T", event)
		}
		if deliveredMsg.TopicPartition.Error != nil {
			return deliveredMsg.TopicPartition.Error
		}
		return nil
	case <-time.After(dlqDeliveryTimeout):
		return fmt.Errorf("timed out waiting for DLQ delivery after %s", dlqDeliveryTimeout)
	}
}
