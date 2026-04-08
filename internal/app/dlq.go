package app

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const dlqDeliveryTimeout = 30 * time.Second

// sendToDLQ sends a single message to the DLQ topic (used for per-message deserialization errors).
func sendToDLQ(producer *kafka.Producer, topic string, dlqSuffix string, key, value []byte, errorMsg string) error {
	dlqTopic := topic + dlqSuffix
	dlqRecord := map[string]interface{}{
		"original_topic":   topic,
		"error":            errorMsg,
		"timestamp":        time.Now().UnixMilli(),
		"key_base64":       base64.StdEncoding.EncodeToString(key),
		"value_base64":     base64.StdEncoding.EncodeToString(value),
		"payload_encoding": "base64",
	}
	payload, err := json.Marshal(dlqRecord)
	if err != nil {
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
