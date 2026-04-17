package app

import (
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// newTaskWithObserver returns a SinkTask configured with the given metadata
// mapping, plus a log observer so tests can assert warnings.
func newTaskWithObserver(t *testing.T, m *KafkaMetadataMapping) (*SinkTask, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core).Sugar().With("topic", "test")
	task := &SinkTask{
		sugar:           logger,
		metadataMapping: resolveMetadataMapping(m),
	}
	return task, logs
}

func newTestMessage(topic string, partition int32, offset int64, ts time.Time, key []byte, headers []kafka.Header) *kafka.Message {
	t := topic
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &t,
			Partition: partition,
			Offset:    kafka.Offset(offset),
		},
		Timestamp: ts,
		Key:       key,
		Headers:   headers,
	}
}

func TestInjectKafkaMetadataAllFields(t *testing.T) {
	task, _ := newTaskWithObserver(t, &KafkaMetadataMapping{
		Offset: "__offset", Partition: "__partition", Topic: "__topic",
		Timestamp: "__timestamp", Key: "__key", Headers: "__headers",
	})
	ts := time.Date(2024, 1, 2, 3, 4, 5, 6_000_000, time.UTC)
	msg := newTestMessage("orders", 7, 42, ts, []byte("order-123"), []kafka.Header{
		{Key: "trace_id", Value: []byte("abc")},
		{Key: "source", Value: []byte("web")},
	})

	record := map[string]interface{}{"id": int64(1)}
	task.injectKafkaMetadata(record, msg)

	if got, ok := record["__offset"].(int64); !ok || got != 42 {
		t.Errorf("__offset: got %#v", record["__offset"])
	}
	if got, ok := record["__partition"].(int32); !ok || got != 7 {
		t.Errorf("__partition: got %#v", record["__partition"])
	}
	if got, ok := record["__topic"].(string); !ok || got != "orders" {
		t.Errorf("__topic: got %#v", record["__topic"])
	}
	if got, ok := record["__timestamp"].(time.Time); !ok || !got.Equal(ts) {
		t.Errorf("__timestamp: got %#v", record["__timestamp"])
	}
	if got, ok := record["__key"].(string); !ok || got != "order-123" {
		t.Errorf("__key: got %#v", record["__key"])
	}
	got, ok := record["__headers"].(map[string]string)
	if !ok || got["trace_id"] != "abc" || got["source"] != "web" {
		t.Errorf("__headers: got %#v", record["__headers"])
	}
	// payload untouched
	if record["id"].(int64) != 1 {
		t.Errorf("payload id mutated: %#v", record["id"])
	}
}

func TestInjectKafkaMetadataPartial(t *testing.T) {
	task, _ := newTaskWithObserver(t, &KafkaMetadataMapping{
		Offset: "__offset",
		Topic:  "__topic",
	})
	msg := newTestMessage("orders", 0, 1, time.Now(), []byte("k"), nil)
	record := map[string]interface{}{}
	task.injectKafkaMetadata(record, msg)

	if _, ok := record["__offset"]; !ok {
		t.Error("expected __offset to be set")
	}
	if _, ok := record["__topic"]; !ok {
		t.Error("expected __topic to be set")
	}
	for _, col := range []string{"__partition", "__timestamp", "__key", "__headers"} {
		if _, ok := record[col]; ok {
			t.Errorf("did not expect %q to be set", col)
		}
	}
}

func TestInjectKafkaMetadataNilMappingIsNoop(t *testing.T) {
	task, _ := newTaskWithObserver(t, nil)
	if task.metadataMapping != nil {
		t.Fatal("expected nil mapping")
	}
	msg := newTestMessage("orders", 0, 1, time.Now(), nil, nil)
	record := map[string]interface{}{"id": int64(1)}
	task.injectKafkaMetadata(record, msg)
	if len(record) != 1 {
		t.Errorf("expected record unchanged, got %#v", record)
	}
}

func TestInjectKafkaMetadataEmptyMappingIsNoop(t *testing.T) {
	task, _ := newTaskWithObserver(t, &KafkaMetadataMapping{})
	msg := newTestMessage("orders", 0, 1, time.Now(), nil, nil)
	record := map[string]interface{}{"id": int64(1)}
	task.injectKafkaMetadata(record, msg)
	if len(record) != 1 {
		t.Errorf("expected record unchanged, got %#v", record)
	}
}

func TestInjectKafkaMetadataZeroTimestamp(t *testing.T) {
	task, _ := newTaskWithObserver(t, &KafkaMetadataMapping{Timestamp: "__timestamp"})
	msg := newTestMessage("orders", 0, 1, time.Time{}, nil, nil)
	record := map[string]interface{}{}
	task.injectKafkaMetadata(record, msg)
	got, ok := record["__timestamp"].(time.Time)
	if !ok {
		t.Fatalf("__timestamp: expected time.Time, got %T", record["__timestamp"])
	}
	if !got.IsZero() {
		t.Errorf("expected zero time.Time, got %v", got)
	}
}

func TestInjectKafkaMetadataNilHeadersYieldsEmptyMap(t *testing.T) {
	task, _ := newTaskWithObserver(t, &KafkaMetadataMapping{Headers: "__headers"})
	msg := newTestMessage("orders", 0, 1, time.Now(), nil, nil)
	record := map[string]interface{}{}
	task.injectKafkaMetadata(record, msg)
	got, ok := record["__headers"].(map[string]string)
	if !ok {
		t.Fatalf("__headers: expected map[string]string, got %T", record["__headers"])
	}
	if len(got) != 0 {
		t.Errorf("expected empty headers map, got %v", got)
	}
}

func TestInjectKafkaMetadataBinaryKeyPreserved(t *testing.T) {
	task, _ := newTaskWithObserver(t, &KafkaMetadataMapping{Key: "__key"})
	keyBytes := []byte{0x00, 0xff, 0x7f}
	msg := newTestMessage("orders", 0, 1, time.Now(), keyBytes, nil)
	record := map[string]interface{}{}
	task.injectKafkaMetadata(record, msg)
	got, ok := record["__key"].(string)
	if !ok || got != string(keyBytes) {
		t.Errorf("__key: got %#v (len=%d), want %#v", got, len(got), string(keyBytes))
	}
}

func TestInjectKafkaMetadataCollisionLogsOncePerColumn(t *testing.T) {
	task, logs := newTaskWithObserver(t, &KafkaMetadataMapping{
		Offset: "__offset",
		Topic:  "__topic",
	})
	msg := newTestMessage("orders", 0, 42, time.Now(), nil, nil)

	// First call: __offset collides, __topic does not.
	record := map[string]interface{}{"__offset": "payload-value"}
	task.injectKafkaMetadata(record, msg)
	if record["__offset"].(int64) != 42 {
		t.Errorf("metadata should win on collision, got %#v", record["__offset"])
	}

	// Second call: __offset collides again → should NOT log a new warning.
	record2 := map[string]interface{}{"__offset": "other"}
	task.injectKafkaMetadata(record2, msg)

	// Third call: __topic now also collides → should log a separate warning.
	record3 := map[string]interface{}{"__topic": "payload-topic"}
	task.injectKafkaMetadata(record3, msg)
	if record3["__topic"].(string) != "orders" {
		t.Errorf("metadata should win on __topic collision, got %#v", record3["__topic"])
	}

	warnings := logs.FilterMessageSnippet("kafka_metadata column").All()
	if len(warnings) != 2 {
		t.Fatalf("expected exactly 2 collision warnings (one per column), got %d: %+v", len(warnings), warnings)
	}
	seen := map[string]int{}
	for _, w := range warnings {
		for _, col := range []string{"__offset", "__topic"} {
			if strings.Contains(w.Message, col) {
				seen[col]++
			}
		}
	}
	if seen["__offset"] != 1 || seen["__topic"] != 1 {
		t.Errorf("expected one warning per column, got %v", seen)
	}
}

func TestInjectKafkaMetadataTopicPointerNil(t *testing.T) {
	task, _ := newTaskWithObserver(t, &KafkaMetadataMapping{Topic: "__topic"})
	msg := &kafka.Message{} // TopicPartition.Topic is nil
	record := map[string]interface{}{}
	task.injectKafkaMetadata(record, msg)
	if got, ok := record["__topic"].(string); !ok || got != "" {
		t.Errorf("expected empty string for nil topic pointer, got %#v", record["__topic"])
	}
}
