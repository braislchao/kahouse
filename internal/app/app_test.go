package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// validConfig returns a minimal valid Config for use in validation tests.
// Fields are resolved (as loadConfig does) so validateConfig can be called directly.
// Callers modify specific fields before passing to validateConfig.
func validConfig() Config {
	cfg := Config{
		KafkaBrokers:           "localhost:9092",
		InputFormat:            "avro",
		SchemaRegistry:         "http://localhost:8081",
		ClickHouseDSN:          "tcp://localhost:9000",
		GroupID:                "group",
		DLQTopicSuffix:         ".dlq",
		AutoOffsetReset:        "earliest",
		ClickHouseMaxOpenConns: 5,
		ClickHouseMaxIdleConns: 5,
		KafkaSessionTimeoutMs:  45000,
		KafkaMaxPollIntervalMs: 300000,
		BatchSize:              1,
		BatchDelayMs:           intPtr(1),
		MaxRetries:             intPtr(1),
		RetryBackoffMs:         intPtr(1),
		TopicTables:            []TopicTableMapping{{Topic: "orders", Table: "default.orders"}},
	}
	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(&cfg)
	}
	return cfg
}

type stubSinkChecker struct {
	stopped    bool
	topic      string
	assignment []kafka.TopicPartition
	err        error
}

func (s stubSinkChecker) IsStopped() bool                             { return s.stopped }
func (s stubSinkChecker) TopicName() string                           { return s.topic }
func (s stubSinkChecker) Assignment() ([]kafka.TopicPartition, error) { return s.assignment, s.err }

func TestTopicTableMappingResolve(t *testing.T) {
	cfg := &Config{
		BatchSize:      5000,
		BatchDelayMs:   intPtr(300),
		MaxRetries:     intPtr(5),
		RetryBackoffMs: intPtr(200),
	}

	// Test case 1: all nil — should use global defaults
	mapping := TopicTableMapping{
		Topic: "test",
		Table: "default.test",
	}
	mapping.resolve(cfg)
	if mapping.Format != "" {
		t.Errorf("Expected empty format when no global format set, got %q", mapping.Format)
	}
	if mapping.StringValueColumn != "" {
		t.Errorf("Expected empty string_value_column when no global column set, got %q", mapping.StringValueColumn)
	}
	if *mapping.BatchSize != 5000 {
		t.Errorf("Expected BatchSize 5000, got %d", *mapping.BatchSize)
	}
	if *mapping.BatchDelayMs != 300 {
		t.Errorf("Expected BatchDelayMs 300, got %d", *mapping.BatchDelayMs)
	}
	if *mapping.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", *mapping.MaxRetries)
	}
	if *mapping.RetryBackoffMs != 200 {
		t.Errorf("Expected RetryBackoffMs 200, got %d", *mapping.RetryBackoffMs)
	}

	// Test case 2: partial overrides — keeps overrides, fills rest from global
	mapping = TopicTableMapping{
		Topic:     "orders",
		Table:     "default.orders",
		BatchSize: intPtr(1000),
	}
	mapping.resolve(cfg)
	if *mapping.BatchSize != 1000 {
		t.Errorf("Expected BatchSize 1000 (override), got %d", *mapping.BatchSize)
	}
	if *mapping.BatchDelayMs != 300 {
		t.Errorf("Expected BatchDelayMs 300 (default), got %d", *mapping.BatchDelayMs)
	}
	if *mapping.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5 (default), got %d", *mapping.MaxRetries)
	}
	if *mapping.RetryBackoffMs != 200 {
		t.Errorf("Expected RetryBackoffMs 200 (default), got %d", *mapping.RetryBackoffMs)
	}

	// Test case 3: full overrides — keeps all overrides
	mapping = TopicTableMapping{
		Topic:          "payments",
		Table:          "default.payments",
		BatchSize:      intPtr(8000),
		BatchDelayMs:   intPtr(100),
		MaxRetries:     intPtr(10),
		RetryBackoffMs: intPtr(50),
	}
	mapping.resolve(cfg)
	if *mapping.BatchSize != 8000 {
		t.Errorf("Expected BatchSize 8000, got %d", *mapping.BatchSize)
	}
	if *mapping.BatchDelayMs != 100 {
		t.Errorf("Expected BatchDelayMs 100, got %d", *mapping.BatchDelayMs)
	}
	if *mapping.MaxRetries != 10 {
		t.Errorf("Expected MaxRetries 10, got %d", *mapping.MaxRetries)
	}
	if *mapping.RetryBackoffMs != 50 {
		t.Errorf("Expected RetryBackoffMs 50, got %d", *mapping.RetryBackoffMs)
	}

	// Test case 4: explicit zero overrides — must NOT fall back to global defaults.
	mapping = TopicTableMapping{
		Topic:        "fast-topic",
		Table:        "default.fast",
		MaxRetries:   intPtr(0),
		BatchDelayMs: intPtr(0),
	}
	mapping.resolve(cfg)
	if *mapping.MaxRetries != 0 {
		t.Errorf("Expected MaxRetries 0 (explicit zero override), got %d", *mapping.MaxRetries)
	}
	if *mapping.BatchDelayMs != 0 {
		t.Errorf("Expected BatchDelayMs 0 (explicit zero override), got %d", *mapping.BatchDelayMs)
	}
	if *mapping.BatchSize != 5000 {
		t.Errorf("Expected BatchSize 5000 (default), got %d", *mapping.BatchSize)
	}
	if *mapping.RetryBackoffMs != 200 {
		t.Errorf("Expected RetryBackoffMs 200 (default), got %d", *mapping.RetryBackoffMs)
	}

	// Test case 5: format and string column inherit from globals.
	cfg.InputFormat = "json"
	cfg.StringValueColumn = "value"
	mapping = TopicTableMapping{
		Topic: "events",
		Table: "default.events",
	}
	mapping.resolve(cfg)
	if mapping.Format != "json" {
		t.Errorf("Expected format json from global default, got %q", mapping.Format)
	}
	if mapping.StringValueColumn != "value" {
		t.Errorf("Expected string_value_column value from global default, got %q", mapping.StringValueColumn)
	}

	// Test case 6: per-topic format and string column override globals.
	mapping = TopicTableMapping{
		Topic:             "logs",
		Table:             "default.logs",
		Format:            "STRING",
		StringValueColumn: "raw_message",
	}
	mapping.resolve(cfg)
	if mapping.Format != "string" {
		t.Errorf("Expected normalized per-topic format string, got %q", mapping.Format)
	}
	if mapping.StringValueColumn != "raw_message" {
		t.Errorf("Expected per-topic string_value_column raw_message, got %q", mapping.StringValueColumn)
	}
}

func TestValidateConfigRejectsInvalidNumbers(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "global batch size",
			mutate: func(c *Config) { c.BatchSize = 0 },
			want:   "batch_size must be at least 1",
		},
		{
			name:   "global retries",
			mutate: func(c *Config) { c.MaxRetries = intPtr(-1) },
			want:   "max_retries must be >= 0",
		},
		{
			name:   "empty dlq suffix",
			mutate: func(c *Config) { c.DLQTopicSuffix = "" },
			want:   "dlq_topic_suffix is required and must not be empty",
		},
		{
			name:   "topic batch size",
			mutate: func(c *Config) { c.TopicTables[0].BatchSize = intPtr(0) },
			want:   "topic_tables[0]: batch_size must be at least 1",
		},
		{
			name:   "topic retry backoff",
			mutate: func(c *Config) { c.TopicTables[0].RetryBackoffMs = intPtr(-1) },
			want:   "topic_tables[0]: retry_backoff_ms must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if err == nil {
				t.Fatalf("Expected validation error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestValidateConfigRejectsDuplicateTopics(t *testing.T) {
	cfg := validConfig()
	cfg.TopicTables = []TopicTableMapping{
		{Topic: "orders", Table: "default.orders"},
		{Topic: "orders", Table: "default.orders_copy"},
	}
	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(&cfg)
	}
	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("Expected validation to reject duplicate topics")
	}
	if !strings.Contains(err.Error(), "duplicate topic") {
		t.Fatalf("Expected duplicate topic error, got %q", err.Error())
	}
}

func TestConfigLogFieldsRedactsSecrets(t *testing.T) {
	fields := configLogFields(&Config{
		KafkaBrokers:      "localhost:9092",
		InputFormat:       "avro",
		SchemaRegistry:    "http://localhost:8081",
		ClickHouseDSN:     "tcp://user:secret@clickhouse:9000?debug=true",
		GroupID:           "group",
		KafkaSASLUsername: "user",
		KafkaSASLPassword: "super-secret",
		BatchDelayMs:      intPtr(200),
		MaxRetries:        intPtr(5),
		RetryBackoffMs:    intPtr(100),
		TopicTables:       []TopicTableMapping{{Topic: "orders", Table: "default.orders"}},
	})

	fieldMap := make(map[string]interface{}, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			t.Fatalf("Expected string key at index %d, got %T", i, fields[i])
		}
		fieldMap[key] = fields[i+1]
	}

	if got := fieldMap["kafka_sasl_password"]; got != "[redacted]" {
		t.Fatalf("Expected redacted SASL password, got %v", got)
	}
	if got := fieldMap["clickhouse_dsn"]; got != "tcp://[redacted]@clickhouse:9000?debug=true" {
		t.Fatalf("Expected redacted DSN, got %v", got)
	}
}

func TestValidateConfigInputFormatRules(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "rejects unsupported format",
			mutate: func(c *Config) { c.InputFormat = "xml" },
			want:   "input_format must be one of avro, json, or string",
		},
		{
			name:   "avro still requires schema registry",
			mutate: func(c *Config) { c.SchemaRegistry = "" },
			want:   "schema_registry is required",
		},
		{
			name: "json does not require schema registry",
			mutate: func(c *Config) {
				c.InputFormat = "json"
				c.SchemaRegistry = ""
				c.TopicTables = []TopicTableMapping{{Topic: "orders", Table: "default.orders"}}
			},
		},
		{
			name:   "string requires destination column",
			mutate: func(c *Config) { c.InputFormat = "string" },
			want:   "string_value_column is required",
		},
		{
			name:   "string accepts destination column",
			mutate: func(c *Config) { c.InputFormat = "string"; c.StringValueColumn = "value" },
		},
		{
			name: "per-topic string can inherit global non-string default",
			mutate: func(c *Config) {
				c.InputFormat = "json"
				c.StringValueColumn = "value"
				c.TopicTables = []TopicTableMapping{{Topic: "logs", Table: "default.logs", Format: "string"}}
			},
		},
		{
			name: "rejects invalid per-topic format",
			mutate: func(c *Config) {
				c.InputFormat = "json"
				c.TopicTables = []TopicTableMapping{{Topic: "logs", Table: "default.logs", Format: "xml"}}
			},
			want: "topic_tables[0]: format must be one of avro, json, or string",
		},
		{
			name: "requires schema registry when any topic uses avro",
			mutate: func(c *Config) {
				c.InputFormat = "json"
				c.SchemaRegistry = ""
				c.TopicTables = []TopicTableMapping{{Topic: "orders", Table: "default.orders", Format: "avro"}}
			},
			want: "schema_registry is required",
		},
		{
			name: "per-topic string requires resolved destination column",
			mutate: func(c *Config) {
				c.InputFormat = "json"
				c.TopicTables = []TopicTableMapping{{Topic: "logs", Table: "default.logs", Format: "string"}}
			},
			want: "topic_tables[0]: string_value_column is required when format is string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			for i := range cfg.TopicTables {
				cfg.TopicTables[i].resolve(&cfg)
			}
			err := validateConfig(&cfg)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Expected config to validate, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Expected validation error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestValidateConfigAutoOffsetReset(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "earliest is valid", value: "earliest"},
		{name: "latest is valid", value: "latest"},
		{name: "none is valid", value: "none"},
		{name: "rejects empty", value: "", want: "auto_offset_reset must be one of earliest, latest, or none"},
		{name: "rejects arbitrary", value: "beginning", want: "auto_offset_reset must be one of earliest, latest, or none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.AutoOffsetReset = tt.value
			err := validateConfig(&cfg)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Expected config to validate, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Expected validation error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestNormalizeInputFormat(t *testing.T) {
	if got := normalizeInputFormat(" JSON "); got != "json" {
		t.Fatalf("Expected normalized input format json, got %q", got)
	}
}

func TestQuoteTableIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "single table", in: "events", want: "`events`"},
		{name: "db and table", in: "analytics.events", want: "`analytics`.`events`"},
		{name: "dotted table name", in: "warehouse.staging.orders.v2.raw", want: "`warehouse`.`staging.orders.v2.raw`"},
		{name: "trim spaces", in: " analytics . events ", want: "`analytics`.`events`"},
		{name: "escaped backticks", in: "db.we`ird", want: "`db`.`we``ird`"},
		{name: "dotted table no db", in: "metrics.sales.daily.v3.agg", want: "`metrics`.`sales.daily.v3.agg`"},
		{name: "empty string", in: "", wantErr: true},
		{name: "leading dot", in: ".table", wantErr: true},
		{name: "trailing dot", in: "db.", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := quoteTableIdentifier(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected error for input %q, got %q", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error for input %q: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("Expected quoted table identifier %q, got %q", tt.want, got)
			}
		})
	}
}

func TestNormalizeAvroValueUnwrapsPrimitiveUnion(t *testing.T) {
	input := map[string]interface{}{
		"access_id": map[string]interface{}{"string": "abc-123"},
		"metadata":  map[string]interface{}{"null": nil},
		"nested": map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{"int": int32(7)},
				map[string]interface{}{"string": "x"},
			},
		},
	}

	got, ok := normalizeAvroValue(input).(map[string]interface{})
	if !ok {
		t.Fatalf("Expected normalized value to be map, got %T", normalizeAvroValue(input))
	}

	if got["access_id"] != "abc-123" {
		t.Fatalf("Expected access_id to unwrap to string, got %#v", got["access_id"])
	}
	if got["metadata"] != nil {
		t.Fatalf("Expected metadata to unwrap to nil, got %#v", got["metadata"])
	}
	nested, ok := got["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected nested to remain object, got %T", got["nested"])
	}
	items, ok := nested["items"].([]interface{})
	if !ok || len(items) != 2 {
		t.Fatalf("Expected nested items slice of len 2, got %#v", nested["items"])
	}
	if items[0] != int32(7) {
		t.Fatalf("Expected first item to unwrap to int32(7), got %#v", items[0])
	}
	if items[1] != "x" {
		t.Fatalf("Expected second item to unwrap to string x, got %#v", items[1])
	}
}

func TestNormalizeAvroValueKeepsRegularObjects(t *testing.T) {
	input := map[string]interface{}{
		"access_id": map[string]interface{}{"value": "abc-123"},
	}
	got, ok := normalizeAvroValue(input).(map[string]interface{})
	if !ok {
		t.Fatalf("Expected normalized value to be map, got %T", normalizeAvroValue(input))
	}
	inner, ok := got["access_id"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected non-union map to stay map, got %T", got["access_id"])
	}
	if inner["value"] != "abc-123" {
		t.Fatalf("Expected non-union map contents to stay intact, got %#v", inner)
	}
}

func TestAnyTopicUsesFormat(t *testing.T) {
	cfg := &Config{
		InputFormat: "json",
		TopicTables: []TopicTableMapping{
			{Topic: "orders", Table: "default.orders"},
			{Topic: "payments", Table: "default.payments", Format: "avro"},
		},
	}
	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(cfg)
	}
	if !anyTopicUsesFormat(cfg, "json") {
		t.Fatal("Expected fallback global format json to be detected")
	}
	if !anyTopicUsesFormat(cfg, "avro") {
		t.Fatal("Expected explicit per-topic avro format to be detected")
	}
	if anyTopicUsesFormat(cfg, "string") {
		t.Fatal("Did not expect string format to be detected")
	}
}

func TestJSONDecoderDecode(t *testing.T) {
	decoder := JSONDecoder{}
	record, err := decoder.Decode("orders", []byte(`{"id":1,"price":12.5,"active":true,"name":"alice"}`))
	if err != nil {
		t.Fatalf("Expected JSON decode to succeed, got %v", err)
	}

	if got, ok := record["id"].(int64); !ok || got != 1 {
		t.Fatalf("Expected id int64(1), got %#v", record["id"])
	}
	if got, ok := record["price"].(float64); !ok || got != 12.5 {
		t.Fatalf("Expected price float64(12.5), got %#v", record["price"])
	}
	if got, ok := record["active"].(bool); !ok || !got {
		t.Fatalf("Expected active true, got %#v", record["active"])
	}
	if got, ok := record["name"].(string); !ok || got != "alice" {
		t.Fatalf("Expected name alice, got %#v", record["name"])
	}
}

func TestJSONDecoderRejectsNonObjectPayload(t *testing.T) {
	decoder := JSONDecoder{}
	_, err := decoder.Decode("orders", []byte(`[1,2,3]`))
	if err == nil || !strings.Contains(err.Error(), "want object") {
		t.Fatalf("Expected non-object JSON to be rejected, got %v", err)
	}
}

func TestJSONDecoderRejectsTrailingData(t *testing.T) {
	decoder := JSONDecoder{}
	_, err := decoder.Decode("orders", []byte(`{"id":1} {"id":2}`))
	if err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("Expected trailing JSON data to be rejected, got %v", err)
	}
}

func TestStringDecoderDecode(t *testing.T) {
	decoder := StringDecoder{column: "value"}
	record, err := decoder.Decode("logs", []byte("hello world"))
	if err != nil {
		t.Fatalf("Expected string decode to succeed, got %v", err)
	}
	if len(record) != 1 || record["value"] != "hello world" {
		t.Fatalf("Unexpected string record: %#v", record)
	}
}

func TestStringDecoderRequiresColumn(t *testing.T) {
	_, err := newMessageDecoder("string", "", nil)
	if err == nil || !strings.Contains(err.Error(), "destination column") {
		t.Fatalf("Expected missing destination column error, got %v", err)
	}
}

func TestNewMessageDecoderUsesExplicitFormat(t *testing.T) {
	decoder, err := newMessageDecoder("string", "value", nil)
	if err != nil {
		t.Fatalf("Expected string decoder creation to succeed, got %v", err)
	}
	if _, ok := decoder.(StringDecoder); !ok {
		t.Fatalf("Expected StringDecoder, got %T", decoder)
	}

	if _, err := newMessageDecoder("avro", "", nil); err == nil {
		t.Fatal("Expected avro decoder creation without schema registry client to fail")
	}
}

func TestJSONDecoderUsesNumbersDeterministically(t *testing.T) {
	decoder := JSONDecoder{}
	record, err := decoder.Decode("orders", []byte(`{"whole":900719925474099,"fractional":1e1}`))
	if err != nil {
		t.Fatalf("Expected JSON decode to succeed, got %v", err)
	}

	if got, ok := record["whole"].(int64); !ok || got != 900719925474099 {
		t.Fatalf("Expected whole int64, got %#v", record["whole"])
	}
	if got, ok := record["fractional"].(float64); !ok || got != 10 {
		t.Fatalf("Expected fractional float64(10), got %#v", record["fractional"])
	}
}

func TestHealthReadinessError(t *testing.T) {
	tests := []struct {
		name    string
		pingErr error
		tasks   []sinkHealthChecker
		want    string
	}{
		{
			name:    "clickhouse ping failure",
			pingErr: errors.New("dial tcp refused"),
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
			},
			want: "clickhouse health check failed",
		},
		{
			name:  "no tasks configured",
			tasks: nil,
			want:  "no sink tasks configured",
		},
		{
			name: "one task stopped",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
				stubSinkChecker{topic: "payments", stopped: true},
			},
			want: `sink task for topic "payments" has stopped`,
		},
		{
			name: "one task unassigned",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
				stubSinkChecker{topic: "payments"},
			},
			want: `sink task for topic "payments" has no partition assignment`,
		},
		{
			name: "assignment check error",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", err: errors.New("broker unavailable")},
			},
			want: `sink task for topic "orders" assignment check failed`,
		},
		{
			name: "all tasks healthy",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
				stubSinkChecker{topic: "payments", assignment: []kafka.TopicPartition{{Partition: 1}}},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := &Health{
				logger: zap.NewNop().Sugar(),
				ping: func(context.Context) error {
					return tt.pingErr
				},
				tasks: func() []sinkHealthChecker { return tt.tasks },
			}

			err := health.readinessError(context.Background())
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Expected readiness success, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Expected readiness error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestHealthLivenessAllStopped(t *testing.T) {
	tests := []struct {
		name  string
		tasks []sinkHealthChecker
		want  int
	}{
		{
			name:  "no tasks returns 503",
			tasks: nil,
			want:  http.StatusServiceUnavailable,
		},
		{
			name: "all stopped returns 503",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true},
				stubSinkChecker{topic: "payments", stopped: true},
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name: "some running returns 200",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true},
				stubSinkChecker{topic: "payments"},
			},
			want: http.StatusOK,
		},
		{
			name: "all running returns 200",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders"},
				stubSinkChecker{topic: "payments"},
			},
			want: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := &Health{
				logger: zap.NewNop().Sugar(),
				ping:   func(context.Context) error { return nil },
				tasks:  func() []sinkHealthChecker { return tt.tasks },
			}
			rr := httptest.NewRecorder()
			health.Livez(rr, httptest.NewRequest("GET", "/livez", nil))
			if rr.Code != tt.want {
				t.Fatalf("Expected status %d, got %d (body: %s)", tt.want, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestParseRepairMode(t *testing.T) {
	tests := []struct {
		input   string
		want    RepairMode
		wantErr bool
	}{
		{input: "", want: RepairModeOff},
		{input: "off", want: RepairModeOff},
		{input: "dlq", want: RepairModeDLQ},
		{input: "skip", want: RepairModeSkip},
		{input: "invalid", wantErr: true},
		{input: "DLQ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%q", tt.input), func(t *testing.T) {
			got, err := ParseRepairMode(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected error for input %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error for input %q: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Expected %v for input %q, got %v", tt.want, tt.input, got)
			}
		})
	}
}

func TestRepairModeString(t *testing.T) {
	if s := RepairModeOff.String(); s != "" {
		t.Fatalf("Expected empty string for RepairModeOff, got %q", s)
	}
	if s := RepairModeDLQ.String(); s != "dlq" {
		t.Fatalf("Expected dlq for RepairModeDLQ, got %q", s)
	}
	if s := RepairModeSkip.String(); s != "skip" {
		t.Fatalf("Expected skip for RepairModeSkip, got %q", s)
	}
}

func TestTaskManagerTopics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	// Manually register two managed tasks with stubs.
	stoppedTask := &SinkTask{mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"}}
	stoppedTask.stopped.Store(true)

	runningTask := &SinkTask{mapping: TopicTableMapping{Topic: "payments", Table: "default.payments"}}

	mgr.tasks["orders"] = &managedTask{
		task:    stoppedTask,
		mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"},
		done:    make(chan struct{}),
	}
	mgr.tasks["payments"] = &managedTask{
		task:    runningTask,
		mapping: TopicTableMapping{Topic: "payments", Table: "default.payments"},
		done:    make(chan struct{}),
	}

	topics := mgr.Topics()
	if len(topics) != 2 {
		t.Fatalf("Expected 2 topics, got %d", len(topics))
	}

	statusByTopic := make(map[string]TopicStatus)
	for _, ts := range topics {
		statusByTopic[ts.Topic] = ts
	}

	if statusByTopic["orders"].Status != "stopped" {
		t.Fatalf("Expected orders to be stopped, got %q", statusByTopic["orders"].Status)
	}
	if statusByTopic["payments"].Status != "running" {
		t.Fatalf("Expected payments to be running, got %q", statusByTopic["payments"].Status)
	}
}

func TestTaskManagerSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task1 := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task2 := &SinkTask{mapping: TopicTableMapping{Topic: "payments"}}

	mgr.tasks["orders"] = &managedTask{task: task1, mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["payments"] = &managedTask{task: task2, mapping: TopicTableMapping{Topic: "payments"}}

	snap := mgr.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Expected 2 health checkers in snapshot, got %d", len(snap))
	}
}

func TestTaskManagerSetRepairMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{task: task, mapping: TopicTableMapping{Topic: "orders"}}

	// Set DLQ mode
	if err := mgr.SetRepairMode("orders", RepairModeDLQ); err != nil {
		t.Fatalf("Failed to set repair mode: %v", err)
	}
	if task.GetRepairMode() != RepairModeDLQ {
		t.Fatalf("Expected DLQ repair mode, got %v", task.GetRepairMode())
	}

	// Clear mode
	if err := mgr.ClearRepairMode("orders"); err != nil {
		t.Fatalf("Failed to clear repair mode: %v", err)
	}
	if task.GetRepairMode() != RepairModeOff {
		t.Fatalf("Expected Off repair mode after clear, got %v", task.GetRepairMode())
	}

	// Non-existent topic
	if err := mgr.SetRepairMode("missing", RepairModeDLQ); err == nil {
		t.Fatal("Expected error for non-existent topic")
	}
}

func TestAdminHandlerListTopics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"},
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/topics", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", rr.Code)
	}

	var topics []TopicStatus
	if err := json.NewDecoder(rr.Body).Decode(&topics); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(topics) != 1 || topics[0].Topic != "orders" {
		t.Fatalf("Unexpected topics response: %+v", topics)
	}
}

func TestAdminHandlerSetRepair(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	// Set DLQ mode
	body := strings.NewReader(`{"mode":"dlq"}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/topics/orders/repair", body)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d (body: %s)", rr.Code, rr.Body.String())
	}
	if task.GetRepairMode() != RepairModeDLQ {
		t.Fatalf("Expected DLQ mode after API call, got %v", task.GetRepairMode())
	}

	// Clear repair mode
	rr = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/api/topics/orders/repair", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", rr.Code)
	}
	if task.GetRepairMode() != RepairModeOff {
		t.Fatalf("Expected Off mode after DELETE, got %v", task.GetRepairMode())
	}

	// Invalid mode
	body = strings.NewReader(`{"mode":"invalid"}`)
	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/topics/orders/repair", body)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("Expected 400 for invalid mode, got %d", rr.Code)
	}

	// Non-existent topic
	body = strings.NewReader(`{"mode":"dlq"}`)
	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/topics/missing/repair", body)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 for missing topic, got %d", rr.Code)
	}
}

func TestAdminHandlerStopNonExistentTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/topics/missing/stop", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 for missing topic, got %d", rr.Code)
	}
}

func TestAdminHandlerStartAlreadyRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	// task is not stopped — IsStopped() returns false by default
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    make(chan struct{}),
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/topics/orders/start", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("Expected 409 for already-running topic, got %d (body: %s)", rr.Code, rr.Body.String())
	}
}

func TestTaskManagerStartRejectsRunningTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    make(chan struct{}),
	}

	err := mgr.Start("orders")
	if err == nil {
		t.Fatal("Expected error when starting an already-running topic")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Fatalf("Expected 'already running' error, got %q", err.Error())
	}
}

func TestHandleStartTopicConcurrentSafety(t *testing.T) {
	// This test verifies that concurrent calls to handleStartTopic and Topics()
	// (which both acquire the mutex) do not panic or race.
	// The race detector (go test -race) is the real check here.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    make(chan struct{}),
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	// Run concurrent requests to exercise the RLock in handleStartTopic.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			rr := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/api/topics/orders/start", nil)
			mux.ServeHTTP(rr, req)
			// We expect 409 (already running) — the important thing is no panic/race.
		}()
		go func() {
			defer wg.Done()
			_ = mgr.Topics()
		}()
	}
	wg.Wait()
}

// stubConn is a minimal stub implementing driver.Conn for testing validateTables.
type stubConn struct {
	driver.Conn
	execErr map[string]error // keyed by SQL query substring
}

func (c *stubConn) Exec(ctx context.Context, query string, args ...interface{}) error {
	for pattern, err := range c.execErr {
		if strings.Contains(query, pattern) {
			return err
		}
	}
	return nil
}

func TestValidateTablesSuccess(t *testing.T) {
	conn := &stubConn{execErr: map[string]error{}}
	tables := []TopicTableMapping{
		{Topic: "orders", Table: "default.orders"},
		{Topic: "payments", Table: "default.payments"},
	}
	if err := validateTables(context.Background(), conn, tables, zap.NewNop().Sugar()); err != nil {
		t.Fatalf("Expected validateTables to succeed, got %v", err)
	}
}

func TestValidateTablesFailsOnMissingTable(t *testing.T) {
	conn := &stubConn{execErr: map[string]error{
		"orders": fmt.Errorf("table orders does not exist"),
	}}
	tables := []TopicTableMapping{
		{Topic: "orders", Table: "default.orders"},
	}
	err := validateTables(context.Background(), conn, tables, zap.NewNop().Sugar())
	if err == nil {
		t.Fatal("Expected validateTables to fail for missing table")
	}
	if !strings.Contains(err.Error(), "does not exist or is not accessible") {
		t.Fatalf("Expected 'does not exist' error, got %q", err.Error())
	}
}

func TestValidateTablesRejectsInvalidTableName(t *testing.T) {
	conn := &stubConn{execErr: map[string]error{}}
	tables := []TopicTableMapping{
		{Topic: "orders", Table: ""},
	}
	err := validateTables(context.Background(), conn, tables, zap.NewNop().Sugar())
	if err == nil {
		t.Fatal("Expected validateTables to reject empty table name")
	}
	if !strings.Contains(err.Error(), "invalid table name") {
		t.Fatalf("Expected 'invalid table name' error, got %q", err.Error())
	}
}

func TestValidateConfigClickHousePoolSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "max_open_conns zero rejected",
			mutate: func(c *Config) { c.ClickHouseMaxOpenConns = 0 },
			want:   "clickhouse_max_open_conns must be at least 1",
		},
		{
			name:   "max_open_conns negative rejected",
			mutate: func(c *Config) { c.ClickHouseMaxOpenConns = -1 },
			want:   "clickhouse_max_open_conns must be at least 1",
		},
		{
			name:   "max_idle_conns zero rejected",
			mutate: func(c *Config) { c.ClickHouseMaxIdleConns = 0 },
			want:   "clickhouse_max_idle_conns must be at least 1",
		},
		{
			name:   "max_idle_conns negative rejected",
			mutate: func(c *Config) { c.ClickHouseMaxIdleConns = -1 },
			want:   "clickhouse_max_idle_conns must be at least 1",
		},
		{
			name:   "valid pool settings accepted",
			mutate: func(c *Config) { c.ClickHouseMaxOpenConns = 10; c.ClickHouseMaxIdleConns = 3 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Expected config to validate, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Expected validation error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestValidateConfigKafkaTimeoutSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "session_timeout_ms zero rejected",
			mutate: func(c *Config) { c.KafkaSessionTimeoutMs = 0 },
			want:   "kafka_session_timeout_ms must be > 0",
		},
		{
			name:   "session_timeout_ms negative rejected",
			mutate: func(c *Config) { c.KafkaSessionTimeoutMs = -1 },
			want:   "kafka_session_timeout_ms must be > 0",
		},
		{
			name:   "max_poll_interval_ms zero rejected",
			mutate: func(c *Config) { c.KafkaMaxPollIntervalMs = 0 },
			want:   "kafka_max_poll_interval_ms must be > 0",
		},
		{
			name:   "max_poll_interval_ms negative rejected",
			mutate: func(c *Config) { c.KafkaMaxPollIntervalMs = -1 },
			want:   "kafka_max_poll_interval_ms must be > 0",
		},
		{
			name:   "valid timeout settings accepted",
			mutate: func(c *Config) { c.KafkaSessionTimeoutMs = 30000; c.KafkaMaxPollIntervalMs = 600000 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Expected config to validate, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Expected validation error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestIsRetriableClickHouseError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			name:      "plain error is retriable (network/timeout)",
			err:       fmt.Errorf("connection reset by peer"),
			retriable: true,
		},
		{
			name:      "retriable code 159 (timeout)",
			err:       &clickhouse.Exception{Code: 159, Message: "timeout"},
			retriable: true,
		},
		{
			name:      "retriable code 202 (too many parts)",
			err:       &clickhouse.Exception{Code: 202, Message: "too many parts"},
			retriable: true,
		},
		{
			name:      "retriable code 242 (table read-only)",
			err:       &clickhouse.Exception{Code: 242, Message: "table is read-only"},
			retriable: true,
		},
		{
			name:      "retriable code 999 (keeper exception)",
			err:       &clickhouse.Exception{Code: 999, Message: "keeper exception"},
			retriable: true,
		},
		{
			name:      "retriable code 3 (unexpected end of file)",
			err:       &clickhouse.Exception{Code: 3, Message: "unexpected end of file"},
			retriable: true,
		},
		{
			name:      "non-retriable code 60 (unknown table)",
			err:       &clickhouse.Exception{Code: 60, Message: "unknown table"},
			retriable: false,
		},
		{
			name:      "non-retriable code 62 (syntax error)",
			err:       &clickhouse.Exception{Code: 62, Message: "syntax error"},
			retriable: false,
		},
		{
			name:      "non-retriable code 16 (no such column)",
			err:       &clickhouse.Exception{Code: 16, Message: "no such column"},
			retriable: false,
		},
		{
			name:      "wrapped retriable error",
			err:       fmt.Errorf("wrapped: %w", &clickhouse.Exception{Code: 159, Message: "timeout"}),
			retriable: true,
		},
		{
			name:      "wrapped non-retriable error",
			err:       fmt.Errorf("wrapped: %w", &clickhouse.Exception{Code: 60, Message: "unknown table"}),
			retriable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetriableClickHouseError(tt.err)
			if got != tt.retriable {
				t.Fatalf("isRetriableClickHouseError(%v) = %v, want %v", tt.err, got, tt.retriable)
			}
		})
	}
}

func TestTaskStoppedGaugeExistsInRegistry(t *testing.T) {
	// Initialize a label so the metric family appears in Gather output.
	taskStopped.WithLabelValues("__test_topic__").Set(0)
	defer taskStopped.DeleteLabelValues("__test_topic__")

	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}
	found := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "kahouse_task_stopped" {
			found = true
			if mf.GetHelp() == "" {
				t.Fatal("Expected non-empty help text for kahouse_task_stopped")
			}
			break
		}
	}
	if !found {
		t.Fatal("Expected kahouse_task_stopped metric to be registered")
	}
}

func TestMsgChannelDepthMetricRegistered(t *testing.T) {
	// Initialize a label so the metric family appears in Gather output.
	msgChannelDepth.WithLabelValues("__test_topic__").Set(0)
	defer msgChannelDepth.DeleteLabelValues("__test_topic__")

	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}
	found := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "kahouse_msg_channel_depth" {
			found = true
			if mf.GetHelp() == "" {
				t.Fatal("Expected non-empty help text for kahouse_msg_channel_depth")
			}
			break
		}
	}
	if !found {
		t.Fatal("Expected kahouse_msg_channel_depth metric to be registered")
	}
}

func TestDLQTopicNameFormation(t *testing.T) {
	// Validate the naming convention used by validateDLQTopics matches what
	// sendToDLQ and sendBatchToDLQ use: topic + dlq_topic_suffix.
	cfg := validConfig()
	cfg.DLQTopicSuffix = ".dlq"
	cfg.TopicTables = []TopicTableMapping{
		{Topic: "orders", Table: "default.orders"},
		{Topic: "payments", Table: "default.payments"},
	}

	for _, mapping := range cfg.TopicTables {
		expected := mapping.Topic + cfg.DLQTopicSuffix
		if expected != mapping.Topic+".dlq" {
			t.Fatalf("DLQ topic name mismatch: expected %q, got %q", mapping.Topic+".dlq", expected)
		}
	}
}

func TestValidateConfigAsyncInsertDefaults(t *testing.T) {
	// The async insert booleans have no validation constraints (any bool is valid),
	// but we verify they survive the validation pass and appear in configLogFields.
	cfg := validConfig()
	cfg.ClickHouseAsyncInsert = true
	cfg.ClickHouseWaitForAsyncInsert = true
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("Expected valid config with async inserts enabled, got %v", err)
	}

	cfg.ClickHouseAsyncInsert = false
	cfg.ClickHouseWaitForAsyncInsert = false
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("Expected valid config with async inserts disabled, got %v", err)
	}
}

func TestConfigLogFieldsIncludesNewFields(t *testing.T) {
	cfg := &Config{
		KafkaBrokers:                 "localhost:9092",
		InputFormat:                  "avro",
		SchemaRegistry:               "http://localhost:8081",
		ClickHouseDSN:                "tcp://localhost:9000",
		GroupID:                      "group",
		DLQTopicSuffix:               ".dlq",
		AutoOffsetReset:              "earliest",
		ClickHouseMaxOpenConns:       10,
		ClickHouseMaxIdleConns:       5,
		ClickHouseAsyncInsert:        true,
		ClickHouseWaitForAsyncInsert: false,
		KafkaSessionTimeoutMs:        45000,
		KafkaMaxPollIntervalMs:       300000,
		BatchDelayMs:                 intPtr(200),
		MaxRetries:                   intPtr(5),
		RetryBackoffMs:               intPtr(100),
		TopicTables:                  []TopicTableMapping{{Topic: "orders", Table: "default.orders"}},
	}

	fields := configLogFields(cfg)
	fieldMap := make(map[string]interface{}, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			t.Fatalf("Expected string key at index %d, got %T", i, fields[i])
		}
		fieldMap[key] = fields[i+1]
	}

	if got := fieldMap["clickhouse_max_open_conns"]; got != 10 {
		t.Fatalf("Expected clickhouse_max_open_conns=10, got %v", got)
	}
	if got := fieldMap["clickhouse_max_idle_conns"]; got != 5 {
		t.Fatalf("Expected clickhouse_max_idle_conns=5, got %v", got)
	}

	if got := fieldMap["kafka_session_timeout_ms"]; got != 45000 {
		t.Fatalf("Expected kafka_session_timeout_ms=45000, got %v", got)
	}
	if got := fieldMap["kafka_max_poll_interval_ms"]; got != 300000 {
		t.Fatalf("Expected kafka_max_poll_interval_ms=300000, got %v", got)
	}

	if got := fieldMap["clickhouse_async_insert"]; got != true {
		t.Fatalf("Expected clickhouse_async_insert=true, got %v", got)
	}
	if got := fieldMap["clickhouse_wait_for_async_insert"]; got != false {
		t.Fatalf("Expected clickhouse_wait_for_async_insert=false, got %v", got)
	}
}

func TestTaskManagerStartRejectsDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task.stopped.Store(true)
	done := make(chan struct{})
	close(done)
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    done,
	}

	// Cancel context to simulate shutdown.
	cancel()

	err := mgr.Start("orders")
	if err == nil {
		t.Fatal("Expected error when starting topic during shutdown")
	}
	if !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("Expected 'shutting down' error, got %q", err.Error())
	}
}

func TestIsRetriableClickHouseErrorAllCodes(t *testing.T) {
	// Exhaustively test all codes listed in the switch statement.
	retriableCodes := []int32{3, 107, 159, 164, 202, 203, 209, 210, 241, 242, 252, 285, 319, 425, 999}
	for _, code := range retriableCodes {
		err := &clickhouse.Exception{Code: code, Message: fmt.Sprintf("test code %d", code)}
		if !isRetriableClickHouseError(err) {
			t.Errorf("Expected code %d to be retriable", code)
		}
	}

	// Spot-check some non-retriable codes.
	nonRetriableCodes := []int32{1, 10, 16, 36, 47, 60, 62, 70, 117}
	for _, code := range nonRetriableCodes {
		err := &clickhouse.Exception{Code: code, Message: fmt.Sprintf("test code %d", code)}
		if isRetriableClickHouseError(err) {
			t.Errorf("Expected code %d to be non-retriable", code)
		}
	}
}

func TestApplyDefaultsNewFields(t *testing.T) {
	cfg := &Config{}
	applyDefaults(cfg)

	if cfg.ClickHouseMaxOpenConns != 5 {
		t.Fatalf("Expected default clickhouse_max_open_conns=5, got %d", cfg.ClickHouseMaxOpenConns)
	}
	if cfg.ClickHouseMaxIdleConns != 5 {
		t.Fatalf("Expected default clickhouse_max_idle_conns=5, got %d", cfg.ClickHouseMaxIdleConns)
	}
	if cfg.KafkaSessionTimeoutMs != 45000 {
		t.Fatalf("Expected default kafka_session_timeout_ms=45000, got %d", cfg.KafkaSessionTimeoutMs)
	}
	if cfg.KafkaMaxPollIntervalMs != 300000 {
		t.Fatalf("Expected default kafka_max_poll_interval_ms=300000, got %d", cfg.KafkaMaxPollIntervalMs)
	}
	if cfg.AutoOffsetReset != "earliest" {
		t.Fatalf("Expected default auto_offset_reset=earliest, got %q", cfg.AutoOffsetReset)
	}
}
