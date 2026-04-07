package app

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.yaml.in/yaml/v3"
)

// TopicTableMapping defines a single topic-to-table mapping with optional per-topic overrides.
// Numeric fields are pointers so that nil ("not set") can be distinguished from an explicit zero.
// A nil field inherits the global default; a non-nil field (including *0) is used as-is.
type TopicTableMapping struct {
	Topic             string `yaml:"topic"`
	Table             string `yaml:"table"`
	Format            string `yaml:"format,omitempty"`
	StringValueColumn string `yaml:"string_value_column,omitempty"`
	BatchSize         *int   `yaml:"batch_size,omitempty"`
	BatchDelayMs      *int   `yaml:"batch_delay_ms,omitempty"`
	MaxRetries        *int   `yaml:"max_retries,omitempty"`
	RetryBackoffMs    *int   `yaml:"retry_backoff_ms,omitempty"`
}

// Config holds all configuration for the application.
type Config struct {
	KafkaBrokers   string `yaml:"kafka_brokers"`
	SchemaRegistry string `yaml:"schema_registry"`
	ClickHouseDSN  string `yaml:"clickhouse_dsn"`
	GroupID        string `yaml:"group_id"`
	DLQTopicSuffix string `yaml:"dlq_topic_suffix"`
	InputFormat    string `yaml:"input_format"`

	// String input settings.
	StringValueColumn string `yaml:"string_value_column"`

	// Schema Registry authentication (optional — omit for unauthenticated registries)
	SchemaRegistryUsername string `yaml:"schema_registry_username"`
	SchemaRegistryPassword string `yaml:"schema_registry_password"`

	// Kafka authentication (all optional — omit for unauthenticated clusters)
	KafkaSecurityProtocol string `yaml:"kafka_security_protocol"`
	KafkaSASLMechanism    string `yaml:"kafka_sasl_mechanism"`
	KafkaSASLUsername     string `yaml:"kafka_sasl_username"`
	KafkaSASLPassword     string `yaml:"kafka_sasl_password"`
	KafkaSSLCALocation    string `yaml:"kafka_ssl_ca_location"`

	// Server settings
	MetricsPort int `yaml:"metrics_port"`

	// Batch defaults (overridable per topic)
	BatchSize      int `yaml:"batch_size"`
	BatchDelayMs   int `yaml:"batch_delay_ms"`
	MaxRetries     int `yaml:"max_retries"`
	RetryBackoffMs int `yaml:"retry_backoff_ms"`

	TopicTables []TopicTableMapping `yaml:"topic_tables"`
}

// intPtr returns a pointer to a copy of v.
func intPtr(v int) *int { return &v }

// resolve fills nil pointer fields from global defaults.
// A nil pointer means "not configured by the user"; a non-nil pointer (including *0) is respected.
func (m *TopicTableMapping) resolve(cfg *Config) {
	m.Format = normalizeInputFormat(m.Format)
	if m.Format == "" {
		m.Format = cfg.InputFormat
	}
	m.StringValueColumn = strings.TrimSpace(m.StringValueColumn)
	if m.StringValueColumn == "" {
		m.StringValueColumn = cfg.StringValueColumn
	}
	if m.BatchSize == nil {
		m.BatchSize = intPtr(cfg.BatchSize)
	}
	if m.BatchDelayMs == nil {
		m.BatchDelayMs = intPtr(cfg.BatchDelayMs)
	}
	if m.MaxRetries == nil {
		m.MaxRetries = intPtr(cfg.MaxRetries)
	}
	if m.RetryBackoffMs == nil {
		m.RetryBackoffMs = intPtr(cfg.RetryBackoffMs)
	}
}

func applyDefaults(cfg *Config) {
	if cfg.KafkaBrokers == "" {
		cfg.KafkaBrokers = "localhost:9092"
	}
	if cfg.SchemaRegistry == "" {
		cfg.SchemaRegistry = "http://localhost:8081"
	}
	if cfg.ClickHouseDSN == "" {
		cfg.ClickHouseDSN = "tcp://localhost:9000"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "kahouse"
	}
	if cfg.DLQTopicSuffix == "" {
		cfg.DLQTopicSuffix = ".dlq"
	}
	if cfg.InputFormat == "" {
		cfg.InputFormat = "avro"
	}
	if cfg.StringValueColumn == "" {
		cfg.StringValueColumn = "value"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10000
	}
	if cfg.BatchDelayMs == 0 {
		cfg.BatchDelayMs = 200
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 5
	}
	if cfg.RetryBackoffMs == 0 {
		cfg.RetryBackoffMs = 100
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = 9090
	}
}

// configPath returns the config file path from -config flag or KAHOUSE_CONFIG env var.
// Defaults to "kahouse.yaml".
func configPath() string {
	path := flag.String("config", "", "path to config file (default: kahouse.yaml or KAHOUSE_CONFIG)")
	flag.Parse()

	if *path != "" {
		return *path
	}
	if env := os.Getenv("KAHOUSE_CONFIG"); env != "" {
		return env
	}
	return "kahouse.yaml"
}

func loadConfig() (*Config, error) {
	path := configPath()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %q: %w", path, err)
	}

	applyDefaults(&cfg)
	cfg.InputFormat = normalizeInputFormat(cfg.InputFormat)
	cfg.StringValueColumn = strings.TrimSpace(cfg.StringValueColumn)

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(&cfg)
	}

	return &cfg, nil
}

func validateConfig(cfg *Config) error {
	switch cfg.InputFormat {
	case "avro", "json", "string":
	case "":
		return fmt.Errorf("input_format is required")
	default:
		return fmt.Errorf("input_format must be one of avro, json, or string, got %q", cfg.InputFormat)
	}

	if strings.TrimSpace(cfg.KafkaBrokers) == "" {
		return fmt.Errorf("kafka_brokers is required")
	}
	if anyTopicUsesFormat(cfg, "avro") && strings.TrimSpace(cfg.SchemaRegistry) == "" {
		return fmt.Errorf("schema_registry is required")
	}
	if strings.TrimSpace(cfg.ClickHouseDSN) == "" {
		return fmt.Errorf("clickhouse_dsn is required")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		return fmt.Errorf("group_id is required")
	}
	if strings.TrimSpace(cfg.DLQTopicSuffix) == "" {
		return fmt.Errorf("dlq_topic_suffix is required and must not be empty")
	}
	if cfg.InputFormat == "string" && cfg.StringValueColumn == "" {
		return fmt.Errorf("string_value_column is required when input_format is string")
	}
	if cfg.BatchSize < 1 {
		return fmt.Errorf("batch_size must be at least 1, got %d", cfg.BatchSize)
	}
	if cfg.BatchDelayMs < 0 {
		return fmt.Errorf("batch_delay_ms must be >= 0, got %d", cfg.BatchDelayMs)
	}
	if cfg.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0, got %d", cfg.MaxRetries)
	}
	if cfg.RetryBackoffMs < 0 {
		return fmt.Errorf("retry_backoff_ms must be >= 0, got %d", cfg.RetryBackoffMs)
	}
	if len(cfg.TopicTables) == 0 {
		return fmt.Errorf("at least one topic_tables mapping is required")
	}
	seenTopics := make(map[string]int, len(cfg.TopicTables))
	for i, tt := range cfg.TopicTables {
		trimmedTopic := strings.TrimSpace(tt.Topic)
		if trimmedTopic == "" {
			return fmt.Errorf("topic_tables[%d]: topic is required", i)
		}
		if prev, exists := seenTopics[trimmedTopic]; exists {
			return fmt.Errorf("topic_tables[%d]: duplicate topic %q (first defined at index %d)", i, trimmedTopic, prev)
		}
		seenTopics[trimmedTopic] = i
		if strings.TrimSpace(tt.Table) == "" {
			return fmt.Errorf("topic_tables[%d]: table is required", i)
		}
		format := normalizeInputFormat(tt.Format)
		if format == "" {
			format = cfg.InputFormat
		}
		switch format {
		case "avro", "json", "string":
		default:
			return fmt.Errorf("topic_tables[%d]: format must be one of avro, json, or string, got %q", i, tt.Format)
		}
		if format == "string" {
			stringValueColumn := strings.TrimSpace(tt.StringValueColumn)
			if stringValueColumn == "" {
				stringValueColumn = cfg.StringValueColumn
			}
			if stringValueColumn == "" {
				return fmt.Errorf("topic_tables[%d]: string_value_column is required when format is string", i)
			}
		}
		if tt.BatchSize != nil && *tt.BatchSize < 1 {
			return fmt.Errorf("topic_tables[%d]: batch_size must be at least 1, got %d", i, *tt.BatchSize)
		}
		if tt.BatchDelayMs != nil && *tt.BatchDelayMs < 0 {
			return fmt.Errorf("topic_tables[%d]: batch_delay_ms must be >= 0, got %d", i, *tt.BatchDelayMs)
		}
		if tt.MaxRetries != nil && *tt.MaxRetries < 0 {
			return fmt.Errorf("topic_tables[%d]: max_retries must be >= 0, got %d", i, *tt.MaxRetries)
		}
		if tt.RetryBackoffMs != nil && *tt.RetryBackoffMs < 0 {
			return fmt.Errorf("topic_tables[%d]: retry_backoff_ms must be >= 0, got %d", i, *tt.RetryBackoffMs)
		}
	}

	return nil
}

func configLogFields(cfg *Config) []interface{} {
	return []interface{}{
		"kafka_brokers", cfg.KafkaBrokers,
		"schema_registry", cfg.SchemaRegistry,
		"schema_registry_username", cfg.SchemaRegistryUsername,
		"schema_registry_password", redactSecret(cfg.SchemaRegistryPassword),
		"clickhouse_dsn", redactDSN(cfg.ClickHouseDSN),
		"group_id", cfg.GroupID,
		"dlq_topic_suffix", cfg.DLQTopicSuffix,
		"input_format", cfg.InputFormat,
		"string_value_column", cfg.StringValueColumn,
		"kafka_security_protocol", cfg.KafkaSecurityProtocol,
		"kafka_sasl_mechanism", cfg.KafkaSASLMechanism,
		"kafka_sasl_username", cfg.KafkaSASLUsername,
		"kafka_sasl_password", redactSecret(cfg.KafkaSASLPassword),
		"kafka_ssl_ca_location", cfg.KafkaSSLCALocation,
		"batch_size", cfg.BatchSize,
		"batch_delay_ms", cfg.BatchDelayMs,
		"max_retries", cfg.MaxRetries,
		"retry_backoff_ms", cfg.RetryBackoffMs,
		"topic_tables", cfg.TopicTables,
	}
}

func normalizeInputFormat(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func anyTopicUsesFormat(cfg *Config, format string) bool {
	format = normalizeInputFormat(format)
	for _, topicTable := range cfg.TopicTables {
		topicFormat := normalizeInputFormat(topicTable.Format)
		if topicFormat == "" {
			topicFormat = cfg.InputFormat
		}
		if topicFormat == format {
			return true
		}
	}
	return false
}

func redactSecret(value string) string {
	if value == "" {
		return ""
	}
	return "[redacted]"
}

func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	if idx := strings.Index(dsn, "@"); idx != -1 {
		if schemeIdx := strings.Index(dsn, "://"); schemeIdx != -1 && schemeIdx < idx {
			return dsn[:schemeIdx+3] + "[redacted]" + dsn[idx:]
		}
	}
	return dsn
}

// buildKafkaConfig applies auth fields from cfg onto a base kafka.ConfigMap.
// Fields are only set when non-empty, so omitting them keeps existing unauthenticated behavior.
func buildKafkaConfig(base kafka.ConfigMap, cfg *Config) kafka.ConfigMap {
	if cfg.KafkaSecurityProtocol != "" {
		base["security.protocol"] = cfg.KafkaSecurityProtocol
	}
	if cfg.KafkaSASLMechanism != "" {
		base["sasl.mechanism"] = cfg.KafkaSASLMechanism
	}
	if cfg.KafkaSASLUsername != "" {
		base["sasl.username"] = cfg.KafkaSASLUsername
	}
	if cfg.KafkaSASLPassword != "" {
		base["sasl.password"] = cfg.KafkaSASLPassword
	}
	if cfg.KafkaSSLCALocation != "" {
		base["ssl.ca.location"] = cfg.KafkaSSLCALocation
	}
	return base
}
