package app

import (
	"flag"
	"fmt"
	"net/url"
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

	// Consumer offset reset policy
	AutoOffsetReset string `yaml:"auto_offset_reset"`

	// Kafka authentication (all optional — omit for unauthenticated clusters)
	KafkaSecurityProtocol string `yaml:"kafka_security_protocol"`
	KafkaSASLMechanism    string `yaml:"kafka_sasl_mechanism"`
	KafkaSASLUsername     string `yaml:"kafka_sasl_username"`
	KafkaSASLPassword     string `yaml:"kafka_sasl_password"`
	KafkaSSLCALocation    string `yaml:"kafka_ssl_ca_location"`

	// Server settings
	MetricsPort int `yaml:"metrics_port"`

	// ClickHouse connection pool
	ClickHouseMaxOpenConns int `yaml:"clickhouse_max_open_conns"`
	ClickHouseMaxIdleConns int `yaml:"clickhouse_max_idle_conns"`

	// ClickHouse async insert settings
	ClickHouseAsyncInsert        bool `yaml:"clickhouse_async_insert"`
	ClickHouseWaitForAsyncInsert bool `yaml:"clickhouse_wait_for_async_insert"`

	// Kafka consumer timeout settings
	KafkaSessionTimeoutMs  int `yaml:"kafka_session_timeout_ms"`
	KafkaMaxPollIntervalMs int `yaml:"kafka_max_poll_interval_ms"`

	// Batch defaults (overridable per topic)
	BatchSize      int  `yaml:"batch_size"`
	BatchDelayMs   *int `yaml:"batch_delay_ms"`
	MaxRetries     *int `yaml:"max_retries"`
	RetryBackoffMs *int `yaml:"retry_backoff_ms"`

	// Shutdown timeout (seconds) for flushing remaining batches and stopping the HTTP server
	ShutdownTimeoutS int `yaml:"shutdown_timeout_s"`

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
		m.BatchDelayMs = cfg.BatchDelayMs
	}
	if m.MaxRetries == nil {
		m.MaxRetries = cfg.MaxRetries
	}
	if m.RetryBackoffMs == nil {
		m.RetryBackoffMs = cfg.RetryBackoffMs
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
	if cfg.AutoOffsetReset == "" {
		cfg.AutoOffsetReset = "earliest"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10000
	}
	if cfg.BatchDelayMs == nil {
		cfg.BatchDelayMs = intPtr(200)
	}
	if cfg.MaxRetries == nil {
		cfg.MaxRetries = intPtr(5)
	}
	if cfg.RetryBackoffMs == nil {
		cfg.RetryBackoffMs = intPtr(100)
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = 9090
	}
	if cfg.ClickHouseMaxOpenConns == 0 {
		cfg.ClickHouseMaxOpenConns = 5
	}
	if cfg.ClickHouseMaxIdleConns == 0 {
		cfg.ClickHouseMaxIdleConns = 5
	}
	// ClickHouseAsyncInsert and ClickHouseWaitForAsyncInsert default to false (zero value for bool).
	// This is intentional: async inserts are opt-in.
	if cfg.KafkaSessionTimeoutMs == 0 {
		cfg.KafkaSessionTimeoutMs = 45000
	}
	if cfg.KafkaMaxPollIntervalMs == 0 {
		cfg.KafkaMaxPollIntervalMs = 300000
	}
	if cfg.ShutdownTimeoutS == 0 {
		cfg.ShutdownTimeoutS = 5
	}
}

// ConfigPath returns the config file path from -config flag or KAHOUSE_CONFIG env var.
// Defaults to "kahouse.yaml". Call flag.Parse() before calling this.
func ConfigPath() string {
	if *configFlag != "" {
		return *configFlag
	}
	if env := os.Getenv("KAHOUSE_CONFIG"); env != "" {
		return env
	}
	return "kahouse.yaml"
}

var configFlag = flag.String("config", "", "path to config file (default: kahouse.yaml or KAHOUSE_CONFIG)")

func loadConfig(path string) (*Config, error) {
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

	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(&cfg)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
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

	switch cfg.AutoOffsetReset {
	case "earliest", "latest", "none":
	default:
		return fmt.Errorf("auto_offset_reset must be one of earliest, latest, or none, got %q", cfg.AutoOffsetReset)
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
	if cfg.ClickHouseMaxOpenConns < 1 {
		return fmt.Errorf("clickhouse_max_open_conns must be at least 1, got %d", cfg.ClickHouseMaxOpenConns)
	}
	if cfg.ClickHouseMaxIdleConns < 1 {
		return fmt.Errorf("clickhouse_max_idle_conns must be at least 1, got %d", cfg.ClickHouseMaxIdleConns)
	}
	if cfg.BatchSize < 1 {
		return fmt.Errorf("batch_size must be at least 1, got %d", cfg.BatchSize)
	}
	if *cfg.BatchDelayMs < 0 {
		return fmt.Errorf("batch_delay_ms must be >= 0, got %d", *cfg.BatchDelayMs)
	}
	if *cfg.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0, got %d", *cfg.MaxRetries)
	}
	if *cfg.RetryBackoffMs < 0 {
		return fmt.Errorf("retry_backoff_ms must be >= 0, got %d", *cfg.RetryBackoffMs)
	}
	if cfg.KafkaSessionTimeoutMs <= 0 {
		return fmt.Errorf("kafka_session_timeout_ms must be > 0, got %d", cfg.KafkaSessionTimeoutMs)
	}
	if cfg.KafkaMaxPollIntervalMs <= 0 {
		return fmt.Errorf("kafka_max_poll_interval_ms must be > 0, got %d", cfg.KafkaMaxPollIntervalMs)
	}
	if cfg.ShutdownTimeoutS <= 0 {
		return fmt.Errorf("shutdown_timeout_s must be > 0, got %d", cfg.ShutdownTimeoutS)
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
		switch tt.Format {
		case "avro", "json", "string":
		default:
			return fmt.Errorf("topic_tables[%d]: format must be one of avro, json, or string, got %q", i, tt.Format)
		}
		if tt.Format == "string" && tt.StringValueColumn == "" {
			return fmt.Errorf("topic_tables[%d]: string_value_column is required when format is string", i)
		}
		if *tt.BatchSize < 1 {
			return fmt.Errorf("topic_tables[%d]: batch_size must be at least 1, got %d", i, *tt.BatchSize)
		}
		if *tt.BatchDelayMs < 0 {
			return fmt.Errorf("topic_tables[%d]: batch_delay_ms must be >= 0, got %d", i, *tt.BatchDelayMs)
		}
		if *tt.MaxRetries < 0 {
			return fmt.Errorf("topic_tables[%d]: max_retries must be >= 0, got %d", i, *tt.MaxRetries)
		}
		if *tt.RetryBackoffMs < 0 {
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
		"clickhouse_max_open_conns", cfg.ClickHouseMaxOpenConns,
		"clickhouse_max_idle_conns", cfg.ClickHouseMaxIdleConns,
		"clickhouse_async_insert", cfg.ClickHouseAsyncInsert,
		"clickhouse_wait_for_async_insert", cfg.ClickHouseWaitForAsyncInsert,
		"group_id", cfg.GroupID,
		"dlq_topic_suffix", cfg.DLQTopicSuffix,
		"input_format", cfg.InputFormat,
		"string_value_column", cfg.StringValueColumn,
		"auto_offset_reset", cfg.AutoOffsetReset,
		"kafka_security_protocol", cfg.KafkaSecurityProtocol,
		"kafka_sasl_mechanism", cfg.KafkaSASLMechanism,
		"kafka_sasl_username", cfg.KafkaSASLUsername,
		"kafka_sasl_password", redactSecret(cfg.KafkaSASLPassword),
		"kafka_ssl_ca_location", cfg.KafkaSSLCALocation,
		"batch_size", cfg.BatchSize,
		"batch_delay_ms", *cfg.BatchDelayMs,
		"max_retries", *cfg.MaxRetries,
		"retry_backoff_ms", *cfg.RetryBackoffMs,
		"kafka_session_timeout_ms", cfg.KafkaSessionTimeoutMs,
		"kafka_max_poll_interval_ms", cfg.KafkaMaxPollIntervalMs,
		"shutdown_timeout_s", cfg.ShutdownTimeoutS,
		"topic_tables", cfg.TopicTables,
	}
}

func normalizeInputFormat(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func anyTopicUsesFormat(cfg *Config, format string) bool {
	for _, tt := range cfg.TopicTables {
		if tt.Format == format {
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

// sanitizeDSN percent-encodes the user and password portions of a DSN so that
// special characters (?, @, #, etc.) in credentials don't break url.Parse.
// It locates the userinfo section between "://" and the last "@", splits on
// the first ":" to separate user from password, and encodes both using
// url.UserPassword which correctly escapes characters reserved in userinfo.
func sanitizeDSN(dsn string) string {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd == -1 {
		return dsn
	}

	rest := dsn[schemeEnd+3:]
	lastAt := strings.LastIndex(rest, "@")
	if lastAt == -1 {
		return dsn // no userinfo
	}

	userinfo := rest[:lastAt]
	hostAndRest := rest[lastAt+1:]
	scheme := dsn[:schemeEnd+3]

	colonIdx := strings.Index(userinfo, ":")
	if colonIdx == -1 {
		// Username only, no password.
		encoded := url.User(userinfo).String()
		return scheme + encoded + "@" + hostAndRest
	}

	user := userinfo[:colonIdx]
	pass := userinfo[colonIdx+1:]

	encoded := url.UserPassword(user, pass).String()
	return scheme + encoded + "@" + hostAndRest
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
