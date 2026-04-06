package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

// TopicTableMapping defines a single topic-to-table mapping with optional per-topic overrides.
// Numeric fields are pointers so that nil ("not set") can be distinguished from an explicit zero.
// A nil field inherits the global default; a non-nil field (including *0) is used as-is.
type TopicTableMapping struct {
	Topic             string `mapstructure:"topic" json:"topic"`
	Table             string `mapstructure:"table" json:"table"`
	Format            string `mapstructure:"format" json:"format,omitempty"`
	StringValueColumn string `mapstructure:"string_value_column" json:"string_value_column,omitempty"`
	BatchSize         *int   `mapstructure:"batch_size" json:"batch_size,omitempty"`
	BatchDelayMs      *int   `mapstructure:"batch_delay_ms" json:"batch_delay_ms,omitempty"`
	MaxRetries        *int   `mapstructure:"max_retries" json:"max_retries,omitempty"`
	RetryBackoffMs    *int   `mapstructure:"retry_backoff_ms" json:"retry_backoff_ms,omitempty"`
}

// Config holds all configuration for the application.
type Config struct {
	KafkaBrokers   string `mapstructure:"kafka_brokers"`
	SchemaRegistry string `mapstructure:"schema_registry"`
	ClickHouseDSN  string `mapstructure:"clickhouse_dsn"`
	GroupID        string `mapstructure:"group_id"`
	DLQTopicSuffix string `mapstructure:"dlq_topic_suffix"`
	InputFormat    string `mapstructure:"input_format"`

	// String input settings.
	StringValueColumn string `mapstructure:"string_value_column"`

	// Schema Registry authentication (optional — omit for unauthenticated registries)
	SchemaRegistryUsername string `mapstructure:"schema_registry_username"`
	SchemaRegistryPassword string `mapstructure:"schema_registry_password"`

	// Kafka authentication (all optional — omit for unauthenticated clusters)
	KafkaSecurityProtocol string `mapstructure:"kafka_security_protocol"`
	KafkaSASLMechanism    string `mapstructure:"kafka_sasl_mechanism"`
	KafkaSASLUsername     string `mapstructure:"kafka_sasl_username"`
	KafkaSASLPassword     string `mapstructure:"kafka_sasl_password"`
	KafkaSSLCALocation    string `mapstructure:"kafka_ssl_ca_location"`

	// Server settings
	MetricsPort int `mapstructure:"metrics_port"`

	// Batch defaults (overridable per topic)
	BatchSize      int `mapstructure:"batch_size"`
	BatchDelayMs   int `mapstructure:"batch_delay_ms"`
	MaxRetries     int `mapstructure:"max_retries"`
	RetryBackoffMs int `mapstructure:"retry_backoff_ms"`

	TopicTables []TopicTableMapping `mapstructure:"topic_tables"`
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

// defaults holds the default configuration values.
// topic_tables has no default entry — an explicit mapping is required.
var defaults = map[string]interface{}{
	"kafka_brokers":            "localhost:9092",
	"schema_registry":          "http://localhost:8081",
	"schema_registry_username": "",
	"schema_registry_password": "",
	"clickhouse_dsn":           "tcp://localhost:9000",
	"group_id":                 "kahouse",
	"batch_size":               10000,
	"batch_delay_ms":           200,
	"max_retries":              3,
	"retry_backoff_ms":         100,
	"metrics_port":             9090,
	"dlq_topic_suffix":         ".dlq",
	"input_format":             "avro",
	"string_value_column":      "value",
	"kafka_security_protocol":  "",
	"kafka_sasl_mechanism":     "",
	"kafka_sasl_username":      "",
	"kafka_sasl_password":      "",
	"kafka_ssl_ca_location":    "",
}

func loadConfig() (*Config, error) {
	v := viper.New()

	for key, value := range defaults {
		v.SetDefault(key, value)
	}

	v.SetEnvPrefix("kahouse")
	v.AutomaticEnv()

	if err := readConfigFile(v); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config into struct: %w", err)
	}

	topicTables, ok, err := topicTablesFromEnv()
	if err != nil {
		return nil, err
	}
	if ok {
		cfg.TopicTables = topicTables
	}

	cfg.InputFormat = normalizeInputFormat(cfg.InputFormat)
	cfg.StringValueColumn = strings.TrimSpace(cfg.StringValueColumn)

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	// Resolve per-topic defaults
	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(&cfg)
	}

	return &cfg, nil
}

func topicTablesFromEnv() ([]TopicTableMapping, bool, error) {
	rawTopicTables := strings.TrimSpace(os.Getenv("KAHOUSE_TOPIC_TABLES"))
	if rawTopicTables == "" {
		return nil, false, nil
	}

	var topicTables []TopicTableMapping
	if err := json.Unmarshal([]byte(rawTopicTables), &topicTables); err != nil {
		return nil, false, fmt.Errorf("failed to parse KAHOUSE_TOPIC_TABLES as JSON: %w", err)
	}
	return topicTables, true, nil
}

func readConfigFile(v *viper.Viper) error {
	configFile, err := findConfigFile(configSearchPaths())
	if err != nil {
		return err
	}
	if configFile == "" {
		return nil
	}

	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("config file %q could not be parsed: %w", configFile, err)
	}
	return nil
}

func configSearchPaths() []string {
	dirs := []string{"."}
	if homeDir, err := os.UserHomeDir(); err == nil && strings.TrimSpace(homeDir) != "" {
		dirs = append(dirs, homeDir)
	}
	dirs = append(dirs, "/etc/kahouse")

	var paths []string
	for _, dir := range dirs {
		for _, name := range []string{"kahouse.yaml", "config.yaml"} {
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	return paths
}

func findConfigFile(paths []string) (string, error) {
	for _, path := range paths {
		info, err := os.Stat(path)
		if err == nil {
			if info.IsDir() {
				continue
			}
			return path, nil
		}
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("failed to inspect config file %q: %w", path, err)
		}
	}
	return "", nil
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
		return fmt.Errorf("at least one topic_table mapping is required (set topic_tables in config file or KAHOUSE_TOPIC_TABLES env var)")
	}
	seenTopics := make(map[string]int, len(cfg.TopicTables))
	for i, tt := range cfg.TopicTables {
		if prev, exists := seenTopics[tt.Topic]; exists {
			return fmt.Errorf("topic_tables[%d]: duplicate topic %q (first defined at index %d)", i, tt.Topic, prev)
		}
		seenTopics[tt.Topic] = i
		if strings.TrimSpace(tt.Topic) == "" {
			return fmt.Errorf("topic_tables[%d]: topic is required", i)
		}
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
