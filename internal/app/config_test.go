package app

import (
	"os"
	"strings"
	"testing"
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
		ShutdownTimeoutS:       5,
		TopicTables:            []TopicTableMapping{{Topic: "orders", Table: "default.orders"}},
	}
	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(&cfg)
	}
	return cfg
}

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
			name:   "conn_max_lifetime_s negative rejected",
			mutate: func(c *Config) { c.ClickHouseConnMaxLifetimeS = -1 },
			want:   "clickhouse_conn_max_lifetime_s must be >= 0",
		},
		{
			name: "valid pool settings accepted",
			mutate: func(c *Config) {
				c.ClickHouseMaxOpenConns = 10
				c.ClickHouseMaxIdleConns = 3
				c.ClickHouseConnMaxLifetimeS = 0
			},
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

func TestValidateConfigAutoRestartSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "initial_backoff_ms negative rejected",
			mutate: func(c *Config) { c.AutoRestart.InitialBackoffMs = -1 },
			want:   "auto_restart.initial_backoff_ms must be >= 0",
		},
		{
			name:   "max_backoff_ms below initial rejected",
			mutate: func(c *Config) { c.AutoRestart.InitialBackoffMs = 5000; c.AutoRestart.MaxBackoffMs = 1000 },
			want:   "auto_restart.max_backoff_ms",
		},
		{
			name:   "reset_after_s negative rejected",
			mutate: func(c *Config) { c.AutoRestart.ResetAfterS = -1 },
			want:   "auto_restart.reset_after_s must be >= 0",
		},
		{
			name: "valid auto_restart settings accepted",
			mutate: func(c *Config) {
				c.AutoRestart.InitialBackoffMs = 5000
				c.AutoRestart.MaxBackoffMs = 300000
				c.AutoRestart.ResetAfterS = 120
			},
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

func TestValidateConfigAsyncInsertDefaults(t *testing.T) {
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
		ClickHouseConnMaxLifetimeS:   300,
		ClickHouseAsyncInsert:        true,
		ClickHouseWaitForAsyncInsert: false,
		KafkaSessionTimeoutMs:        45000,
		KafkaMaxPollIntervalMs:       300000,
		BatchDelayMs:                 intPtr(200),
		MaxRetries:                   intPtr(5),
		RetryBackoffMs:               intPtr(100),
		AutoRestart:                  AutoRestartConfig{Enabled: boolPtr(true), InitialBackoffMs: 5000, MaxBackoffMs: 300000, ResetAfterS: 120, MaxStuckS: 900},
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

	if got := fieldMap["clickhouse_conn_max_lifetime_s"]; got != 300 {
		t.Fatalf("Expected clickhouse_conn_max_lifetime_s=300, got %v", got)
	}
	if got := fieldMap["auto_restart_enabled"]; got != true {
		t.Fatalf("Expected auto_restart_enabled=true, got %v", got)
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
	if cfg.ClickHouseConnMaxLifetimeS != 300 {
		t.Fatalf("Expected default clickhouse_conn_max_lifetime_s=300, got %d", cfg.ClickHouseConnMaxLifetimeS)
	}
	if cfg.AutoRestart.Enabled == nil || !*cfg.AutoRestart.Enabled {
		t.Fatal("Expected auto_restart enabled by default")
	}
	if cfg.AutoRestart.InitialBackoffMs != 5000 {
		t.Fatalf("Expected default auto_restart.initial_backoff_ms=5000, got %d", cfg.AutoRestart.InitialBackoffMs)
	}
	if cfg.AutoRestart.MaxBackoffMs != 300000 {
		t.Fatalf("Expected default auto_restart.max_backoff_ms=300000, got %d", cfg.AutoRestart.MaxBackoffMs)
	}
	if cfg.AutoRestart.ResetAfterS != 120 {
		t.Fatalf("Expected default auto_restart.reset_after_s=120, got %d", cfg.AutoRestart.ResetAfterS)
	}
	if cfg.AutoRestart.MaxStuckS != 900 {
		t.Fatalf("Expected default auto_restart.max_stuck_s=900, got %d", cfg.AutoRestart.MaxStuckS)
	}

	// An explicitly-set lifetime is respected, not overwritten by the default.
	explicit := &Config{ClickHouseConnMaxLifetimeS: 60}
	applyDefaults(explicit)
	if explicit.ClickHouseConnMaxLifetimeS != 60 {
		t.Fatalf("Expected explicit clickhouse_conn_max_lifetime_s=60, got %d", explicit.ClickHouseConnMaxLifetimeS)
	}

	// An explicit auto_restart disable is respected, not overwritten by the default.
	disabled := &Config{AutoRestart: AutoRestartConfig{Enabled: boolPtr(false)}}
	applyDefaults(disabled)
	if disabled.AutoRestart.Enabled == nil || *disabled.AutoRestart.Enabled {
		t.Fatal("Expected explicit auto_restart disable to be respected")
	}
}

func TestSanitizeDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "no userinfo unchanged",
			in:   "clickhouse://host:9440/db?secure=true",
			want: "clickhouse://host:9440/db?secure=true",
		},
		{
			name: "plain credentials unchanged",
			in:   "clickhouse://user:password@host:9440/db",
			want: "clickhouse://user:password@host:9440/db",
		},
		{
			name: "question mark in password encoded",
			in:   "clickhouse://user:p4ss?w0rd@host:9440/db?secure=true",
			want: "clickhouse://user:p4ss%3Fw0rd@host:9440/db?secure=true",
		},
		{
			name: "at sign in password encoded",
			in:   "clickhouse://user:p@ss@host:9000/db",
			want: "clickhouse://user:p%40ss@host:9000/db",
		},
		{
			name: "hash in password encoded",
			in:   "clickhouse://user:p#ss@host:9000/db",
			want: "clickhouse://user:p%23ss@host:9000/db",
		},
		{
			name: "tcp scheme works",
			in:   "tcp://user:p?ss@host:9000",
			want: "tcp://user:p%3Fss@host:9000",
		},
		{
			name: "no scheme unchanged",
			in:   "localhost:9000",
			want: "localhost:9000",
		},
		{
			name: "empty string unchanged",
			in:   "",
			want: "",
		},
		{
			name: "username only no password",
			in:   "clickhouse://user@host:9000/db",
			want: "clickhouse://user@host:9000/db",
		},
		{
			name: "multiple special chars in password",
			in:   "clickhouse://user:a?b#c@@host:9000/db",
			want: "clickhouse://user:a%3Fb%23c%40@host:9000/db",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeDSN(tt.in)
			if got != tt.want {
				t.Errorf("sanitizeDSN(%q)\n got  %q\n want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestDLQTopicNameFormation(t *testing.T) {
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

func TestKafkaMetadataMappingIsEmpty(t *testing.T) {
	if !(*KafkaMetadataMapping)(nil).IsEmpty() {
		t.Fatal("nil mapping should be empty")
	}
	if !(&KafkaMetadataMapping{}).IsEmpty() {
		t.Fatal("zero-value mapping should be empty")
	}
	if (&KafkaMetadataMapping{Offset: "__offset"}).IsEmpty() {
		t.Fatal("mapping with offset set should not be empty")
	}
}

func TestTopicTableMappingResolveTrimsKafkaMetadata(t *testing.T) {
	cfg := &Config{
		BatchSize:      1,
		BatchDelayMs:   intPtr(1),
		MaxRetries:     intPtr(1),
		RetryBackoffMs: intPtr(1),
	}
	mapping := TopicTableMapping{
		Topic: "t",
		Table: "default.t",
		KafkaMetadata: &KafkaMetadataMapping{
			Offset:    "  __offset  ",
			Partition: "__partition",
			Topic:     "",
			Timestamp: "   ", // whitespace-only → treated as omitted after trim
			Key:       "__key",
			Headers:   "__headers",
		},
	}
	mapping.resolve(cfg)
	if mapping.KafkaMetadata.Offset != "__offset" {
		t.Errorf("Offset not trimmed: got %q", mapping.KafkaMetadata.Offset)
	}
	if mapping.KafkaMetadata.Timestamp != "" {
		t.Errorf("whitespace-only Timestamp should resolve to empty, got %q", mapping.KafkaMetadata.Timestamp)
	}
}

func TestValidateConfigKafkaMetadata(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // substring; empty means expect success
	}{
		{
			name:   "absent block is valid",
			mutate: func(c *Config) {},
		},
		{
			name: "full block is valid",
			mutate: func(c *Config) {
				c.TopicTables[0].KafkaMetadata = &KafkaMetadataMapping{
					Offset: "__offset", Partition: "__partition", Topic: "__topic",
					Timestamp: "__ts", Key: "__key", Headers: "__headers",
				}
			},
		},
		{
			name: "partial block is valid",
			mutate: func(c *Config) {
				c.TopicTables[0].KafkaMetadata = &KafkaMetadataMapping{Offset: "__offset", Topic: "__topic"}
			},
		},
		{
			name: "empty block is valid (no columns configured)",
			mutate: func(c *Config) {
				c.TopicTables[0].KafkaMetadata = &KafkaMetadataMapping{}
			},
		},
		{
			name: "duplicate column names rejected",
			mutate: func(c *Config) {
				c.TopicTables[0].KafkaMetadata = &KafkaMetadataMapping{Offset: "x", Partition: "x"}
			},
			wantErr: "duplicate column name",
		},
		{
			name: "whitespace-only value treated as unset, no error",
			mutate: func(c *Config) {
				c.TopicTables[0].KafkaMetadata = &KafkaMetadataMapping{Offset: "   "}
			},
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
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestLoadConfigParsesKafkaMetadata(t *testing.T) {
	yaml := `
kafka_brokers: "localhost:9092"
schema_registry: "http://localhost:8081"
clickhouse_dsn: "tcp://localhost:9000"
group_id: "kahouse"
input_format: "json"
dlq_topic_suffix: ".dlq"
topic_tables:
  - topic: "with_metadata"
    table: "default.with_metadata"
    format: "json"
    kafka_metadata:
      offset:    "__offset"
      partition: "__partition"
      topic:     "__topic"
      timestamp: "__timestamp"
      key:       "__key"
      headers:   "__headers"
  - topic: "without_metadata"
    table: "default.without_metadata"
    format: "json"
`
	dir := t.TempDir()
	path := dir + "/kahouse.yaml"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}
	if len(cfg.TopicTables) != 2 {
		t.Fatalf("expected 2 topic_tables, got %d", len(cfg.TopicTables))
	}
	m := cfg.TopicTables[0].KafkaMetadata
	if m == nil {
		t.Fatal("expected KafkaMetadata to be parsed on first topic")
	}
	if m.Offset != "__offset" || m.Partition != "__partition" || m.Topic != "__topic" ||
		m.Timestamp != "__timestamp" || m.Key != "__key" || m.Headers != "__headers" {
		t.Fatalf("unexpected metadata mapping: %+v", m)
	}
	if cfg.TopicTables[1].KafkaMetadata != nil {
		t.Fatalf("expected nil metadata on second topic, got %+v", cfg.TopicTables[1].KafkaMetadata)
	}
}
