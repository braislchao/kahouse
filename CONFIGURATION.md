# Configuration Reference

kahouse is configured with a single YAML file. Pass it with `-config <path>`, the `KAHOUSE_CONFIG` environment variable, or place it at `kahouse.yaml` in the working directory (the default).

Resolution order: `-config` flag > `KAHOUSE_CONFIG` env var > `kahouse.yaml`.

## Global fields

### Required

These fields have defaults for local development, but you will always set them explicitly in production.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `kafka_brokers` | string | `localhost:9092` | Kafka bootstrap servers (comma-separated). |
| `clickhouse_dsn` | string | `tcp://localhost:9000` | ClickHouse connection string. |
| `group_id` | string | `kahouse` | Base consumer group ID. Each topic gets its own group: `kahouse-<group_id>-<topic>`. |
| `topic_tables` | list | *(none)* | At least one topic-to-table mapping is required. See [topic_tables](#topic_tables). |

### Format

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `input_format` | string | `avro` | Default input format for all topics. One of `avro`, `json`, or `string`. |
| `schema_registry` | string | `http://localhost:8081` | Confluent Schema Registry URL. Required when any topic uses `avro`. |
| `string_value_column` | string | `value` | ClickHouse column name for `string` format messages. Required when any topic uses `string`. |

### Batching and retries

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `10000` | Max records per batch before flushing to ClickHouse. Must be >= 1. |
| `batch_delay_ms` | int | `200` | Max milliseconds to wait before flushing a partial batch. Must be >= 0. |
| `max_retries` | int | `5` | Retry attempts for failed ClickHouse writes. After exhausting retries, the task stops. Must be >= 0. |
| `retry_backoff_ms` | int | `100` | Initial backoff between retries in milliseconds. Doubles on each retry (exponential backoff). Must be >= 0. |

### DLQ

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dlq_topic_suffix` | string | `.dlq` | Suffix appended to the topic name to form the DLQ topic (e.g. `orders` -> `orders.dlq`). |

### Server

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `metrics_port` | int | `9090` | HTTP port for health endpoints, admin API, and Prometheus metrics. |

### Kafka authentication

All auth fields are optional. Omit them entirely for unauthenticated clusters.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `kafka_security_protocol` | string | *(empty)* | Security protocol. Typical values: `SASL_SSL`, `SASL_PLAINTEXT`, `SSL`. |
| `kafka_sasl_mechanism` | string | *(empty)* | SASL mechanism. Typical values: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `kafka_sasl_username` | string | *(empty)* | SASL username or API key. |
| `kafka_sasl_password` | string | *(empty)* | SASL password or API secret. |
| `kafka_ssl_ca_location` | string | *(empty)* | Path to a custom CA certificate file. Only needed for private CAs. |

### Schema Registry authentication

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema_registry_username` | string | *(empty)* | Schema Registry username or API key. |
| `schema_registry_password` | string | *(empty)* | Schema Registry password or API secret. |

## topic_tables

Each entry maps a Kafka topic to a ClickHouse table. At least one mapping is required.

```yaml
topic_tables:
  - topic: "orders"
    table: "default.orders"
```

### Required fields

| Field | Type | Description |
|-------|------|-------------|
| `topic` | string | Kafka topic name. Must be unique across all mappings. |
| `table` | string | ClickHouse destination table. Use `database.table` format. |

### Per-topic overrides

Every field below is optional. When omitted, the value is inherited from the corresponding global field. When set -- even to `0` -- the per-topic value takes effect.

| Field | Type | Inherits from |
|-------|------|---------------|
| `format` | string | `input_format` |
| `string_value_column` | string | `string_value_column` |
| `batch_size` | int | `batch_size` |
| `batch_delay_ms` | int | `batch_delay_ms` |
| `max_retries` | int | `max_retries` |
| `retry_backoff_ms` | int | `retry_backoff_ms` |

Example with overrides:

```yaml
topic_tables:
  - topic: "orders"
    table: "default.orders"
    format: "json"
    batch_size: 5000

  - topic: "logs"
    table: "default.logs"
    format: "string"
    string_value_column: "raw"
    max_retries: 0          # stop on first write failure
    batch_delay_ms: 0       # flush immediately
```

## Full example

```yaml
kafka_brokers: "kafka-1:9092,kafka-2:9092"
schema_registry: "http://schema-registry:8081"
clickhouse_dsn: "tcp://clickhouse:9000"
group_id: "prod"
input_format: "avro"
dlq_topic_suffix: ".dlq"

batch_size: 10000
batch_delay_ms: 200
max_retries: 5
retry_backoff_ms: 100

metrics_port: 9090

kafka_security_protocol: "SASL_SSL"
kafka_sasl_mechanism: "PLAIN"
kafka_sasl_username: "your-api-key"
kafka_sasl_password: "your-api-secret"

schema_registry_username: "your-sr-api-key"
schema_registry_password: "your-sr-api-secret"

topic_tables:
  - topic: "orders"
    table: "analytics.orders"
    format: "json"
    batch_size: 5000

  - topic: "events"
    table: "analytics.events"
    # inherits avro format and all global defaults

  - topic: "logs"
    table: "analytics.logs"
    format: "string"
    string_value_column: "message"
    max_retries: 0
```

## Validation rules

kahouse validates the config at startup and exits with an error if any rule is violated.

- `kafka_brokers`, `clickhouse_dsn`, `group_id`, `dlq_topic_suffix` must not be empty.
- `schema_registry` is required when any topic uses `avro` format.
- `string_value_column` is required (globally or per-topic) when the resolved format is `string`.
- `input_format` must be one of `avro`, `json`, or `string`.
- `batch_size` must be >= 1 (both global and per-topic).
- `batch_delay_ms`, `max_retries`, `retry_backoff_ms` must be >= 0.
- `topic_tables` must have at least one entry.
- Each topic must have a non-empty `topic` and `table`.
- Topic names must be unique -- duplicates are rejected.
