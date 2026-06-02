# Configuration Reference

kahouse is configured with a single YAML file. Pass it with `-config <path>`, the `KAHOUSE_CONFIG` environment variable, or place it at `kahouse.yaml` in the working directory (the default).

Resolution order: `-config` flag > `KAHOUSE_CONFIG` env var > `kahouse.yaml`.

## Global fields

### Required

These fields have defaults for local development, but you will always set them explicitly in production.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `kafka_brokers` | string | `localhost:9092` | Kafka bootstrap servers (comma-separated). |
| `clickhouse_dsn` | string | `tcp://localhost:9000` | ClickHouse connection string. Authentication and database are part of the DSN (e.g. `tcp://user:password@host:9000/mydb`). |
| `group_id` | string | `kahouse` | Base consumer group ID. Each topic gets its own group: `kahouse-<group_id>-<topic>`. |
| `topic_tables` | list | *(none)* | At least one topic-to-table mapping is required. See [topic_tables](#topic_tables). |

### Format

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `input_format` | string | `avro` | Default input format for all topics. One of `avro`, `json`, or `string`. |
| `schema_registry` | string | `http://localhost:8081` | Confluent Schema Registry URL. Required when any topic uses `avro`. |
| `string_value_column` | string | `value` | ClickHouse column name for `string` format messages. Required when any topic uses `string`. |

### Consumer

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_offset_reset` | string | `earliest` | Where to start consuming when no committed offset exists. One of `earliest`, `latest`, or `none`. |
| `kafka_session_timeout_ms` | int | `45000` | Kafka consumer session timeout in milliseconds. Must be > 0. |
| `kafka_max_poll_interval_ms` | int | `300000` | Maximum time between poll calls before the consumer is considered failed, in milliseconds. Must be > 0. |

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

### Auto-restart

Supervises sink tasks: a task that stops with a **transient** (recoverable) error is
restarted in place with exponential backoff, instead of staying stopped until an
operator intervenes. **Fatal** crashes (poison messages in strict mode, non-retriable
ClickHouse errors) are never auto-restarted. Configured under the `auto_restart` key.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_restart.enabled` | bool | `true` | Master switch. `false` restores the passive behavior (crashed tasks stay stopped until an admin-API restart). |
| `auto_restart.initial_backoff_ms` | int | `5000` | Delay before the first restart; doubles each consecutive failure. Must be >= 0. |
| `auto_restart.max_backoff_ms` | int | `300000` | Cap on the restart backoff. Must be >= `initial_backoff_ms`. |
| `auto_restart.reset_after_s` | int | `120` | Healthy uptime after which a restarted task's consecutive-failure counter resets to zero. Must be >= 0. |
| `auto_restart.max_stuck_s` | int | `900` | If a task keeps crash-looping this long, the supervisor gives up and fails `/livez` so Kubernetes recycles the pod. A negative value disables escalation. |

A restart re-reads from the last committed Kafka offset; because offsets are committed
only after a successful write, no data is lost (at-least-once, same as a manual restart).

### ClickHouse connection

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `clickhouse_max_open_conns` | int | `5` | Maximum number of open connections to ClickHouse. Must be >= 1. |
| `clickhouse_max_idle_conns` | int | `5` | Maximum number of idle connections in the pool. Must be >= 1. |
| `clickhouse_conn_max_lifetime_s` | int | `300` | Recycle pooled connections after this many seconds. ClickHouse Cloud closes idle connections server-side; the driver default (1h) is too long and causes `connection reset by peer` on stale connections. Keep below the server idle timeout. Must be >= 0 (0 = driver default). |
| `clickhouse_async_insert` | bool | `false` | Enable ClickHouse async inserts (`async_insert=1`). |
| `clickhouse_wait_for_async_insert` | bool | `false` | Wait for async insert to complete before returning (`wait_for_async_insert=1`). Only relevant when `clickhouse_async_insert` is `true`. |

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
| `kafka_metadata` | object | *(none — opt-in, see below)* |

### kafka_metadata

Optional per-topic block that injects Kafka message metadata as extra columns
on each row before it is written to ClickHouse. Gives parity with the Kafka
Connect `InsertField$Value` SMT.

All subfields are optional: an omitted (or whitespace-only) field is not
injected. Column names are free-form and must exist in the target ClickHouse
table.

| Subfield | Kafka source | Go type | Recommended ClickHouse column type |
|----------|--------------|---------|-------------------------------------|
| `offset` | `msg.TopicPartition.Offset` | `int64` | `UInt64` or `Int64` |
| `partition` | `msg.TopicPartition.Partition` | `int32` | `UInt32`, `Int32`, or `UInt64` |
| `topic` | `*msg.TopicPartition.Topic` | `string` | `LowCardinality(String)` or `String` |
| `timestamp` | `msg.Timestamp` | `time.Time` | `DateTime64(3)` or `Nullable(DateTime64(3))` |
| `key` | `msg.Key` (raw bytes) | `string` | `String` or `FixedString(N)` |
| `headers` | `msg.Headers` | `map[string]string` | `Map(String, String)` |

Collision: if the decoded record already contains a key matching a configured
metadata column, the metadata value wins and a warning is logged once per
column per task.

Example:

```yaml
topic_tables:
  - topic: "orders"
    table: "default.orders_enriched"
    format: "json"
    kafka_metadata:
      offset:    "__offset"
      partition: "__partition"
      topic:     "__topic"
      timestamp: "__timestamp"
      key:       "__key"
      headers:   "__headers"
```

#### Per-topic starting position (`start_at`)

`start_at` configures where a topic starts consuming **only when its consumer group has no committed offset for a partition**. Once a partition has any committed offset, kahouse never re-seeks it -- so `start_at` is safe to leave configured across restarts and is fully idempotent. It acts as a more expressive, per-topic version of `auto_offset_reset`.

Exactly one variant must be set per topic.

```yaml
topic_tables:
  # A. Named position -- per-topic override of auto_offset_reset
  - topic: events.foo
    table: events_foo
    start_at:
      position: earliest    # earliest | latest

  # B. Timestamp -- resolved per partition via Kafka's OffsetsForTimes
  - topic: events.bar
    table: events_bar
    start_at:
      timestamp: "2026-04-01T00:00:00Z"   # RFC3339
      # or:
      # unix_ms: 1717200000000

  # C. Explicit per-partition offsets
  - topic: events.baz
    table: events_baz
    start_at:
      offsets:
        0: 12345
        1: 9000
        2: 0
```

Behavior notes:

- Partitions with an existing committed offset always win; `start_at` is ignored for them.
- For `offsets`, partitions not listed in the map fall back to the global `auto_offset_reset` (logged as `fallback_auto_reset`).
- For `timestamp`/`unix_ms`, partitions with no message at-or-after the timestamp start at the partition's end.
- Each per-partition decision emits a structured log line and the metric `kahouse_start_at_applied_total{topic,decision}` where `decision` is one of `committed`, `start_at_offset`, `start_at_timestamp`, `start_at_position`, or `fallback_auto_reset`.

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
auto_offset_reset: "earliest"
dlq_topic_suffix: ".dlq"

batch_size: 10000
batch_delay_ms: 200
max_retries: 5
retry_backoff_ms: 100

metrics_port: 9090

clickhouse_max_open_conns: 5
clickhouse_max_idle_conns: 5
clickhouse_async_insert: false
clickhouse_wait_for_async_insert: false

kafka_session_timeout_ms: 45000
kafka_max_poll_interval_ms: 300000

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
- `auto_offset_reset` must be one of `earliest`, `latest`, or `none`.
- `clickhouse_max_open_conns` and `clickhouse_max_idle_conns` must be >= 1; `clickhouse_conn_max_lifetime_s` must be >= 0.
- `kafka_session_timeout_ms` and `kafka_max_poll_interval_ms` must be > 0.
- `batch_size` must be >= 1 (both global and per-topic).
- `batch_delay_ms`, `max_retries`, `retry_backoff_ms` must be >= 0.
- `auto_restart.initial_backoff_ms` and `auto_restart.reset_after_s` must be >= 0; `auto_restart.max_backoff_ms` must be >= `initial_backoff_ms`.
- `topic_tables` must have at least one entry.
- Each topic must have a non-empty `topic` and `table`.
- Topic names must be unique -- duplicates are rejected.
- `start_at` (when set) must specify exactly one of `position`, `timestamp`/`unix_ms`, or `offsets`.
- `start_at.position` must be `earliest` or `latest`.
- `start_at.timestamp` must be RFC3339; `start_at.unix_ms` must be >= 0; the two are mutually exclusive.
- `start_at.offsets` keys (partitions) and values (offsets) must be >= 0.
- Within a single topic's `kafka_metadata` block, column names must be unique across the six subfields.
- `flatten` (when enabled) requires `format: json`; `delimiter` must not be empty; `max_depth` must be >= 0.

#### Per-topic JSON flattening (`flatten`)

`flatten` configures optional JSON object flattening before insertion, providing parity with Kafka Connect's `org.apache.kafka.connect.transforms.Flatten$Value` SMT. When enabled, nested JSON objects are recursively collapsed into a single-level map by joining keys with a configurable delimiter.

This is useful for topics produced by systems that emit nested JSON where ClickHouse expects flat columns.

```yaml
topic_tables:
  - topic: nested.events
    table: default.events
    format: json
    flatten:
      enabled: true
      delimiter: "_"         # join nested keys with underscore (default)
      max_depth: 0           # 0 = unlimited recursion (default)
      preserve_arrays: true  # arrays are not descended into (default)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable flattening. When `false` (default), no transformation is applied. |
| `delimiter` | string | `_` | Character(s) used to join nested keys. |
| `max_depth` | int | `0` | Maximum recursion depth. `0` = unlimited. `1` = only flatten the first level of nesting. |
| `preserve_arrays` | bool | `true` | When `true`, arrays are preserved as-is (not descended into), matching Kafka Connect behavior. |

**Behavior:**
- Nested objects are recursively walked; keys are joined with the delimiter (e.g. `{"a":{"b":1}}` → `{"a_b":1}`).
- Arrays are preserved as-is when `preserve_arrays` is `true`.
- Null values emit the flattened key with `null` (not dropped).
- Flat input passes through unchanged.
- Only supported with `format: json`. Validation rejects `flatten.enabled: true` with other formats.
- Flattening runs **before** metadata injection (`kafka_metadata:`), so metadata columns never collide with payload keys.
