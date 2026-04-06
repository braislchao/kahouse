# kahouse

A lightweight Go service that consumes Kafka messages and writes them in batches to ClickHouse. Each topic gets its own independent consumer and batch pipeline.

## How it works

- Subscribes to one or more Kafka topics (one consumer per topic)
- Decodes messages as `avro`, `json`, or `string`
- Buffers records and flushes to ClickHouse in batches (by size or time)
- On write failure: retries with exponential backoff, then sends to a Dead Letter Queue (DLQ)
- Exposes Prometheus metrics and health endpoints (default `:9090`, configurable)

## Configuration

Copy `config.yaml.example` to `kahouse.yaml` or `config.yaml` and edit:

```yaml
kafka_brokers: "localhost:9092"
schema_registry: "http://localhost:8081"
clickhouse_dsn: "tcp://localhost:9000"
group_id: "kahouse"
input_format: "avro"       # avro | json | string
string_value_column: "value" # used only for string input

batch_size: 10000       # max records per batch
batch_delay_ms: 200     # max time to wait before flushing
max_retries: 3
retry_backoff_ms: 100

topic_tables:
  - topic: "orders"
    table: "default.orders"
    format: "json"
  - topic: "payments"
    table: "default.payments"
    format: "string"
    string_value_column: "value"
    max_retries: 0        # fail fast to DLQ, no retries
```

Each topic can override `format`, `string_value_column`, `batch_size`, `batch_delay_ms`, `max_retries`, and `retry_backoff_ms`. Omit a field to inherit the global default.

Format behavior:

- `avro`: expects Confluent-wire-format Avro payloads and uses Schema Registry
- `json`: expects each message value to be a single JSON object; integers decode as `Int64`, decimals as `Float64`
- `string`: stores the raw message value in the configured `string_value_column`

`input_format` is the global default. Set `topic_tables[].format` when different topics need different payload formats in the same sink deployment.

**Environment variables** are also supported with the `KAHOUSE_` prefix (e.g. `KAHOUSE_KAFKA_BROKERS`).

Config is loaded from the first file found in this order:

- `./kahouse.yaml`
- `./config.yaml`
- `$HOME/kahouse.yaml`
- `$HOME/config.yaml`
- `/etc/kahouse/kahouse.yaml`
- `/etc/kahouse/config.yaml`

## Running

```bash
# Build
go build -o kahouse ./cmd/kahouse

# Run
./kahouse
```

Or with Docker Compose (see Testing below).

## Health & Metrics

| Endpoint | Description |
|----------|-------------|
| `GET :9090/livez` | Liveness check (port configurable via `metrics_port`) |
| `GET :9090/readyz` | Readiness check (ClickHouse healthy and every configured consumer assigned) |
| `GET :9090/metrics` | Prometheus metrics |

Key metrics (all labeled by `topic`):

| Metric | Description |
|--------|-------------|
| `kahouse_msg_consumed_total` | Messages read from Kafka |
| `kahouse_msg_produced_total` | Messages written to ClickHouse |
| `kahouse_msg_failed_total` | Deserialization errors + write failures |
| `kahouse_msg_dlq_total` | Messages sent to DLQ |
| `kahouse_batch_size` | Batch size histogram |
| `kahouse_process_latency_seconds` | ClickHouse write duration |

## Testing

### Integration test (automated)

Starts all dependencies, verifies a mixed-format sink deployment end to end, and also exercises DLQ paths for invalid Avro, malformed JSON, and ClickHouse write failures:

```bash
./scripts/test-integration.sh
```

### Manual with Docker Compose

```bash
# Start infrastructure
docker-compose up -d zookeeper kafka schema-registry clickhouse

# Create topics, schemas, and ClickHouse tables
docker-compose up create-resources clickhouse-init

# Start the sink
docker-compose up -d kahouse

# Check health
curl http://localhost:9090/readyz

# View metrics
curl http://localhost:9090/metrics
```

### Unit tests

```bash
go test ./...
```

### Cleanup

The automated integration script cleans up the Docker stack on exit.

Manual cleanup:

```bash
docker-compose down -v
```

## Dead Letter Queue

Failed messages are forwarded to `<topic>.dlq` (configurable via `dlq_topic_suffix`). Each DLQ record contains the original key/value, the error message, and a timestamp.

To inspect:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.dlq \
  --from-beginning
```
