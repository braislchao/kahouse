# kahouse - Testing Guide

## Quick Start

### 1. Run Integration Test
```bash
./scripts/test-integration.sh
```

This script will:
- Start Zookeeper, Kafka, Schema Registry, and ClickHouse
- Create test topics and register Avro schemas where needed
- Start kahouse with mixed per-topic formats
- Verify successful writes for Avro, JSON, and string topics in one run
- Exercise DLQ handling for invalid Avro, malformed JSON, and ClickHouse write failures

### 2. Manual Testing

#### Start the infrastructure:
```bash
docker-compose up -d zookeeper kafka schema-registry clickhouse
```

#### Create resources (topics, schemas, tables):
```bash
docker-compose up create-resources clickhouse-init
```

#### Start the sink:
```bash
docker-compose up -d kahouse
```

#### Check sink health:
```bash
curl http://localhost:9090/readyz
```

#### View metrics:
```bash
curl http://localhost:9090/metrics
```

## Test Components

### Services
| Service | Port | Purpose |
|---------|------|---------|
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092 | Message broker |
| Schema Registry | 8081 | Avro schema management |
| ClickHouse | 9000, 8123 | Data storage |
| kahouse | 9090 | Metrics and health |
| ClickHouse UI | 8080 | Web interface (optional) |

### Test Topics
| Topic | Partitions | Format | ClickHouse Table |
|-------|-----------|--------|-----------------|
| `test` | 3 | Avro | `default.test` |
| `orders` | 3 | JSON | `default.orders` |
| `payments` | 3 | String | `default.payments` |

### ClickHouse Tables
The automated test creates per-topic tables that match each format:

- `default.test`: Avro fields plus Kafka metadata
- `default.orders`: JSON fields plus Kafka metadata
- `default.payments`: raw string `value` plus Kafka metadata

## Testing Scenarios

### 1. Basic Flow Test (per topic)
```bash
# Send a single JSON message to the "orders" topic
echo '{"id": 1, "name": "order1", "value": 123.456, "timestamp": 1234567890}' | \
  docker-compose exec -T kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic orders

# Check ClickHouse
docker-compose exec clickhouse clickhouse-client --query "SELECT * FROM default.orders"
```

### 2. Multi-Topic Batch Processing Test
```bash
# Send test messages using the configured per-topic formats
docker-compose up test-producer

# Monitor per-topic processing
docker-compose logs -f kahouse
```

### 3. Topic Isolation Test
Verify that failure on one topic doesn't affect others:
```bash
# Send invalid payload to one topic only
docker-compose exec kafka bash -c 'echo "invalid-avro" | kafka-console-producer --bootstrap-server localhost:9092 --topic orders'

# Check that only orders.dlq has messages, other DLQs are empty
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.dlq \
  --from-beginning

# Verify other topics still process correctly
docker-compose exec clickhouse clickhouse-client --query "SELECT count() FROM default.payments"
```

### 4. Per-Topic DLQ Test
```bash
# Each topic has its own DLQ topic
for topic in test orders payments; do
  echo "=== ${topic}.dlq ==="
  docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "${topic}.dlq" \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null
done
```

### 5. Health Check Test
```bash
# Check liveness
curl http://localhost:9090/livez

# Check readiness (ClickHouse healthy and every configured consumer assigned)
curl http://localhost:9090/readyz
```

### 6. Per-Topic Metrics Test
```bash
# View per-topic metrics
curl -s http://localhost:9090/metrics | grep -E "kahouse_msg_consumed_total|kahouse_msg_produced_total" | grep -E 'topic="(test|orders|payments)"'

# Expected output:
# kahouse_msg_consumed_total{topic="orders"} 100
# kahouse_msg_consumed_total{topic="payments"} 100
# kahouse_msg_consumed_total{topic="test"} 100
# kahouse_msg_produced_total{topic="orders"} 100
# kahouse_msg_produced_total{topic="payments"} 100
# kahouse_msg_produced_total{topic="test"} 100
```

### 7. Performance Test
```bash
# Send 1000 JSON messages to the orders topic
for i in $(seq 1 1000); do
  timestamp=$(( $(date +%s) * 1000 ))
  echo "{\"id\": $i, \"name\": \"order_$i\", \"value\": $((RANDOM % 1000)).0, \"timestamp\": $timestamp}"
done | docker-compose exec -T kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders
```

## Monitoring

### Prometheus Metrics (per-topic labels)
All metrics are namespaced under `kahouse_` to avoid collisions.

- `kahouse_msg_consumed_total{topic="..."}`: Messages read from Kafka per topic
- `kahouse_msg_produced_total{topic="..."}`: Messages written to ClickHouse per topic
- `kahouse_msg_failed_total{topic="..."}`: Failed messages per topic (deserialization errors + batch write failures)
- `kahouse_msg_dlq_total{topic="..."}`: Messages sent to DLQ per topic
- `kahouse_batch_size{topic="..."}`: Batch size histogram per topic
- `kahouse_batch_delay_seconds{topic="..."}`: Age of oldest message in batch at flush time (message staleness)
- `kahouse_process_latency_seconds{topic="..."}`: ClickHouse write duration per topic (includes retry backoff)
- `kahouse_write_retry_count{topic="..."}`: Retry attempts per ClickHouse batch write

### Logs
```bash
# View all logs
docker-compose logs -f

# View only sink logs
docker-compose logs -f kahouse

# View Kafka logs
docker-compose logs -f kafka

# View ClickHouse logs
docker-compose logs -f clickhouse
```

## Configuration

### Config File (config.yaml)
```yaml
input_format: "json"   # default for topics that omit format

topic_tables:
  - topic: "orders"
    table: "default.orders"
    format: "json"
    batch_size: 5000          # per-topic override
  - topic: "payments"
    table: "default.payments"
    format: "string"
    string_value_column: "value"
  - topic: "users"
    table: "default.users"
    format: "avro"
    max_retries: 5            # per-topic override
```

### Environment Variables
```bash
KAHOUSE_TOPIC_TABLES='[{"topic":"orders","table":"default.orders"},{"topic":"payments","table":"default.payments"}]'
```

### Per-Topic Overrides
Each topic can override global defaults. Omit a field to inherit the global value.
Setting a field to `0` is valid — it is not treated as "not configured":

| Field | Effect of `0` |
|-------|--------------|
| `batch_size` | Not meaningful (would flush empty batches) — use at least 1 |
| `batch_delay_ms` | Flush as fast as possible, no accumulation delay |
| `max_retries` | No retries — failed batches go directly to DLQ |
| `retry_backoff_ms` | No backoff between retries |

Example:
```yaml
topic_tables:
  - topic: "orders"
    table: "default.orders"
    batch_size: 5000        # smaller batches
  - topic: "payments"
    table: "default.payments"
    max_retries: 0          # fail-fast to DLQ, no retries
    batch_delay_ms: 0       # flush every message immediately
```

## Troubleshooting

### Sink won't start
1. Check if all dependencies are healthy:
   ```bash
   docker-compose ps
   ```
2. Check sink logs:
   ```bash
   docker-compose logs kahouse
   ```

### No messages in ClickHouse for a specific topic
1. Check if messages are in the Kafka topic:
   ```bash
   docker-compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic orders \
     --from-beginning
   ```
2. Check per-topic sink metrics:
   ```bash
   curl http://localhost:9090/metrics | grep 'topic="orders"'
   ```

### Messages in DLQ for one topic (but others work)
This is expected behavior — topics are fully isolated. Check the specific topic's DLQ:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.dlq \
  --from-beginning
```

### One consumer not getting partitions
Check if the topic exists and has messages. Each consumer gets its own consumer group:
```bash
# List consumer groups
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Check specific group
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group kahouse-orders \
  --describe
```

## Cleanup

### Stop and remove everything:
```bash
docker-compose down -v
```

### Remove only containers (keep volumes):
```bash
docker-compose down
```

### View persistent data:
```bash
docker volume ls
docker volume ls | grep clickhouse
```

## Advanced Testing

### Add a new topic at runtime
1. Add the topic-table mapping to config
2. Restart the sink (new consumer starts automatically)

### Test schema evolution:
1. Register new schema version:
   ```bash
   curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data '{"schema": "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"double\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"extra\",\"type\":\"string\",\"default\":\"default\"}]}"}' \
     http://localhost:8081/subjects/test-value/versions
   ```

2. Send messages with new schema

## Expected Results

After running the integration test:
- **ClickHouse default.test**: 100 rows
- **ClickHouse default.orders**: 100 rows
- **ClickHouse default.payments**: 100 rows
- **DLQ topics**: All empty (no errors)
- **Metrics**: Per-topic `kahouse_msg_consumed_total`=100, `kahouse_msg_produced_total`=100, `kahouse_msg_failed_total`=0, `kahouse_msg_dlq_total`=0
- **Health**: Both `/livez` and `/readyz` return `OK`
