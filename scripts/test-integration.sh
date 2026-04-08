#!/bin/bash

set -euo pipefail

message_count=${TEST_MESSAGE_COUNT:-20}

echo "=== kahouse Integration Test ==="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    if [ "$1" -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
    fi
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

echo "Checking prerequisites..."
docker --version > /dev/null 2>&1
print_status $? "Docker installed"
docker-compose --version > /dev/null 2>&1
print_status $? "Docker Compose installed"

docker info > /dev/null 2>&1
print_status $? "Docker daemon accessible"

compose() {
    docker-compose "$@"
}

docker_cmd() {
    docker "$@"
}

check_service() {
    local service=$1
    local container=$2
    local max_attempts=30
    local attempt=1
    local container_id
    local status

    while [ $attempt -le $max_attempts ]; do
        container_id=$(compose ps -q "$container" 2>/dev/null | tr -d '\r')
        status=""
        if [ -n "$container_id" ]; then
            status=$(docker_cmd inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)
        fi
        if [ "$status" = "healthy" ] || [ "$status" = "running" ]; then
            print_status 0 "$service is ready"
            return 0
        fi
        echo "Waiting for $service (attempt $attempt/$max_attempts, status: ${status:-unknown})..."
        sleep 2
        attempt=$((attempt + 1))
    done

    print_status 1 "$service failed to become ready"
    return 1
}

cleanup() {
    compose down -v >/dev/null 2>&1 || true
}

register_avro_schemas() {
    compose exec -T schema-registry curl -sS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{"schema": "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"double\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}"}' http://localhost:8081/subjects/test-value/versions > /dev/null
}

wait_for_sink() {
    print_info "Waiting for sink to be ready..."
    sleep 10
    check_service "Sink" kahouse || {
        echo "Checking sink logs..."
        compose logs kahouse | tail -50
        exit 1
    }
}

start_base_services() {
    print_info "Starting Kafka, Schema Registry, and ClickHouse..."
    compose up -d zookeeper kafka schema-registry clickhouse

    print_info "Waiting for services to be ready..."
    sleep 15

    echo ""
    echo "Checking service health..."
    check_service "Zookeeper" zookeeper
    check_service "Kafka" kafka
    check_service "Schema Registry" schema-registry
    check_service "ClickHouse" clickhouse
}

create_topics() {
    print_info "Creating Kafka topics..."
    compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic test --partitions 3 --replication-factor 1 --if-not-exists
    compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic orders --partitions 3 --replication-factor 1 --if-not-exists
    compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic payments --partitions 3 --replication-factor 1 --if-not-exists
    compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic logs --partitions 3 --replication-factor 1 --if-not-exists
    register_avro_schemas
    print_status $? "Kafka resources created"
}

dlq_count_for_topic() {
    local topic=$1
    compose exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$topic" --from-beginning --timeout-ms 5000 2>/dev/null | wc -l
}

assert_dlq_count() {
    local topic=$1
    local expected=$2
    local actual

    actual=$(dlq_count_for_topic "$topic")
    if [ "$actual" -eq "$expected" ]; then
        print_status 0 "$topic: $actual messages (expected $expected)"
    else
        print_status 1 "$topic: $actual messages (expected $expected)"
        exit 1
    fi
}

assert_table_count() {
    local table=$1
    local expected=$2
    local actual

    actual=$(compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM default.$table")
    if [ "$actual" -eq "$expected" ]; then
        print_status 0 "default.$table: $actual rows (expected $expected)"
    else
        print_status 1 "default.$table: $actual rows (expected $expected)"
        exit 1
    fi
}

show_table_sample() {
    local table=$1
    local query=$2
    echo "Sample data from default.$table:"
    compose exec -T clickhouse clickhouse-client --query "$query"
    echo ""
}

create_all_tables() {
    print_info "Creating ClickHouse tables..."
    compose exec -T clickhouse clickhouse-client --query "CREATE TABLE IF NOT EXISTS default.test (id Int32, name String, value Float64, timestamp Int64) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192"
    compose exec -T clickhouse clickhouse-client --query "CREATE TABLE IF NOT EXISTS default.orders (id Int64, name String, value Float64, timestamp Int64) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192"
    compose exec -T clickhouse clickhouse-client --query "CREATE TABLE IF NOT EXISTS default.payments (value String) ENGINE = MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192"
    print_status $? "ClickHouse tables created"
}

run_mixed_format_success_test() {
    print_info "Running mixed-format success test..."
    cleanup

    start_base_services
    create_topics
    create_all_tables

    print_info "Starting sink for mixed-format test..."
    compose up -d kahouse
    wait_for_sink

    print_info "Sending mixed-format test messages..."
    export TEST_PRODUCER_IMAGE="confluentinc/cp-schema-registry:7.6.0"
    export TEST_INPUT_FORMAT="avro"
    export TEST_TOPIC_FILTER="test"
    export TEST_MESSAGE_COUNT="$message_count"
    compose run --rm test-producer

    export TEST_PRODUCER_IMAGE="confluentinc/cp-kafka:7.6.0"
    export TEST_INPUT_FORMAT="json"
    export TEST_TOPIC_FILTER="orders"
    compose run --rm test-producer

    export TEST_INPUT_FORMAT="string"
    export TEST_TOPIC_FILTER="payments"
    compose run --rm test-producer
    print_status $? "Mixed-format messages sent"

    print_info "Waiting for mixed-format messages to be processed..."
    sleep 15

    echo ""
    echo "Verifying mixed-format results in ClickHouse..."
    assert_table_count test "$message_count"
    assert_table_count orders "$message_count"
    assert_table_count payments "$message_count"

    show_table_sample test "SELECT * FROM default.test ORDER BY id LIMIT 5 FORMAT PrettyCompact"
    show_table_sample orders "SELECT * FROM default.orders ORDER BY id LIMIT 5 FORMAT PrettyCompact"
    show_table_sample payments "SELECT value FROM default.payments LIMIT 5 FORMAT PrettyCompact"

    echo "Checking DLQs for mixed-format success test:"
    assert_dlq_count test.dlq 0
    assert_dlq_count orders.dlq 0
    assert_dlq_count payments.dlq 0

    echo "Checking sink metrics:"
    compose exec -T kahouse wget -qO- http://localhost:9090/metrics | grep -E "msg_consumed_total|msg_produced_total|msg_failed_total|msg_dlq_total" | head -20
    print_status 0 "Mixed-format success test passed"
}

run_avro_dlq_test() {
    print_info "Running Avro DLQ test..."
    cleanup

    start_base_services
    create_topics
    create_all_tables

    compose up -d kahouse
    wait_for_sink

    print_info "Enabling DLQ repair mode on topic 'test'..."
    compose exec -T kahouse wget -qO- --post-data='{"mode":"dlq"}' --header='Content-Type: application/json' http://localhost:9090/api/topics/test/repair
    echo ""

    print_info "Sending invalid non-Avro payload to Avro topic..."
    compose exec -T kafka bash -lc "printf 'not-avro\n' | kafka-console-producer --bootstrap-server localhost:9092 --topic test"

    print_info "Waiting for Avro DLQ handling..."
    sleep 10

    assert_table_count test 0
    assert_dlq_count test.dlq 1
    print_status 0 "Avro DLQ test passed"
}

run_json_dlq_test() {
    print_info "Running JSON DLQ test..."
    cleanup

    start_base_services
    create_topics
    create_all_tables

    compose up -d kahouse
    wait_for_sink

    print_info "Enabling DLQ repair mode on topic 'orders'..."
    compose exec -T kahouse wget -qO- --post-data='{"mode":"dlq"}' --header='Content-Type: application/json' http://localhost:9090/api/topics/orders/repair
    echo ""

    print_info "Sending malformed JSON payload..."
    compose exec -T kafka bash -lc "printf '{\"id\":1\n' | kafka-console-producer --bootstrap-server localhost:9092 --topic orders"

    print_info "Waiting for JSON DLQ handling..."
    sleep 10

    assert_table_count orders 0
    assert_dlq_count orders.dlq 1
    print_status 0 "JSON DLQ test passed"
}

trap cleanup EXIT

print_info "Cleaning up existing containers..."
cleanup

print_info "Building the application..."
compose build kahouse

run_mixed_format_success_test
run_avro_dlq_test
run_json_dlq_test

echo ""
echo "=== Integration Test Complete ==="
echo ""
echo "Summary:"
echo "  - Success coverage: mixed avro/json/string in one sink run"
echo "  - DLQ coverage: invalid avro, malformed json (repair mode: dlq)"
echo "  - Kafka Topics: test, orders, payments"
echo "  - ClickHouse Tables: default.test, default.orders, default.payments"
echo "  - Sink Metrics: docker-compose exec kahouse wget -qO- http://localhost:9090/metrics"
echo "  - Sink Health: docker-compose exec kahouse wget -qO- http://localhost:9090/readyz"
echo ""
print_status 0 "Integration test completed successfully!"
