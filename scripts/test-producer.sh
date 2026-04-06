#!/bin/bash

set -euo pipefail

echo 'Waiting for sink to be ready...'
sleep 10

message_count=${TEST_MESSAGE_COUNT:-20}
input_format=${TEST_INPUT_FORMAT:-avro}
topic_filter=${TEST_TOPIC_FILTER:-}

topics=(test orders payments)
if [ -n "$topic_filter" ]; then
  read -r -a topics <<< "$topic_filter"
fi

schema_name_for_topic() {
  case "$1" in
    test)
      printf 'TestRecord'
      ;;
    orders)
      printf 'OrderRecord'
      ;;
    payments)
      printf 'PaymentRecord'
      ;;
    *)
      echo "Unknown topic: $1" >&2
      exit 1
      ;;
  esac
}

generate_structured_payloads() {
  local topic=$1
  local i

  for ((i = 1; i <= message_count; i++)); do
    timestamp=$(( $(date +%s) * 1000 ))
    value=$(printf '%d.%03d' $(( (i % 1000) + 1 )) $(( (i * 37) % 1000 )))
    printf '{"id": %d, "name": "%s_%d", "value": %s, "timestamp": %d}\n' "$i" "$topic" "$i" "$value" "$timestamp"
  done
}

generate_string_payloads() {
  local topic=$1
  local i

  for ((i = 1; i <= message_count; i++)); do
    printf '%s_plain_text_%d\n' "$topic" "$i"
  done
}

echo "Sending $message_count $input_format test messages to each topic..."
for topic in "${topics[@]}"; do
  case "$input_format" in
    avro)
      schema_name=$(schema_name_for_topic "$topic")
      generate_structured_payloads "$topic" | kafka-avro-console-producer \
        --bootstrap-server kafka:29092 \
        --topic "$topic" \
        --property schema.registry.url=http://schema-registry:8081 \
        --property value.schema="{\"type\":\"record\",\"name\":\"$schema_name\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"double\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}"
      ;;
    json)
      generate_structured_payloads "$topic" | kafka-console-producer \
        --bootstrap-server kafka:29092 \
        --topic "$topic"
      ;;
    string)
      generate_string_payloads "$topic" | kafka-console-producer \
        --bootstrap-server kafka:29092 \
        --topic "$topic"
      ;;
    *)
      echo "Unsupported TEST_INPUT_FORMAT: $input_format" >&2
      exit 1
      ;;
  esac

  echo "Sent $message_count messages to $topic"
done

echo 'All messages sent!'
echo 'Waiting for messages to be processed...'
sleep 10
