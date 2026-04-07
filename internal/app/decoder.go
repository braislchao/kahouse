package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type MessageDecoder interface {
	Decode(topic string, payload []byte) (map[string]interface{}, error)
}

func newMessageDecoder(format string, stringValueColumn string, srClient schemaregistry.Client) (MessageDecoder, error) {
	switch normalizeInputFormat(format) {
	case "avro":
		if srClient == nil {
			return nil, fmt.Errorf("schema registry client is required for avro input")
		}
		return NewAvroDecoder(srClient), nil
	case "json":
		return JSONDecoder{}, nil
	case "string":
		return StringDecoder{column: stringValueColumn}, nil
	default:
		return nil, fmt.Errorf("unsupported input format %q", format)
	}
}

type JSONDecoder struct{}

func (JSONDecoder) Decode(_ string, payload []byte) (map[string]interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()

	var decoded interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("failed to decode JSON payload: %w", err)
	}
	if err := ensureNoTrailingJSON(decoder); err != nil {
		return nil, err
	}

	normalized, err := normalizeJSONValue(decoded)
	if err != nil {
		return nil, err
	}
	record, ok := normalized.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("decoded JSON payload was %T, want object", decoded)
	}
	return record, nil
}

func ensureNoTrailingJSON(decoder *json.Decoder) error {
	var extra interface{}
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to decode trailing JSON data: %w", err)
	}
	return fmt.Errorf("JSON payload contains trailing data")
}

func normalizeJSONValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		normalized := make(map[string]interface{}, len(v))
		for key, item := range v {
			converted, err := normalizeJSONValue(item)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
			normalized[key] = converted
		}
		return normalized, nil
	case []interface{}:
		normalized := make([]interface{}, len(v))
		for i, item := range v {
			converted, err := normalizeJSONValue(item)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			normalized[i] = converted
		}
		return normalized, nil
	case json.Number:
		return normalizeJSONNumber(v)
	default:
		return value, nil
	}
}

func normalizeJSONNumber(value json.Number) (interface{}, error) {
	raw := value.String()
	if strings.ContainsAny(raw, ".eE") {
		parsed, err := value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid JSON number %q: %w", raw, err)
		}
		return parsed, nil
	}

	parsed, err := value.Int64()
	if err == nil {
		return parsed, nil
	}

	floatParsed, floatErr := value.Float64()
	if floatErr != nil {
		return nil, fmt.Errorf("invalid JSON number %q: %w", raw, err)
	}
	return floatParsed, nil
}

type StringDecoder struct {
	column string
}

func (d StringDecoder) Decode(_ string, payload []byte) (map[string]interface{}, error) {
	if strings.TrimSpace(d.column) == "" {
		return nil, fmt.Errorf("string decoder requires a destination column")
	}
	return map[string]interface{}{d.column: string(payload)}, nil
}
