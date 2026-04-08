package app

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/linkedin/goavro/v2"
)

const confluentMagicByte byte = 0x0

// maxCodecCacheSize is the upper bound on cached Avro codecs. When exceeded, the entire cache is cleared
const maxCodecCacheSize = 512

var avroPrimitiveUnionBranches = map[string]struct{}{
	"null":    {},
	"boolean": {},
	"int":     {},
	"long":    {},
	"float":   {},
	"double":  {},
	"bytes":   {},
	"string":  {},
}

type AvroDecoder struct {
	client schemaregistry.Client

	mu     sync.RWMutex
	codecs map[int]*goavro.Codec
}

func NewAvroDecoder(client schemaregistry.Client) *AvroDecoder {
	return &AvroDecoder{
		client: client,
		codecs: make(map[int]*goavro.Codec),
	}
}

func (d *AvroDecoder) Decode(topic string, payload []byte) (map[string]interface{}, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("payload too short: %d bytes", len(payload))
	}
	if payload[0] != confluentMagicByte {
		return nil, fmt.Errorf("unknown magic byte: %d", payload[0])
	}

	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	codec, err := d.codecFor(topic, schemaID)
	if err != nil {
		return nil, err
	}

	record, _, err := codec.NativeFromBinary(payload[5:])
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro payload with schema id %d: %w", schemaID, err)
	}

	m, ok := record.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("decoded Avro payload was %T, want map[string]interface{}", record)
	}
	result := normalizeAvroValue(m)
	normalized, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("normalized Avro payload was %T, want map[string]interface{}", result)
	}
	return normalized, nil
}

// normalizeAvroValue recursively unwraps goavro union wrappers.
// For unions like ["null", "string"], goavro decodes values as map[string]interface{}{"string": "..."}.
func normalizeAvroValue(v interface{}) interface{} {
	switch value := v.(type) {
	case map[string]interface{}:
		if len(value) == 1 {
			for branch, inner := range value {
				if isAvroUnionBranch(branch) {
					if branch == "null" {
						return nil
					}
					return normalizeAvroValue(inner)
				}
			}
		}
		normalized := make(map[string]interface{}, len(value))
		for k, inner := range value {
			normalized[k] = normalizeAvroValue(inner)
		}
		return normalized
	case []interface{}:
		normalized := make([]interface{}, len(value))
		for i, inner := range value {
			normalized[i] = normalizeAvroValue(inner)
		}
		return normalized
	default:
		return value
	}
}

func isAvroUnionBranch(branch string) bool {
	if _, ok := avroPrimitiveUnionBranches[branch]; ok {
		return true
	}
	return strings.Contains(branch, ".")
}

func (d *AvroDecoder) codecFor(topic string, schemaID int) (*goavro.Codec, error) {
	d.mu.RLock()
	codec, ok := d.codecs[schemaID]
	d.mu.RUnlock()
	if ok {
		return codec, nil
	}

	subject := topic + "-value"
	info, err := d.client.GetBySubjectAndID(subject, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema %d for subject %q: %w", schemaID, subject, err)
	}

	codec, err = goavro.NewCodec(info.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to build Avro codec for schema %d: %w", schemaID, err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.codecs[schemaID]; ok {
		return existing, nil
	}
	// Evict all entries when cache grows beyond the limit
	if len(d.codecs) >= maxCodecCacheSize {
		d.codecs = make(map[int]*goavro.Codec)
	}
	d.codecs[schemaID] = codec
	return codec, nil
}
