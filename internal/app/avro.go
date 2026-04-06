package app

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/linkedin/goavro/v2"
)

const confluentMagicByte byte = 0x0

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
	return m, nil
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
	d.codecs[schemaID] = codec
	return codec, nil
}
