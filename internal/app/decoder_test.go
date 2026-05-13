package app

import (
	"strings"
	"testing"
)

func TestJSONDecoderDecode(t *testing.T) {
	decoder := JSONDecoder{}
	record, err := decoder.Decode("orders", []byte(`{"id":1,"price":12.5,"active":true,"name":"alice"}`))
	if err != nil {
		t.Fatalf("Expected JSON decode to succeed, got %v", err)
	}

	if got, ok := record["id"].(int64); !ok || got != 1 {
		t.Fatalf("Expected id int64(1), got %#v", record["id"])
	}
	if got, ok := record["price"].(float64); !ok || got != 12.5 {
		t.Fatalf("Expected price float64(12.5), got %#v", record["price"])
	}
	if got, ok := record["active"].(bool); !ok || !got {
		t.Fatalf("Expected active true, got %#v", record["active"])
	}
	if got, ok := record["name"].(string); !ok || got != "alice" {
		t.Fatalf("Expected name alice, got %#v", record["name"])
	}
}

func TestJSONDecoderRejectsNonObjectPayload(t *testing.T) {
	decoder := JSONDecoder{}
	_, err := decoder.Decode("orders", []byte(`[1,2,3]`))
	if err == nil || !strings.Contains(err.Error(), "want object") {
		t.Fatalf("Expected non-object JSON to be rejected, got %v", err)
	}
}

func TestJSONDecoderRejectsTrailingData(t *testing.T) {
	decoder := JSONDecoder{}
	_, err := decoder.Decode("orders", []byte(`{"id":1} {"id":2}`))
	if err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("Expected trailing JSON data to be rejected, got %v", err)
	}
}

func TestStringDecoderDecode(t *testing.T) {
	decoder := StringDecoder{column: "value"}
	record, err := decoder.Decode("logs", []byte("hello world"))
	if err != nil {
		t.Fatalf("Expected string decode to succeed, got %v", err)
	}
	if len(record) != 1 || record["value"] != "hello world" {
		t.Fatalf("Unexpected string record: %#v", record)
	}
}

func TestStringDecoderRequiresColumn(t *testing.T) {
	_, err := newMessageDecoder("string", "", nil)
	if err == nil || !strings.Contains(err.Error(), "destination column") {
		t.Fatalf("Expected missing destination column error, got %v", err)
	}
}

func TestNewMessageDecoderUsesExplicitFormat(t *testing.T) {
	decoder, err := newMessageDecoder("string", "value", nil)
	if err != nil {
		t.Fatalf("Expected string decoder creation to succeed, got %v", err)
	}
	if _, ok := decoder.(StringDecoder); !ok {
		t.Fatalf("Expected StringDecoder, got %T", decoder)
	}

	if _, err := newMessageDecoder("avro", "", nil); err == nil {
		t.Fatal("Expected avro decoder creation without schema registry client to fail")
	}
}

func TestJSONDecoderUsesNumbersDeterministically(t *testing.T) {
	decoder := JSONDecoder{}
	record, err := decoder.Decode("orders", []byte(`{"whole":900719925474099,"fractional":1e1}`))
	if err != nil {
		t.Fatalf("Expected JSON decode to succeed, got %v", err)
	}

	if got, ok := record["whole"].(int64); !ok || got != 900719925474099 {
		t.Fatalf("Expected whole int64, got %#v", record["whole"])
	}
	if got, ok := record["fractional"].(float64); !ok || got != 10 {
		t.Fatalf("Expected fractional float64(10), got %#v", record["fractional"])
	}
}
