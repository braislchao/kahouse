package app

import (
	"testing"
	"time"
)

func TestNormalizeAvroValueUnwrapsPrimitiveUnion(t *testing.T) {
	input := map[string]interface{}{
		"access_id": map[string]interface{}{"string": "abc-123"},
		"metadata":  map[string]interface{}{"null": nil},
		"nested": map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{"int": int32(7)},
				map[string]interface{}{"string": "x"},
			},
		},
	}

	got, ok := normalizeAvroValue(input).(map[string]interface{})
	if !ok {
		t.Fatalf("Expected normalized value to be map, got %T", normalizeAvroValue(input))
	}

	if got["access_id"] != "abc-123" {
		t.Fatalf("Expected access_id to unwrap to string, got %#v", got["access_id"])
	}
	if got["metadata"] != nil {
		t.Fatalf("Expected metadata to unwrap to nil, got %#v", got["metadata"])
	}
	nested, ok := got["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected nested to remain object, got %T", got["nested"])
	}
	items, ok := nested["items"].([]interface{})
	if !ok || len(items) != 2 {
		t.Fatalf("Expected nested items slice of len 2, got %#v", nested["items"])
	}
	if items[0] != int32(7) {
		t.Fatalf("Expected first item to unwrap to int32(7), got %#v", items[0])
	}
	if items[1] != "x" {
		t.Fatalf("Expected second item to unwrap to string x, got %#v", items[1])
	}
}

func TestNormalizeAvroValueKeepsRegularObjects(t *testing.T) {
	input := map[string]interface{}{
		"access_id": map[string]interface{}{"value": "abc-123"},
	}
	got, ok := normalizeAvroValue(input).(map[string]interface{})
	if !ok {
		t.Fatalf("Expected normalized value to be map, got %T", normalizeAvroValue(input))
	}
	inner, ok := got["access_id"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected non-union map to stay map, got %T", got["access_id"])
	}
	if inner["value"] != "abc-123" {
		t.Fatalf("Expected non-union map contents to stay intact, got %#v", inner)
	}
}

func TestCoerceValueTemporalToInt64EmitsMillis(t *testing.T) {
	// A midnight date (Avro logicalType "date", decoded by goavro to a midnight-UTC time.Time)
	// written to an Int64 column must be encoded as epoch-MILLISECONDS — the contract downstream
	// consumers rely on (matching the Confluent kafka-connect ClickHouse sink) — NOT days-since-epoch.
	dateVal := time.Date(2024, 4, 12, 0, 0, 0, 0, time.UTC)
	expected := dateVal.UnixMilli() // 1712880000000, not 19825 days

	got := coerceValue(dateVal, "Int64")
	ms, ok := got.(int64)
	if !ok {
		t.Fatalf("Expected int64 for Int64 column, got %T (%v)", got, got)
	}
	if ms != expected {
		t.Fatalf("Expected %d epoch-ms, got %d", expected, ms)
	}

	if got2, _ := coerceValue(dateVal, "Nullable(Int64)").(int64); got2 != expected {
		t.Fatalf("Expected %d epoch-ms for Nullable(Int64), got %d", expected, got2)
	}

	// Date-family columns still receive time.Time (driver handles them natively).
	if _, ok := coerceValue(dateVal, "Date").(time.Time); !ok {
		t.Fatalf("Expected time.Time for Date column")
	}
	if _, ok := coerceValue(dateVal, "Nullable(Date32)").(time.Time); !ok {
		t.Fatalf("Expected time.Time for Nullable(Date32) column")
	}

	// Regression for the removed midnight heuristic: a timestamp that lands exactly on 00:00:00
	// must NOT be misread as a date. 2016-11-09 00:00:00 UTC -> epoch-ms, not 17114 days.
	midnightTS := time.Date(2016, 11, 9, 0, 0, 0, 0, time.UTC)
	if got := coerceValue(midnightTS, "Int64").(int64); got != midnightTS.UnixMilli() {
		t.Fatalf("Midnight timestamp must be epoch-ms (%d), got %d", midnightTS.UnixMilli(), got)
	}

	// Pre-1970 date -> negative epoch-ms.
	pre1970 := time.Date(1960, 6, 15, 0, 0, 0, 0, time.UTC)
	if got := coerceValue(pre1970, "Int64").(int64); got != pre1970.UnixMilli() || got >= 0 {
		t.Fatalf("Expected negative epoch-ms %d for pre-1970 date, got %d", pre1970.UnixMilli(), got)
	}

	// Unix epoch itself -> 0.
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := coerceValue(epoch, "Int64").(int64); got != 0 {
		t.Fatalf("Expected 0 epoch-ms for unix epoch, got %d", got)
	}
}

func TestCoerceValueTimestampToMillis(t *testing.T) {
	tsVal := time.Date(2024, 4, 12, 14, 30, 45, 123000000, time.UTC)

	got := coerceValue(tsVal, "Int64")
	millis, ok := got.(int64)
	if !ok {
		t.Fatalf("Expected int64 for timestamp to Int64 column, got %T (%v)", got, got)
	}
	if millis != tsVal.UnixMilli() {
		t.Fatalf("Expected %d millis, got %d", tsVal.UnixMilli(), millis)
	}
}

func TestCoerceValueNonTimePassthrough(t *testing.T) {
	got := coerceValue(int32(42), "Int64")
	if got != int32(42) {
		t.Fatalf("Expected int32(42) passthrough, got %T (%v)", got, got)
	}

	got2 := coerceValue("hello", "String")
	if got2 != "hello" {
		t.Fatalf("Expected string passthrough, got %T (%v)", got2, got2)
	}
}

func TestCoerceValueStringRFC3339ToDateTime(t *testing.T) {
	// RFC3339 string should be parsed to time.Time for DateTime columns.
	got := coerceValue("2026-05-06T15:52:34Z", "DateTime")
	tv, ok := got.(time.Time)
	if !ok {
		t.Fatalf("Expected time.Time for RFC3339 string to DateTime, got %T (%v)", got, got)
	}
	expected := time.Date(2026, 5, 6, 15, 52, 34, 0, time.UTC)
	if !tv.Equal(expected) {
		t.Fatalf("Expected %v, got %v", expected, tv)
	}

	// Nullable(DateTime64(3)) should also work.
	got2 := coerceValue("2026-05-06T15:52:34.123Z", "Nullable(DateTime64(3))")
	tv2, ok := got2.(time.Time)
	if !ok {
		t.Fatalf("Expected time.Time for RFC3339Nano string to Nullable(DateTime64(3)), got %T (%v)", got2, got2)
	}
	expected2 := time.Date(2026, 5, 6, 15, 52, 34, 123000000, time.UTC)
	if !tv2.Equal(expected2) {
		t.Fatalf("Expected %v, got %v", expected2, tv2)
	}

	// String for non-DateTime column should pass through.
	got3 := coerceValue("2026-05-06T15:52:34Z", "String")
	if _, ok := got3.(string); !ok {
		t.Fatalf("Expected string passthrough for String column, got %T (%v)", got3, got3)
	}

	// Non-RFC3339 string for DateTime column should pass through.
	got4 := coerceValue("not-a-date", "DateTime")
	if got4 != "not-a-date" {
		t.Fatalf("Expected passthrough for non-RFC3339 string, got %T (%v)", got4, got4)
	}

	// Date-only string (YYYY-MM-DD) should be parsed to midnight UTC for DateTime columns.
	got5 := coerceValue("2100-01-01", "DateTime")
	tv5, ok := got5.(time.Time)
	if !ok {
		t.Fatalf("Expected time.Time for date-only string to DateTime, got %T (%v)", got5, got5)
	}
	expected5 := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	if !tv5.Equal(expected5) {
		t.Fatalf("Expected %v, got %v", expected5, tv5)
	}
}
