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

func TestCoerceValueDateToInt64(t *testing.T) {
	dateVal := time.Date(2024, 4, 12, 0, 0, 0, 0, time.UTC)
	expected := int64(19825) // 2024-04-12 is 19825 days since epoch

	got := coerceValue(dateVal, "Int64")
	days, ok := got.(int64)
	if !ok {
		t.Fatalf("Expected int64 for Int64 column, got %T (%v)", got, got)
	}
	if days != expected {
		t.Fatalf("Expected %d days since epoch, got %d", expected, days)
	}

	got2 := coerceValue(dateVal, "Nullable(Int64)")
	days2, ok := got2.(int64)
	if !ok {
		t.Fatalf("Expected int64 for Nullable(Int64) column, got %T (%v)", got2, got2)
	}
	if days2 != expected {
		t.Fatalf("Expected %d days, got %d", expected, days2)
	}

	got3 := coerceValue(dateVal, "Date")
	if _, ok := got3.(time.Time); !ok {
		t.Fatalf("Expected time.Time for Date column, got %T (%v)", got3, got3)
	}

	got4 := coerceValue(dateVal, "Nullable(Date32)")
	if _, ok := got4.(time.Time); !ok {
		t.Fatalf("Expected time.Time for Nullable(Date32) column, got %T (%v)", got4, got4)
	}

	// Pre-1970 date: 1960-06-15 is -3488 days since epoch.
	pre1970 := time.Date(1960, 6, 15, 0, 0, 0, 0, time.UTC)
	got5 := coerceValue(pre1970, "Int64")
	days5, ok := got5.(int64)
	if !ok {
		t.Fatalf("Expected int64 for pre-1970 date, got %T (%v)", got5, got5)
	}
	if days5 >= 0 {
		t.Fatalf("Expected negative days for pre-1970 date, got %d", days5)
	}
	roundTrip := unixEpoch.AddDate(0, 0, int(days5))
	if !roundTrip.Equal(pre1970) {
		t.Fatalf("Round-trip failed: expected %v, got %v (days=%d)", pre1970, roundTrip, days5)
	}

	// Unix epoch itself should be 0.
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	got6 := coerceValue(epoch, "Int64")
	days6, ok := got6.(int64)
	if !ok {
		t.Fatalf("Expected int64 for epoch, got %T (%v)", got6, got6)
	}
	if days6 != 0 {
		t.Fatalf("Expected 0 days for epoch, got %d", days6)
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
