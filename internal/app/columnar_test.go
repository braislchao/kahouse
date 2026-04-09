package app

import (
	"testing"
	"time"
)

func TestStripTypeWrappers(t *testing.T) {
	tests := []struct {
		input    string
		wantBase string
		wantNull bool
	}{
		{"String", "String", false},
		{"Nullable(String)", "String", true},
		{"LowCardinality(String)", "String", false},
		{"LowCardinality(Nullable(String))", "String", true},
		{"Nullable(Int32)", "Int32", true},
		{"DateTime64(3)", "DateTime64(3)", false},
		{"Nullable(DateTime64(3))", "DateTime64(3)", true},
	}
	for _, tt := range tests {
		base, nullable := stripTypeWrappers(tt.input)
		if base != tt.wantBase || nullable != tt.wantNull {
			t.Errorf("stripTypeWrappers(%q) = (%q, %v), want (%q, %v)",
				tt.input, base, nullable, tt.wantBase, tt.wantNull)
		}
	}
}

func TestBuildColumnSliceString(t *testing.T) {
	values := []interface{}{"hello", "world", nil}

	result, err := buildColumnSlice("String", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]string)
	if len(got) != 3 || got[0] != "hello" || got[1] != "world" || got[2] != "" {
		t.Errorf("unexpected result: %v", got)
	}
}

func TestBuildColumnSliceNullableString(t *testing.T) {
	values := []interface{}{"hello", nil, "world"}

	result, err := buildColumnSlice("Nullable(String)", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]*string)
	if len(got) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(got))
	}
	if *got[0] != "hello" {
		t.Errorf("got[0] = %q, want hello", *got[0])
	}
	if got[1] != nil {
		t.Errorf("got[1] = %v, want nil", got[1])
	}
	if *got[2] != "world" {
		t.Errorf("got[2] = %q, want world", *got[2])
	}
}

func TestBuildColumnSliceInt64(t *testing.T) {
	values := []interface{}{int64(1), int64(2), int64(3)}

	result, err := buildColumnSlice("Int64", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]int64)
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceInt32FromInt64(t *testing.T) {
	// JSON decoder produces int64 for all integers; column may be Int32.
	values := []interface{}{int64(42), int64(-1), nil}

	result, err := buildColumnSlice("Int32", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]int32)
	if got[0] != 42 || got[1] != -1 || got[2] != 0 {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceNullableInt32(t *testing.T) {
	values := []interface{}{int64(10), nil, int64(30)}

	result, err := buildColumnSlice("Nullable(Int32)", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]*int32)
	if *got[0] != 10 || got[1] != nil || *got[2] != 30 {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceFloat64(t *testing.T) {
	values := []interface{}{float64(1.5), float64(2.5)}

	result, err := buildColumnSlice("Float64", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]float64)
	if got[0] != 1.5 || got[1] != 2.5 {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceFloat32FromFloat64(t *testing.T) {
	values := []interface{}{float64(1.5)}

	result, err := buildColumnSlice("Float32", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]float32)
	if got[0] != 1.5 {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceDateTime64(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	values := []interface{}{now, nil}

	result, err := buildColumnSlice("DateTime64(3)", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]time.Time)
	if !got[0].Equal(now) {
		t.Errorf("got %v, want %v", got[0], now)
	}
	if !got[1].IsZero() {
		t.Errorf("expected zero time for nil, got %v", got[1])
	}
}

func TestBuildColumnSliceDateTimeFromEpochMillis(t *testing.T) {
	values := []interface{}{int64(1712592000000)} // 2024-04-08T16:00:00Z

	result, err := buildColumnSlice("DateTime64(3)", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]time.Time)
	expected := time.UnixMilli(1712592000000)
	if !got[0].Equal(expected) {
		t.Errorf("got %v, want %v", got[0], expected)
	}
}

func TestBuildColumnSliceBool(t *testing.T) {
	values := []interface{}{true, false, nil}

	result, err := buildColumnSlice("Bool", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]bool)
	if got[0] != true || got[1] != false || got[2] != false {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceUInt64(t *testing.T) {
	values := []interface{}{uint64(100), int64(200)}

	result, err := buildColumnSlice("UInt64", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]uint64)
	if got[0] != 100 || got[1] != 200 {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceLowCardinality(t *testing.T) {
	values := []interface{}{"a", "b"}

	result, err := buildColumnSlice("LowCardinality(String)", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]string)
	if got[0] != "a" || got[1] != "b" {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceLowCardinalityNullable(t *testing.T) {
	values := []interface{}{"a", nil}

	result, err := buildColumnSlice("LowCardinality(Nullable(String))", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]*string)
	if *got[0] != "a" || got[1] != nil {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceUnsupportedType(t *testing.T) {
	_, err := buildColumnSlice("Array(String)", []interface{}{})
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestBuildColumnSliceConversionError(t *testing.T) {
	// Pass a struct that can't convert to int64.
	values := []interface{}{struct{}{}}

	_, err := buildColumnSlice("Int64", values)
	if err == nil {
		t.Error("expected error for unconvertible value")
	}
}

func TestBuildColumnSliceEnum(t *testing.T) {
	values := []interface{}{"active", "inactive"}

	result, err := buildColumnSlice("Enum8('active'=1,'inactive'=2)", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]string)
	if got[0] != "active" || got[1] != "inactive" {
		t.Errorf("unexpected: %v", got)
	}
}

func TestBuildColumnSliceUUID(t *testing.T) {
	values := []interface{}{"550e8400-e29b-41d4-a716-446655440000"}

	result, err := buildColumnSlice("UUID", values)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]string)
	if got[0] != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("unexpected: %v", got)
	}
}

func TestToInt64Conversions(t *testing.T) {
	tests := []struct {
		input interface{}
		want  int64
	}{
		{int64(42), 42},
		{int32(42), 42},
		{int16(42), 42},
		{int8(42), 42},
		{int(42), 42},
		{float64(42.9), 42},
		{float32(42.0), 42},
		{uint64(42), 42},
		{uint32(42), 42},
		{uint16(42), 42},
		{uint8(42), 42},
	}
	for _, tt := range tests {
		got, err := toInt64(tt.input)
		if err != nil {
			t.Errorf("toInt64(%T(%v)) error: %v", tt.input, tt.input, err)
		}
		if got != tt.want {
			t.Errorf("toInt64(%T(%v)) = %d, want %d", tt.input, tt.input, got, tt.want)
		}
	}
}

func TestToTimeConversions(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	epoch := now.UnixMilli()

	tests := []struct {
		input interface{}
		want  time.Time
	}{
		{now, now},
		{int64(epoch), now},
		{float64(epoch), now},
	}
	for _, tt := range tests {
		got, err := toTime(tt.input)
		if err != nil {
			t.Errorf("toTime(%T) error: %v", tt.input, err)
		}
		if !got.Equal(tt.want) {
			t.Errorf("toTime(%T) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestEmptyBatch(t *testing.T) {
	result, err := buildColumnSlice("String", []interface{}{})
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]string)
	if len(got) != 0 {
		t.Errorf("expected empty slice, got %v", got)
	}
}
