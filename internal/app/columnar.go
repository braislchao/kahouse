package app

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// schemaCache stores column name→type maps keyed by table name.
var schemaCache sync.Map

// columnarCache stores per-table columnar eligibility (bool) so we only check once.
var columnarCache sync.Map

// getColumnTypes returns a map of column name → ClickHouse type for the given table.
// Results are cached for the lifetime of the process.
func getColumnTypes(ctx context.Context, conn driver.Conn, table string) (map[string]string, error) {
	if cached, ok := schemaCache.Load(table); ok {
		return cached.(map[string]string), nil
	}

	parts := strings.SplitN(table, ".", 2)
	var rows driver.Rows
	var err error
	if len(parts) == 2 {
		rows, err = conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = ? AND table = ?", parts[0], parts[1])
	} else {
		rows, err = conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ?", table)
	}
	if err != nil {
		return nil, fmt.Errorf("query column types for %s: %w", table, err)
	}
	defer rows.Close() //nolint:errcheck

	types := make(map[string]string)
	for rows.Next() {
		var name, colType string
		if err := rows.Scan(&name, &colType); err != nil {
			return nil, fmt.Errorf("scan column type for %s: %w", table, err)
		}
		types[name] = colType
	}
	if len(types) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", table)
	}

	schemaCache.Store(table, types)
	return types, nil
}

// CanUseColumnar checks whether all column types in the given type map are
// supported by the columnar insert path. The result is cached per table name
// so the check runs at most once per table per process lifetime.
// Returns true if all types are supported, false if any type requires the
// row-by-row fallback path.
func CanUseColumnar(table string, colTypes map[string]string) bool {
	if cached, ok := columnarCache.Load(table); ok {
		return cached.(bool)
	}
	for _, chType := range colTypes {
		if _, err := buildColumnSlice(chType, []interface{}{}); err != nil {
			columnarCache.Store(table, false)
			return false
		}
	}
	columnarCache.Store(table, true)
	return true
}

// stripTypeWrappers removes LowCardinality and Nullable wrappers from a ClickHouse type,
// returning the base type and whether Nullable was present.
func stripTypeWrappers(chType string) (baseType string, nullable bool) {
	baseType = chType
	if strings.HasPrefix(baseType, "LowCardinality(") && strings.HasSuffix(baseType, ")") {
		baseType = baseType[15 : len(baseType)-1]
	}
	if strings.HasPrefix(baseType, "Nullable(") && strings.HasSuffix(baseType, ")") {
		nullable = true
		baseType = baseType[9 : len(baseType)-1]
	}
	return
}

// parseArrayInnerType extracts the inner type from "Array(T)".
// Returns the inner type string and true if the type is an Array with a
// supported primitive inner type. Returns ("", false) for non-Array types
// or Arrays with complex inner types (e.g. Tuple, Nested, Map).
func parseArrayInnerType(baseType string) (string, bool) {
	if !strings.HasPrefix(baseType, "Array(") || !strings.HasSuffix(baseType, ")") {
		return "", false
	}
	inner := baseType[6 : len(baseType)-1]
	// Reject complex nested types that we cannot handle in columnar mode.
	if strings.HasPrefix(inner, "Tuple(") || strings.HasPrefix(inner, "Nested(") ||
		strings.HasPrefix(inner, "Map(") || strings.HasPrefix(inner, "Array(") {
		return "", false
	}
	return inner, true
}

// buildColumnSlice converts a slice of interface{} values to a typed slice
// matching the given ClickHouse column type, suitable for BatchColumn.Append.
func buildColumnSlice(chType string, values []interface{}) (interface{}, error) {
	baseType, nullable := stripTypeWrappers(chType)

	// Handle Array types first — nullable doesn't apply to the array itself,
	// only to the element type (which we handle inside buildArraySlice).
	if innerType, ok := parseArrayInnerType(baseType); ok {
		return buildArraySlice(innerType, values)
	}

	switch {
	case baseType == "String" || strings.HasPrefix(baseType, "FixedString") ||
		baseType == "UUID" || strings.HasPrefix(baseType, "Enum8") ||
		strings.HasPrefix(baseType, "Enum16"):
		if nullable {
			return buildNullableSlice(values, toString)
		}
		return buildSlice(values, toString)

	case baseType == "Int8":
		if nullable {
			return buildNullableSlice(values, toInt8)
		}
		return buildSlice(values, toInt8)
	case baseType == "Int16":
		if nullable {
			return buildNullableSlice(values, toInt16)
		}
		return buildSlice(values, toInt16)
	case baseType == "Int32":
		if nullable {
			return buildNullableSlice(values, toInt32)
		}
		return buildSlice(values, toInt32)
	case baseType == "Int64":
		if nullable {
			return buildNullableSlice(values, toInt64)
		}
		return buildSlice(values, toInt64)

	case baseType == "UInt8":
		if nullable {
			return buildNullableSlice(values, toUint8)
		}
		return buildSlice(values, toUint8)
	case baseType == "UInt16":
		if nullable {
			return buildNullableSlice(values, toUint16)
		}
		return buildSlice(values, toUint16)
	case baseType == "UInt32":
		if nullable {
			return buildNullableSlice(values, toUint32)
		}
		return buildSlice(values, toUint32)
	case baseType == "UInt64":
		if nullable {
			return buildNullableSlice(values, toUint64)
		}
		return buildSlice(values, toUint64)

	case baseType == "Float32":
		if nullable {
			return buildNullableSlice(values, toFloat32)
		}
		return buildSlice(values, toFloat32)
	case baseType == "Float64":
		if nullable {
			return buildNullableSlice(values, toFloat64)
		}
		return buildSlice(values, toFloat64)

	case strings.HasPrefix(baseType, "DateTime64") || baseType == "DateTime" ||
		baseType == "Date" || baseType == "Date32":
		if nullable {
			return buildNullableSlice(values, toTime)
		}
		return buildSlice(values, toTime)

	case baseType == "Bool":
		if nullable {
			return buildNullableSlice(values, toBool)
		}
		return buildSlice(values, toBool)

	default:
		return nil, fmt.Errorf("unsupported ClickHouse type %q for columnar insert", chType)
	}
}

// buildSlice converts []interface{} to a typed []T using the given converter.
// Nil values become the zero value of T.
func buildSlice[T any](values []interface{}, convert func(interface{}) (T, error)) ([]T, error) {
	out := make([]T, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		var err error
		out[i], err = convert(v)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
	}
	return out, nil
}

// buildNullableSlice converts []interface{} to a typed []*T using the given converter.
// Nil values become nil pointers.
func buildNullableSlice[T any](values []interface{}, convert func(interface{}) (T, error)) ([]*T, error) {
	out := make([]*T, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		val, err := convert(v)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		out[i] = &val
	}
	return out, nil
}

// --- Conversion functions ---

func toString(v interface{}) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func toInt8(v interface{}) (int8, error) {
	n, err := toInt64(v)
	return int8(n), err
}

func toInt16(v interface{}) (int16, error) {
	n, err := toInt64(v)
	return int16(n), err
}

func toInt32(v interface{}) (int32, error) {
	n, err := toInt64(v)
	return int32(n), err
}

func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toUint8(v interface{}) (uint8, error) {
	n, err := toUint64(v)
	return uint8(n), err
}

func toUint16(v interface{}) (uint16, error) {
	n, err := toUint64(v)
	return uint16(n), err
}

func toUint32(v interface{}) (uint32, error) {
	n, err := toUint64(v)
	return uint32(n), err
}

func toUint64(v interface{}) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case uint32:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case int64:
		return uint64(val), nil
	case int32:
		return uint64(val), nil
	case int:
		return uint64(val), nil
	case float64:
		return uint64(val), nil
	case float32:
		return uint64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

func toFloat32(v interface{}) (float32, error) {
	switch val := v.(type) {
	case float32:
		return val, nil
	case float64:
		return float32(val), nil
	case int64:
		return float32(val), nil
	case int32:
		return float32(val), nil
	case int:
		return float32(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float32", v)
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toTime(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val, nil
	case int64:
		return time.UnixMilli(val), nil
	case float64:
		return time.UnixMilli(int64(val)), nil
	case int32:
		return time.Unix(int64(val), 0), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}

func toBool(v interface{}) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case int64:
		return val != 0, nil
	case int32:
		return val != 0, nil
	case uint8:
		return val != 0, nil
	case int:
		return val != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

// buildArraySlice converts a column of array values (each row is a []interface{}
// from JSON/Avro deserialization) into a typed [][]T suitable for columnar Append.
// The innerType is the unwrapped element type (e.g. "UInt64" from "Array(UInt64)").
// Nil row values produce nil inner slices (empty arrays in ClickHouse).
func buildArraySlice(innerType string, values []interface{}) (interface{}, error) {
	// Strip wrappers from the inner type (e.g. Array(Nullable(String)) is not
	// supported in columnar mode and would not reach here — parseArrayInnerType
	// rejects complex inners — but handle LowCardinality just in case).
	baseInner, _ := stripTypeWrappers(innerType)

	switch {
	case baseInner == "String" || strings.HasPrefix(baseInner, "FixedString") ||
		baseInner == "UUID" || strings.HasPrefix(baseInner, "Enum8") ||
		strings.HasPrefix(baseInner, "Enum16"):
		return buildTypedArraySlice(values, toString)

	case baseInner == "Int8":
		return buildTypedArraySlice(values, toInt8)
	case baseInner == "Int16":
		return buildTypedArraySlice(values, toInt16)
	case baseInner == "Int32":
		return buildTypedArraySlice(values, toInt32)
	case baseInner == "Int64":
		return buildTypedArraySlice(values, toInt64)

	case baseInner == "UInt8":
		return buildTypedArraySlice(values, toUint8)
	case baseInner == "UInt16":
		return buildTypedArraySlice(values, toUint16)
	case baseInner == "UInt32":
		return buildTypedArraySlice(values, toUint32)
	case baseInner == "UInt64":
		return buildTypedArraySlice(values, toUint64)

	case baseInner == "Float32":
		return buildTypedArraySlice(values, toFloat32)
	case baseInner == "Float64":
		return buildTypedArraySlice(values, toFloat64)

	case strings.HasPrefix(baseInner, "DateTime64") || baseInner == "DateTime" ||
		baseInner == "Date" || baseInner == "Date32":
		return buildTypedArraySlice(values, toTime)

	case baseInner == "Bool":
		return buildTypedArraySlice(values, toBool)

	default:
		return nil, fmt.Errorf("unsupported Array inner type %q for columnar insert", innerType)
	}
}

// buildTypedArraySlice converts []interface{} (where each element is itself a
// []interface{} or nil) into [][]T. Each inner []interface{} is converted
// element-by-element using the provided converter function.
func buildTypedArraySlice[T any](values []interface{}, convert func(interface{}) (T, error)) ([][]T, error) {
	out := make([][]T, len(values))
	for i, v := range values {
		if v == nil {
			// nil → nil slice, which ClickHouse treats as an empty array
			continue
		}
		arr, ok := v.([]interface{})
		if !ok {
			return nil, fmt.Errorf("row %d: expected []interface{} for array column, got %T", i, v)
		}
		inner := make([]T, len(arr))
		for j, elem := range arr {
			if elem == nil {
				continue // zero value of T
			}
			val, err := convert(elem)
			if err != nil {
				return nil, fmt.Errorf("row %d, element %d: %w", i, j, err)
			}
			inner[j] = val
		}
		out[i] = inner
	}
	return out, nil
}
