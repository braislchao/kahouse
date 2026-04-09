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
	defer rows.Close()

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

// resetSchemaCache clears the cached column types (used in tests).
func resetSchemaCache() {
	schemaCache = sync.Map{}
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

// buildColumnSlice converts a slice of interface{} values to a typed slice
// matching the given ClickHouse column type, suitable for BatchColumn.Append.
func buildColumnSlice(chType string, values []interface{}) (interface{}, error) {
	baseType, nullable := stripTypeWrappers(chType)

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
