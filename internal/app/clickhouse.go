package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// isRetriableClickHouseError checks if a ClickHouse error is transient and can be retried.
func isRetriableClickHouseError(err error) bool {
	var chErr *clickhouse.Exception
	if !errors.As(err, &chErr) {
		// Network errors, timeouts, etc. are retriable
		return true
	}
	switch chErr.Code {
	case 3, // UNEXPECTED_END_OF_FILE
		107, // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO (keeper/zk transient)
		159, // TIMEOUT_EXCEEDED
		164, // READONLY (server in read-only mode)
		202, // TOO_MANY_SIMULTANEOUS_QUERIES
		203, // NO_FREE_CONNECTION
		209, // SOCKET_TIMEOUT
		210, // NETWORK_ERROR
		241, // MEMORY_LIMIT_EXCEEDED
		242, // TABLE_IS_READ_ONLY
		252, // TOO_MANY_PARTS
		285, // PART_IS_TEMPORARILY_LOCKED
		319, // UNKNOWN_STATUS_OF_INSERT
		425, // SYSTEM_ERROR
		999: // KEEPER_EXCEPTION
		return true
	default:
		return false
	}
}

// quoteIdentifier wraps a ClickHouse identifier in backticks, escaping any
// embedded backticks to prevent SQL injection via column or table names.
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// quoteTableIdentifier quotes a table identifier that may optionally be
// prefixed with a database name: db.table -> `db`.`table`.
// Only the first dot is treated as a separator so that dots within the
// table name itself are preserved (e.g. "mydb.events.clicks.v2.raw"
// becomes `mydb`.`events.clicks.v2.raw`).
// Returns an error if the name is empty or contains empty segments.
func quoteTableIdentifier(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("table name is empty")
	}
	parts := strings.SplitN(trimmed, ".", 2)
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", fmt.Errorf("table name %q contains empty segment", name)
		}
		quoted = append(quoted, quoteIdentifier(part))
	}
	return strings.Join(quoted, "."), nil
}

// ColumnInfo maps column name to its ClickHouse type string (e.g. "Nullable(Int64)", "Date", "String").
type ColumnInfo map[string]string

// TableColumns maps a table name to its column info.
type TableColumns map[string]ColumnInfo

// getTableColumns queries ClickHouse for the column names and types of the given table.
func getTableColumns(ctx context.Context, chConn driver.Conn, table string) (ColumnInfo, error) {
	quotedTable, err := quoteTableIdentifier(table)
	if err != nil {
		return nil, fmt.Errorf("invalid table name %q: %w", table, err)
	}
	rows, err := chConn.Query(ctx, fmt.Sprintf("DESCRIBE TABLE %s", quotedTable))
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %q: %w", table, err)
	}
	defer rows.Close()

	cols := make(ColumnInfo)
	for rows.Next() {
		var name, colType, defaultType, defaultExpr, comment, codecExpr, ttlExpr string
		if err := rows.Scan(&name, &colType, &defaultType, &defaultExpr, &comment, &codecExpr, &ttlExpr); err != nil {
			return nil, fmt.Errorf("failed to scan DESCRIBE row for table %q: %w", table, err)
		}
		cols[name] = colType
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating DESCRIBE rows for table %q: %w", table, err)
	}
	return cols, nil
}

// validateTables checks which target tables exist and are accessible in ClickHouse.
// It returns a TableColumns map with the column names for each table that validated,
// which is used both to filter out Avro fields without a matching ClickHouse column
// and to decide which sink tasks to start. Tables that fail validation (missing or
// inaccessible) are logged as warnings and omitted from the map so a single missing
// table does not take down the whole sink; the caller skips their tasks.
func validateTables(ctx context.Context, chConn driver.Conn, tables []TopicTableMapping, sugar *zap.SugaredLogger) TableColumns {
	tc := make(TableColumns, len(tables))
	for _, mapping := range tables {
		cols, err := getTableColumns(ctx, chConn, mapping.Table)
		if err != nil {
			sugar.Warnf("Skipping topic %s: table %q failed validation: %v", mapping.Topic, mapping.Table, err)
			continue
		}
		if len(cols) == 0 {
			sugar.Warnf("Skipping topic %s: table %q has no columns or does not exist", mapping.Topic, mapping.Table)
			continue
		}
		tc[mapping.Table] = cols
		sugar.Infof("Validated table exists: %s -> %s (%d columns)", mapping.Topic, mapping.Table, len(cols))
	}
	return tc
}

// batchTiming holds the duration of each phase inside writeBatch.
type batchTiming struct {
	Prepare time.Duration
	Append  time.Duration
	Send    time.Duration
}

// isIntegerType returns true if the ClickHouse type (after stripping Nullable) is an integer type.
func isIntegerType(chType string) bool {
	// Strip Nullable wrapper
	t := chType
	if strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")") {
		t = t[len("Nullable(") : len(t)-1]
	}
	switch t {
	case "Int8", "Int16", "Int32", "Int64",
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Int128", "Int256", "UInt128", "UInt256":
		return true
	}
	return false
}

// isDateTimeType returns true if the ClickHouse type (after stripping Nullable) is a
// DateTime-family type that accepts time.Time via the native protocol driver.
func isDateTimeType(chType string) bool {
	t := chType
	if strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")") {
		t = t[len("Nullable(") : len(t)-1]
	}
	switch {
	case t == "DateTime", t == "Date", t == "Date32":
		return true
	case strings.HasPrefix(t, "DateTime64"):
		return true
	}
	return false
}

// coerceValue converts Go types that clickhouse-go cannot handle natively for
// certain column types. Currently handles:
//   - time.Time → int64 (epoch-milliseconds) when the target column is an integer type.
//     ClickHouse columns ALTERed from Date/DateTime to Int64 (e.g. to hold overflow dates
//     like year 3029) cannot accept time.Time via the native protocol driver, so we encode
//     the instant as epoch-ms. See the integer branch below for why ms (not days).
//   - string (RFC3339/ISO8601) → time.Time when the target column is a DateTime-family
//     type. clickhouse-go's native protocol rejects RFC3339 strings (with "T" separator)
//     but accepts time.Time natively for DateTime/DateTime64 columns.
func coerceValue(val interface{}, chType string) interface{} {
	// Handle string timestamps for DateTime columns.
	if s, ok := val.(string); ok {
		if isDateTimeType(chType) {
			if parsed, err := time.Parse(time.RFC3339, s); err == nil {
				return parsed
			}
			// Also try RFC3339Nano for sub-second precision.
			if parsed, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return parsed
			}
			// Also try date-only format (YYYY-MM-DD) → midnight UTC.
			if parsed, err := time.Parse("2006-01-02", s); err == nil {
				return parsed
			}
		}
		return val
	}

	t, ok := val.(time.Time)
	if !ok {
		return val
	}
	if !isIntegerType(chType) {
		// Date, Date32, DateTime, DateTime64 — driver handles time.Time natively.
		return val
	}
	// Emit epoch-milliseconds for every temporal value written to an integer column.
	//
	// goavro decodes BOTH Avro logicalType "date" (days since epoch) and
	// "timestamp-millis"/"timestamp-micros" (an instant) into time.Time, erasing the
	// distinction. We must therefore pick ONE integer encoding, and it has to be epoch-ms:
	// that is the contract every downstream ClickHouse consumer was built against, and it
	// matches the Confluent kafka-connect ClickHouse sink (e.g. consumers do `toDate(col / 1000)`).
	//
	// The earlier "midnight UTC -> days-since-epoch, otherwise -> milliseconds" heuristic was
	// ambiguous and produced a mixed encoding no consumer could read: a real date and a timestamp
	// that happens to land on 00:00:00 are indistinguishable once decoded, so date fields (and any
	// midnight timestamp) were written as days while non-midnight timestamps were written as ms.
	// e.g. a leave start-date became 17114 instead of 1478649600000 and was silently dropped by
	// `toDate(col / 1000)`. epoch-ms fits Int64 for any realistic year, so there is no overflow
	// reason to prefer days.
	return t.UnixMilli()
}

// writeBatch writes a batch of records to a ClickHouse table.
// If columnInfo is non-nil, only columns present in the map are included in the INSERT
// (mirroring Kafka Connect's cleanupExtraFields), and values are coerced to match
// the ClickHouse column type when needed.
func writeBatch(ctx context.Context, table string, chConn driver.Conn, batch []map[string]interface{}, asyncInsert bool, waitForAsyncInsert bool, columnInfo ColumnInfo) (batchTiming, error) {
	var timing batchTiming
	if len(batch) == 0 {
		return timing, nil
	}

	// Collect the union of all column names across the batch to handle sparse JSON
	// objects where different records may have different sets of keys.
	colSet := make(map[string]struct{})
	for _, record := range batch {
		for k := range record {
			colSet[k] = struct{}{}
		}
	}

	// Filter out columns that don't exist in the ClickHouse table (like Kafka Connect's
	// cleanupExtraFields). This prevents errors when the Avro schema has fields that
	// were never added to the ClickHouse table.
	if columnInfo != nil {
		for col := range colSet {
			if _, ok := columnInfo[col]; !ok {
				delete(colSet, col)
			}
		}
	}

	columns := make([]string, 0, len(colSet))
	for k := range colSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quoteIdentifier(col)
	}
	if asyncInsert {
		waitVal := 0
		if waitForAsyncInsert {
			waitVal = 1
		}
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"async_insert":          1,
			"wait_for_async_insert": waitVal,
		}))
	}
	quotedTable, err := quoteTableIdentifier(table)
	if err != nil {
		return timing, fmt.Errorf("invalid table name: %w", err)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES", quotedTable, strings.Join(quoted, ", "))

	prepareStart := time.Now()
	batchStmt, err := chConn.PrepareBatch(ctx, insertSQL)
	timing.Prepare = time.Since(prepareStart)
	if err != nil {
		return timing, fmt.Errorf("failed to prepare batch insert: %w", err)
	}

	// Append expects all column values for a single row in one variadic call.
	appendStart := time.Now()
	for _, record := range batch {
		row := make([]interface{}, len(columns))
		for i, col := range columns {
			val := record[col]
			if columnInfo != nil {
				if chType, ok := columnInfo[col]; ok {
					val = coerceValue(val, chType)
				}
			}
			row[i] = val
		}
		if err := batchStmt.Append(row...); err != nil {
			timing.Append = time.Since(appendStart)
			return timing, fmt.Errorf("failed to append row to batch: %w", err)
		}
	}
	timing.Append = time.Since(appendStart)

	sendStart := time.Now()
	if err := batchStmt.Send(); err != nil {
		timing.Send = time.Since(sendStart)
		return timing, fmt.Errorf("failed to send batch insert: %w", err)
	}
	timing.Send = time.Since(sendStart)

	return timing, nil
}
