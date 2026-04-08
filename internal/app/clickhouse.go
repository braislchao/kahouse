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

// validateTables checks that all target tables exist and are accessible in ClickHouse.
func validateTables(ctx context.Context, chConn driver.Conn, tables []TopicTableMapping, sugar *zap.SugaredLogger) error {
	for _, mapping := range tables {
		quotedTable, err := quoteTableIdentifier(mapping.Table)
		if err != nil {
			return fmt.Errorf("topic %s: invalid table name %q: %w", mapping.Topic, mapping.Table, err)
		}
		query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", quotedTable)
		if err := chConn.Exec(ctx, query); err != nil {
			return fmt.Errorf("topic %s: table %q does not exist or is not accessible: %w", mapping.Topic, mapping.Table, err)
		}
		sugar.Infof("Validated table exists: %s -> %s", mapping.Topic, mapping.Table)
	}
	return nil
}

// batchTiming holds the duration of each phase inside writeBatch.
type batchTiming struct {
	Prepare time.Duration
	Append  time.Duration
	Send    time.Duration
}

// writeBatch writes a batch of records to a ClickHouse table.
func writeBatch(ctx context.Context, table string, chConn driver.Conn, batch []map[string]interface{}, asyncInsert bool, waitForAsyncInsert bool) (batchTiming, error) {
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
			row[i] = record[col]
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
