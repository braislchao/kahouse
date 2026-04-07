package app

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// quoteIdentifier wraps a ClickHouse identifier in backticks, escaping any
// embedded backticks to prevent SQL injection via column or table names.
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// quoteTableIdentifier quotes each table path segment (e.g. db.table -> `db`.`table`).
// Returns an error if the name is empty or contains empty segments (e.g. "db..table").
func quoteTableIdentifier(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("table name is empty")
	}
	parts := strings.Split(trimmed, ".")
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

// writeBatch writes a batch of records to a ClickHouse table.
func writeBatch(ctx context.Context, table string, chConn driver.Conn, batch []map[string]interface{}) error {
	if len(batch) == 0 {
		return nil
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
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":          1,
		"wait_for_async_insert": 1,
	}))
	quotedTable, err := quoteTableIdentifier(table)
	if err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES", quotedTable, strings.Join(quoted, ", "))
	batchStmt, err := chConn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare batch insert: %w", err)
	}

	// Append expects all column values for a single row in one variadic call.
	for _, record := range batch {
		row := make([]interface{}, len(columns))
		for i, col := range columns {
			row[i] = record[col]
		}
		if err := batchStmt.Append(row...); err != nil {
			return fmt.Errorf("failed to append row to batch: %w", err)
		}
	}

	if err := batchStmt.Send(); err != nil {
		return fmt.Errorf("failed to send batch insert: %w", err)
	}

	return nil
}
