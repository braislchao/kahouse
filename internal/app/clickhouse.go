package app

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// quoteIdentifier wraps a ClickHouse identifier in backticks, escaping any
// embedded backticks to prevent SQL injection via column or table names.
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// writeBatch writes a batch of records to a ClickHouse table.
func writeBatch(ctx context.Context, table string, chConn driver.Conn, batch []map[string]interface{}, sugar *zap.SugaredLogger) error {
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
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES", quoteIdentifier(table), strings.Join(quoted, ", "))
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
