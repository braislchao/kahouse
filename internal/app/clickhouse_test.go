package app

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// stubConn is a minimal stub implementing driver.Conn for testing validateTables.
type stubConn struct {
	driver.Conn
	execErr  map[string]error        // keyed by SQL query substring
	queryErr map[string]error        // keyed by SQL query substring
	columns  map[string][]stubColumn // table pattern -> columns (name + type)
}

type stubColumn struct {
	name    string
	colType string
}

func (c *stubConn) Exec(ctx context.Context, query string, args ...interface{}) error {
	for pattern, err := range c.execErr {
		if strings.Contains(query, pattern) {
			return err
		}
	}
	return nil
}

func (c *stubConn) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	for pattern, err := range c.queryErr {
		if strings.Contains(query, pattern) {
			return nil, err
		}
	}
	for pattern, cols := range c.columns {
		if strings.Contains(query, pattern) {
			return &stubRows{columns: cols, pos: 0}, nil
		}
	}
	return &stubRows{columns: nil, pos: 0}, nil
}

// stubRows implements driver.Rows for DESCRIBE TABLE results.
type stubRows struct {
	columns []stubColumn
	pos     int
}

func (r *stubRows) Next() bool {
	return r.pos < len(r.columns)
}

func (r *stubRows) Scan(dest ...interface{}) error {
	if r.pos >= len(r.columns) {
		return fmt.Errorf("no more rows")
	}
	col := r.columns[r.pos]
	vals := []string{col.name, col.colType, "", "", "", "", ""}
	for i, d := range dest {
		if i < len(vals) {
			if dp, ok := d.(*string); ok {
				*dp = vals[i]
			}
		}
	}
	r.pos++
	return nil
}

func (r *stubRows) Close() error                     { return nil }
func (r *stubRows) Err() error                       { return nil }
func (r *stubRows) Columns() []string                { return nil }
func (r *stubRows) Types() []string                  { return nil }
func (r *stubRows) ScanStruct(dest any) error        { return fmt.Errorf("not implemented") }
func (r *stubRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *stubRows) Totals(dest ...any) error         { return nil }
func (r *stubRows) HasData() bool                    { return r.pos < len(r.columns) }

func TestQuoteTableIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "single table", in: "events", want: "`events`"},
		{name: "db and table", in: "analytics.events", want: "`analytics`.`events`"},
		{name: "dotted table name", in: "warehouse.staging.orders.v2.raw", want: "`warehouse`.`staging.orders.v2.raw`"},
		{name: "trim spaces", in: " analytics . events ", want: "`analytics`.`events`"},
		{name: "escaped backticks", in: "db.we`ird", want: "`db`.`we``ird`"},
		{name: "dotted table no db", in: "metrics.sales.daily.v3.agg", want: "`metrics`.`sales.daily.v3.agg`"},
		{name: "empty string", in: "", wantErr: true},
		{name: "leading dot", in: ".table", wantErr: true},
		{name: "trailing dot", in: "db.", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := quoteTableIdentifier(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected error for input %q, got %q", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error for input %q: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("Expected quoted table identifier %q, got %q", tt.want, got)
			}
		})
	}
}

func TestValidateTablesSuccess(t *testing.T) {
	conn := &stubConn{
		columns: map[string][]stubColumn{
			"orders":   {{name: "id", colType: "String"}, {name: "amount", colType: "Float64"}},
			"payments": {{name: "id", colType: "String"}, {name: "status", colType: "String"}},
		},
	}
	tables := []TopicTableMapping{
		{Topic: "orders", Table: "default.orders"},
		{Topic: "payments", Table: "default.payments"},
	}
	tc, err := validateTables(context.Background(), conn, tables, zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("Expected validateTables to succeed, got %v", err)
	}
	if len(tc) != 2 {
		t.Fatalf("Expected 2 table column sets, got %d", len(tc))
	}
	if _, ok := tc["default.orders"]["id"]; !ok {
		t.Fatal("Expected 'id' column in default.orders")
	}
}

func TestValidateTablesFailsOnMissingTable(t *testing.T) {
	conn := &stubConn{
		queryErr: map[string]error{
			"orders": fmt.Errorf("table orders does not exist"),
		},
	}
	tables := []TopicTableMapping{
		{Topic: "orders", Table: "default.orders"},
	}
	_, err := validateTables(context.Background(), conn, tables, zap.NewNop().Sugar())
	if err == nil {
		t.Fatal("Expected validateTables to fail for missing table")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Expected 'does not exist' error, got %q", err.Error())
	}
}

func TestValidateTablesRejectsInvalidTableName(t *testing.T) {
	conn := &stubConn{}
	tables := []TopicTableMapping{
		{Topic: "orders", Table: ""},
	}
	_, err := validateTables(context.Background(), conn, tables, zap.NewNop().Sugar())
	if err == nil {
		t.Fatal("Expected validateTables to reject empty table name")
	}
	if !strings.Contains(err.Error(), "invalid table name") {
		t.Fatalf("Expected 'invalid table name' error, got %q", err.Error())
	}
}

func TestIsRetriableClickHouseError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			name:      "plain error is retriable (network/timeout)",
			err:       fmt.Errorf("connection reset by peer"),
			retriable: true,
		},
		{
			name:      "retriable code 159 (timeout)",
			err:       &clickhouse.Exception{Code: 159, Message: "timeout"},
			retriable: true,
		},
		{
			name:      "retriable code 202 (too many parts)",
			err:       &clickhouse.Exception{Code: 202, Message: "too many parts"},
			retriable: true,
		},
		{
			name:      "retriable code 242 (table read-only)",
			err:       &clickhouse.Exception{Code: 242, Message: "table is read-only"},
			retriable: true,
		},
		{
			name:      "retriable code 999 (keeper exception)",
			err:       &clickhouse.Exception{Code: 999, Message: "keeper exception"},
			retriable: true,
		},
		{
			name:      "retriable code 3 (unexpected end of file)",
			err:       &clickhouse.Exception{Code: 3, Message: "unexpected end of file"},
			retriable: true,
		},
		{
			name:      "non-retriable code 60 (unknown table)",
			err:       &clickhouse.Exception{Code: 60, Message: "unknown table"},
			retriable: false,
		},
		{
			name:      "non-retriable code 62 (syntax error)",
			err:       &clickhouse.Exception{Code: 62, Message: "syntax error"},
			retriable: false,
		},
		{
			name:      "non-retriable code 16 (no such column)",
			err:       &clickhouse.Exception{Code: 16, Message: "no such column"},
			retriable: false,
		},
		{
			name:      "wrapped retriable error",
			err:       fmt.Errorf("wrapped: %w", &clickhouse.Exception{Code: 159, Message: "timeout"}),
			retriable: true,
		},
		{
			name:      "wrapped non-retriable error",
			err:       fmt.Errorf("wrapped: %w", &clickhouse.Exception{Code: 60, Message: "unknown table"}),
			retriable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetriableClickHouseError(tt.err)
			if got != tt.retriable {
				t.Fatalf("isRetriableClickHouseError(%v) = %v, want %v", tt.err, got, tt.retriable)
			}
		})
	}
}

func TestIsRetriableClickHouseErrorAllCodes(t *testing.T) {
	retriableCodes := []int32{3, 107, 159, 164, 202, 203, 209, 210, 241, 242, 252, 285, 319, 425, 999}
	for _, code := range retriableCodes {
		err := &clickhouse.Exception{Code: code, Message: fmt.Sprintf("test code %d", code)}
		if !isRetriableClickHouseError(err) {
			t.Errorf("Expected code %d to be retriable", code)
		}
	}

	nonRetriableCodes := []int32{1, 10, 16, 36, 47, 60, 62, 70, 117}
	for _, code := range nonRetriableCodes {
		err := &clickhouse.Exception{Code: code, Message: fmt.Sprintf("test code %d", code)}
		if isRetriableClickHouseError(err) {
			t.Errorf("Expected code %d to be non-retriable", code)
		}
	}
}
