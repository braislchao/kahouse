package app

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestStopClassString(t *testing.T) {
	tests := []struct {
		class StopClass
		want  string
	}{
		{StopClassUnknown, ""},
		{StopClassTransient, "transient"},
		{StopClassFatal, "fatal"},
	}
	for _, tt := range tests {
		if got := tt.class.String(); got != tt.want {
			t.Fatalf("StopClass(%d).String() = %q, want %q", tt.class, got, tt.want)
		}
	}
}

func TestClassifyWriteError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want StopClass
	}{
		{"nil error", nil, StopClassUnknown},
		{"context deadline exceeded", context.DeadlineExceeded, StopClassTransient},
		{"context canceled", context.Canceled, StopClassTransient},
		{"network error", errors.New("dial tcp: connection refused"), StopClassTransient},
		{"clickhouse memory limit (241)", &clickhouse.Exception{Code: 241, Message: "memory limit exceeded"}, StopClassTransient},
		{"clickhouse socket timeout (209)", &clickhouse.Exception{Code: 209, Message: "socket timeout"}, StopClassTransient},
		{"clickhouse table missing (60)", &clickhouse.Exception{Code: 60, Message: "table does not exist"}, StopClassFatal},
		{"clickhouse type mismatch (53)", &clickhouse.Exception{Code: 53, Message: "type mismatch"}, StopClassFatal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyWriteError(tt.err); got != tt.want {
				t.Fatalf("classifyWriteError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
