package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type stubSinkChecker struct {
	stopped           bool
	stoppedByOperator bool
	needsRecycle      bool
	topic             string
	assignment        []kafka.TopicPartition
	err               error
}

func (s stubSinkChecker) IsStopped() bool                             { return s.stopped }
func (s stubSinkChecker) StoppedByOperator() bool                     { return s.stoppedByOperator }
func (s stubSinkChecker) NeedsRecycle() bool                          { return s.needsRecycle }
func (s stubSinkChecker) TopicName() string                           { return s.topic }
func (s stubSinkChecker) Assignment() ([]kafka.TopicPartition, error) { return s.assignment, s.err }

func TestHealthReadinessError(t *testing.T) {
	tests := []struct {
		name    string
		pingErr error
		tasks   []sinkHealthChecker
		want    string
	}{
		{
			name:    "clickhouse ping failure",
			pingErr: errors.New("dial tcp refused"),
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
			},
			want: "clickhouse health check failed",
		},
		{
			name:  "no tasks configured",
			tasks: nil,
			want:  "no sink tasks configured",
		},
		{
			name: "one task stopped",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
				stubSinkChecker{topic: "payments", stopped: true},
			},
			want: `sink task for topic "payments" has stopped`,
		},
		{
			name: "one task unassigned",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
				stubSinkChecker{topic: "payments"},
			},
			want: `sink task for topic "payments" has no partition assignment`,
		},
		{
			name: "assignment check error",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", err: errors.New("broker unavailable")},
			},
			want: `sink task for topic "orders" assignment check failed`,
		},
		{
			name: "all tasks healthy",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", assignment: []kafka.TopicPartition{{Partition: 0}}},
				stubSinkChecker{topic: "payments", assignment: []kafka.TopicPartition{{Partition: 1}}},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := &Health{
				logger: zap.NewNop().Sugar(),
				ping: func(context.Context) error {
					return tt.pingErr
				},
				tasks: func() []sinkHealthChecker { return tt.tasks },
			}

			err := health.readinessError(context.Background())
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Expected readiness success, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Expected readiness error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestHealthLivenessAllStopped(t *testing.T) {
	tests := []struct {
		name  string
		tasks []sinkHealthChecker
		want  int
	}{
		{
			name:  "no tasks returns 503",
			tasks: nil,
			want:  http.StatusServiceUnavailable,
		},
		{
			name: "all stopped unexpectedly returns 503",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true},
				stubSinkChecker{topic: "payments", stopped: true},
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name: "all stopped by operator returns 200",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true, stoppedByOperator: true},
				stubSinkChecker{topic: "payments", stopped: true, stoppedByOperator: true},
			},
			want: http.StatusOK,
		},
		{
			name: "all stopped, mixed operator flag returns 503",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true, stoppedByOperator: true},
				stubSinkChecker{topic: "payments", stopped: true},
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name: "some running with stopped-by-operator returns 200",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true, stoppedByOperator: true},
				stubSinkChecker{topic: "payments"},
			},
			want: http.StatusOK,
		},
		{
			name: "some running with crashed task returns 200",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true},
				stubSinkChecker{topic: "payments"},
			},
			want: http.StatusOK,
		},
		{
			name: "task flagged for recycle returns 503 even with others running",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders", stopped: true, needsRecycle: true},
				stubSinkChecker{topic: "payments"},
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name: "all running returns 200",
			tasks: []sinkHealthChecker{
				stubSinkChecker{topic: "orders"},
				stubSinkChecker{topic: "payments"},
			},
			want: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := &Health{
				logger: zap.NewNop().Sugar(),
				ping:   func(context.Context) error { return nil },
				tasks:  func() []sinkHealthChecker { return tt.tasks },
			}
			rr := httptest.NewRecorder()
			health.Livez(rr, httptest.NewRequest("GET", "/livez", nil))
			if rr.Code != tt.want {
				t.Fatalf("Expected status %d, got %d (body: %s)", tt.want, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestTaskStoppedGaugeExistsInRegistry(t *testing.T) {
	taskStopped.WithLabelValues("__test_topic__").Set(0)
	defer taskStopped.DeleteLabelValues("__test_topic__")

	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}
	found := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "kahouse_task_stopped" {
			found = true
			if mf.GetHelp() == "" {
				t.Fatal("Expected non-empty help text for kahouse_task_stopped")
			}
			break
		}
	}
	if !found {
		t.Fatal("Expected kahouse_task_stopped metric to be registered")
	}
}
