package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

// sinkHealthChecker abstracts the health-relevant parts of a SinkTask for testability.
type sinkHealthChecker interface {
	IsStopped() bool
	TopicName() string
	Assignment() ([]kafka.TopicPartition, error)
}

// Health contains health check state
type Health struct {
	logger *zap.SugaredLogger
	ping   func(context.Context) error
	tasks  func() []sinkHealthChecker
}

// NewHealth creates a new Health checker.
// The tasks function is called on each health check to get a live snapshot,
// supporting dynamic task management (tasks may be stopped and restarted at runtime).
func NewHealth(logger *zap.SugaredLogger, chConn driver.Conn, tasks func() []sinkHealthChecker) *Health {
	return &Health{
		logger: logger,
		ping:   chConn.Ping,
		tasks:  tasks,
	}
}

// Livez handler returns 200 if at least one sink task is still running.
// Returns 503 when all tasks have stopped, signalling that the process should be restarted.
func (h *Health) Livez(w http.ResponseWriter, r *http.Request) {
	if h.allTasksStopped() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("All sink tasks have stopped"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// allTasksStopped returns true when every registered task has exited.
func (h *Health) allTasksStopped() bool {
	tasks := h.tasks()
	if len(tasks) == 0 {
		return true
	}
	for _, task := range tasks {
		if !task.IsStopped() {
			return false
		}
	}
	return true
}

// Readyz handler returns 200 if the service is ready to serve traffic
func (h *Health) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := h.readinessError(ctx); err != nil {
		h.logger.Warnf("Readiness check failed: %v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Not Ready: " + err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *Health) readinessError(ctx context.Context) error {
	if err := h.ping(ctx); err != nil {
		return fmt.Errorf("clickhouse health check failed: %w", err)
	}

	tasks := h.tasks()
	if len(tasks) == 0 {
		return fmt.Errorf("no sink tasks configured")
	}

	for _, task := range tasks {
		topic := task.TopicName()
		if task.IsStopped() {
			return fmt.Errorf("sink task for topic %q has stopped", topic)
		}
		assignment, err := task.Assignment()
		if err != nil {
			return fmt.Errorf("sink task for topic %q assignment check failed: %w", topic, err)
		}
		if len(assignment) == 0 {
			return fmt.Errorf("sink task for topic %q has no partition assignment", topic)
		}
	}

	return nil
}

// RegisterHealthEndpoints registers the health check endpoints on the given mux
func RegisterHealthEndpoints(h *Health, mux *http.ServeMux) {
	mux.HandleFunc("/livez", h.Livez)
	mux.HandleFunc("/readyz", h.Readyz)
}
