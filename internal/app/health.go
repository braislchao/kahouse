package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// sinkHealthChecker abstracts the health-relevant parts of a SinkTask for testability.
type sinkHealthChecker interface {
	IsStopped() bool
	StoppedByOperator() bool
	NeedsRecycle() bool
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

// Livez handler returns 200 unless either:
//   - the supervisor has flagged a task for recycle (it exhausted auto-restart for a
//     transient crash), or
//   - every sink task has stopped AND at least one stopped unexpectedly (i.e. without
//     an operator-initiated stop).
//
// Operator-initiated stops (admin API Stop/Restart, SIGTERM) keep /livez at 200 so
// kubelet does not kill the pod during maintenance or graceful shutdown. A single
// transient crash does NOT fail liveness on its own — the supervisor restarts it in
// place; liveness only fails once the supervisor gives up (pod recycle as last resort).
func (h *Health) Livez(w http.ResponseWriter, r *http.Request) {
	if h.livenessShouldFail() {
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := w.Write([]byte("All sink tasks have stopped unexpectedly")); err != nil {
			h.logger.Warnf("Failed to write livez response: %v", err)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		h.logger.Warnf("Failed to write livez response: %v", err)
	}
}

// livenessShouldFail returns true when every registered task is stopped and at
// least one of those stops was unexpected. An empty task set also fails liveness,
// since a process with no tasks configured is misconfigured.
func (h *Health) livenessShouldFail() bool {
	tasks := h.tasks()
	if len(tasks) == 0 {
		return true
	}
	allStopped := true
	anyCrashed := false
	for _, task := range tasks {
		// Backstop: the supervisor exhausted auto-restart for a transient crash and
		// asked for a pod recycle. Fail liveness so kubelet replaces the pod.
		if task.NeedsRecycle() {
			return true
		}
		if !task.IsStopped() {
			allStopped = false
			continue
		}
		if !task.StoppedByOperator() {
			anyCrashed = true
		}
	}
	return allStopped && anyCrashed
}

// Readyz handler returns 200 if the service is ready to serve traffic
func (h *Health) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := h.readinessError(ctx); err != nil {
		h.logger.Warnf("Readiness check failed: %v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, writeErr := w.Write([]byte("Not Ready: " + err.Error())); writeErr != nil {
			h.logger.Warnf("Failed to write readyz response: %v", writeErr)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		h.logger.Warnf("Failed to write readyz response: %v", err)
	}
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
