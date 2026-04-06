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

type consumerAssignmentChecker interface {
	Assignment() ([]kafka.TopicPartition, error)
}

// Health contains health check state
type Health struct {
	logger    *zap.SugaredLogger
	ping      func(context.Context) error
	consumers []consumerAssignmentChecker
}

// NewHealth creates a new Health checker
func NewHealth(logger *zap.SugaredLogger, chConn driver.Conn, consumers []*kafka.Consumer) *Health {
	assignmentCheckers := make([]consumerAssignmentChecker, len(consumers))
	for i, consumer := range consumers {
		assignmentCheckers[i] = consumer
	}

	return &Health{
		logger:    logger,
		ping:      chConn.Ping,
		consumers: assignmentCheckers,
	}
}

// Livez handler returns 200 if the process is alive (not deadlocked)
func (h *Health) Livez(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
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

	if len(h.consumers) == 0 {
		return fmt.Errorf("no Kafka consumers configured")
	}

	for i, consumer := range h.consumers {
		assignment, err := consumer.Assignment()
		if err != nil {
			return fmt.Errorf("consumer %d assignment check failed: %w", i, err)
		}
		if len(assignment) == 0 {
			return fmt.Errorf("consumer %d has no partition assignment", i)
		}
	}

	return nil
}

// RegisterHealthEndpoints registers the health check endpoints on the given mux
func RegisterHealthEndpoints(h *Health, mux *http.ServeMux) {
	mux.HandleFunc("/livez", h.Livez)
	mux.HandleFunc("/readyz", h.Readyz)
}
