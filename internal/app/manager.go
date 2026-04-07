package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"go.uber.org/zap"
)

// managedTask tracks a running SinkTask alongside the controls needed to stop or replace it.
type managedTask struct {
	task    *SinkTask
	cancel  context.CancelFunc // cancels the per-task context (external stop)
	done    chan struct{}      // closed when task.Run returns
	mapping TopicTableMapping
}

// TopicStatus is the JSON-serialisable status of a single topic, returned by the admin API.
type TopicStatus struct {
	Topic      string `json:"topic"`
	Table      string `json:"table"`
	Status     string `json:"status"`      // "running" or "stopped"
	RepairMode string `json:"repair_mode"` // "", "dlq", or "skip"
}

// TaskManager is a passive supervisor: it launches configured tasks, monitors their state,
// and provides an HTTP admin API for manual stop, restart, and repair-mode control.
// It does NOT auto-restart failed tasks.
type TaskManager struct {
	mu          sync.RWMutex
	tasks       map[string]*managedTask
	cfg         *Config
	chConn      driver.Conn
	srClient    schemaregistry.Client
	dlqProducer *kafka.Producer
	logger      *zap.SugaredLogger
	parentCtx   context.Context
}

// NewTaskManager creates a TaskManager bound to the given parent context.
// The parent context controls the overall process lifetime; cancelling it stops all tasks.
func NewTaskManager(
	ctx context.Context,
	cfg *Config,
	chConn driver.Conn,
	srClient schemaregistry.Client,
	dlqProducer *kafka.Producer,
	logger *zap.SugaredLogger,
) *TaskManager {
	return &TaskManager{
		tasks:       make(map[string]*managedTask),
		cfg:         cfg,
		chConn:      chConn,
		srClient:    srClient,
		dlqProducer: dlqProducer,
		logger:      logger,
		parentCtx:   ctx,
	}
}

// StartAll launches a SinkTask for every topic-table mapping in the config.
func (m *TaskManager) StartAll() error {
	for _, mapping := range m.cfg.TopicTables {
		if err := m.startTask(mapping); err != nil {
			return fmt.Errorf("failed to start task for topic %s: %w", mapping.Topic, err)
		}
	}
	return nil
}

// startTask creates a new SinkTask, registers it in the manager, and launches it.
func (m *TaskManager) startTask(mapping TopicTableMapping) error {
	task, err := NewSinkTask(mapping, m.cfg, m.chConn, m.srClient, m.dlqProducer, m.logger)
	if err != nil {
		return err
	}

	taskCtx, taskCancel := context.WithCancel(m.parentCtx)
	done := make(chan struct{})

	mt := &managedTask{
		task:    task,
		cancel:  taskCancel,
		done:    done,
		mapping: mapping,
	}

	m.mu.Lock()
	m.tasks[mapping.Topic] = mt
	m.mu.Unlock()

	go func() {
		defer close(done)
		task.Run(taskCtx)
	}()

	m.logger.Infof("Started sink task: topic=%s table=%s format=%s", mapping.Topic, mapping.Table, mapping.Format)
	return nil
}

// Stop gracefully stops the task for the given topic.
// It blocks until the task's Run method returns.
func (m *TaskManager) Stop(topic string) error {
	m.mu.RLock()
	mt, exists := m.tasks[topic]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %q not found", topic)
	}
	if mt.task.IsStopped() {
		return nil
	}

	mt.cancel()
	<-mt.done
	return nil
}

// Restart stops a topic (if running), creates a brand-new SinkTask, and launches it.
// Repair mode is reset to off on restart.
func (m *TaskManager) Restart(topic string) error {
	if m.parentCtx.Err() != nil {
		return fmt.Errorf("cannot restart topic %q: process is shutting down", topic)
	}

	m.mu.RLock()
	mt, exists := m.tasks[topic]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %q not found", topic)
	}

	// Stop current task if still running.
	mt.cancel()
	<-mt.done

	taskRestartsTotal.WithLabelValues(topic).Inc()
	return m.startTask(mt.mapping)
}

// SetRepairMode sets the repair mode on the task for the given topic.
func (m *TaskManager) SetRepairMode(topic string, mode RepairMode) error {
	m.mu.RLock()
	mt, exists := m.tasks[topic]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %q not found", topic)
	}

	mt.task.SetRepairMode(mode)
	m.logger.Infof("Set repair mode for topic %s: %s", topic, mode)
	return nil
}

// ClearRepairMode resets repair mode to off for the given topic.
func (m *TaskManager) ClearRepairMode(topic string) error {
	return m.SetRepairMode(topic, RepairModeOff)
}

// Topics returns the current status of all managed topics.
func (m *TaskManager) Topics() []TopicStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]TopicStatus, 0, len(m.tasks))
	for _, mt := range m.tasks {
		status := "running"
		if mt.task.IsStopped() {
			status = "stopped"
		}
		result = append(result, TopicStatus{
			Topic:      mt.mapping.Topic,
			Table:      mt.mapping.Table,
			Status:     status,
			RepairMode: mt.task.GetRepairMode().String(),
		})
	}
	return result
}

// Snapshot returns a point-in-time slice of health checkers for all managed tasks.
// This is used by the Health checker to get a dynamic view of the task list.
func (m *TaskManager) Snapshot() []sinkHealthChecker {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]sinkHealthChecker, 0, len(m.tasks))
	for _, mt := range m.tasks {
		result = append(result, mt.task)
	}
	return result
}

// Wait blocks until the parent context is cancelled (e.g. SIGINT/SIGTERM),
// then waits for all tasks to finish. The process stays alive even if all tasks
// have stopped, so the admin API can restart them.
func (m *TaskManager) Wait() {
	<-m.parentCtx.Done()

	m.mu.RLock()
	snapshot := make([]*managedTask, 0, len(m.tasks))
	for _, mt := range m.tasks {
		snapshot = append(snapshot, mt)
	}
	m.mu.RUnlock()

	for _, mt := range snapshot {
		<-mt.done
	}
}

// --- HTTP admin handlers ---

func (m *TaskManager) handleListTopics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.Topics())
}

func (m *TaskManager) handleStopTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := m.Stop(topic); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "topic %q stopped", topic)
}

func (m *TaskManager) handleRestartTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := m.Restart(topic); err != nil {
		m.logger.Errorf("Failed to restart topic %s: %v", topic, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "topic %q restarted", topic)
}

func (m *TaskManager) handleSetRepair(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")

	var body struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	mode, err := ParseRepairMode(body.Mode)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if mode == RepairModeOff {
		http.Error(w, "use DELETE /api/topics/{topic}/repair to clear repair mode", http.StatusBadRequest)
		return
	}

	if err := m.SetRepairMode(topic, mode); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "repair mode %q set for topic %q", mode, topic)
}

func (m *TaskManager) handleClearRepair(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := m.ClearRepairMode(topic); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "repair mode cleared for topic %q", topic)
}

// RegisterAdminEndpoints registers the admin API endpoints on the given mux.
// Requires Go 1.22+ ServeMux with method and path-parameter support.
func RegisterAdminEndpoints(mgr *TaskManager, mux *http.ServeMux) {
	mux.HandleFunc("GET /api/topics", mgr.handleListTopics)
	mux.HandleFunc("POST /api/topics/{topic}/stop", mgr.handleStopTopic)
	mux.HandleFunc("POST /api/topics/{topic}/restart", mgr.handleRestartTopic)
	mux.HandleFunc("POST /api/topics/{topic}/repair", mgr.handleSetRepair)
	mux.HandleFunc("DELETE /api/topics/{topic}/repair", mgr.handleClearRepair)
}
