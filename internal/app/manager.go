package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"go.uber.org/zap"
)

// managedTask tracks a running SinkTask alongside the controls needed to stop or replace it.
type managedTask struct {
	task      *SinkTask
	cancel    context.CancelFunc // cancels the per-task context (external stop)
	done      chan struct{}      // closed when task.Run returns
	mapping   TopicTableMapping
	startedAt time.Time // when this incarnation of the task was launched
}

// restartTracker holds the supervisor's per-topic backoff state across task incarnations.
type restartTracker struct {
	failures       int       // consecutive transient crashes since the last healthy run
	firstFailureAt time.Time // start of the current crash-loop window (for MaxStuck escalation)
}

// TopicStatus is the JSON-serialisable status of a single topic, returned by the admin API.
type TopicStatus struct {
	Topic      string `json:"topic"`
	Table      string `json:"table"`
	Status     string `json:"status"`      // "running" or "stopped"
	StopReason string `json:"stop_reason"` // "", "operator", "crash", or "table_missing"
	StopClass  string `json:"stop_class"`  // "", "transient", or "fatal" (only set for crashes)
	RepairMode string `json:"repair_mode"` // "", "dlq", or "skip"
}

// TaskManager launches configured tasks, monitors their state, and provides an HTTP
// admin API for manual stop, restart, and repair-mode control. When auto-restart is
// enabled (the default) it also supervises tasks: a task that stops with a transient
// (recoverable) error is restarted in place with exponential backoff. Fatal crashes
// (poison messages, non-retriable ClickHouse errors) and operator stops are left alone.
type TaskManager struct {
	mu           sync.RWMutex
	tasks        map[string]*managedTask
	restart      map[string]*restartTracker // per-topic supervisor backoff state
	cfg          *Config
	chConn       driver.Conn
	srClient     schemaregistry.Client
	dlqProducer  *kafka.Producer
	logger       *zap.SugaredLogger
	parentCtx    context.Context
	tableColumns TableColumns
	// skippedTables holds topics that were not started because their target table
	// failed validation. They never enter tasks (so health/readiness ignore them),
	// but the admin API surfaces them as stopped/table_missing so a configured-but-
	// not-syncing table is visible instead of silently dropped.
	skippedTables []TopicTableMapping
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
	tableColumns TableColumns,
) *TaskManager {
	return &TaskManager{
		tasks:        make(map[string]*managedTask),
		restart:      make(map[string]*restartTracker),
		cfg:          cfg,
		chConn:       chConn,
		srClient:     srClient,
		dlqProducer:  dlqProducer,
		logger:       logger,
		parentCtx:    ctx,
		tableColumns: tableColumns,
	}
}

// StartAll launches a SinkTask for every topic-table mapping in the config.
func (m *TaskManager) StartAll() error {
	for _, mapping := range m.cfg.TopicTables {
		if _, ok := m.tableColumns[mapping.Table]; !ok {
			m.logger.Warnf("Skipping sink task for topic %s: table %q did not validate", mapping.Topic, mapping.Table)
			m.skippedTables = append(m.skippedTables, mapping)
			continue
		}
		if err := m.startTask(mapping); err != nil {
			return fmt.Errorf("failed to start task for topic %s: %w", mapping.Topic, err)
		}
	}
	return nil
}

// startTask creates a new SinkTask, registers it in the manager, and launches it.
func (m *TaskManager) startTask(mapping TopicTableMapping) error {
	task, err := NewSinkTask(mapping, m.cfg, m.chConn, m.srClient, m.dlqProducer, m.logger, m.tableColumns[mapping.Table])
	if err != nil {
		return err
	}

	taskCtx, taskCancel := context.WithCancel(m.parentCtx)
	done := make(chan struct{})

	mt := &managedTask{
		task:      task,
		cancel:    taskCancel,
		done:      done,
		mapping:   mapping,
		startedAt: time.Now(),
	}

	m.mu.Lock()
	m.tasks[mapping.Topic] = mt
	m.mu.Unlock()

	go func() {
		task.Run(taskCtx)
		close(done)
		// After the task exits, let the supervisor decide whether to auto-restart it.
		m.handleTaskExit(mt)
	}()

	taskStopped.WithLabelValues(mapping.Topic).Set(0)

	m.logger.Infof("Started sink task: topic=%s table=%s format=%s", mapping.Topic, mapping.Table, mapping.Format)
	return nil
}

// autoRestartEnabled reports whether the supervisor should act on task exits.
func (m *TaskManager) autoRestartEnabled() bool {
	return m.cfg != nil && m.cfg.AutoRestart.Enabled != nil && *m.cfg.AutoRestart.Enabled
}

// handleTaskExit is invoked on the task goroutine once Run returns. When auto-restart
// is enabled and the task stopped with a transient (recoverable) error, it waits a
// backoff interval and relaunches the task in place. Operator stops, graceful shutdown,
// and fatal crashes are left untouched. If a task keeps crash-looping past
// auto_restart.max_stuck_s, it is flagged for a pod recycle via /livez.
func (m *TaskManager) handleTaskExit(mt *managedTask) {
	if !m.autoRestartEnabled() {
		return
	}
	topic := mt.mapping.Topic
	// Operator stops and process shutdown are intentional — never auto-restart.
	if mt.task.StoppedByOperator() || m.parentCtx.Err() != nil {
		return
	}
	// Only transient crashes are eligible; fatal crashes wait for an operator.
	if mt.task.StopClass() != StopClassTransient {
		m.logger.Warnf("auto-restart: topic %s stopped (class=%q); leaving stopped for operator intervention", topic, mt.task.StopClass())
		return
	}

	ar := m.cfg.AutoRestart

	m.mu.Lock()
	// Identity guard: bail if this incarnation was already replaced (e.g. operator Restart).
	if cur, ok := m.tasks[topic]; !ok || cur != mt {
		m.mu.Unlock()
		return
	}
	tr := m.restart[topic]
	if tr == nil {
		tr = &restartTracker{}
		m.restart[topic] = tr
	}
	// Reset the crash-loop window if this incarnation ran healthily long enough.
	if time.Since(mt.startedAt) >= time.Duration(ar.ResetAfterS)*time.Second {
		tr.failures = 0
		tr.firstFailureAt = time.Time{}
	}
	if tr.failures == 0 {
		tr.firstFailureAt = time.Now()
	}
	tr.failures++
	failures := tr.failures
	stuckFor := time.Since(tr.firstFailureAt)
	m.mu.Unlock()

	// Backstop: crash-looping past the limit → escalate to a pod recycle via /livez.
	if ar.MaxStuckS > 0 && stuckFor >= time.Duration(ar.MaxStuckS)*time.Second {
		mt.task.MarkNeedsRecycle()
		taskRecycleEscalationsTotal.WithLabelValues(topic).Inc()
		m.logger.Errorf("auto-restart: topic %s still crashing after %s (%d attempts); escalating to pod recycle via /livez",
			topic, stuckFor.Round(time.Second), failures)
		return
	}

	backoff := computeBackoff(ar, failures)
	m.logger.Warnf("auto-restart: topic %s crashed (transient); scheduling restart #%d in %s", topic, failures, backoff.Round(time.Millisecond))

	select {
	case <-time.After(backoff):
	case <-m.parentCtx.Done():
		return
	}

	// Re-check identity and shutdown state before relaunching.
	m.mu.RLock()
	cur, ok := m.tasks[topic]
	m.mu.RUnlock()
	if !ok || cur != mt || m.parentCtx.Err() != nil {
		return
	}

	taskAutoRestartsTotal.WithLabelValues(topic).Inc()
	m.logger.Infof("auto-restart: restarting sink task for topic %s (attempt #%d)", topic, failures)
	if err := m.startTask(mt.mapping); err != nil {
		// No new task will be launched, so the topic would silently stay dead.
		// Flag for a pod recycle so the process is replaced instead.
		m.logger.Errorf("auto-restart: failed to restart topic %s: %v — escalating to pod recycle", topic, err)
		mt.task.MarkNeedsRecycle()
		taskRecycleEscalationsTotal.WithLabelValues(topic).Inc()
	}
}

// computeBackoff returns the delay before restart attempt number `failures`
// (1-based): exponential from InitialBackoffMs, doubling per failure, capped at
// MaxBackoffMs, plus up to 25% jitter to avoid synchronized restarts across topics.
func computeBackoff(ar AutoRestartConfig, failures int) time.Duration {
	initial := time.Duration(ar.InitialBackoffMs) * time.Millisecond
	maxBackoff := time.Duration(ar.MaxBackoffMs) * time.Millisecond
	if initial <= 0 {
		initial = time.Second
	}
	if maxBackoff < initial {
		maxBackoff = initial
	}
	d := initial
	for i := 1; i < failures && d < maxBackoff; i++ {
		d *= 2
		if d >= maxBackoff {
			d = maxBackoff
			break
		}
	}
	if d > 0 {
		d += time.Duration(rand.Int63n(int64(d)/4 + 1))
	}
	return d
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

	mt.task.MarkOperatorStop()
	mt.cancel()
	<-mt.done
	return nil
}

// Start launches a stopped topic. Returns an error if the topic is already running.
func (m *TaskManager) Start(topic string) error {
	if m.parentCtx.Err() != nil {
		return fmt.Errorf("cannot start topic %q: process is shutting down", topic)
	}

	m.mu.Lock()
	mt, exists := m.tasks[topic]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("topic %q not found", topic)
	}
	if !mt.task.IsStopped() {
		m.mu.Unlock()
		return fmt.Errorf("topic %q is already running", topic)
	}
	// Hold the lock reference to done, then wait outside the lock to avoid
	// blocking other operations, but re-acquire for startTask.
	done := mt.done
	mapping := mt.mapping
	m.mu.Unlock()

	<-done // ensure previous Run has fully returned
	return m.startTask(mapping)
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
	mt.task.MarkOperatorStop()
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
		stopReason := ""
		stopClass := ""
		if mt.task.IsStopped() {
			status = "stopped"
			if mt.task.StoppedByOperator() {
				stopReason = "operator"
			} else {
				stopReason = "crash"
				stopClass = mt.task.StopClass().String()
			}
		}
		result = append(result, TopicStatus{
			Topic:      mt.mapping.Topic,
			Table:      mt.mapping.Table,
			Status:     status,
			StopReason: stopReason,
			StopClass:  stopClass,
			RepairMode: mt.task.GetRepairMode().String(),
		})
	}
	// Topics configured but never started because their table did not validate.
	// Surfaced as stopped/table_missing so they are visible in the admin API/TUI.
	for _, mapping := range m.skippedTables {
		result = append(result, TopicStatus{
			Topic:      mapping.Topic,
			Table:      mapping.Table,
			Status:     "stopped",
			StopReason: "table_missing",
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

	// Mark all tasks as operator-stopped before waiting so /livez never observes
	// a stopped task without the flag set during process shutdown.
	for _, mt := range snapshot {
		mt.task.MarkOperatorStop()
	}
	for _, mt := range snapshot {
		<-mt.done
	}
}

// --- HTTP admin handlers ---

func (m *TaskManager) handleListTopics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(m.Topics()); err != nil {
		m.logger.Errorf("Failed to encode topics response: %v", err)
	}
}

func (m *TaskManager) handleStopTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := m.Stop(topic); err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprintf(w, "topic %q stopped", topic); err != nil {
		m.logger.Errorf("Failed to write stop response for topic %s: %v", topic, err)
	}
}

func (m *TaskManager) handleStartTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := m.Start(topic); err != nil {
		m.logger.Errorf("Failed to start topic %s: %v", topic, err)
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		} else if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprintf(w, "topic %q started", topic); err != nil {
		m.logger.Errorf("Failed to write start response for topic %s: %v", topic, err)
	}
}

func (m *TaskManager) handleRestartTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := m.Restart(topic); err != nil {
		m.logger.Errorf("Failed to restart topic %s: %v", topic, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprintf(w, "topic %q restarted", topic); err != nil {
		m.logger.Errorf("Failed to write restart response for topic %s: %v", topic, err)
	}
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
	if _, err := fmt.Fprintf(w, "repair mode %q set for topic %q", mode, topic); err != nil {
		m.logger.Errorf("Failed to write set repair response for topic %s: %v", topic, err)
	}
}

func (m *TaskManager) handleClearRepair(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := m.ClearRepairMode(topic); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprintf(w, "repair mode cleared for topic %q", topic); err != nil {
		m.logger.Errorf("Failed to write clear repair response for topic %s: %v", topic, err)
	}
}

// RegisterAdminEndpoints registers the admin API endpoints on the given mux.
// Requires Go 1.22+ ServeMux with method and path-parameter support.
func RegisterAdminEndpoints(mgr *TaskManager, mux *http.ServeMux) {
	mux.HandleFunc("GET /api/topics", mgr.handleListTopics)
	mux.HandleFunc("POST /api/topics/{topic}/stop", mgr.handleStopTopic)
	mux.HandleFunc("POST /api/topics/{topic}/start", mgr.handleStartTopic)
	mux.HandleFunc("POST /api/topics/{topic}/restart", mgr.handleRestartTopic)
	mux.HandleFunc("POST /api/topics/{topic}/repair", mgr.handleSetRepair)
	mux.HandleFunc("DELETE /api/topics/{topic}/repair", mgr.handleClearRepair)
}
