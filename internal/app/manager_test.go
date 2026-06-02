package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestParseRepairMode(t *testing.T) {
	tests := []struct {
		input   string
		want    RepairMode
		wantErr bool
	}{
		{input: "", want: RepairModeOff},
		{input: "off", want: RepairModeOff},
		{input: "dlq", want: RepairModeDLQ},
		{input: "skip", want: RepairModeSkip},
		{input: "invalid", wantErr: true},
		{input: "DLQ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%q", tt.input), func(t *testing.T) {
			got, err := ParseRepairMode(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected error for input %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error for input %q: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Expected %v for input %q, got %v", tt.want, tt.input, got)
			}
		})
	}
}

func TestRepairModeString(t *testing.T) {
	if s := RepairModeOff.String(); s != "" {
		t.Fatalf("Expected empty string for RepairModeOff, got %q", s)
	}
	if s := RepairModeDLQ.String(); s != "dlq" {
		t.Fatalf("Expected dlq for RepairModeDLQ, got %q", s)
	}
	if s := RepairModeSkip.String(); s != "skip" {
		t.Fatalf("Expected skip for RepairModeSkip, got %q", s)
	}
}

func TestTaskManagerTopics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	crashedTask := &SinkTask{mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"}}
	crashedTask.stopped.Store(true)

	operatorStoppedTask := &SinkTask{mapping: TopicTableMapping{Topic: "invoices", Table: "default.invoices"}}
	operatorStoppedTask.stopped.Store(true)
	operatorStoppedTask.MarkOperatorStop()

	runningTask := &SinkTask{mapping: TopicTableMapping{Topic: "payments", Table: "default.payments"}}

	mgr.tasks["orders"] = &managedTask{
		task:    crashedTask,
		mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"},
		done:    make(chan struct{}),
	}
	mgr.tasks["invoices"] = &managedTask{
		task:    operatorStoppedTask,
		mapping: TopicTableMapping{Topic: "invoices", Table: "default.invoices"},
		done:    make(chan struct{}),
	}
	mgr.tasks["payments"] = &managedTask{
		task:    runningTask,
		mapping: TopicTableMapping{Topic: "payments", Table: "default.payments"},
		done:    make(chan struct{}),
	}

	topics := mgr.Topics()
	if len(topics) != 3 {
		t.Fatalf("Expected 3 topics, got %d", len(topics))
	}

	statusByTopic := make(map[string]TopicStatus)
	for _, ts := range topics {
		statusByTopic[ts.Topic] = ts
	}

	if statusByTopic["orders"].Status != "stopped" {
		t.Fatalf("Expected orders to be stopped, got %q", statusByTopic["orders"].Status)
	}
	if statusByTopic["orders"].StopReason != "crash" {
		t.Fatalf("Expected orders stop_reason=crash, got %q", statusByTopic["orders"].StopReason)
	}
	if statusByTopic["invoices"].Status != "stopped" {
		t.Fatalf("Expected invoices to be stopped, got %q", statusByTopic["invoices"].Status)
	}
	if statusByTopic["invoices"].StopReason != "operator" {
		t.Fatalf("Expected invoices stop_reason=operator, got %q", statusByTopic["invoices"].StopReason)
	}
	if statusByTopic["payments"].Status != "running" {
		t.Fatalf("Expected payments to be running, got %q", statusByTopic["payments"].Status)
	}
	if statusByTopic["payments"].StopReason != "" {
		t.Fatalf("Expected payments stop_reason to be empty, got %q", statusByTopic["payments"].StopReason)
	}
}

func TestTaskManagerSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task1 := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task2 := &SinkTask{mapping: TopicTableMapping{Topic: "payments"}}

	mgr.tasks["orders"] = &managedTask{task: task1, mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["payments"] = &managedTask{task: task2, mapping: TopicTableMapping{Topic: "payments"}}

	snap := mgr.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Expected 2 health checkers in snapshot, got %d", len(snap))
	}
}

func TestTaskManagerSetRepairMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{task: task, mapping: TopicTableMapping{Topic: "orders"}}

	if err := mgr.SetRepairMode("orders", RepairModeDLQ); err != nil {
		t.Fatalf("Failed to set repair mode: %v", err)
	}
	if task.GetRepairMode() != RepairModeDLQ {
		t.Fatalf("Expected DLQ repair mode, got %v", task.GetRepairMode())
	}

	if err := mgr.ClearRepairMode("orders"); err != nil {
		t.Fatalf("Failed to clear repair mode: %v", err)
	}
	if task.GetRepairMode() != RepairModeOff {
		t.Fatalf("Expected Off repair mode after clear, got %v", task.GetRepairMode())
	}

	if err := mgr.SetRepairMode("missing", RepairModeDLQ); err == nil {
		t.Fatal("Expected error for non-existent topic")
	}
}

func TestAdminHandlerListTopics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders", Table: "default.orders"},
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/topics", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", rr.Code)
	}

	var topics []TopicStatus
	if err := json.NewDecoder(rr.Body).Decode(&topics); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if len(topics) != 1 || topics[0].Topic != "orders" {
		t.Fatalf("Unexpected topics response: %+v", topics)
	}
}

func TestAdminHandlerSetRepair(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	// Set DLQ mode
	body := strings.NewReader(`{"mode":"dlq"}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/topics/orders/repair", body)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d (body: %s)", rr.Code, rr.Body.String())
	}
	if task.GetRepairMode() != RepairModeDLQ {
		t.Fatalf("Expected DLQ mode after API call, got %v", task.GetRepairMode())
	}

	// Clear repair mode
	rr = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/api/topics/orders/repair", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", rr.Code)
	}
	if task.GetRepairMode() != RepairModeOff {
		t.Fatalf("Expected Off mode after DELETE, got %v", task.GetRepairMode())
	}

	// Invalid mode
	body = strings.NewReader(`{"mode":"invalid"}`)
	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/topics/orders/repair", body)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("Expected 400 for invalid mode, got %d", rr.Code)
	}

	// Non-existent topic
	body = strings.NewReader(`{"mode":"dlq"}`)
	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/topics/missing/repair", body)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 for missing topic, got %d", rr.Code)
	}
}

func TestAdminHandlerStopNonExistentTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/topics/missing/stop", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 for missing topic, got %d", rr.Code)
	}
}

func TestAdminHandlerStartAlreadyRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    make(chan struct{}),
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/topics/orders/start", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("Expected 409 for already-running topic, got %d (body: %s)", rr.Code, rr.Body.String())
	}
}

func TestTaskManagerStartRejectsRunningTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    make(chan struct{}),
	}

	err := mgr.Start("orders")
	if err == nil {
		t.Fatal("Expected error when starting an already-running topic")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Fatalf("Expected 'already running' error, got %q", err.Error())
	}
}

func TestHandleStartTopicConcurrentSafety(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    make(chan struct{}),
	}

	mux := http.NewServeMux()
	RegisterAdminEndpoints(mgr, mux)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			rr := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/api/topics/orders/start", nil)
			mux.ServeHTTP(rr, req)
		}()
		go func() {
			defer wg.Done()
			_ = mgr.Topics()
		}()
	}
	wg.Wait()
}

func TestTaskManagerStartRejectsDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task.stopped.Store(true)
	done := make(chan struct{})
	close(done)
	mgr.tasks["orders"] = &managedTask{
		task:    task,
		mapping: TopicTableMapping{Topic: "orders"},
		done:    done,
	}

	cancel()

	err := mgr.Start("orders")
	if err == nil {
		t.Fatal("Expected error when starting topic during shutdown")
	}
	if !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("Expected 'shutting down' error, got %q", err.Error())
	}
}

// TestSinkTaskOperatorStopFlag verifies the StoppedByOperator flag is independent
// of the IsStopped flag and only flips when MarkOperatorStop is called.
func TestSinkTaskOperatorStopFlag(t *testing.T) {
	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	if task.StoppedByOperator() {
		t.Fatal("Expected StoppedByOperator to be false on a fresh task")
	}
	if task.IsStopped() {
		t.Fatal("Expected IsStopped to be false on a fresh task")
	}

	task.MarkOperatorStop()
	if !task.StoppedByOperator() {
		t.Fatal("Expected StoppedByOperator to be true after MarkOperatorStop")
	}
	if task.IsStopped() {
		t.Fatal("MarkOperatorStop must not flip IsStopped")
	}

	task.stopped.Store(true)
	if !task.IsStopped() {
		t.Fatal("Expected IsStopped to be true after stopped.Store(true)")
	}
	if !task.StoppedByOperator() {
		t.Fatal("StoppedByOperator must remain true once set")
	}
}

// TestTaskManagerStopMarksOperatorFlag verifies that TaskManager.Stop marks the
// task as operator-stopped before cancelling its context.
func TestTaskManagerStopMarksOperatorFlag(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: parentCtx,
		logger:    zap.NewNop().Sugar(),
	}

	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	taskCtx, taskCancel := context.WithCancel(parentCtx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		<-taskCtx.Done()
		task.stopped.Store(true)
	}()

	mgr.tasks["orders"] = &managedTask{
		task:    task,
		cancel:  taskCancel,
		done:    done,
		mapping: TopicTableMapping{Topic: "orders"},
	}

	if err := mgr.Stop("orders"); err != nil {
		t.Fatalf("Stop returned unexpected error: %v", err)
	}

	if !task.IsStopped() {
		t.Fatal("Expected task to be stopped after Stop returns")
	}
	if !task.StoppedByOperator() {
		t.Fatal("Expected StoppedByOperator to be true after admin Stop")
	}

	health := &Health{
		logger: zap.NewNop().Sugar(),
		ping:   func(context.Context) error { return nil },
		tasks:  mgr.Snapshot,
	}
	rr := httptest.NewRecorder()
	health.Livez(rr, httptest.NewRequest("GET", "/livez", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("Expected /livez 200 after operator stop, got %d (body: %s)", rr.Code, rr.Body.String())
	}
}

// TestTaskManagerWaitMarksAllTasksOperatorStop verifies that on parent-context
// cancellation, Wait marks every task as operator-stopped.
func TestTaskManagerWaitMarksAllTasksOperatorStop(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())

	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		parentCtx: parentCtx,
		logger:    zap.NewNop().Sugar(),
	}

	tasks := []*SinkTask{
		{mapping: TopicTableMapping{Topic: "orders"}},
		{mapping: TopicTableMapping{Topic: "payments"}},
	}
	for _, task := range tasks {
		task := task
		taskCtx, taskCancel := context.WithCancel(parentCtx)
		_ = taskCancel
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-taskCtx.Done()
			task.stopped.Store(true)
		}()
		mgr.tasks[task.mapping.Topic] = &managedTask{
			task:    task,
			cancel:  taskCancel,
			done:    done,
			mapping: task.mapping,
		}
	}

	parentCancel()
	mgr.Wait()

	for _, task := range tasks {
		if !task.IsStopped() {
			t.Fatalf("Expected task %q to be stopped after Wait", task.mapping.Topic)
		}
		if !task.StoppedByOperator() {
			t.Fatalf("Expected task %q to be marked operator-stopped after Wait", task.mapping.Topic)
		}
	}
}

func TestComputeBackoff(t *testing.T) {
	ar := AutoRestartConfig{InitialBackoffMs: 1000, MaxBackoffMs: 16000}
	tests := []struct {
		failures int
		min, max time.Duration // base .. base+25% jitter
	}{
		{1, 1000 * time.Millisecond, 1250 * time.Millisecond},
		{2, 2000 * time.Millisecond, 2500 * time.Millisecond},
		{3, 4000 * time.Millisecond, 5000 * time.Millisecond},
		{5, 16000 * time.Millisecond, 20000 * time.Millisecond},  // capped at max + jitter
		{50, 16000 * time.Millisecond, 20000 * time.Millisecond}, // stays capped, no overflow
	}
	for _, tt := range tests {
		for i := 0; i < 20; i++ { // sample repeatedly since jitter is random
			got := computeBackoff(ar, tt.failures)
			if got < tt.min || got > tt.max {
				t.Fatalf("computeBackoff(failures=%d) = %s, want within [%s, %s]", tt.failures, got, tt.min, tt.max)
			}
		}
	}
}

// newSupervisedManager builds a TaskManager with auto-restart enabled (defaults applied)
// and a single stopped task registered, without touching Kafka or ClickHouse.
func newSupervisedManager(task *SinkTask) (*TaskManager, *managedTask, context.CancelFunc) {
	cfg := &Config{}
	applyDefaults(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	mgr := &TaskManager{
		tasks:     make(map[string]*managedTask),
		restart:   make(map[string]*restartTracker),
		cfg:       cfg,
		parentCtx: ctx,
		logger:    zap.NewNop().Sugar(),
	}
	mt := &managedTask{task: task, mapping: TopicTableMapping{Topic: task.mapping.Topic}, startedAt: time.Now()}
	mgr.tasks[task.mapping.Topic] = mt
	return mgr, mt, cancel
}

func TestHandleTaskExitLeavesFatalCrashStopped(t *testing.T) {
	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task.stopped.Store(true)
	task.setStopClass(StopClassFatal)

	mgr, mt, cancel := newSupervisedManager(task)
	defer cancel()

	mgr.handleTaskExit(mt)
	if task.NeedsRecycle() {
		t.Fatal("Expected fatal crash not to be flagged for recycle")
	}
}

func TestHandleTaskExitSkipsOperatorStop(t *testing.T) {
	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task.stopped.Store(true)
	task.setStopClass(StopClassTransient)
	task.MarkOperatorStop()

	mgr, mt, cancel := newSupervisedManager(task)
	defer cancel()

	mgr.handleTaskExit(mt)
	if task.NeedsRecycle() {
		t.Fatal("Expected operator-stopped task not to be flagged for recycle")
	}
}

func TestHandleTaskExitDisabledIsNoop(t *testing.T) {
	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task.stopped.Store(true)
	task.setStopClass(StopClassTransient)

	mgr, mt, cancel := newSupervisedManager(task)
	defer cancel()
	*mgr.cfg.AutoRestart.Enabled = false

	mgr.handleTaskExit(mt)
	if task.NeedsRecycle() {
		t.Fatal("Expected disabled supervisor not to flag for recycle")
	}
}

func TestHandleTaskExitEscalatesWhenStuck(t *testing.T) {
	task := &SinkTask{mapping: TopicTableMapping{Topic: "orders"}}
	task.stopped.Store(true)
	task.setStopClass(StopClassTransient)

	mgr, mt, cancel := newSupervisedManager(task)
	defer cancel()

	// Seed a crash-loop window that began past max_stuck_s ago so the next transient
	// exit escalates to a pod recycle instead of restarting.
	mgr.restart["orders"] = &restartTracker{
		failures:       3,
		firstFailureAt: time.Now().Add(-time.Duration(mgr.cfg.AutoRestart.MaxStuckS+60) * time.Second),
	}

	mgr.handleTaskExit(mt)
	if !task.NeedsRecycle() {
		t.Fatal("Expected task crash-looping past max_stuck_s to be flagged for recycle")
	}
}
