package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

type flowStateCleanupWorker interface {
	ReconcileOne(context.Context) (bool, error)
}

type flowStateCleanupDependency interface {
	Close() error
}

type FlowStateCleanupRuntimeHealth struct {
	Started              bool      `json:"started"`
	Running              bool      `json:"running"`
	Ready                bool      `json:"ready"`
	ReconcileWorkedTotal uint64    `json:"reconcile_worked_total"`
	ReconcileIdleTotal   uint64    `json:"reconcile_idle_total"`
	ReconcileErrorTotal  uint64    `json:"reconcile_error_total"`
	LastSuccessAt        time.Time `json:"last_success_at,omitempty"`
}

type PlatformRuntimeHealth struct {
	FlowStateCleanup           FlowStateCleanupRuntimeStatus           `json:"flow_state_cleanup"`
	CollectorPrincipalProvider CollectorPrincipalProviderRuntimeStatus `json:"collector_principal_provider"`
}

type FlowStateCleanupRuntimeStatus struct {
	Enabled bool                          `json:"enabled"`
	Health  FlowStateCleanupRuntimeHealth `json:"health"`
}

// FlowStateCleanupRuntime supervises the MySQL-leased state cleanup
// reconciler. A dependency outage degrades readiness and is retried in place;
// it does not terminate the watchdog HTTP process or create a second queue.
type FlowStateCleanupRuntime struct {
	worker       flowStateCleanupWorker
	dependencies []flowStateCleanupDependency
	pollInterval time.Duration
	retryMin     time.Duration
	retryMax     time.Duration

	mu         sync.Mutex
	started    bool
	running    bool
	ready      bool
	closed     bool
	lastError  error
	lastOK     time.Time
	worked     uint64
	idle       uint64
	errors     uint64
	cancel     context.CancelFunc
	wait       sync.WaitGroup
	closeOnce  sync.Once
	closeError error
	controller *FlowStateCleanupJobService
}

func newFlowStateCleanupRuntime(worker flowStateCleanupWorker, dependencies []flowStateCleanupDependency, pollInterval, retryMin, retryMax time.Duration) (*FlowStateCleanupRuntime, error) {
	if worker == nil || pollInterval <= 0 || retryMin <= 0 || retryMax < retryMin {
		return nil, errors.New("flow state-cleanup runtime configuration is invalid")
	}
	for _, dependency := range dependencies {
		if dependency == nil {
			return nil, errors.New("flow state-cleanup runtime dependency is nil")
		}
	}
	return &FlowStateCleanupRuntime{
		worker: worker, dependencies: append([]flowStateCleanupDependency(nil), dependencies...),
		pollInterval: pollInterval, retryMin: retryMin, retryMax: retryMax,
	}, nil
}

func newConfiguredFlowStateCleanupRuntime(store *MySQLStore, cfg FlowStateCleanupConfig) (*FlowStateCleanupRuntime, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("flow state-cleanup MySQL store is required")
	}
	evidence, err := NewMySQLFlowStateCleanupEvidenceProvider(store.db)
	if err != nil {
		return nil, err
	}
	kafkaConfig := cfg.Kafka.FlowCollectKafkaConfig()
	scanner, err := flowcollect.NewKafkaStateKeyScanner(kafkaConfig, cfg.WorkerID)
	if err != nil {
		return nil, err
	}
	reader, err := NewKafkaFlowStateCleanupStateReader(scanner)
	if err != nil {
		_ = scanner.Close()
		return nil, err
	}
	writer, err := flowcollect.NewKafkaStateTombstoneWriter(kafkaConfig, cfg.WorkerID)
	if err != nil {
		_ = scanner.Close()
		return nil, err
	}
	reconciler, err := NewFlowStateCleanupReconciler(store, evidence, reader, writer, cfg.ReconcilerConfig())
	if err != nil {
		_ = writer.Close()
		_ = scanner.Close()
		return nil, err
	}
	runtime, err := newFlowStateCleanupRuntime(
		reconciler,
		[]flowStateCleanupDependency{writer, scanner},
		cfg.PollInterval,
		cfg.RetryMin,
		cfg.RetryMax,
	)
	if err != nil {
		_ = writer.Close()
		_ = scanner.Close()
		return nil, err
	}
	controller, err := NewFlowStateCleanupJobService(store, scanner)
	if err != nil {
		_ = runtime.Close()
		return nil, err
	}
	runtime.controller = controller
	return runtime, nil
}

func (r *FlowStateCleanupRuntime) Controller() FlowStateCleanupJobController {
	if r == nil {
		return nil
	}
	return r.controller
}

func (r *FlowStateCleanupRuntime) Start(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errors.New("flow state-cleanup runtime and context are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("flow state-cleanup runtime is closed")
	}
	if r.started {
		return errors.New("flow state-cleanup runtime is already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.started = true
	r.running = true
	r.cancel = cancel
	r.wait.Add(1)
	go r.run(runCtx)
	return nil
}

func (r *FlowStateCleanupRuntime) run(ctx context.Context) {
	defer r.wait.Done()
	defer func() {
		r.mu.Lock()
		r.running = false
		r.ready = false
		r.mu.Unlock()
	}()
	retryDelay := r.retryMin
	for {
		worked, err := r.worker.ReconcileOne(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			r.recordError(err)
			if !waitFlowStateCleanupRuntime(ctx, retryDelay) {
				return
			}
			if retryDelay < r.retryMax {
				if retryDelay > r.retryMax/2 {
					retryDelay = r.retryMax
				} else {
					retryDelay *= 2
				}
			}
			continue
		}
		r.recordSuccess(worked)
		retryDelay = r.retryMin
		if worked {
			continue
		}
		if !waitFlowStateCleanupRuntime(ctx, r.pollInterval) {
			return
		}
	}
}

func waitFlowStateCleanupRuntime(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *FlowStateCleanupRuntime) recordSuccess(worked bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ready = true
	r.lastError = nil
	r.lastOK = time.Now().UTC()
	if worked {
		r.worked++
	} else {
		r.idle++
	}
}

func (r *FlowStateCleanupRuntime) recordError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ready = false
	r.lastError = err
	r.errors++
}

func (r *FlowStateCleanupRuntime) Health() FlowStateCleanupRuntimeHealth {
	if r == nil {
		return FlowStateCleanupRuntimeHealth{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return FlowStateCleanupRuntimeHealth{
		Started: r.started, Running: r.running, Ready: r.ready,
		ReconcileWorkedTotal: r.worked, ReconcileIdleTotal: r.idle,
		ReconcileErrorTotal: r.errors, LastSuccessAt: r.lastOK,
	}
}

func (r *FlowStateCleanupRuntime) PrometheusText() []byte {
	health := r.Health()
	var out strings.Builder
	out.WriteString("# TYPE watchdog_flow_state_cleanup_worker_up gauge\n")
	out.WriteString("watchdog_flow_state_cleanup_worker_up ")
	out.WriteString(boolMetricValue(health.Running))
	out.WriteByte('\n')
	out.WriteString("# TYPE watchdog_flow_state_cleanup_ready gauge\n")
	out.WriteString("watchdog_flow_state_cleanup_ready ")
	out.WriteString(boolMetricValue(health.Ready))
	out.WriteByte('\n')
	out.WriteString("# TYPE watchdog_flow_state_cleanup_reconcile_total counter\n")
	for _, item := range []struct {
		result string
		value  uint64
	}{
		{result: "worked", value: health.ReconcileWorkedTotal},
		{result: "idle", value: health.ReconcileIdleTotal},
		{result: "error", value: health.ReconcileErrorTotal},
	} {
		out.WriteString("watchdog_flow_state_cleanup_reconcile_total{result=\"")
		out.WriteString(item.result)
		out.WriteString("\"} ")
		out.WriteString(strconv.FormatUint(item.value, 10))
		out.WriteByte('\n')
	}
	out.WriteString("# TYPE watchdog_flow_state_cleanup_last_success_timestamp_seconds gauge\n")
	out.WriteString("watchdog_flow_state_cleanup_last_success_timestamp_seconds ")
	lastSuccess := int64(0)
	if !health.LastSuccessAt.IsZero() {
		lastSuccess = health.LastSuccessAt.Unix()
	}
	out.WriteString(strconv.FormatInt(lastSuccess, 10))
	out.WriteByte('\n')
	return []byte(out.String())
}

func boolMetricValue(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func (r *FlowStateCleanupRuntime) Ready() error {
	if r == nil {
		return errors.New("flow state-cleanup runtime is not initialized")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case !r.started:
		return errors.New("flow state-cleanup runtime has not started")
	case !r.running:
		return errors.New("flow state-cleanup runtime is not running")
	case !r.ready && r.lastError != nil:
		return fmt.Errorf("flow state-cleanup reconciliation failed: %w", r.lastError)
	case !r.ready:
		return errors.New("flow state-cleanup runtime has not completed its first reconciliation")
	default:
		return nil
	}
}

func (r *FlowStateCleanupRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		cancel := r.cancel
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		r.wait.Wait()
		if r.controller != nil {
			r.closeError = errors.Join(r.closeError, r.controller.Close())
		}
		for _, dependency := range r.dependencies {
			r.closeError = errors.Join(r.closeError, dependency.Close())
		}
	})
	return r.closeError
}
