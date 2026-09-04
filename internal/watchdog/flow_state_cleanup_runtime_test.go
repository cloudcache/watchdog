package watchdog

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type flowStateCleanupWorkerResult struct {
	worked bool
	err    error
}

type flowStateCleanupRuntimeTestWorker struct {
	mu      sync.Mutex
	results []flowStateCleanupWorkerResult
	calls   int
	entered chan struct{}
	exited  *atomic.Bool
}

func (w *flowStateCleanupRuntimeTestWorker) ReconcileOne(ctx context.Context) (bool, error) {
	w.mu.Lock()
	w.calls++
	if len(w.results) > 0 {
		result := w.results[0]
		w.results = w.results[1:]
		w.mu.Unlock()
		return result.worked, result.err
	}
	entered := w.entered
	w.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		if w.exited != nil {
			w.exited.Store(true)
		}
		return false, ctx.Err()
	}
	return false, nil
}

type flowStateCleanupRuntimeTestCloser struct {
	workerExited *atomic.Bool
	closed       atomic.Bool
	err          error
}

func (c *flowStateCleanupRuntimeTestCloser) Close() error {
	if c.workerExited != nil && !c.workerExited.Load() {
		return errors.New("dependency closed before worker exited")
	}
	c.closed.Store(true)
	return c.err
}

func TestFlowStateCleanupRuntimeRetriesAndRecoversReadiness(t *testing.T) {
	worker := &flowStateCleanupRuntimeTestWorker{results: []flowStateCleanupWorkerResult{
		{err: errors.New("mysql unavailable")},
		{worked: true},
	}}
	runtime, err := newFlowStateCleanupRuntime(worker, nil, 5*time.Millisecond, time.Millisecond, 4*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	eventuallyFlowStateCleanupRuntime(t, func() bool {
		health := runtime.Health()
		return health.Ready && health.ReconcileErrorTotal == 1 && health.ReconcileWorkedTotal == 1 && health.ReconcileIdleTotal > 0
	})
	if err := runtime.Ready(); err != nil {
		t.Fatalf("readiness did not recover: %v", err)
	}
	metrics := string(runtime.PrometheusText())
	if !strings.Contains(metrics, `watchdog_flow_state_cleanup_reconcile_total{result="error"} 1`) || strings.Contains(metrics, "mysql unavailable") {
		t.Fatalf("unexpected metrics: %s", metrics)
	}
}

func TestFlowStateCleanupRuntimeStopsWorkerBeforeDependencies(t *testing.T) {
	var exited atomic.Bool
	worker := &flowStateCleanupRuntimeTestWorker{entered: make(chan struct{}, 1), exited: &exited}
	closer := &flowStateCleanupRuntimeTestCloser{workerExited: &exited}
	runtime, err := newFlowStateCleanupRuntime(worker, []flowStateCleanupDependency{closer}, time.Second, time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-worker.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not enter reconciliation")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if !exited.Load() || !closer.closed.Load() {
		t.Fatalf("worker exited=%t dependency closed=%t", exited.Load(), closer.closed.Load())
	}
	if err := runtime.Start(context.Background()); err == nil {
		t.Fatal("expected closed runtime start to fail")
	}
}

func TestFlowStateCleanupRuntimeRejectsDuplicateStart(t *testing.T) {
	worker := &flowStateCleanupRuntimeTestWorker{}
	runtime, err := newFlowStateCleanupRuntime(worker, nil, time.Millisecond, time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.Start(ctx); err == nil {
		t.Fatal("expected duplicate start to fail")
	}
}

func TestFlowStateCleanupRuntimeJoinsDependencyErrors(t *testing.T) {
	first := errors.New("close writer")
	second := errors.New("close scanner")
	runtime, err := newFlowStateCleanupRuntime(&flowStateCleanupRuntimeTestWorker{}, []flowStateCleanupDependency{
		&flowStateCleanupRuntimeTestCloser{err: first},
		&flowStateCleanupRuntimeTestCloser{err: second},
	}, time.Millisecond, time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.Close()
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("close error = %v", err)
	}
}

func eventuallyFlowStateCleanupRuntime(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met")
}
