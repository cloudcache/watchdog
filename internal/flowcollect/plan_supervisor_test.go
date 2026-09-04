package flowcollect

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlanSupervisorActivatesValidatedRevisionAndPreservesQueuedDatagramBoundary(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	writePlanPublicKey(t, keyPath, publicKey)
	first := validPlan(now)
	writeSignedPlan(t, planPath, first, privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, filepath.Join(dir, "history"), 4, now)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := OpenWAL(filepath.Join(dir, "wal"), first.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	runner := &Runner{Registry: history.Active(), Plans: history}
	if err := runner.ActivateRegistry(history.Active()); err != nil {
		t.Fatal(err)
	}
	metrics := &Metrics{}
	runtime := NewRuntimeState()
	validated := 0
	supervisor, err := NewPlanSupervisor(time.Minute, history, wal, runner, metrics, runtime, func(registries []*Registry) error {
		validated++
		if len(registries) != 2 {
			t.Fatalf("validated registries=%d, want 2", len(registries))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	activated := uint64(0)
	supervisor.OnActivated = func(registry *Registry) { activated = registry.Plan().Revision }
	second := validPlan(now)
	second.Revision = 2
	second.PartitionMapVersion = 2
	second.NotBefore = now.Add(5 * time.Second)
	writeSignedPlan(t, planPath, second, privateKey)
	if err := supervisor.Refresh(now.Add(6 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if validated != 1 || runner.ActiveRegistry().plan.Revision != 2 || activated != 2 || metrics.PlanRefreshChanges.Load() != 1 {
		t.Fatalf("validated=%d revision=%d callback_revision=%d metrics=%+v", validated, runner.ActiveRegistry().plan.Revision, activated, metrics.Snapshot())
	}
	if registry := runner.registryAt(now.Add(time.Second)); registry == nil || registry.plan.Revision != 1 {
		t.Fatalf("pre-not_before datagram used registry %+v", registry)
	}
	if registry := runner.registryAt(now.Add(6 * time.Second)); registry == nil || registry.plan.Revision != 2 {
		t.Fatalf("post-not_before datagram used registry %+v", registry)
	}
	versions := runner.AdmissibleRegistryVersions()
	if len(versions) != 2 || versions[0] != 2 || versions[1] != 1 {
		t.Fatalf("admissible revisions=%v, want [2 1]", versions)
	}
	snapshot := runtime.Snapshot().Plan
	if !snapshot.Healthy || !snapshot.Accepting || snapshot.ActiveRevision != 2 {
		t.Fatalf("unexpected plan runtime snapshot: %+v", snapshot)
	}
}

func TestPlanSupervisorKeepsUnexpiredPlanButDegradesOnRefreshFailure(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	writeSignedPlan(t, planPath, plan, privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, filepath.Join(dir, "history"), 4, now)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := OpenWAL(filepath.Join(dir, "wal"), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	runner := &Runner{Registry: history.Active(), Plans: history}
	if err := runner.ActivateRegistry(history.Active()); err != nil {
		t.Fatal(err)
	}
	metrics := &Metrics{}
	runtime := NewRuntimeState()
	supervisor, err := NewPlanSupervisor(time.Minute, history, wal, runner, metrics, runtime, func([]*Registry) error { return errors.New("Kafka unavailable") })
	if err != nil {
		t.Fatal(err)
	}
	plan.Revision = 2
	plan.PartitionMapVersion = 2
	writeSignedPlan(t, planPath, plan, privateKey)
	refreshErr := supervisor.Refresh(now.Add(time.Second))
	if refreshErr == nil {
		t.Fatal("dependency failure was accepted")
	}
	var failure planRefreshFailure
	if !errors.As(refreshErr, &failure) || failure.stage != "dependency" || failure.code != "KAFKA_CONTRACT" {
		t.Fatalf("refresh failure=%T %v, stage=%q code=%q", refreshErr, refreshErr, failure.stage, failure.code)
	}
	snapshot := runtime.Snapshot().Plan
	if snapshot.Healthy || !snapshot.Accepting || snapshot.ActiveRevision != 1 || metrics.PlanRefreshFailures.Load() != 1 {
		t.Fatalf("unexpected degraded plan state: snapshot=%+v failures=%d", snapshot, metrics.PlanRefreshFailures.Load())
	}
}

func TestPlanSupervisorStopsAtExactExpiryWhenNoValidReplacementExists(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	plan.ExpiresAt = now.Add(80 * time.Millisecond)
	writeSignedPlan(t, planPath, plan, privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, filepath.Join(dir, "history"), 4, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	wal, err := OpenWAL(filepath.Join(dir, "wal"), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	runner := &Runner{Registry: history.Active(), Plans: history}
	if err := runner.ActivateRegistry(history.Active()); err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewPlanSupervisor(20*time.Millisecond, history, wal, runner, &Metrics{}, NewRuntimeState(), func([]*Registry) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = supervisor.Run(ctx)
	if !errors.Is(err, ErrActivePlanExpired) {
		t.Fatalf("supervisor error=%v, want ErrActivePlanExpired", err)
	}
}
