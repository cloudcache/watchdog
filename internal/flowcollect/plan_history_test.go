package flowcollect

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlanHistoryPersistsAndFallsBackToUnexpiredLKG(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	writeSignedPlan(t, planPath, plan, privateKey)
	historyDir := filepath.Join(dir, "history")
	history, err := OpenPlanHistory(planPath, keyPath, historyDir, 8, now)
	if err != nil {
		t.Fatal(err)
	}
	if history.UsedLKG() || history.Active().Plan().Revision != plan.Revision {
		t.Fatalf("unexpected initial history state: lkg=%v active=%+v", history.UsedLKG(), history.Active().Plan())
	}
	if _, err := os.Stat(filepath.Join(historyDir, "00000000000000000001.plan")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte("not a signed plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenPlanHistory(planPath, keyPath, historyDir, 8, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.UsedLKG() || restarted.Active().Plan().Revision != 1 {
		t.Fatalf("signed LKG was not selected: lkg=%v active=%+v", restarted.UsedLKG(), restarted.Active().Plan())
	}
}

func TestPlanHistoryResolvesOldRevisionAndRejectsDowngrade(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	historyDir := filepath.Join(dir, "history")
	writePlanPublicKey(t, keyPath, publicKey)
	first := validPlan(now)
	first.PartitionMap[0] = 3
	writeSignedPlan(t, planPath, first, privateKey)
	if _, err := OpenPlanHistory(planPath, keyPath, historyDir, 8, now); err != nil {
		t.Fatal(err)
	}
	second := validPlan(now)
	second.Revision = 2
	second.PartitionMapVersion = 2
	second.PartitionMap[0] = 7
	writeSignedPlan(t, planPath, second, privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, historyDir, 8, now)
	if err != nil {
		t.Fatal(err)
	}
	old, ok := history.Resolve(1)
	if !ok || old.Plan().PartitionMap[0] != 3 || history.Active().Plan().Revision != 2 {
		t.Fatalf("history did not preserve revisions: revisions=%v", history.Revisions())
	}
	writeSignedPlan(t, planPath, first, privateKey)
	if _, err := OpenPlanHistory(planPath, keyPath, historyDir, 8, now); err == nil {
		t.Fatal("plan revision downgrade was accepted")
	}
}

func TestPlanHistoryRejectsImmutableRevisionChange(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	historyDir := filepath.Join(dir, "history")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	writeSignedPlan(t, planPath, plan, privateKey)
	if _, err := OpenPlanHistory(planPath, keyPath, historyDir, 8, now); err != nil {
		t.Fatal(err)
	}
	plan.PartitionMap[0]++
	writeSignedPlan(t, planPath, plan, privateKey)
	if _, err := OpenPlanHistory(planPath, keyPath, historyDir, 8, now); err == nil {
		t.Fatal("same revision with a different signed payload was accepted")
	}
}

func TestHistoricalPlanCanBeResolvedAfterExpiry(t *testing.T) {
	now := time.Now()
	plan := validPlan(now.Add(-2 * time.Hour))
	plan.ExpiresAt = now.Add(-time.Hour)
	registry, err := compileHistoricalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if registry.Plan().Revision != plan.Revision {
		t.Fatalf("unexpected historical revision: %d", registry.Plan().Revision)
	}
	if _, err := CompilePlan(plan, now); err == nil {
		t.Fatal("expired historical plan was accepted as active")
	}
}

func TestPlanHistoryPrunesOnlyRevisionsUnreferencedByWAL(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	historyDir := filepath.Join(dir, "history")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	for revision := uint64(1); revision <= 3; revision++ {
		plan.Revision = revision
		plan.PartitionMapVersion = uint32(revision)
		writeSignedPlan(t, planPath, plan, privateKey)
		if _, err := OpenPlanHistory(planPath, keyPath, historyDir, 2, now); err != nil {
			t.Fatalf("open revision %d: %v", revision, err)
		}
	}
	history, err := OpenPlanHistory(planPath, keyPath, historyDir, 2, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := history.Revisions(); len(got) != 3 {
		t.Fatalf("temporary startup overflow was not retained: %v", got)
	}
	if _, err := history.Prune(map[uint64]struct{}{9: {}}); err == nil {
		t.Fatal("missing WAL plan revision was accepted")
	}
	removed, err := history.Prune(map[uint64]struct{}{1: {}})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("pruned revisions=%d, want 1", removed)
	}
	if _, ok := history.Resolve(1); !ok {
		t.Fatal("WAL-referenced revision was pruned")
	}
	if _, ok := history.Resolve(2); ok {
		t.Fatal("unreferenced revision was retained")
	}
	if _, ok := history.Resolve(3); !ok {
		t.Fatal("active revision was pruned")
	}
}

func TestPlanHistoryRefreshPersistsBeforeActivationAndRetainsPreviousActive(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	historyDir := filepath.Join(dir, "history")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	writeSignedPlan(t, planPath, plan, privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, historyDir, 3, now)
	if err != nil {
		t.Fatal(err)
	}
	plan.Revision = 2
	plan.PartitionMapVersion = 2
	writeSignedPlan(t, planPath, plan, privateKey)
	validated := false
	result, err := history.Refresh(now.Add(time.Second), func() (map[uint64]struct{}, error) { return nil, nil }, func(registries []*Registry) error {
		validated = true
		if _, err := os.Stat(filepath.Join(historyDir, "00000000000000000002.plan")); !os.IsNotExist(err) {
			t.Fatalf("candidate was persisted before dependency validation: %v", err)
		}
		if len(registries) != 2 {
			t.Fatalf("validator registries=%d, want 2", len(registries))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !validated || !result.Changed || result.Revision != 2 || history.Active().Plan().Revision != 2 {
		t.Fatalf("unexpected refresh result=%+v active=%d", result, history.Active().Plan().Revision)
	}
	if _, ok := history.Resolve(1); !ok {
		t.Fatal("previous active revision was not retained across the pointer-swap window")
	}
	if _, err := os.Stat(filepath.Join(historyDir, "00000000000000000002.plan")); err != nil {
		t.Fatal(err)
	}
}

func TestPlanHistoryRefreshFailureLeavesActiveUntouched(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	historyDir := filepath.Join(dir, "history")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	writeSignedPlan(t, planPath, plan, privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, historyDir, 3, now)
	if err != nil {
		t.Fatal(err)
	}
	plan.Revision = 2
	plan.PartitionMapVersion = 2
	writeSignedPlan(t, planPath, plan, privateKey)
	if _, err := history.Refresh(now, func() (map[uint64]struct{}, error) { return nil, nil }, func([]*Registry) error { return errors.New("Kafka unavailable") }); err == nil {
		t.Fatal("dependency validation failure was accepted")
	}
	if history.Active().Plan().Revision != 1 {
		t.Fatal("failed refresh changed the active revision")
	}
	if _, err := os.Stat(filepath.Join(historyDir, "00000000000000000002.plan")); !os.IsNotExist(err) {
		t.Fatalf("failed refresh persisted candidate: %v", err)
	}
	if err := os.WriteFile(planPath, []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := history.Refresh(now, nil, func([]*Registry) error { return nil }); err == nil {
		t.Fatal("invalid signature envelope was accepted")
	}
	if history.Active().Plan().Revision != 1 {
		t.Fatal("invalid envelope changed the active revision")
	}
}

func TestPlanHistoryRefreshRejectsUnsafeRetentionAndImmutableMutation(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	historyDir := filepath.Join(dir, "history")
	writePlanPublicKey(t, keyPath, publicKey)
	plan := validPlan(now)
	writeSignedPlan(t, planPath, plan, privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, historyDir, 2, now)
	if err != nil {
		t.Fatal(err)
	}
	plan.Revision = 2
	plan.PartitionMapVersion = 2
	writeSignedPlan(t, planPath, plan, privateKey)
	if _, err := history.Refresh(now, func() (map[uint64]struct{}, error) { return map[uint64]struct{}{1: {}}, nil }, func([]*Registry) error { return nil }); err != nil {
		t.Fatal(err)
	}
	plan.Revision = 3
	plan.PartitionMapVersion = 3
	writeSignedPlan(t, planPath, plan, privateKey)
	if _, err := history.Refresh(now, func() (map[uint64]struct{}, error) { return map[uint64]struct{}{1: {}}, nil }, func([]*Registry) error { return nil }); err == nil {
		t.Fatal("refresh exceeding the safe retained-revision limit was accepted")
	}
	if history.Active().Plan().Revision != 2 {
		t.Fatal("retention failure changed the active revision")
	}

	plan.Revision = 2
	plan.PartitionMapVersion = 99
	writeSignedPlan(t, planPath, plan, privateKey)
	if _, err := history.Refresh(now, nil, func([]*Registry) error { return nil }); err == nil {
		t.Fatal("same revision with mutated payload was accepted")
	}
}

func TestPlanHistoryUnchangedRefreshDoesNotScanWALOrRevalidateKafka(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	keyPath := filepath.Join(dir, "plan.pub")
	writePlanPublicKey(t, keyPath, publicKey)
	writeSignedPlan(t, planPath, validPlan(now), privateKey)
	history, err := OpenPlanHistory(planPath, keyPath, filepath.Join(dir, "history"), 4, now)
	if err != nil {
		t.Fatal(err)
	}
	history.usedLKG = true
	referencesCalled, validatorCalled := false, false
	result, err := history.Refresh(now.Add(time.Second), func() (map[uint64]struct{}, error) {
		referencesCalled = true
		return nil, errors.New("must not be called")
	}, func([]*Registry) error {
		validatorCalled = true
		return errors.New("must not be called")
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Changed || referencesCalled || validatorCalled || history.UsedLKG() {
		t.Fatalf("unexpected unchanged refresh result=%+v references=%v validator=%v lkg=%v", result, referencesCalled, validatorCalled, history.UsedLKG())
	}
}

func writePlanPublicKey(t *testing.T, path string, publicKey ed25519.PublicKey) {
	t.Helper()
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(publicKey)), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeSignedPlan(t *testing.T, path string, plan Plan, privateKey ed25519.PrivateKey) {
	t.Helper()
	payload, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(signedPlanEnvelope{SchemaVersion: 1, Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, envelope, 0o600); err != nil {
		t.Fatal(err)
	}
}

func testPlanHistory(active *Registry, registries ...*Registry) *PlanHistory {
	entries := make(map[uint64]planHistoryEntry, len(registries))
	for _, registry := range registries {
		entries[registry.Plan().Revision] = planHistoryEntry{registry: registry}
	}
	return &PlanHistory{entries: entries, active: active}
}
