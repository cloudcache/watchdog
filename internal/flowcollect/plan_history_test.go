package flowcollect

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
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
