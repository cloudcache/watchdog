package flowplan

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

func TestHistoricalSignedPlanAllowsExpiredReplayButStillRequiresSignature(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{
		SchemaVersion: 2, Revision: 7, CollectorID: "collector-a",
		NotBefore: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC),
		Sources: []SourceBinding{{
			Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.0/24", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a",
			OwnershipEpoch: 1, SamplingMode: SamplingModeSampled, Enabled: true,
		}},
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	envelopeData, err := json.Marshal(signedPlanEnvelope{
		SchemaVersion: 1, Payload: base64.StdEncoding.EncodeToString(payload),
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	planPath, keyPath := filepath.Join(directory, "plan.json"), filepath.Join(directory, "plan.pub")
	if err := os.WriteFile(planPath, envelopeData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(publicKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSignedPlan(planPath, keyPath, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("expired plan was accepted for current collector admission")
	}
	registry, err := LoadHistoricalSignedPlan(planPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if registry.Plan().Revision != 7 {
		t.Fatalf("revision=%d", registry.Plan().Revision)
	}

	var corrupted signedPlanEnvelope
	if err := json.Unmarshal(envelopeData, &corrupted); err != nil {
		t.Fatal(err)
	}
	corrupted.Signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	corruptedData, err := json.Marshal(corrupted)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, corruptedData, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadHistoricalSignedPlan(planPath, keyPath); err == nil {
		t.Fatal("historical loader accepted a corrupted signed envelope")
	}
}
