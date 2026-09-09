package agentplan

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func signedTestPlan(t *testing.T, agentID, kind string, version, supersedes uint64, now time.Time) ([]byte, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return signedTestPlanWithKey(t, agentID, kind, version, supersedes, now, publicKey, privateKey), publicKey
}

func signedTestPlanWithKey(t *testing.T, agentID, kind string, version, supersedes uint64, now time.Time, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) []byte {
	t.Helper()
	capability := map[string]string{
		"system": "system.samples/v1", "snmp": "snmp.poll/v1",
		"flow_collect": "flow.receive.sflow/v1", "flow_worker": "flow.write.clickhouse/v1",
	}[kind]
	payload, _ := json.Marshal(Spec{Kind: kind, APIVersion: "v1", RequiredCapabilities: []string{capability}, Config: json.RawMessage(`{"interval_seconds":60}`)})
	canonical, digest, _, err := CanonicalSpec(payload)
	if err != nil {
		t.Fatal(err)
	}
	metadata := Metadata{
		PlanID: "plan-test", AgentID: agentID, PlanVersion: version, PlanSchemaVersion: SchemaVersion,
		Kind: kind, APIVersion: "v1", PayloadSHA256: digest, SigningKeyID: "test-key",
		NotBeforeUnixMilli: now.Add(-time.Minute).UnixMilli(), ExpiresAtUnixMilli: now.Add(time.Hour).UnixMilli(),
		SupersedesPlanVersion: supersedes,
	}
	envelope, _, err := (Signer{KeyID: "test-key", PrivateKey: privateKey}).Sign(metadata, canonical)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestSignedPlanCanonicalCompatibilityAndValidity(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
	data, publicKey := signedTestPlan(t, "agent-test", "system", 2, 1, now)
	envelope, spec, err := Verify(data, publicKey, now, true)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Metadata.PlanVersion != 2 || spec.Kind != "system" {
		t.Fatalf("unexpected verified plan: %+v %+v", envelope.Metadata, spec)
	}
	if err := ValidateCompatibility(spec, "agent-test", "system", "v1", []string{"system.samples/v1"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCompatibility(spec, "agent-test", "snmp", "v1", []string{"snmp.poll/v1"}); err == nil {
		t.Fatal("cross-kind plan was accepted")
	}
	if _, _, err := Verify(data, publicKey, now.Add(2*time.Hour), true); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired plan result: %v", err)
	}

	var tampered map[string]any
	if err := json.Unmarshal(data, &tampered); err != nil {
		t.Fatal(err)
	}
	payload := tampered["payload"].(map[string]any)
	payload["config"].(map[string]any)["interval_seconds"] = float64(61)
	tamperedData, _ := json.Marshal(tampered)
	if _, _, err := Verify(tamperedData, publicKey, now, true); err == nil {
		t.Fatal("tampered plan was accepted")
	}
}

func TestDiskLKGIsAtomicMonotonicAndHistorical(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	data2 := signedTestPlanWithKey(t, "agent-test", "system", 2, 1, now, publicKey, privateKey)
	lkg := DiskLKG{Path: filepath.Join(t.TempDir(), "plan.lkg")}
	capabilities := []string{"system.samples/v1"}
	if _, _, err := lkg.InstallAt(data2, publicKey, "agent-test", "system", "v1", capabilities, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := lkg.InstallAt(data2, publicKey, "agent-test", "system", "v1", capabilities, now); err != nil {
		t.Fatalf("idempotent install failed: %v", err)
	}
	// Historical restore deliberately ignores expiry but still verifies the signature.
	if _, envelope, _, err := lkg.Load(publicKey, "agent-test", "system", "v1", capabilities); err != nil || envelope.Metadata.PlanVersion != 2 {
		t.Fatalf("historical LKG restore failed: version=%d err=%v", envelope.Metadata.PlanVersion, err)
	}
	data1 := signedTestPlanWithKey(t, "agent-test", "system", 1, 0, now, publicKey, privateKey)
	if _, _, err := lkg.InstallAt(data1, publicKey, "agent-test", "system", "v1", capabilities, now); !errors.Is(err, ErrLKGRollback) {
		t.Fatalf("rollback result=%v", err)
	}
}

func TestClientRemoteACKOfflineLKGAndRevocation(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
	data, publicKey := signedTestPlan(t, "agent-test", "system", 1, 0, now)
	metadata := EnvelopeMetadata(data)
	expectedETag := `"p1-` + metadata.PayloadSHA256 + `"`
	var mode atomic.Int32
	var acknowledgements atomic.Int32
	var conditionalFetches atomic.Int32
	transport := roundTripFunc(func(request *http.Request) *http.Response {
		status := http.StatusOK
		body := data
		if request.Method == http.MethodGet && request.Header.Get("If-None-Match") != "" {
			conditionalFetches.Add(1)
		}
		if request.Header.Get("Authorization") != "Bearer secret" {
			status, body = http.StatusUnauthorized, nil
		}
		if status == http.StatusOK && mode.Load() == 1 {
			status, body = http.StatusServiceUnavailable, nil
		}
		if status == http.StatusOK && mode.Load() == 2 {
			status, body = http.StatusUnauthorized, nil
		}
		if status == http.StatusOK && mode.Load() == 3 {
			status, body = http.StatusConflict, nil
		}
		if status == http.StatusOK && mode.Load() == 4 {
			if request.Header.Get("If-None-Match") == expectedETag {
				status, body = http.StatusNotModified, nil
			} else {
				status, body = http.StatusPreconditionFailed, nil
			}
		}
		if status == http.StatusOK && request.Method == http.MethodPost {
			acknowledgements.Add(1)
			status, body = http.StatusAccepted, nil
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}
	})
	client := Client{
		BaseURL: "http://control-plane.test", AgentID: "agent-test", Token: "secret", PublicKey: publicKey,
		HTTPClient: &http.Client{Transport: transport},
		LKG:        DiskLKG{Path: filepath.Join(t.TempDir(), "plan.lkg")}, Kind: "system", APIVersion: "v1",
		Capabilities: []string{"system.samples/v1"}, BootID: "boot-1", Software: "test", Now: func() time.Time { return now },
	}
	applied := 0
	result, err := client.Sync(context.Background(), func(context.Context, Spec) error { applied++; return nil })
	if err != nil || result.Source != "remote" || result.AckError != nil || acknowledgements.Load() != 1 {
		t.Fatalf("remote sync=%+v applied=%d ack=%d err=%v", result, applied, acknowledgements.Load(), err)
	}
	mode.Store(1)
	result, err = client.Sync(context.Background(), func(context.Context, Spec) error { applied++; return nil })
	if err != nil || result.Source != "lkg" || conditionalFetches.Load() == 0 {
		t.Fatalf("offline LKG sync=%+v err=%v", result, err)
	}
	mode.Store(4)
	result, err = client.Sync(context.Background(), func(context.Context, Spec) error { applied++; return nil })
	if err != nil || result.Source != "lkg" {
		t.Fatalf("not-modified LKG sync=%+v err=%v", result, err)
	}
	mode.Store(3)
	if _, err := client.Sync(context.Background(), func(context.Context, Spec) error { return nil }); err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatalf("permanent control-plane rejection used LKG: %v", err)
	}
	mode.Store(2)
	if _, err := client.Sync(context.Background(), func(context.Context, Spec) error { return nil }); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked credential used LKG: %v", err)
	}
}

type roundTripFunc func(*http.Request) *http.Response

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request), nil
}
