package flowcollect

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

type planDeliveryServerState struct {
	mu          sync.Mutex
	token       string
	envelope    []byte
	metadata    PlanSignatureMetadata
	acks        []planDeliveryAckTestRequest
	failures    []planDeliveryFailureTestRequest
	getCalls    int
	notModified int
	ackSignal   chan struct{}
}

type planDeliveryRoundTripFunc func(*http.Request) (*http.Response, error)

func (f planDeliveryRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type planDeliveryAckTestRequest struct {
	ConfigVersion   uint64 `json:"config_version"`
	SpecHash        string `json:"spec_hash"`
	BootID          string `json:"boot_id"`
	SoftwareVersion string `json:"software_version"`
}

type planDeliveryFailureTestRequest struct {
	FailedConfigVersion uint64 `json:"failed_config_version"`
	BootID              string `json:"boot_id"`
	SoftwareVersion     string `json:"software_version"`
	Stage               string `json:"stage"`
	Code                string `json:"code"`
	Detail              string `json:"detail"`
}

func TestPlanDeliveryClientFetchesAtomicallyReloadsTokenAndAcknowledgesExactActivation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	remotePlan := validPlan(now)
	remotePlan.Revision = 2
	remotePlan.PartitionMapVersion = 2
	envelope, metadata := controlPlanePlanEnvelope(t, remotePlan, privateKey)
	serverState := &planDeliveryServerState{token: "token-one", envelope: envelope, metadata: metadata}

	directory := t.TempDir()
	planPath := filepath.Join(directory, "plan.json")
	keyPath := filepath.Join(directory, "plan.pub")
	tokenPath := filepath.Join(directory, "collector.token")
	writePlanPublicKey(t, keyPath, publicKey)
	writeSignedPlan(t, planPath, validPlan(now), privateKey)
	if err := os.WriteFile(tokenPath, []byte("token-one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	metrics, runtime := &Metrics{}, NewRuntimeState()
	config := planDeliveryTestConfig("http://127.0.0.1:18090", planPath, keyPath, tokenPath)
	client, err := NewPlanDeliveryClient(config, remotePlan.CollectorID, "boot-test", "1.2.3", 1, metrics, runtime)
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.httpClient = planDeliveryTestHTTPClient(http.HandlerFunc(serverState.serveHTTP))
	if err := client.fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	persisted, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	registry, persistedMetadata, err := VerifyControlPlaneSignedPlan(persisted, []byte(base64.StdEncoding.EncodeToString(publicKey)), now)
	if err != nil {
		t.Fatal(err)
	}
	if registry.Plan().Revision != 2 || persistedMetadata.SpecHash != metadata.SpecHash || client.pending.ConfigVersion != 2 || metrics.PlanDeliveryFetchSuccesses.Load() != 1 {
		t.Fatalf("persisted plan=%+v metadata=%+v pending=%+v metrics=%+v", registry.Plan(), persistedMetadata, client.pending, metrics.Snapshot())
	}
	info, err := os.Stat(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("plan mode=%o", info.Mode().Perm())
	}

	serverState.mu.Lock()
	serverState.token = "token-two"
	serverState.mu.Unlock()
	if err := os.WriteFile(tokenPath, []byte("token-two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := client.fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if metrics.PlanDeliveryNotModified.Load() != 1 {
		t.Fatalf("not-modified metric=%d", metrics.PlanDeliveryNotModified.Load())
	}
	client.activeRevision = 2
	if err := client.acknowledge(context.Background()); err != nil {
		t.Fatal(err)
	}
	serverState.mu.Lock()
	defer serverState.mu.Unlock()
	if len(serverState.acks) != 1 || serverState.acks[0].ConfigVersion != 2 || serverState.acks[0].SpecHash != metadata.SpecHash || serverState.acks[0].BootID != "boot-test" || serverState.acks[0].SoftwareVersion != "1.2.3" {
		t.Fatalf("acknowledgements=%+v", serverState.acks)
	}
}

func TestPlanDeliveryClientRejectsInvalidEnvelopeWithoutReplacingLKGAndReportsFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	remotePlan := validPlan(now)
	remotePlan.Revision = 2
	envelope, metadata := controlPlanePlanEnvelope(t, remotePlan, privateKey)
	envelope[len(envelope)-2] ^= 1
	serverState := &planDeliveryServerState{token: "token", envelope: envelope, metadata: metadata}
	directory := t.TempDir()
	planPath := filepath.Join(directory, "plan.json")
	keyPath := filepath.Join(directory, "plan.pub")
	tokenPath := filepath.Join(directory, "collector.token")
	writePlanPublicKey(t, keyPath, publicKey)
	writeSignedPlan(t, planPath, validPlan(now), privateKey)
	original, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	metrics := &Metrics{}
	client, err := NewPlanDeliveryClient(planDeliveryTestConfig("http://127.0.0.1:18090", planPath, keyPath, tokenPath), remotePlan.CollectorID, "boot-test", "1.2.3", 1, metrics, NewRuntimeState())
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.httpClient = planDeliveryTestHTTPClient(http.HandlerFunc(serverState.serveHTTP))
	if err := client.fetch(context.Background()); err == nil {
		t.Fatal("invalid envelope was accepted")
	}
	current, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(current) != string(original) || metrics.PlanDeliveryFetchFailures.Load() != 1 {
		t.Fatalf("invalid delivery changed LKG or metrics: changed=%v failures=%d", string(current) != string(original), metrics.PlanDeliveryFetchFailures.Load())
	}
	serverState.mu.Lock()
	defer serverState.mu.Unlock()
	if len(serverState.failures) != 1 || serverState.failures[0].Stage != "verify" || serverState.failures[0].Code != "PLAN_SIGNATURE_INVALID" || serverState.failures[0].BootID != "boot-test" {
		t.Fatalf("failure reports=%+v", serverState.failures)
	}
}

func TestRemoteDeliveryWakesSupervisorAndAcknowledgesOnlyAfterActivation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	first := validPlan(now)
	second := validPlan(now)
	second.Revision = 2
	second.PartitionMapVersion = 2
	envelope, metadata := controlPlanePlanEnvelope(t, second, privateKey)
	serverState := &planDeliveryServerState{token: "token", envelope: envelope, metadata: metadata, ackSignal: make(chan struct{}, 1)}
	directory := t.TempDir()
	planPath := filepath.Join(directory, "plan.json")
	keyPath := filepath.Join(directory, "plan.pub")
	tokenPath := filepath.Join(directory, "collector.token")
	writePlanPublicKey(t, keyPath, publicKey)
	writeSignedPlan(t, planPath, first, privateKey)
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	history, err := OpenPlanHistory(planPath, keyPath, filepath.Join(directory, "history"), 4, now)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := OpenWAL(filepath.Join(directory, "wal"), first.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	runner := &Runner{Registry: history.Active(), Plans: history}
	if err := runner.ActivateRegistry(history.Active()); err != nil {
		t.Fatal(err)
	}
	metrics, runtime := &Metrics{}, NewRuntimeState()
	config := planDeliveryTestConfig("http://127.0.0.1:18090", planPath, keyPath, tokenPath)
	config.PlanRefreshInterval = time.Hour
	client, err := NewPlanDeliveryClient(config, first.CollectorID, "boot-test", "1.2.3", 1, metrics, runtime)
	if err != nil {
		t.Fatal(err)
	}
	client.httpClient = planDeliveryTestHTTPClient(http.HandlerFunc(serverState.serveHTTP))
	supervisor, err := NewPlanSupervisor(time.Hour, history, wal, runner, metrics, runtime, func([]*Registry) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Wake = client.Delivered()
	supervisor.OnActivated = client.NotifyActivated
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clientDone := make(chan error, 1)
	supervisorDone := make(chan error, 1)
	go func() { clientDone <- client.Run(ctx) }()
	go func() { supervisorDone <- supervisor.Run(ctx) }()
	select {
	case <-serverState.ackSignal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for activation acknowledgement")
	}
	if runner.ActiveRegistry().Plan().Revision != 2 {
		t.Fatalf("active revision=%d", runner.ActiveRegistry().Plan().Revision)
	}
	serverState.mu.Lock()
	acks := append([]planDeliveryAckTestRequest(nil), serverState.acks...)
	serverState.mu.Unlock()
	if len(acks) != 1 || acks[0].ConfigVersion != 2 || acks[0].SpecHash != metadata.SpecHash {
		t.Fatalf("acknowledgements=%+v", acks)
	}
	cancel()
	if err := <-clientDone; err != nil {
		t.Fatal(err)
	}
	if err := <-supervisorDone; err != nil {
		t.Fatal(err)
	}
}

func TestPlanDeliveryRetryDelayIsExponentiallyBounded(t *testing.T) {
	client := &PlanDeliveryClient{config: Config{
		ControlPlaneRetryMin: time.Second,
		ControlPlaneRetryMax: 5 * time.Second,
	}}
	for _, test := range []struct {
		failures int
		expected time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {4, 5 * time.Second}, {9, 5 * time.Second}} {
		if actual := client.retryDelay(test.failures); actual != test.expected {
			t.Fatalf("retry delay after %d failures=%s, want %s", test.failures, actual, test.expected)
		}
	}
}

func (s *planDeliveryServerState) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Header.Get("X-Watchdog-Agent-Token") != s.token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodGet && filepath.Base(r.URL.Path) == "plan":
		s.getCalls++
		etag := planDeliveryETag(s.metadata.ConfigVersion, s.metadata.SpecHash)
		if r.Header.Get("If-None-Match") == etag {
			s.notModified++
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", etag)
		w.Header().Set("X-Watchdog-Plan-Version", strconv.FormatUint(s.metadata.ConfigVersion, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.envelope)
	case r.Method == http.MethodPost && filepath.Base(r.URL.Path) == "plan-ack":
		var request planDeliveryAckTestRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.acks = append(s.acks, request)
		if s.ackSignal != nil {
			select {
			case s.ackSignal <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodPost && filepath.Base(r.URL.Path) == "heartbeat":
		var request planDeliveryFailureTestRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.failures = append(s.failures, request)
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func controlPlanePlanEnvelope(t *testing.T, plan Plan, privateKey ed25519.PrivateKey) ([]byte, PlanSignatureMetadata) {
	t.Helper()
	plan.NotBefore = plan.NotBefore.UTC().Truncate(time.Millisecond)
	plan.ExpiresAt = plan.ExpiresAt.UTC().Truncate(time.Millisecond)
	payload, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	metadata := PlanSignatureMetadata{
		PlanID: "plan-delivery-test", TenantID: "tenant-delivery-test", CollectorID: plan.CollectorID,
		ConfigVersion: plan.Revision, PlanSchemaVersion: uint16(plan.SchemaVersion), SpecHash: hex.EncodeToString(digest[:]),
		SigningKeyID: "delivery-key", NotBeforeUnixMilli: plan.NotBefore.UnixMilli(), ExpiresAtUnixMilli: plan.ExpiresAt.UnixMilli(),
	}
	signingPayload, err := BuildPlanSignaturePayload(metadata)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := MarshalControlPlaneSignedPlan(metadata, payload, ed25519.Sign(privateKey, signingPayload))
	if err != nil {
		t.Fatal(err)
	}
	return envelope, metadata
}

func planDeliveryTestConfig(serverURL, planPath, keyPath, tokenPath string) Config {
	config := DefaultConfig()
	config.ControlPlaneURL = serverURL
	config.ControlPlaneTokenFile = tokenPath
	config.PlanFile = planPath
	config.PlanPublicKeyFile = keyPath
	config.PlanRefreshInterval = time.Minute
	config.ControlPlaneRequestTimeout = time.Second
	config.Normalize()
	return config
}

func planDeliveryTestHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: planDeliveryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		result := response.Result()
		result.Request = request
		return result, nil
	})}
}
