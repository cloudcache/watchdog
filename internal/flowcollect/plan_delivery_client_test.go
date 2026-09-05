package flowcollect

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
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
	heartbeats  []planDeliveryRuntimeHeartbeat
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
	trustBundlePath := writePlanDeliveryTrustBundle(t, directory, publicKey, now)
	writePlanPublicKey(t, keyPath, publicKey)
	writeSignedPlan(t, planPath, validPlan(now), privateKey)
	if err := os.WriteFile(tokenPath, []byte("token-one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	metrics, runtime := &Metrics{}, NewRuntimeState()
	wal := openPlanDeliveryTestWAL(t, directory, remotePlan.CollectorID)
	defer wal.Close()
	config := planDeliveryTestConfig("http://127.0.0.1:18090", planPath, keyPath, tokenPath)
	config.PlanTrustBundleFile = trustBundlePath
	client, err := NewPlanDeliveryClient(config, remotePlan.CollectorID, "boot-test", "1.2.3", 1, metrics, runtime, wal)
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
	trustBundlePath := writePlanDeliveryTrustBundle(t, directory, publicKey, now)
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
	wal := openPlanDeliveryTestWAL(t, directory, remotePlan.CollectorID)
	defer wal.Close()
	config := planDeliveryTestConfig("http://127.0.0.1:18090", planPath, keyPath, tokenPath)
	config.PlanTrustBundleFile = trustBundlePath
	client, err := NewPlanDeliveryClient(config, remotePlan.CollectorID, "boot-test", "1.2.3", 1, metrics, NewRuntimeState(), wal)
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
	trustBundlePath := writePlanDeliveryTrustBundle(t, directory, publicKey, now)
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
	config.PlanTrustBundleFile = trustBundlePath
	config.PlanRefreshInterval = time.Hour
	client, err := NewPlanDeliveryClient(config, first.CollectorID, "boot-test", "1.2.3", 1, metrics, runtime, wal)
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

func TestPlanDeliveryRuntimeHeartbeatRetriesExactPayloadAndSeparatesCapabilities(t *testing.T) {
	now := time.UnixMilli(2_000_000_000_000).UTC()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	plan := validPlan(now)
	directory := t.TempDir()
	planPath := filepath.Join(directory, "plan.json")
	keyPath := filepath.Join(directory, "plan.pub")
	tokenPath := filepath.Join(directory, "collector.token")
	writePlanPublicKey(t, keyPath, publicKey)
	writeSignedPlan(t, planPath, plan, privateKey)
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	wal := openPlanDeliveryTestWAL(t, directory, plan.CollectorID)
	defer wal.Close()
	metrics := &Metrics{}
	metrics.ReceivedDatagrams.Store(100)
	metrics.ReceiveQueueDrops.Store(2)
	metrics.QuarantineQueueDrops.Store(3)
	metrics.UDPKernelDropsSFlow.Store(4)
	metrics.UDPKernelDropsNetFlow.Store(5)
	metrics.ReceiveQueueDepth.Store(7)
	metrics.ReceiveQueueCapacity.Store(70)
	metrics.DecodeQueueDepth.Store(8)
	metrics.DecodeQueueCapacity.Store(80)
	metrics.QuarantineQueueDepth.Store(9)
	metrics.QuarantineQueueCapacity.Store(90)
	metrics.PublishFailures.Store(1)
	metrics.QuarantinePublishFailures.Store(2)
	metrics.DLQPublishFailures.Store(3)
	metrics.CollectStateFailures.Store(4)
	metrics.QualityCheckpointFailures.Store(5)
	runtime := NewRuntimeState()
	runtime.start(now.Add(-10 * time.Second))
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	runtime.observePlan(nil, registry, false, now)
	config := planDeliveryTestConfig("http://127.0.0.1:18090", planPath, keyPath, tokenPath)
	client, err := NewPlanDeliveryClient(config, plan.CollectorID, "boot-heartbeat", "1.2.3", plan.Revision, metrics, runtime, wal)
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	var bodies [][]byte
	responseStatus := http.StatusServiceUnavailable
	client.httpClient = planDeliveryTestHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Error(readErr)
		}
		bodies = append(bodies, append([]byte(nil), body...))
		w.WriteHeader(responseStatus)
	}))
	if err := client.reportRuntime(context.Background()); err == nil {
		t.Fatal("failed heartbeat unexpectedly succeeded")
	}
	responseStatus = http.StatusAccepted
	if err := client.reportRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) || metrics.HeartbeatFailures.Load() != 1 || metrics.HeartbeatSuccesses.Load() != 1 {
		t.Fatalf("heartbeat retry bodies=%d equal=%v success=%d failure=%d", len(bodies), len(bodies) == 2 && bytes.Equal(bodies[0], bodies[1]), metrics.HeartbeatSuccesses.Load(), metrics.HeartbeatFailures.Load())
	}
	var request planDeliveryHeartbeatRequest
	if err := json.Unmarshal(bodies[0], &request); err != nil {
		t.Fatal(err)
	}
	if request.SchemaVersion != 1 || request.Kind != "runtime" || request.Runtime == nil || request.PlanFailure != nil || request.Runtime.Sequence != 1 || request.Runtime.BootID != "boot-heartbeat" || request.Runtime.PlanSchemaMax != 2 || len(request.Runtime.Capabilities.PlanEnvelopeVersions) != 2 || request.Runtime.Observation.UptimeSeconds != 10 || request.Runtime.Observation.ControlPlaneHealthy || request.Runtime.Observation.Queues.Receive.Depth != 7 || request.Runtime.Observation.Counters.UDPKernelDrops != 9 || request.Runtime.Observation.Counters.PublishFailures != 15 || request.Runtime.Capabilities.SchemaVersion != 1 {
		t.Fatalf("runtime heartbeat=%+v", request)
	}
	if err := client.reportRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 3 || bytes.Equal(bodies[1], bodies[2]) {
		t.Fatalf("next heartbeat did not create a new payload: bodies=%d", len(bodies))
	}
	request = planDeliveryHeartbeatRequest{}
	if err := json.Unmarshal(bodies[2], &request); err != nil || request.Runtime == nil || request.Runtime.Sequence != 2 {
		t.Fatalf("second heartbeat=%+v err=%v", request, err)
	}
	responseStatus = http.StatusConflict
	if err := client.reportRuntime(context.Background()); err == nil || len(client.pendingHeartbeat) != 0 || client.heartbeatSequence != 2 {
		t.Fatalf("conflicting heartbeat error=%v pending=%d sequence=%d", err, len(client.pendingHeartbeat), client.heartbeatSequence)
	}
	metrics.ReceiveQueueDepth.Store(11)
	responseStatus = http.StatusAccepted
	if err := client.reportRuntime(context.Background()); err != nil {
		t.Fatal(err)
	}
	var conflicted, rebuilt planDeliveryHeartbeatRequest
	if err := json.Unmarshal(bodies[3], &conflicted); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bodies[4], &rebuilt); err != nil {
		t.Fatal(err)
	}
	if conflicted.Runtime == nil || rebuilt.Runtime == nil || conflicted.Runtime.Sequence != 3 || rebuilt.Runtime.Sequence != 3 || rebuilt.Runtime.Observation.Queues.Receive.Depth != 11 || bytes.Equal(bodies[3], bodies[4]) {
		t.Fatalf("conflict rebuild old=%+v new=%+v", conflicted.Runtime, rebuilt.Runtime)
	}
	responseStatus = http.StatusPreconditionFailed
	err = client.reportRuntime(context.Background())
	var fenced planDeliveryFailure
	if !errors.As(err, &fenced) || fenced.Code != "HEARTBEAT_FENCED" || len(client.pendingHeartbeat) == 0 {
		t.Fatalf("fenced heartbeat error=%v pending=%d", err, len(client.pendingHeartbeat))
	}
}

func TestPlanDeliveryMTLSIdentityReloadsForEveryRequest(t *testing.T) {
	directory := t.TempDir()
	certificatePath := filepath.Join(directory, "collector.crt")
	keyPath := filepath.Join(directory, "collector.key")
	firstDER := writePlanDeliveryClientCertificate(t, certificatePath, keyPath, 1)
	config := DefaultConfig()
	config.ControlPlaneURL = "https://watchdog.example.com"
	config.ControlPlaneTLSCertFile = certificatePath
	config.ControlPlaneTLSKeyFile = keyPath
	client, err := newPlanDeliveryHTTPClient(config)
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || !transport.DisableKeepAlives || transport.TLSClientConfig.GetClientCertificate == nil || len(transport.TLSClientConfig.Certificates) != 0 {
		t.Fatalf("mTLS transport=%T keep_alive_disabled=%v", client.Transport, ok && transport.DisableKeepAlives)
	}
	first, err := transport.TLSClientConfig.GetClientCertificate(nil)
	if err != nil || len(first.Certificate) == 0 || !bytes.Equal(first.Certificate[0], firstDER) {
		t.Fatalf("first client certificate error=%v", err)
	}
	secondDER := writePlanDeliveryClientCertificate(t, certificatePath, keyPath, 2)
	second, err := transport.TLSClientConfig.GetClientCertificate(nil)
	if err != nil || len(second.Certificate) == 0 || !bytes.Equal(second.Certificate[0], secondDER) || bytes.Equal(first.Certificate[0], second.Certificate[0]) {
		t.Fatalf("rotated client certificate error=%v", err)
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
		var request planDeliveryHeartbeatRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 32<<10)).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch request.Kind {
		case "plan_failure":
			if request.PlanFailure == nil || request.Runtime != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			s.failures = append(s.failures, planDeliveryFailureTestRequest(*request.PlanFailure))
		case "runtime":
			if request.Runtime == nil || request.PlanFailure != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			s.heartbeats = append(s.heartbeats, *request.Runtime)
		default:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func openPlanDeliveryTestWAL(t *testing.T, directory, collectorID string) *WAL {
	t.Helper()
	wal, err := OpenWAL(filepath.Join(directory, "delivery-wal"), collectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	return wal
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

func writePlanDeliveryTrustBundle(t *testing.T, directory string, publicKey ed25519.PublicKey, now time.Time) string {
	t.Helper()
	path := filepath.Join(directory, "plan-trust-bundle.json")
	writeTestFile(t, path, marshalPlanTrustBundle(t, planTrustKey{
		ID: "delivery-key", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active",
		NotBeforeUnixMilli: now.Add(-2 * time.Hour).UnixMilli(), NotAfterUnixMilli: now.Add(2 * time.Hour).UnixMilli(),
	}))
	return path
}

func writePlanDeliveryClientCertificate(t *testing.T, certificatePath, keyPath string, serial int64) []byte {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "flow-collector"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certificatePath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return der
}
