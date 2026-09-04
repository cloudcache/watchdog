package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRemoteCollectorPrincipalProviderGrantLookupAndRevokeContract(t *testing.T) {
	grantOperation := strings.Repeat("a", 64)
	grantHash := strings.Repeat("b", 64)
	revokeOperation := strings.Repeat("c", 64)
	revokeHash := strings.Repeat("d", 64)
	var grantApplied atomic.Bool
	var revokeApplied atomic.Bool
	client := &http.Client{Transport: collectorPrincipalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-Watchdog-Provider-Revision") != "flow-runtime-v7" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("headers=%v", r.Header)
		}
		status := http.StatusOK
		var response any
		switch r.URL.Path {
		case "/v1/collector-principal-operations/grants/" + grantOperation:
			if r.Method == http.MethodGet && !grantApplied.Load() {
				return remotePrincipalHTTPResponse(http.StatusNotFound, "application/json", ""), nil
			}
			if r.Method == http.MethodPut {
				var request remoteCollectorPrincipalGrantRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if request.OperationKey != grantOperation || request.RequestHash != grantHash || request.PolicyRevision != "flow-runtime-v7" || request.PrincipalID != "principal-a" || request.TenantID != "tenant-a" || request.CollectorID != "collector-a" || request.ServiceType != "kafka" || request.ACLPropagationDelay != 2500 {
					t.Errorf("grant request=%+v", request)
				}
				grantApplied.Store(true)
			} else if r.Method != http.MethodGet {
				t.Errorf("grant method=%s", r.Method)
			}
			response = map[string]any{
				"operation_key": grantOperation, "request_hash": grantHash,
				"provider": "enterprise-kafka", "principal_ref": "User:collector-a",
				"credential_secret_ref": "vault://watchdog/collector-a",
				"receipt_ref":           "provider://grants/" + grantOperation,
				"receipt":               map[string]any{"acl_profile": "flow-runtime-v7"},
			}
		case "/v1/collector-principal-operations/revocations/" + revokeOperation:
			if r.Method == http.MethodGet && !revokeApplied.Load() {
				return remotePrincipalHTTPResponse(http.StatusNotFound, "application/json", ""), nil
			}
			if r.Method == http.MethodPut {
				var request remoteCollectorPrincipalRevokeRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if request.OperationKey != revokeOperation || request.RequestHash != revokeHash || request.PolicyRevision != "flow-runtime-v7" || request.PrincipalID != "principal-a" || request.PrincipalRef != "User:collector-a" {
					t.Errorf("revoke request=%+v", request)
				}
				revokeApplied.Store(true)
			} else if r.Method != http.MethodGet {
				t.Errorf("revoke method=%s", r.Method)
			}
			response = map[string]any{
				"operation_key": revokeOperation, "request_hash": revokeHash,
				"provider": "enterprise-kafka", "principal_ref": "User:collector-a",
				"receipt_ref": "provider://revocations/" + revokeOperation,
				"receipt":     map[string]any{"write_acl_absent": true},
			}
		default:
			t.Errorf("unexpected path=%s", r.URL.Path)
			status = http.StatusNotFound
		}
		payload, _ := json.Marshal(response)
		return remotePrincipalHTTPResponse(status, "application/json", string(payload)), nil
	})}
	provider := newTestRemoteCollectorPrincipalProvider(t, "https://provider.example", client)
	if _, found, err := provider.LookupGrant(context.Background(), grantOperation); err != nil || found {
		t.Fatalf("initial grant lookup found=%v err=%v", found, err)
	}
	grant, err := provider.Grant(context.Background(), CollectorPrincipalGrantProviderRequest{
		OperationKey: grantOperation, RequestHash: grantHash, PolicyRevision: "flow-runtime-v7",
		PrincipalID: "principal-a", TenantID: "tenant-a", CollectorID: "collector-a",
		ServiceType: "kafka", ACLPropagationDelay: 2500 * time.Millisecond,
	})
	if err != nil || grant.PrincipalRef != "User:collector-a" || grant.CredentialSecretRef != "vault://watchdog/collector-a" || len(grant.Receipt) == 0 {
		t.Fatalf("grant=%+v err=%v", grant, err)
	}
	if replay, found, err := provider.LookupGrant(context.Background(), grantOperation); err != nil || !found || replay.RequestHash != grantHash {
		t.Fatalf("grant lookup=%+v found=%v err=%v", replay, found, err)
	}
	if _, found, err := provider.LookupRevoke(context.Background(), revokeOperation); err != nil || found {
		t.Fatalf("initial revoke lookup found=%v err=%v", found, err)
	}
	revoked, err := provider.RevokeWrite(context.Background(), CollectorPrincipalRevokeProviderRequest{
		OperationKey: revokeOperation, RequestHash: revokeHash, PolicyRevision: "flow-runtime-v7",
		TenantID: "tenant-a", CollectorID: "collector-a", PrincipalID: "principal-a",
		ServiceType: "kafka", PrincipalRef: "User:collector-a",
	})
	if err != nil || revoked.PrincipalRef != "User:collector-a" || len(revoked.Receipt) == 0 {
		t.Fatalf("revoked=%+v err=%v", revoked, err)
	}
}

func TestRemoteCollectorPrincipalProviderRejectsUnsafeResponses(t *testing.T) {
	operationKey := strings.Repeat("a", 64)
	for _, test := range []struct {
		name        string
		contentType string
		status      int
		body        string
	}{
		{name: "provider secret error is redacted", contentType: "text/plain", status: http.StatusInternalServerError, body: "credential=do-not-log"},
		{name: "non json", contentType: "text/plain", status: http.StatusOK, body: "not-json"},
		{name: "unknown field", contentType: "application/json", status: http.StatusOK, body: `{"unexpected":"field"}`},
		{name: "multiple values", contentType: "application/json", status: http.StatusOK, body: `{}` + "\n" + `{}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: collectorPrincipalRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return remotePrincipalHTTPResponse(test.status, test.contentType, test.body), nil
			})}
			provider := newTestRemoteCollectorPrincipalProvider(t, "https://provider.example", client)
			_, _, err := provider.LookupGrant(context.Background(), operationKey)
			if err == nil || strings.Contains(err.Error(), "do-not-log") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestRemoteCollectorPrincipalProviderRequiresAdvertisedRecoveryRetention(t *testing.T) {
	operationKey := strings.Repeat("a", 64)
	client := &http.Client{Transport: collectorPrincipalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		response := remotePrincipalHTTPResponse(http.StatusNotFound, "application/json", "")
		response.Header.Set(collectorPrincipalProviderRetentionHeader, "3600")
		return response, nil
	})}
	provider := newTestRemoteCollectorPrincipalProvider(t, "https://provider.example", client)
	if _, _, err := provider.LookupGrant(context.Background(), operationKey); err == nil || !strings.Contains(err.Error(), "recovery window") {
		t.Fatalf("retention error=%v", err)
	}
}

func TestRemoteCollectorPrincipalProviderCircuitBreaker(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: collectorPrincipalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("transport secret")
	})}
	now := time.Unix(1_000_000, 0)
	config := testRemoteCollectorPrincipalProviderConfig("https://provider.example")
	config.FailureThreshold = 2
	config.CircuitOpenInterval = time.Minute
	provider, err := newRemoteCollectorPrincipalProviderWithClient(config, client, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	operationKey := strings.Repeat("a", 64)
	for index := 0; index < 2; index++ {
		if _, _, err := provider.LookupGrant(context.Background(), operationKey); err == nil || strings.Contains(err.Error(), "transport secret") {
			t.Fatalf("request %d error=%v", index, err)
		}
	}
	if _, _, err := provider.LookupGrant(context.Background(), operationKey); !errors.Is(err, ErrCollectorPrincipalProviderCircuitOpen) || calls.Load() != 2 {
		t.Fatalf("open circuit error=%v calls=%d", err, calls.Load())
	}
	health := provider.Health()
	if !health.CircuitOpen || health.AcceptingRequests || health.ConsecutiveFailures != 2 || health.RequestFailureTotal != 2 || health.CircuitRejectTotal != 1 || !health.LastFailureAt.Equal(now) {
		t.Fatalf("unexpected open-circuit health: %+v", health)
	}
	now = now.Add(time.Minute)
	if _, _, err := provider.LookupGrant(context.Background(), operationKey); err == nil || calls.Load() != 3 {
		t.Fatalf("probe error=%v calls=%d", err, calls.Load())
	}
}

func TestRemoteCollectorPrincipalProviderSemanticFailuresTripCircuit(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: collectorPrincipalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return remotePrincipalHTTPResponse(http.StatusOK, "application/json", `{"operation_key":"wrong"}`), nil
	})}
	config := testRemoteCollectorPrincipalProviderConfig("https://provider.example")
	config.FailureThreshold = 2
	provider, err := newRemoteCollectorPrincipalProviderWithClient(config, client, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	operationKey := strings.Repeat("a", 64)
	for attempt := 0; attempt < 2; attempt++ {
		if _, _, err := provider.LookupGrant(context.Background(), operationKey); !errors.Is(err, ErrCollectorPrincipalProviderResultInvalid) {
			t.Fatalf("semantic failure %d error=%v", attempt, err)
		}
	}
	if _, _, err := provider.LookupGrant(context.Background(), operationKey); !errors.Is(err, ErrCollectorPrincipalProviderCircuitOpen) || calls.Load() != 2 {
		t.Fatalf("semantic failures did not open circuit: error=%v calls=%d health=%+v", err, calls.Load(), provider.Health())
	}
}

func TestRemoteCollectorPrincipalProviderHealthSeparatesSuccessAndRemoteReject(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusNotFound)
	client := &http.Client{Transport: collectorPrincipalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return remotePrincipalHTTPResponse(int(status.Load()), "application/json", ""), nil
	})}
	now := time.Unix(2_000_000, 0).UTC()
	provider, err := newRemoteCollectorPrincipalProviderWithClient(testRemoteCollectorPrincipalProviderConfig("https://provider.example"), client, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	operationKey := strings.Repeat("a", 64)
	if _, found, err := provider.LookupGrant(context.Background(), operationKey); err != nil || found {
		t.Fatalf("lookup found=%v err=%v", found, err)
	}
	status.Store(http.StatusConflict)
	now = now.Add(time.Second)
	if _, _, err := provider.LookupGrant(context.Background(), operationKey); err == nil {
		t.Fatal("remote rejection was accepted")
	}
	health := provider.Health()
	if !health.AcceptingRequests || health.CircuitOpen || health.RequestSuccessTotal != 1 || health.RemoteRejectTotal != 1 || health.RequestFailureTotal != 0 || !health.LastSuccessAt.Equal(now.Add(-time.Second)) || !health.LastRemoteRejectAt.Equal(now) {
		t.Fatalf("unexpected provider health: %+v", health)
	}
	metrics := string(provider.PrometheusText())
	for _, want := range []string{
		"watchdog_collector_principal_provider_accepting_requests 1",
		`watchdog_collector_principal_provider_request_total{result="success"} 1`,
		`watchdog_collector_principal_provider_request_total{result="remote_reject"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	if strings.Contains(metrics, provider.config.Name) || strings.Contains(metrics, provider.config.BaseURL) {
		t.Fatalf("provider identity leaked into metric labels/body: %s", metrics)
	}
}

func TestRemoteCollectorPrincipalProviderAllowsOneHalfOpenProbe(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client := &http.Client{Transport: collectorPrincipalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		close(started)
		<-release
		return nil, errors.New("probe failed")
	})}
	now := time.Unix(3_000_000, 0)
	config := testRemoteCollectorPrincipalProviderConfig("https://provider.example")
	config.FailureThreshold = 1
	config.CircuitOpenInterval = time.Minute
	provider, err := newRemoteCollectorPrincipalProviderWithClient(config, client, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	provider.recordFailure()
	now = now.Add(time.Minute)
	operationKey := strings.Repeat("a", 64)
	done := make(chan error, 1)
	go func() {
		_, _, err := provider.LookupGrant(context.Background(), operationKey)
		done <- err
	}()
	<-started
	if _, _, err := provider.LookupGrant(context.Background(), operationKey); !errors.Is(err, ErrCollectorPrincipalProviderCircuitOpen) {
		t.Fatalf("concurrent half-open request was not rejected: %v", err)
	}
	health := provider.Health()
	if !health.HalfOpenProbe || health.AcceptingRequests || health.CircuitRejectTotal != 1 {
		t.Fatalf("unexpected half-open health: %+v", health)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("half-open probe unexpectedly succeeded")
	}
}

func TestBackendRuntimePublishesPassiveCollectorPrincipalProviderHealth(t *testing.T) {
	now := time.Unix(4_000_000, 0).UTC()
	provider, err := newRemoteCollectorPrincipalProviderWithClient(
		testRemoteCollectorPrincipalProviderConfig("https://provider-secret.example"),
		&http.Client{Transport: collectorPrincipalRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("transport secret")
		})},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	provider.recordFailure()
	runtime := &BackendRuntime{Config: BackendConfig{CollectorPrincipalProvider: RemoteCollectorPrincipalProviderConfig{Enabled: true}}}
	runtime.collectorPrincipalProvider = provider
	health := runtime.Health()
	if !health.CollectorPrincipalProvider.Enabled || health.CollectorPrincipalProvider.Health.RequestFailureTotal != 1 {
		t.Fatalf("provider health missing from backend runtime: %+v", health)
	}
	metrics := string(runtime.RuntimeMetrics())
	for _, want := range []string{
		"watchdog_collector_principal_provider_enabled 1",
		`watchdog_collector_principal_provider_request_total{result="failure"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("runtime metrics missing %q:\n%s", want, metrics)
		}
	}
	if strings.Contains(metrics, "provider-secret") || strings.Contains(metrics, "transport secret") {
		t.Fatalf("provider identity or error leaked into runtime metrics: %s", metrics)
	}
}

func newTestRemoteCollectorPrincipalProvider(t *testing.T, baseURL string, client *http.Client) *remoteCollectorPrincipalProvider {
	t.Helper()
	provider, err := newRemoteCollectorPrincipalProviderWithClient(testRemoteCollectorPrincipalProviderConfig(baseURL), client, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func testRemoteCollectorPrincipalProviderConfig(baseURL string) RemoteCollectorPrincipalProviderConfig {
	return RemoteCollectorPrincipalProviderConfig{
		Enabled: true, Name: "enterprise-kafka", BaseURL: baseURL,
		PolicyRevision: "flow-runtime-v7", RequestTimeout: time.Second,
		FailureThreshold: 5, CircuitOpenInterval: time.Minute,
		MinOperationRetention: 30 * 24 * time.Hour,
		TLSCertFile:           "/test/client.crt", TLSKeyFile: "/test/client.key",
	}
}

func remotePrincipalHTTPResponse(status int, contentType, body string) *http.Response {
	header := make(http.Header)
	header.Set("Content-Type", contentType)
	header.Set(collectorPrincipalProviderRetentionHeader, "2592000")
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

type collectorPrincipalRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn collectorPrincipalRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
