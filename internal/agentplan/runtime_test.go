package agentplan

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeEnrollmentReplacesBootstrapSecretWithMachineCredential(t *testing.T) {
	dir := t.TempDir()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicKeyFile := filepath.Join(dir, "agent-plan.pub")
	if err := WritePublicKey(publicKeyFile, publicKey); err != nil {
		t.Fatal(err)
	}
	enrollmentFile := filepath.Join(dir, "enrollment")
	credentialFile := filepath.Join(dir, "credential")
	if err := os.WriteFile(enrollmentFile, []byte("one-time\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime := RuntimeConfig{
		BaseURL: "http://control-plane.test", AgentID: "snmp-main", Name: "snmp-main", Kind: "snmp",
		Role: "snmp", Mode: "push", SoftwareVersion: "test", APIVersion: "v1",
		Capabilities: []string{"snmp.poll/v2"}, TokenFile: credentialFile, EnrollmentFile: enrollmentFile,
		PublicKeyFile: publicKeyFile, LKGFile: filepath.Join(dir, "plan.lkg"), BootID: "boot-1",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) *http.Response {
			status, body := http.StatusNotFound, ""
			if request.Method == http.MethodPost && request.URL.Path == "/api/v1/agents/register" {
				status, body = http.StatusCreated, `{"credential":{"token":"machine-secret"}}`
			}
			return &http.Response{
				StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request,
			}
		})},
	}
	_, token, err := runtime.Sync(context.Background(), func(context.Context, Spec) error { return nil })
	if !errors.Is(err, ErrNoPlan) || token != "machine-secret" {
		t.Fatalf("sync token=%q err=%v", token, err)
	}
	if _, err := os.Stat(enrollmentFile); !os.IsNotExist(err) {
		t.Fatalf("consumed enrollment file remains: %v", err)
	}
	credential, err := os.ReadFile(credentialFile)
	if err != nil || strings.TrimSpace(string(credential)) != "machine-secret" {
		t.Fatalf("credential=%q err=%v", credential, err)
	}
	info, err := os.Stat(credentialFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("credential mode=%v", info.Mode().Perm())
	}
}

func TestRuntimeSharedTokenRegistersMissingAgentAndRetries(t *testing.T) {
	dir := t.TempDir()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicKeyFile := filepath.Join(dir, "agent-plan.pub")
	if err := WritePublicKey(publicKeyFile, publicKey); err != nil {
		t.Fatal(err)
	}
	registered := false
	runtime := RuntimeConfig{
		BaseURL: "http://control-plane.test", AgentID: "snmp-main", Name: "snmp-main", Kind: "snmp",
		Role: "snmp", Mode: "push", SoftwareVersion: "test", APIVersion: "v1",
		Capabilities: []string{"snmp.poll/v2"}, Token: "installation-secret",
		PublicKeyFile: publicKeyFile, LKGFile: filepath.Join(dir, "plan.lkg"), BootID: "boot-1",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) *http.Response {
			status, body := http.StatusUnauthorized, ""
			switch {
			case request.Method == http.MethodPost && request.URL.Path == "/api/v1/agents/register":
				var payload map[string]any
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				if payload["token"] != "installation-secret" || payload["enrollment_token"] != "" {
					t.Fatalf("registration payload=%v", payload)
				}
				registered = true
				status, body = http.StatusCreated, `{"agent":{"id":"snmp-main"}}`
			case registered && request.Method == http.MethodGet && request.URL.Path == "/api/v1/agents/snmp-main/plan":
				status = http.StatusNotFound
			}
			return &http.Response{
				StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request,
			}
		})},
	}
	_, token, err := runtime.Sync(context.Background(), func(context.Context, Spec) error { return nil })
	if !errors.Is(err, ErrNoPlan) || token != "installation-secret" || !registered {
		t.Fatalf("registered=%v token=%q err=%v", registered, token, err)
	}
}

func TestDecodeConfigIsStrict(t *testing.T) {
	spec := Spec{Config: json.RawMessage(`{"interval_seconds":30}`)}
	var value struct {
		IntervalSeconds int `json:"interval_seconds"`
	}
	if err := DecodeConfig(spec, &value); err != nil || value.IntervalSeconds != 30 {
		t.Fatalf("decode config: value=%+v err=%v", value, err)
	}
	for _, payload := range []string{`{"unknown":1}`, `{} {}`} {
		spec.Config = json.RawMessage(payload)
		if err := DecodeConfig(spec, &value); err == nil {
			t.Fatalf("invalid config accepted: %s", payload)
		}
	}
}

func TestRunHeartbeatsStopsOnRevocation(t *testing.T) {
	reported := make(chan error, 1)
	runtime := RuntimeConfig{
		BaseURL: "http://control-plane.test",
		AgentID: "agent-test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) *http.Response {
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    request,
			}
		})},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		runtime.RunHeartbeats(ctx, "revoked", time.Millisecond, 0, func(err error) { reported <- err })
		close(done)
	}()
	select {
	case err := <-reported:
		if !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("reported error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("revocation was not reported")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("heartbeat loop did not stop after revocation")
	}
}

func TestRunHeartbeatsSendsImmediately(t *testing.T) {
	called := make(chan struct{}, 1)
	runtime := RuntimeConfig{
		BaseURL: "http://control-plane.test",
		AgentID: "agent-test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) *http.Response {
			called <- struct{}{}
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header), Body: http.NoBody, Request: request,
			}
		})},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runtime.RunHeartbeats(ctx, "secret", time.Hour, 0, nil)
		close(done)
	}()
	select {
	case <-called:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("first heartbeat waited for the periodic interval")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not stop after cancellation")
	}
}

func TestHeartbeatReadsPlanVersionsAndAcceptsLegacyEmptyResponse(t *testing.T) {
	responseBody := `{"desired_plan_version":4,"acked_plan_version":3}`
	runtime := RuntimeConfig{
		BaseURL: "http://control-plane.test",
		AgentID: "agent-test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) *http.Response {
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}
		})},
	}
	state, err := runtime.heartbeat(context.Background(), "secret")
	if err != nil || state.DesiredPlanVersion != 4 || state.AckedPlanVersion != 3 {
		t.Fatalf("heartbeat state=%+v err=%v", state, err)
	}
	responseBody = ""
	state, err = runtime.heartbeat(context.Background(), "secret")
	if err != nil || state != (HeartbeatState{}) {
		t.Fatalf("legacy heartbeat state=%+v err=%v", state, err)
	}
}

func TestRunHeartbeatsStopsWhenPlanChanges(t *testing.T) {
	reported := make(chan error, 1)
	runtime := RuntimeConfig{
		BaseURL: "http://control-plane.test",
		AgentID: "agent-test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) *http.Response {
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"desired_plan_version":2,"acked_plan_version":1}`)),
				Request:    request,
			}
		})},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		runtime.RunHeartbeats(ctx, "secret", time.Millisecond, 1, func(err error) { reported <- err })
		close(done)
	}()
	select {
	case err := <-reported:
		if !errors.Is(err, ErrPlanChanged) {
			t.Fatalf("reported error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("plan change was not reported")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("heartbeat loop did not stop after a plan change")
	}
}
