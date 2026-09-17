package agentplan

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

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
