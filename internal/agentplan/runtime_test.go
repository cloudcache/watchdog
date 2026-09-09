package agentplan

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
		runtime.RunHeartbeats(ctx, "revoked", time.Millisecond, func(err error) { reported <- err })
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
