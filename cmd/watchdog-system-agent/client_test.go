package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type systemAgentRoundTripFunc func(*http.Request) (*http.Response, error)

func (f systemAgentRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSystemAgentClientEndpoints(t *testing.T) {
	client := systemAgentClient{HubURL: "http://127.0.0.1:8091/", AgentID: "agent-system-a"}
	endpoint, err := client.endpoint("samples")
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://127.0.0.1:8091/api/v1/system-agents/agent-system-a/samples" {
		t.Fatalf("endpoint = %s", endpoint)
	}
	heartbeatEndpoint, err := client.genericAgentEndpoint("heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	if heartbeatEndpoint != "http://127.0.0.1:8091/api/v1/agents/agent-system-a/heartbeat" {
		t.Fatalf("heartbeat endpoint = %s", heartbeatEndpoint)
	}
}

func TestSystemAgentClientReportStatusDefaultsToSuccess(t *testing.T) {
	var report agentRunReport
	httpClient := &http.Client{Transport: systemAgentRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(req.Body).Decode(&report); err != nil {
			t.Errorf("decode report: %v", err)
		}
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}

	client := systemAgentClient{HubURL: "http://watchdog.invalid", AgentID: "agent-system-a", AgentToken: "secret", HTTPClient: httpClient}
	if err := client.ReportStatus(context.Background(), agentRunReport{}); err != nil {
		t.Fatal(err)
	}
	if report.Status != agentRunSuccess {
		t.Fatalf("status = %q, want %q", report.Status, agentRunSuccess)
	}
}
