package watchdog

import "testing"

func TestSystemAgentClientEndpoints(t *testing.T) {
	client := SystemAgentClient{HubURL: "http://127.0.0.1:8091/", AgentID: "agent-system-a"}
	endpoint, err := client.endpoint("samples")
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://127.0.0.1:8091/api/v1/system-agents/agent-system-a/samples" {
		t.Fatalf("endpoint = %s", endpoint)
	}
	statusEndpoint, err := client.genericAgentEndpoint("status")
	if err != nil {
		t.Fatal(err)
	}
	if statusEndpoint != "http://127.0.0.1:8091/api/v1/agents/agent-system-a/status" {
		t.Fatalf("status endpoint = %s", statusEndpoint)
	}
}
