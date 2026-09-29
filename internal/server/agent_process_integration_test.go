package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
)

// TestAgentProcessesRegisterApplyLKGAndRevocation executes the four production
// agent binaries. It is isolated behind a disposable MySQL DSN because it also
// proves clean-schema registration, plan delivery and revocation end to end.
func TestAgentProcessesRegisterApplyLKGAndRevocation(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_AGENT_PROCESS_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_AGENT_PROCESS_TEST_MYSQL_DSN is not set")
	}
	temporary := t.TempDir()
	const sharedAgentToken = "process-shared-secret"
	s, err := New(Config{
		MySQL:  MySQLConfig{DSN: dsn},
		Admin:  AdminConfig{Username: "process-test-admin", Password: "process-test-password"},
		Agents: AgentsConfig{SharedToken: sharedAgentToken},
		AgentPlans: AgentPlansConfig{
			SigningKeyID: "process-test-key", SigningPrivateKey: filepath.Join(temporary, "private.pem"),
			DefaultTTL: 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var unavailable atomic.Bool
	controlPlane := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if unavailable.Load() {
			http.Error(writer, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		s.engine.ServeHTTP(writer, request)
	}))
	defer controlPlane.Close()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "process-test-admin", "password": "process-test-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login: status=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	auth := map[string]string{"X-CSRF-Token": cookieValue(cookies, csrfCookie)}
	publicKeyFile := filepath.Join(temporary, "public.pem")
	if err := agentplan.WritePublicKey(publicKeyFile, s.agentPlanPublic); err != nil {
		t.Fatal(err)
	}

	root := repositoryRoot(t)
	binaries := map[string]string{}
	for _, command := range []string{"watchdog-system-agent", "watchdog-snmp-collector", "watchdog-flow-collect", "watchdog-flow-worker"} {
		path := filepath.Join(temporary, command)
		build := exec.CommandContext(t.Context(), "go", "build", "-o", path, "./cmd/"+command)
		build.Dir = root
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", command, err, output)
		}
		binaries[command] = path
	}

	processes := []agentProcessFixture{
		{Command: "watchdog-system-agent", AgentID: "process-system", Kind: "system", Capability: "system.samples/v1", Config: map[string]any{"interval_seconds": 15}},
		{Command: "watchdog-snmp-collector", AgentID: "process-snmp", Kind: "snmp", Capability: "snmp.poll/v2", Config: map[string]any{"interval_seconds": 30, "poll_limit": 100}},
		{Command: "watchdog-flow-collect", AgentID: "process-flow-collect", Kind: "flow_collect", Capability: "flow.receive.sflow/v1", Config: map[string]any{"sockets": 2, "max_datagram_bytes": 9000}},
		{Command: "watchdog-flow-worker", AgentID: "process-flow-worker", Kind: "flow_worker", Capability: "flow.write.clickhouse/v1", Config: map[string]any{"kafka_fetch_min_bytes": 1048576, "kafka_fetch_max_wait_ms": 250, "clickhouse_block_max_rows": 50000, "clickhouse_block_max_bytes": 67108864}},
	}

	for index := range processes {
		process := &processes[index]
		process.Binary = binaries[process.Command]
		process.TokenFile = filepath.Join(temporary, process.AgentID+".token")
		process.LKGFile = filepath.Join(temporary, process.AgentID+".lkg")
		// The operator provisions the installation-wide shared token; the process
		// registers itself idempotently on first sync with no per-agent credential.
		if err := os.WriteFile(process.TokenFile, []byte(sharedAgentToken+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if output, err := runAgentProcess(t, *process, controlPlane.URL, publicKeyFile); err == nil || !strings.Contains(string(output), "no desired plan") {
			t.Fatalf("%s did not register before reporting no plan: err=%v\n%s", process.Command, err, output)
		}
		created := requestJSON(t, s, http.MethodPost, "/api/v1/agents/"+process.AgentID+"/plans", map[string]any{
			"required_capabilities": []string{process.Capability}, "config": process.Config,
			"expected_desired_plan_version": 0,
		}, auth, cookies...)
		if created.Code != http.StatusCreated {
			t.Fatalf("plan %s: status=%d body=%s", process.Command, created.Code, created.Body.String())
		}
		if output, err := runAgentProcess(t, *process, controlPlane.URL, publicKeyFile); err != nil {
			t.Fatalf("%s apply: %v\n%s", process.Command, err, output)
		}
		var acked uint64
		if err := s.db.QueryRow(`SELECT acked_plan_version FROM agents WHERE id=?`, process.AgentID).Scan(&acked); err != nil || acked != 1 {
			t.Fatalf("%s ACK: version=%d err=%v", process.Command, acked, err)
		}
		if _, err := os.Stat(process.LKGFile); err != nil {
			t.Fatalf("%s LKG was not persisted: %v", process.Command, err)
		}
	}

	unavailable.Store(true)
	for _, process := range processes {
		// Environment files written by the enrollment-era activate script still
		// pass -agent-enrollment-token-file; it must be ignored, not fatal.
		legacy := process
		legacy.ExtraArgs = []string{"-agent-enrollment-token-file", filepath.Join(temporary, "enrollment.token")}
		output, err := runAgentProcess(t, legacy, controlPlane.URL, publicKeyFile)
		if err != nil || !strings.Contains(string(output), "deprecated and ignored") {
			t.Fatalf("%s offline LKG with the legacy enrollment flag: %v\n%s", process.Command, err, output)
		}
	}
	unavailable.Store(false)

	for _, process := range processes {
		current := requestJSON(t, s, http.MethodGet, "/api/v1/agents/"+process.AgentID, nil, nil, cookies...)
		revoked := requestJSON(t, s, http.MethodPost, "/api/v1/agents/"+process.AgentID+"/revoke", nil, map[string]string{
			"X-CSRF-Token": auth["X-CSRF-Token"], "If-Match": current.Header().Get("ETag"),
		}, cookies...)
		if revoked.Code != http.StatusOK {
			t.Fatalf("revoke %s: status=%d body=%s", process.Command, revoked.Code, revoked.Body.String())
		}
		if output, err := runAgentProcess(t, process, controlPlane.URL, publicKeyFile); err == nil || !strings.Contains(string(output), "credential is invalid or revoked") {
			t.Fatalf("%s used LKG after revocation: err=%v\n%s", process.Command, err, output)
		}
	}
}

type agentProcessFixture struct {
	Command, Binary, AgentID, Kind, Capability string
	TokenFile, LKGFile                         string
	Config                                     map[string]any
	ExtraArgs                                  []string
}

func runAgentProcess(t *testing.T, process agentProcessFixture, controlPlane, publicKey string) ([]byte, error) {
	t.Helper()
	arguments := []string{
		"-agent-id", process.AgentID, "-agent-token-file", process.TokenFile,
		"-agent-plan-public-key", publicKey, "-agent-plan-lkg", process.LKGFile,
		"-agent-plan-check",
	}
	switch process.Kind {
	case "system":
		arguments = append(arguments, "-hub-url", controlPlane)
	case "snmp", "flow_collect":
		arguments = append(arguments, "-control-plane-url", controlPlane)
	case "flow_worker":
		arguments = append(arguments, "-control-plane-url", controlPlane, "-worker-id", process.AgentID)
		arguments = removeFlagPair(arguments, "-agent-id")
	}
	arguments = append(arguments, process.ExtraArgs...)
	command := exec.CommandContext(t.Context(), process.Binary, arguments...)
	return command.CombinedOutput()
}

func removeFlagPair(arguments []string, name string) []string {
	result := make([]string, 0, len(arguments)-2)
	for index := 0; index < len(arguments); index++ {
		if arguments[index] == name && index+1 < len(arguments) {
			index++
			continue
		}
		result = append(result, arguments[index])
	}
	return result
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
