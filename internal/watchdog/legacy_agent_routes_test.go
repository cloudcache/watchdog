package watchdog

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLegacyAgentRoutesAreNotRegistered(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{})
	for _, path := range []string{
		"/api/v1/agent-registry",
		"/api/v1/agent-registry/agent-1",
		"/api/v1/system-agents/agent-1/heartbeat",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		if res.Code != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want %d", path, res.Code, http.StatusNotFound)
		}
	}
}
