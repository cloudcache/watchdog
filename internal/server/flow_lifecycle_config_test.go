package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestFlowPolicyAPIIsReadOnlyWhileConfigManaged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, managed := range []bool{true, false} {
		s := &Server{cfg: Config{Flow: FlowConfig{Lifecycle: FlowLifecycleConfig{Enabled: managed}}}}
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/flow/storage/policies", nil)
		s.rejectConfigManagedFlowPolicy(c)
		if c.IsAborted() != managed || (managed && recorder.Code != http.StatusConflict) {
			t.Fatalf("managed=%v aborted=%v status=%d", managed, c.IsAborted(), recorder.Code)
		}
	}
}
