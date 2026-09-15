package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIHistoricalPreviewRequiresAdmin(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: historicalTestAuth(false)})
	rec := httptest.NewRecorder()
	body := `{"TargetID":"target-a","Start":"2026-06-01T00:00:00Z","End":"2026-07-01T00:00:00Z"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/historical/preview", strings.NewReader(body)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestAPIHistoricalPreviewReturnsEstimate(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: historicalTestAuth(true)})
	rec := httptest.NewRecorder()
	body := `{"TargetID":"target-a","Start":"2026-06-01T00:00:00Z","End":"2026-06-01T01:00:00Z"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/historical/preview?sample_step=300", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"EstimatedSamples":12`) {
		t.Fatalf("body missing estimate: %s", rec.Body.String())
	}
}

func TestAPIHistoricalArchiveAcceptsAdminRequest(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: historicalTestAuth(true)})
	rec := httptest.NewRecorder()
	body := `{"TargetID":"target-a","Start":"2026-06-01T00:00:00Z","End":"2026-07-01T00:00:00Z"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/historical/archive?operation_id=hist-a", strings.NewReader(body)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"Action":"archive"`) {
		t.Fatalf("body missing archive action: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"Status":"pending_manual_execution"`) {
		t.Fatalf("body missing pending status: %s", rec.Body.String())
	}
}

func historicalTestAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		actions := []Action{ActionView}
		if admin {
			actions = append(actions, ActionAdmin)
		}
		return AuthContext{
			TenantID: "tenant-a", UserID: "user-a", IsAdmin: admin,
			Grants: []Permission{{
				TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a",
				ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: actions,
			}},
		}, nil
	}
}

func TestValidateHistoricalDataRequestRejectsMissingResource(t *testing.T) {
	err := ValidateHistoricalDataRequest(HistoricalDataRequest{TenantID: "tenant-a", Action: HistoricalActionPreview})
	if err == nil {
		t.Fatal("expected missing resource error")
	}
}

func TestPreviewHistoricalDataEstimatesBytes(t *testing.T) {
	preview, err := PreviewHistoricalData(HistoricalDataRequest{
		TenantID: "tenant-a",
		TargetID: "target-a",
		Action:   HistoricalActionPreview,
		Start:    time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC),
	}, 5*time.Minute, 100)
	if err != nil {
		t.Fatalf("PreviewHistoricalData() error = %v", err)
	}
	if preview.EstimatedSamples != 12 || preview.EstimatedBytes != 1200 {
		t.Fatalf("preview = %#v", preview)
	}
}

func TestCreateHistoricalDataOperationRequiresManualAction(t *testing.T) {
	_, err := CreateHistoricalDataOperation("hist-a", HistoricalDataRequest{
		TenantID: "tenant-a",
		TargetID: "target-a",
		Action:   HistoricalActionPreview,
		Start:    time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("expected preview action error")
	}
}
