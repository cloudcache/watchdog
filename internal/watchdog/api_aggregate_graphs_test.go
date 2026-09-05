package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIAggregateGraphIfMatchOptimisticLocking(t *testing.T) {
	updatedAt := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	repo := newFakeAggregateGraphRepo()
	repo.graphs = []AggregateGraph{{ID: "graph-a", TenantID: "tenant-a", Name: "Edge", UpdatedAt: updatedAt}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(true), AggregateGraphs: repo})

	// GET exposes a weak ETag derived from updated_at.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/aggregate-graphs/graph-a", nil))
	etag := rec.Header().Get("ETag")
	if rec.Code != http.StatusOK || etag != WeakETagFromTime(updatedAt) {
		t.Fatalf("get status=%d etag=%q", rec.Code, etag)
	}

	body := `{"Name":"Edge renamed"}`

	// A stale If-Match is rejected with 412 on PATCH.
	stale := httptest.NewRecorder()
	staleReq := httptest.NewRequest(http.MethodPatch, "/api/v1/aggregate-graphs/graph-a", strings.NewReader(body))
	staleReq.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(stale, staleReq)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match status = %d, want 412", stale.Code)
	}

	// The current ETag is accepted.
	ok := httptest.NewRecorder()
	okReq := httptest.NewRequest(http.MethodPatch, "/api/v1/aggregate-graphs/graph-a", strings.NewReader(body))
	okReq.Header.Set("If-Match", etag)
	router.ServeHTTP(ok, okReq)
	if ok.Code != http.StatusOK {
		t.Fatalf("matched If-Match status = %d body = %s", ok.Code, ok.Body.String())
	}

	// DELETE also honors a stale If-Match.
	del := httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/aggregate-graphs/graph-a", nil)
	delReq.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(del, delReq)
	if del.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale delete If-Match status = %d, want 412", del.Code)
	}
}
