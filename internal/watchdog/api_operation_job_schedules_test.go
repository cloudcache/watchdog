package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeOperationJobScheduleRepository struct {
	item   OperationJobSchedule
	filter OperationJobScheduleFilter
	total  int64
}

func (f *fakeOperationJobScheduleRepository) ListOperationJobSchedules(_ context.Context, tenantID ID, filter OperationJobScheduleFilter) ([]OperationJobSchedule, int64, error) {
	f.filter = filter
	if f.item.ID == "" || f.item.TenantID != tenantID {
		return []OperationJobSchedule{}, 0, nil
	}
	return []OperationJobSchedule{f.item}, f.total, nil
}

func (f *fakeOperationJobScheduleRepository) GetOperationJobSchedule(_ context.Context, tenantID, scheduleID ID) (OperationJobSchedule, error) {
	if f.item.ID != scheduleID || f.item.TenantID != tenantID {
		return OperationJobSchedule{}, sql.ErrNoRows
	}
	return f.item, nil
}

func (f *fakeOperationJobScheduleRepository) CreateOperationJobSchedule(_ context.Context, item OperationJobSchedule) (OperationJobSchedule, error) {
	item.ID = "schedule-created"
	item.RowVersion = 1
	f.item = item
	f.total = 1
	return item, nil
}

func (f *fakeOperationJobScheduleRepository) UpdateOperationJobSchedule(_ context.Context, item OperationJobSchedule, expected uint64) (OperationJobSchedule, error) {
	if f.item.RowVersion != expected {
		return OperationJobSchedule{}, ErrOperationJobScheduleConflict
	}
	item.RowVersion = expected + 1
	f.item = item
	return item, nil
}

func (f *fakeOperationJobScheduleRepository) DeleteOperationJobSchedule(_ context.Context, tenantID, scheduleID ID, expected uint64) error {
	if f.item.ID != scheduleID || f.item.TenantID != tenantID {
		return sql.ErrNoRows
	}
	if f.item.RowVersion != expected {
		return ErrOperationJobScheduleConflict
	}
	f.item = OperationJobSchedule{}
	return nil
}

func TestOperationJobScheduleAPICRUDAndServerFilter(t *testing.T) {
	repo := &fakeOperationJobScheduleRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: collectorPrincipalAPIAuth(true), OperationJobSchedules: repo,
	})

	create := httptest.NewRequest(http.MethodPost, "/api/v1/operation-job-schedules", strings.NewReader(`{
		"name":"Daily cleanup","job_type":"cleanup","partition_key":"tenant-default",
		"cron_expression":"@daily","timezone":"UTC","payload":{"schema_version":1}
	}`))
	created := httptest.NewRecorder()
	router.ServeHTTP(created, create)
	if created.Code != http.StatusCreated || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create status=%d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	if repo.item.TenantID != "tenant-a" || repo.item.ScopeType != OperationJobScopeTenant || !repo.item.Enabled || repo.item.MaxInflight != 1 || repo.item.NextRunAt.IsZero() {
		t.Fatalf("created item = %#v", repo.item)
	}

	listed := httptest.NewRecorder()
	router.ServeHTTP(listed, httptest.NewRequest(http.MethodGet, "/api/v1/operation-job-schedules?q=clean&job_type=cleanup&enabled=true&sort=next_run_at&order=desc&limit=20&offset=10", nil))
	if listed.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", listed.Code, listed.Body.String())
	}
	if repo.filter.Search != "clean" || repo.filter.JobType != "cleanup" || repo.filter.Enabled == nil || !*repo.filter.Enabled || repo.filter.Sort != "next_run_at" || !repo.filter.Desc || repo.filter.Limit != 20 || repo.filter.Offset != 10 {
		t.Fatalf("list filter = %#v", repo.filter)
	}

	patch := httptest.NewRequest(http.MethodPatch, "/api/v1/operation-job-schedules/schedule-created", strings.NewReader(`{"enabled":false,"max_inflight":2}`))
	patch.Header.Set("If-Match", `"1"`)
	patched := httptest.NewRecorder()
	router.ServeHTTP(patched, patch)
	if patched.Code != http.StatusOK || patched.Header().Get("ETag") != `"2"` || repo.item.Enabled || repo.item.MaxInflight != 2 || repo.item.Name != "Daily cleanup" {
		t.Fatalf("patch status=%d item=%#v body=%s", patched.Code, repo.item, patched.Body.String())
	}

	stale := httptest.NewRequest(http.MethodPatch, "/api/v1/operation-job-schedules/schedule-created", strings.NewReader(`{"enabled":true}`))
	stale.Header.Set("If-Match", `"1"`)
	staleResponse := httptest.NewRecorder()
	router.ServeHTTP(staleResponse, stale)
	if staleResponse.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale patch status=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}

	remove := httptest.NewRequest(http.MethodDelete, "/api/v1/operation-job-schedules/schedule-created", nil)
	remove.Header.Set("If-Match", `"2"`)
	removed := httptest.NewRecorder()
	router.ServeHTTP(removed, remove)
	if removed.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", removed.Code, removed.Body.String())
	}
}

func TestOperationJobScheduleAPIRejectsUnknownFieldsAndRequiresAdmin(t *testing.T) {
	repo := &fakeOperationJobScheduleRepository{}
	viewer := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(false), OperationJobSchedules: repo})
	denied := httptest.NewRecorder()
	viewer.ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/api/v1/operation-job-schedules", strings.NewReader(`{}`)))
	if denied.Code != http.StatusForbidden {
		t.Fatalf("viewer create status=%d", denied.Code)
	}

	admin := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), OperationJobSchedules: repo})
	unknown := httptest.NewRecorder()
	admin.ServeHTTP(unknown, httptest.NewRequest(http.MethodPost, "/api/v1/operation-job-schedules", strings.NewReader(`{"unknown":true}`)))
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", unknown.Code, unknown.Body.String())
	}
	badFilter := httptest.NewRecorder()
	admin.ServeHTTP(badFilter, httptest.NewRequest(http.MethodGet, "/api/v1/operation-job-schedules?enabled=maybe", nil))
	if badFilter.Code != http.StatusBadRequest {
		t.Fatalf("bad filter status=%d body=%s", badFilter.Code, badFilter.Body.String())
	}
}

func TestWriteOperationJobScheduleErrorMapsConflicts(t *testing.T) {
	for _, test := range []struct {
		err  error
		want int
	}{
		{sql.ErrNoRows, http.StatusNotFound},
		{ErrOperationJobScheduleConflict, http.StatusPreconditionFailed},
		{ErrOperationJobScheduleExists, http.StatusConflict},
		{errors.New("invalid"), http.StatusBadRequest},
	} {
		recorder := httptest.NewRecorder()
		writeOperationJobScheduleError(recorder, test.err)
		if recorder.Code != test.want {
			t.Fatalf("error %v status=%d want=%d", test.err, recorder.Code, test.want)
		}
		var body map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
	}
}
