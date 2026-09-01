package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeExportRepository struct {
	tasks []ExportTask
}

func (r *fakeExportRepository) CreateExportTask(_ context.Context, task ExportTask) (ExportTask, error) {
	task = normalizeExportTask(task)
	r.tasks = append(r.tasks, task)
	return task, nil
}

func (r *fakeExportRepository) GetExportTask(_ context.Context, _ ID, taskID ID) (ExportTask, error) {
	for _, task := range r.tasks {
		if task.ID == taskID {
			return task, nil
		}
	}
	return ExportTask{}, errNotFoundForTest{}
}

func (r *fakeExportRepository) ListExportTasks(_ context.Context, _ ID, createdBy ID) ([]ExportTask, error) {
	if createdBy == "" {
		return r.tasks, nil
	}
	tasks := make([]ExportTask, 0, len(r.tasks))
	for _, task := range r.tasks {
		if task.CreatedBy == createdBy {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (*fakeExportRepository) MarkExportRunning(context.Context, ID, ID) error {
	return nil
}

func (r *fakeExportRepository) RetryExportTask(_ context.Context, _ ID, taskID ID) error {
	for i := range r.tasks {
		if r.tasks[i].ID == taskID && r.tasks[i].Status == ExportStatusFailed {
			r.tasks[i].Status = ExportStatusPending
			r.tasks[i].FileRef = ""
			r.tasks[i].ErrorMessage = ""
		}
	}
	return nil
}

func (*fakeExportRepository) MarkExportComplete(context.Context, ID, ID, string) error {
	return nil
}

func (*fakeExportRepository) MarkExportFailed(context.Context, ID, ID, string) error {
	return nil
}

func TestAPIExportsCreate(t *testing.T) {
	repo := &fakeExportRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: exportTestAuth(false), Exports: repo})
	rec := httptest.NewRecorder()
	body := `{"ID":"export-a","TargetID":"target-a","PeriodType":"custom","RangeStart":"2026-06-01T00:00:00Z","RangeEnd":"2026-06-01T01:00:00Z","Step":300000000000,"Aggregation":"p95_5m","ValueMode":"corrected","Format":"csv"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/exports", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.tasks) != 1 || repo.tasks[0].CreatedBy != "user-a" {
		t.Fatalf("tasks = %#v", repo.tasks)
	}
}

func TestAPIExportsCreateGeneratesID(t *testing.T) {
	repo := &fakeExportRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: exportTestAuth(false), Exports: repo})
	rec := httptest.NewRecorder()
	body := `{"TargetID":"target-a","PeriodType":"custom","RangeStart":"2026-06-01T00:00:00Z","RangeEnd":"2026-06-01T01:00:00Z","Step":300000000000,"Aggregation":"p95_5m","ValueMode":"corrected","Format":"csv"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/exports", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.tasks) != 1 || !strings.HasPrefix(string(repo.tasks[0].ID), "export_") {
		t.Fatalf("tasks = %#v", repo.tasks)
	}
}

func TestAPIExportsCreateResolvesTargetFromPort(t *testing.T) {
	repo := &fakeExportRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    exportTestAuth(false),
		Exports: repo,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
		},
	})
	rec := httptest.NewRecorder()
	body := `{"ID":"export-a","PortID":"port-a","PeriodType":"custom","RangeStart":"2026-06-01T00:00:00Z","RangeEnd":"2026-06-01T01:00:00Z","Step":300000000000,"Aggregation":"p95_5m","ValueMode":"corrected","Format":"csv"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/exports", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.tasks) != 1 || repo.tasks[0].TargetID != "target-a" || repo.tasks[0].PortID != "port-a" {
		t.Fatalf("tasks = %#v", repo.tasks)
	}
}

func TestAPIExportsCreateRejectsMismatchedPortTarget(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    exportTestAuth(true),
		Exports: &fakeExportRepository{},
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
		},
	})
	rec := httptest.NewRecorder()
	body := `{"ID":"export-a","TargetID":"target-b","PortID":"port-a","PeriodType":"custom","RangeStart":"2026-06-01T00:00:00Z","RangeEnd":"2026-06-01T01:00:00Z","Step":300000000000,"Aggregation":"p95_5m","ValueMode":"corrected","Format":"csv"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/exports", strings.NewReader(body)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", rec.Code, rec.Body.String())
	}
}

func TestAPIExportsRejectsRawWithoutAdmin(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: exportTestAuth(false), Exports: &fakeExportRepository{}})
	rec := httptest.NewRecorder()
	body := `{"ID":"export-a","TargetID":"target-a","PeriodType":"custom","RangeStart":"2026-06-01T00:00:00Z","RangeEnd":"2026-06-01T01:00:00Z","Step":300000000000,"Aggregation":"p95_5m","ValueMode":"raw","Format":"csv"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/exports", strings.NewReader(body)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestAPIExportsListOnlyCurrentUserUnlessAdminTenantScope(t *testing.T) {
	repo := &fakeExportRepository{tasks: []ExportTask{
		{ID: "export-a", TenantID: "tenant-a", CreatedBy: "user-a", RangeStart: time.Now(), RangeEnd: time.Now().Add(time.Hour), Step: 5 * time.Minute, Aggregation: AggregationP95FiveMinute, ValueMode: ExportValueCorrected, Format: ExportFormatCSV},
		{ID: "export-b", TenantID: "tenant-a", CreatedBy: "user-b", RangeStart: time.Now(), RangeEnd: time.Now().Add(time.Hour), Step: 5 * time.Minute, Aggregation: AggregationP95FiveMinute, ValueMode: ExportValueCorrected, Format: ExportFormatCSV},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: exportTestAuth(false), Exports: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/exports", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "export-a") || strings.Contains(body, "export-b") {
		t.Fatalf("unexpected body = %s", body)
	}
}

func TestAPIExportsDownloadCompleteFile(t *testing.T) {
	files := &CSVExportWriter{Files: map[string][]byte{"exports/export-a.csv": []byte("timestamp,corrected_value\n")}}
	repo := &fakeExportRepository{tasks: []ExportTask{{
		ID:        "export-a",
		TenantID:  "tenant-a",
		CreatedBy: "user-a",
		Status:    ExportStatusComplete,
		FileRef:   "exports/export-a.csv",
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: exportTestAuth(false), Exports: repo, ExportFiles: files})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/exports/export-a/download", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "corrected_value") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/csv; charset=utf-8" {
		t.Fatalf("content-type = %s", got)
	}
}

func TestAPIExportsDownloadRejectsPendingFile(t *testing.T) {
	repo := &fakeExportRepository{tasks: []ExportTask{{
		ID:        "export-a",
		TenantID:  "tenant-a",
		CreatedBy: "user-a",
		Status:    ExportStatusPending,
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: exportTestAuth(false), Exports: repo, ExportFiles: &CSVExportWriter{}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/exports/export-a/download", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestAPIExportsRetryFailedTask(t *testing.T) {
	repo := &fakeExportRepository{tasks: []ExportTask{{
		ID:           "export-a",
		TenantID:     "tenant-a",
		CreatedBy:    "user-a",
		Status:       ExportStatusFailed,
		FileRef:      "exports/export-a.csv",
		ErrorMessage: "vm unavailable",
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: exportTestAuth(false), Exports: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/exports/export-a/retry", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.tasks[0].Status != ExportStatusPending || repo.tasks[0].FileRef != "" || repo.tasks[0].ErrorMessage != "" {
		t.Fatalf("task = %#v", repo.tasks[0])
	}
}

func exportTestAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		actions := []Action{ActionExport}
		if admin {
			actions = append(actions, ActionAdmin)
		}
		return AuthContext{
			TenantID: "tenant-a",
			UserID:   "user-a",
			IsAdmin:  admin,
			Grants: []Permission{{
				TenantID:     "tenant-a",
				SubjectType:  SubjectUser,
				SubjectID:    "user-a",
				ResourceType: ResourceTarget,
				ResourceID:   "target-a",
				Actions:      actions,
			}},
		}, nil
	}
}
