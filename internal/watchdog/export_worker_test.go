package watchdog

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeExportWorkerRepo struct {
	running  bool
	complete bool
	failed   string
	fileRef  string
	pending  []ExportTask
	listed   int
}

func (*fakeExportWorkerRepo) CreateExportTask(context.Context, ExportTask) (ExportTask, error) {
	return ExportTask{}, nil
}
func (*fakeExportWorkerRepo) GetExportTask(context.Context, ID, ID) (ExportTask, error) {
	return ExportTask{}, nil
}
func (*fakeExportWorkerRepo) ListExportTasks(context.Context, ID, ID) ([]ExportTask, error) {
	return nil, nil
}
func (r *fakeExportWorkerRepo) ListPendingExportTasks(context.Context, int) ([]ExportTask, error) {
	r.listed++
	return r.pending, nil
}
func (r *fakeExportWorkerRepo) MarkExportRunning(context.Context, ID, ID) error {
	r.running = true
	return nil
}
func (*fakeExportWorkerRepo) RetryExportTask(context.Context, ID, ID) error {
	return nil
}
func (r *fakeExportWorkerRepo) MarkExportComplete(_ context.Context, _ ID, _ ID, artifact ExportArtifact, _ time.Time) error {
	fileRef := artifact.FileRef
	r.complete = true
	r.fileRef = fileRef
	return nil
}
func (r *fakeExportWorkerRepo) MarkExportFailed(_ context.Context, _ ID, _ ID, message string) error {
	r.failed = message
	return nil
}

type fakeExportDataProvider struct {
	samples []Sample
	err     error
	errFor  map[ID]error
}

func (p fakeExportDataProvider) LoadSamples(_ context.Context, task ExportTask) ([]Sample, error) {
	if p.errFor != nil && p.errFor[task.ID] != nil {
		return nil, p.errFor[task.ID]
	}
	return p.samples, p.err
}

type fakeExportFileWriter struct {
	fileRef string
	err     error
}

func (w fakeExportFileWriter) WriteExport(context.Context, ExportTask, ExportColumns) (ExportArtifact, error) {
	if w.err != nil {
		return ExportArtifact{}, w.err
	}
	return ExportArtifact{
		FileRef: w.fileRef, Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 1, SchemaVersion: ExportArtifactSchemaVersion, ContentType: "text/csv", RowCount: 1,
	}, nil
}

func TestExportWorkerCompletesTask(t *testing.T) {
	repo := &fakeExportWorkerRepo{}
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	task := ExportTask{ID: "export-a", TenantID: "tenant-a", RangeStart: start, RangeEnd: start.Add(10 * time.Minute), Step: 5 * time.Minute}
	worker := ExportWorker{
		Repo:   repo,
		Data:   fakeExportDataProvider{samples: []Sample{{Time: start, Value: 1}, {Time: start.Add(5 * time.Minute), Value: 2}}},
		Writer: fakeExportFileWriter{fileRef: "exports/export-a.csv"},
		CompletenessPolicy: func(task ExportTask) CompletenessPolicy {
			return CompletenessPolicy{Start: task.RangeStart, End: task.RangeEnd, CollectionStep: task.Step}
		},
	}
	if err := worker.RunTask(context.Background(), task); err != nil {
		t.Fatalf("RunTask() error = %v", err)
	}
	if !repo.running || !repo.complete || repo.fileRef == "" {
		t.Fatalf("repo = %#v", repo)
	}
}

func TestExportWorkerMarksFailedOnCompletenessError(t *testing.T) {
	repo := &fakeExportWorkerRepo{}
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	task := ExportTask{ID: "export-a", TenantID: "tenant-a", RangeStart: start, RangeEnd: start.Add(10 * time.Minute), Step: 5 * time.Minute}
	worker := ExportWorker{
		Repo:   repo,
		Data:   fakeExportDataProvider{samples: []Sample{{Time: start, Value: 1}}},
		Writer: fakeExportFileWriter{fileRef: "exports/export-a.csv"},
		CompletenessPolicy: func(task ExportTask) CompletenessPolicy {
			return CompletenessPolicy{Start: task.RangeStart, End: task.RangeEnd, CollectionStep: task.Step}
		},
	}
	if err := worker.RunTask(context.Background(), task); err == nil {
		t.Fatal("expected completeness error")
	}
	if repo.failed == "" || repo.complete {
		t.Fatalf("repo = %#v", repo)
	}
}

func TestExportWorkerMarksFailedOnDataError(t *testing.T) {
	repo := &fakeExportWorkerRepo{}
	worker := ExportWorker{
		Repo:   repo,
		Data:   fakeExportDataProvider{err: errors.New("vm unavailable")},
		Writer: fakeExportFileWriter{fileRef: "exports/export-a.csv"},
	}
	if err := worker.RunTask(context.Background(), ExportTask{ID: "export-a", TenantID: "tenant-a"}); err == nil {
		t.Fatal("expected data error")
	}
	if repo.failed != "vm unavailable" {
		t.Fatalf("failed = %q", repo.failed)
	}
}

func TestExportWorkerRunPendingProcessesQueue(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	repo := &fakeExportWorkerRepo{pending: []ExportTask{
		{ID: "export-a", TenantID: "tenant-a", RangeStart: start, RangeEnd: start.Add(5 * time.Minute), Step: 5 * time.Minute},
		{ID: "export-b", TenantID: "tenant-a", RangeStart: start, RangeEnd: start.Add(5 * time.Minute), Step: 5 * time.Minute},
	}}
	worker := ExportWorker{
		Repo: repo,
		Data: fakeExportDataProvider{
			samples: []Sample{{Time: start, Value: 1}, {Time: start.Add(5 * time.Minute), Value: 2}},
			errFor:  map[ID]error{"export-b": errors.New("vm unavailable")},
		},
		Writer: fakeExportFileWriter{fileRef: "exports/export-a.csv"},
	}
	result, err := worker.RunPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("RunPending() error = %v", err)
	}
	if result.Processed != 2 || result.Completed != 1 || result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	if !repo.complete || repo.failed != "vm unavailable" {
		t.Fatalf("repo = %#v", repo)
	}
}

func TestExportWorkerRunLoopRunsImmediately(t *testing.T) {
	repo := &fakeExportWorkerRepo{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	worker := ExportWorker{
		Repo:   repo,
		Data:   fakeExportDataProvider{},
		Writer: fakeExportFileWriter{},
	}
	err := worker.RunLoop(ctx, ExportWorkerLoopConfig{RunImmediately: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if repo.listed != 1 {
		t.Fatalf("listed = %d", repo.listed)
	}
}
