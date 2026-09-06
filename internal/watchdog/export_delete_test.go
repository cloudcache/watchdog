package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type fakeExportDeletionRepository struct {
	err   error
	calls int
}

func (r *fakeExportDeletionRepository) DeleteExportTask(context.Context, ID, ID) error {
	r.calls++
	return r.err
}

type fakeDestructionReceiptRecorder struct {
	receipts []DestructionReceipt
}

func (r *fakeDestructionReceiptRecorder) RecordDestructionReceipt(_ context.Context, receipt DestructionReceipt) error {
	r.receipts = append(r.receipts, receipt)
	return nil
}

func TestExportDeleteJobHandlerIsIdempotentAfterRowDeletion(t *testing.T) {
	repo := &fakeExportDeletionRepository{}
	files := &CSVExportWriter{Files: map[string][]byte{"exports/export-a.csv": []byte("data")}}
	receipts := &fakeDestructionReceiptRecorder{}
	task := ExportTask{ID: "export-a", FileRef: "exports/export-a.csv", RowCount: 3, SizeBytes: 4}
	payload, _, err := EncodeExportDeletePayload(task)
	if err != nil {
		t.Fatalf("EncodeExportDeletePayload() error = %v", err)
	}
	handler := NewExportDeleteJobHandler(repo, files, receipts)
	job := OperationJob{ID: "job-a", TenantID: "tenant-a", CreatedBy: "user-a", CheckpointJSON: payload}
	if result, err := handler(context.Background(), job); err != nil || result != "deleted:export-a" {
		t.Fatalf("first delete result=%q err=%v", result, err)
	}
	if _, exists := files.Files[task.FileRef]; exists {
		t.Fatal("artifact still exists after delete")
	}
	repo.err = sql.ErrNoRows
	if _, err := handler(context.Background(), job); err != nil {
		t.Fatalf("retry after row deletion error = %v", err)
	}
	if repo.calls != 2 || len(receipts.receipts) != 2 {
		t.Fatalf("repo calls=%d receipts=%d", repo.calls, len(receipts.receipts))
	}
	if got := receipts.receipts[0].Impact; got["artifacts"] != 1 || got["rows"] != 3 || got["bytes"] != 4 {
		t.Fatalf("receipt impact = %#v", got)
	}
}

func TestExportDeleteJobHandlerRejectsNonTerminalExport(t *testing.T) {
	repo := &fakeExportDeletionRepository{err: ErrExportNotTerminal}
	payload, _, err := EncodeExportDeletePayload(ExportTask{ID: "export-a"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewExportDeleteJobHandler(repo, &CSVExportWriter{}, nil)(context.Background(), OperationJob{
		TenantID: "tenant-a", CheckpointJSON: payload,
	})
	var terminal terminalJobError
	if !errors.As(err, &terminal) {
		t.Fatalf("error = %v, want terminalJobError", err)
	}
}
