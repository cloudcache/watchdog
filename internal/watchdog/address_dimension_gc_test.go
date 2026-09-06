package watchdog

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeAddressDimensionGCRepository struct {
	AddressDimensionGCRepository
	candidates []AddressDimensionGCCandidate
	deleted    AddressDimensionObjectDeletion
	deleteErr  error
	deleteHits int
	scheduled  time.Time
	expected   uint64
}

func (repository *fakeAddressDimensionGCRepository) ListAllAddressDimensionGCCandidates(context.Context, time.Time, int) ([]AddressDimensionGCCandidate, error) {
	return repository.candidates, nil
}

func (repository *fakeAddressDimensionGCRepository) ListAddressDimensionGCCandidates(_ context.Context, tenantID ID, _ time.Time, filter AddressDimensionGCFilter) ([]AddressDimensionGCCandidate, string, error) {
	if tenantID != "tenant-dimension" || filter.Limit != 1 {
		return nil, "", ErrAddressDimensionInvalid
	}
	return repository.candidates, "next-page", nil
}

func (repository *fakeAddressDimensionGCRepository) ScheduleAddressDimensionObjectGC(_ context.Context, tenantID, actorID, snapshotID ID, expected uint64, retentionUntil time.Time) (AddressDimensionSnapshot, error) {
	repository.scheduled, repository.expected = retentionUntil, expected
	return AddressDimensionSnapshot{ID: snapshotID, TenantID: tenantID, RowVersion: expected + 1, RetentionUntil: &retentionUntil}, nil
}

func (repository *fakeAddressDimensionGCRepository) DeleteAddressDimensionObject(_ context.Context, tenantID, snapshotID, jobID, actorID ID, objectRef, checksum string, expectedRetention, asOf time.Time) (AddressDimensionObjectDeletion, error) {
	repository.deleteHits++
	if repository.deleteErr != nil {
		err := repository.deleteErr
		repository.deleteErr = nil
		return AddressDimensionObjectDeletion{}, err
	}
	return repository.deleted, nil
}

func TestAddressDimensionObjectGCHandlerRetriesShortFinalization(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	candidate := AddressDimensionGCCandidate{
		TenantID: "tenant-a", SnapshotID: "snapshot-a", ObjectRef: "dimension-snapshots/tenant-a/snapshot-a/bundle.json",
		Checksum: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", RetentionUntil: now.Add(-time.Hour),
	}
	payload, err := EncodeAddressDimensionObjectGCJobPayload(candidate)
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeAddressDimensionGCRepository{deleteErr: errors.New("temporary finalization failure"), deleted: AddressDimensionObjectDeletion{
		SnapshotID: candidate.SnapshotID, ObjectRef: candidate.ObjectRef, DeletedAt: now,
	}}
	handler := NewAddressDimensionObjectGCJobHandler(repository, func() time.Time { return now })
	job := OperationJob{ID: "job-a", TenantID: candidate.TenantID, CheckpointJSON: payload}
	if _, err := handler(context.Background(), job); err == nil || IsTerminalJobError(err) {
		t.Fatalf("first receipt failure = %v", err)
	}
	result, err := handler(context.Background(), job)
	if err != nil || result != "dimension-object-deleted:snapshot-a" || repository.deleteHits != 2 {
		t.Fatalf("retry result=%q err=%v delete_hits=%d", result, err, repository.deleteHits)
	}
}

func TestAddressDimensionObjectGCHandlerRejectsStaleCandidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	candidate := AddressDimensionGCCandidate{
		TenantID: "tenant-a", SnapshotID: "snapshot-a", ObjectRef: "dimension-snapshots/tenant-a/snapshot-a/bundle.json",
		Checksum: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", RetentionUntil: now.Add(-time.Hour),
	}
	payload, err := EncodeAddressDimensionObjectGCJobPayload(candidate)
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeAddressDimensionGCRepository{deleteErr: ErrAddressDimensionGCNotEligible}
	handler := NewAddressDimensionObjectGCJobHandler(repository, nil)
	if _, err := handler(context.Background(), OperationJob{TenantID: candidate.TenantID, CheckpointJSON: payload}); !IsTerminalJobError(err) {
		t.Fatalf("stale candidate error = %v", err)
	}
}

func TestAddressDimensionGCProducerEnqueuesTypedJob(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	candidate := AddressDimensionGCCandidate{
		TenantID: "tenant-a", SnapshotID: "snapshot-a", ObjectRef: "dimension-snapshots/tenant-a/snapshot-a/bundle.json",
		Checksum: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", RetentionUntil: now.Add(-time.Hour),
	}
	repository := &fakeAddressDimensionGCRepository{candidates: []AddressDimensionGCCandidate{candidate}}
	jobs := &fakeAddressDimensionJobQueue{}
	count, err := (AddressDimensionGCProducer{Repository: repository, Jobs: jobs, Batch: 10}).RunOnce(context.Background())
	if err != nil || count != 1 || jobs.job.JobType != AddressDimensionObjectGCJob || jobs.job.TenantID != candidate.TenantID {
		t.Fatalf("producer count=%d err=%v job=%#v", count, err, jobs.job)
	}
	var payload addressDimensionObjectGCJobPayload
	if err := DecodeJobPayload(jobs.job.CheckpointJSON, AddressDimensionObjectGCPayload, &payload); err != nil || payload.SnapshotID != candidate.SnapshotID {
		t.Fatalf("payload=%#v err=%v", payload, err)
	}
}

func TestAddressDimensionGCAPIListsAndSchedules(t *testing.T) {
	retention := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	repository := &fakeAddressDimensionGCRepository{candidates: []AddressDimensionGCCandidate{{
		TenantID: "tenant-dimension", SnapshotID: "snapshot-a", Version: 1,
		ObjectRef: "dimension-snapshots/tenant-dimension/snapshot-a/bundle.json",
		Checksum:  "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", RetentionUntil: retention.Add(-time.Hour),
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressDimensionTestAuth, DimensionGC: repository})

	list := httptest.NewRecorder()
	router.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/dimensions/address/gc/candidates?limit=1", nil))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"next_cursor":"next-page"`) || !strings.Contains(list.Body.String(), `"snapshot_id":"snapshot-a"`) {
		t.Fatalf("list = %d %s", list.Code, list.Body.String())
	}

	schedule := httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/versions/snapshot-a/actions/schedule-gc", strings.NewReader(`{"retention_until":"`+retention.Format(time.RFC3339Nano)+`"}`))
	schedule.Header.Set("If-Match", `"7"`)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, schedule)
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"8"` || repository.expected != 7 || !repository.scheduled.Equal(retention) {
		t.Fatalf("schedule = %d %s repository=%#v", response.Code, response.Body.String(), repository)
	}
}
