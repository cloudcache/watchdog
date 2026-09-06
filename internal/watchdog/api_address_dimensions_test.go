package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeAddressDimensionJobQueue struct {
	OperationJobRepository
	job OperationJob
}

func (q *fakeAddressDimensionJobQueue) EnqueueOperationJob(_ context.Context, job OperationJob) (OperationJob, error) {
	q.job = job
	job.ID = "job-address-dimension"
	return job, nil
}

func TestAddressDimensionPreviewAndAsyncPublishAPI(t *testing.T) {
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	publisher := &fakeAddressDimensionPublisher{preview: AddressDimensionPreview{DraftDigest: digest, EffectiveFrom: effective, PrefixCount: 2}}
	jobs := &fakeAddressDimensionJobQueue{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressDimensionTestAuth, AddressDimensions: publisher, OperationJobs: jobs})

	preview := httptest.NewRecorder()
	router.ServeHTTP(preview, httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/preview", strings.NewReader(`{"effective_from":"2026-09-07T00:00:00Z"}`)))
	if preview.Code != http.StatusOK || !strings.Contains(preview.Body.String(), digest) {
		t.Fatalf("preview = %d %s", preview.Code, preview.Body.String())
	}

	publish := httptest.NewRecorder()
	router.ServeHTTP(publish, httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/publish", strings.NewReader(`{"effective_from":"2026-09-07T00:00:00Z","preview_digest":"`+digest+`"}`)))
	if publish.Code != http.StatusAccepted {
		t.Fatalf("publish = %d %s", publish.Code, publish.Body.String())
	}
	if jobs.job.JobType != AddressDimensionPublishJob || jobs.job.TenantID != "tenant-dimension" || jobs.job.CreatedBy != "user-dimension" {
		t.Fatalf("unexpected job: %#v", jobs.job)
	}
	if !strings.HasSuffix(jobs.job.IdempotencyKey, strings.TrimPrefix(digest, "sha256:")) {
		t.Fatalf("idempotency key does not distinguish the draft digest: %q", jobs.job.IdempotencyKey)
	}
	var payload addressDimensionPublishJobPayload
	if err := DecodeJobPayload(jobs.job.CheckpointJSON, AddressDimensionPayloadV1, &payload); err != nil || payload.PreviewDigest != digest {
		t.Fatalf("job payload = %#v, %v", payload, err)
	}
	var body map[string]any
	if err := json.Unmarshal(publish.Body.Bytes(), &body); err != nil || body["job"] == nil {
		t.Fatalf("publish response = %s, %v", publish.Body.String(), err)
	}
}

func addressDimensionTestAuth(*http.Request) (AuthContext, error) {
	return AuthContext{TenantID: "tenant-dimension", UserID: "user-dimension", IsAdmin: true}, nil
}
