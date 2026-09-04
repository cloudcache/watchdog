package watchdog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

type flowStateCleanupAPIController struct {
	job           FlowStateCleanupJob
	createRequest FlowStateCleanupCreateRequest
	createTenant  ID
	createActor   ID
	retryVersion  uint64
	retryActor    ID
	createCalls   int
	getCalls      int
	retryCalls    int
	createError   error
	getError      error
	retryError    error
}

func (c *flowStateCleanupAPIController) Create(_ context.Context, tenantID, actorID ID, req FlowStateCleanupCreateRequest) (FlowStateCleanupJob, error) {
	c.createCalls++
	c.createTenant, c.createActor, c.createRequest = tenantID, actorID, req
	return c.job, c.createError
}

func (c *flowStateCleanupAPIController) Get(_ context.Context, _, _ ID) (FlowStateCleanupJob, error) {
	c.getCalls++
	return c.job, c.getError
}

func (c *flowStateCleanupAPIController) Retry(_ context.Context, _, _ ID, expectedVersion uint64, actorID ID) (FlowStateCleanupJob, error) {
	c.retryCalls++
	c.retryVersion, c.retryActor = expectedVersion, actorID
	return c.job, c.retryError
}

func TestFlowStateCleanupAPICreatesTypedServerResolvedJob(t *testing.T) {
	controller := &flowStateCleanupAPIController{job: flowStateCleanupAPIJob()}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: flowStateCleanupAPIAuth(true), FlowCleanupJobs: controller})
	identity := sha256.Sum256([]byte("api-state-identity"))
	body := `{"transfer_id":"transfer_cleanup_0000001","state_kind":"decoder","state_identity_key":"` + hex.EncodeToString(identity[:]) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/modules/flow/state-cleanup-jobs", strings.NewReader(body))
	req.Header.Set(flowStateCleanupIdempotencyHeader, "request-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated || rec.Header().Get("ETag") != `"7"` {
		t.Fatalf("status=%d etag=%q body=%s", rec.Code, rec.Header().Get("ETag"), rec.Body.String())
	}
	if controller.createCalls != 1 || controller.createTenant != "tenant-a" || controller.createActor != "user-a" || controller.createRequest.TransferID != "transfer_cleanup_0000001" || controller.createRequest.IdempotencyKey != "request-1" || !bytes.Equal(controller.createRequest.IdentityKey, identity[:]) {
		t.Fatalf("create call tenant=%s actor=%s request=%+v", controller.createTenant, controller.createActor, controller.createRequest)
	}
	if strings.Contains(rec.Body.String(), "kafka_key") || strings.Contains(rec.Body.String(), "payload_sha256") || !strings.Contains(rec.Body.String(), `"old_ownership_epoch":5`) {
		t.Fatalf("unsafe or incomplete response: %s", rec.Body.String())
	}
}

func TestFlowStateCleanupAPIRejectsUnknownFieldsAndRequiresPermission(t *testing.T) {
	controller := &flowStateCleanupAPIController{job: flowStateCleanupAPIJob()}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: flowStateCleanupAPIAuth(true), FlowCleanupJobs: controller})
	identity := sha256.Sum256([]byte("api-state-identity"))
	body := `{"transfer_id":"transfer_cleanup_0000001","state_kind":"decoder","state_identity_key":"` + hex.EncodeToString(identity[:]) + `","checkpoint":"forbidden"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/flow/state-cleanup-jobs", strings.NewReader(body))
	req.Header.Set(flowStateCleanupIdempotencyHeader, "request-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.createCalls != 0 {
		t.Fatalf("unknown-field status=%d calls=%d body=%s", rec.Code, controller.createCalls, rec.Body.String())
	}

	router = NewAPIV1Router(APIV1RouterConfig{Auth: flowStateCleanupAPIAuth(false), FlowCleanupJobs: controller})
	req = httptest.NewRequest(http.MethodGet, "/api/v1/flow/state-cleanup-jobs/flowclean_0000000000000001", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || controller.getCalls != 0 {
		t.Fatalf("permission status=%d calls=%d body=%s", rec.Code, controller.getCalls, rec.Body.String())
	}
}

func TestFlowStateCleanupAPIRetryRequiresStrongCurrentVersion(t *testing.T) {
	controller := &flowStateCleanupAPIController{job: flowStateCleanupAPIJob()}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: flowStateCleanupAPIAuth(true), FlowCleanupJobs: controller})
	path := "/api/v1/flow/state-cleanup-jobs/flowclean_0000000000000001/actions/retry"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusPreconditionRequired || controller.retryCalls != 0 {
		t.Fatalf("missing If-Match status=%d calls=%d", rec.Code, controller.retryCalls)
	}

	req := httptest.NewRequest(http.MethodPost, path, nil)
	req.Header.Set("If-Match", `"7"`)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || controller.retryCalls != 1 || controller.retryVersion != 7 || controller.retryActor != "user-a" {
		t.Fatalf("retry status=%d calls=%d version=%d actor=%s body=%s", rec.Code, controller.retryCalls, controller.retryVersion, controller.retryActor, rec.Body.String())
	}
}

func TestFlowStateCleanupAPICompatibilityRoute(t *testing.T) {
	controller := &flowStateCleanupAPIController{job: flowStateCleanupAPIJob()}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: flowStateCleanupAPIAuth(true), FlowCleanupJobs: controller})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flow/state-cleanup-jobs/flowclean_0000000000000001", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || controller.getCalls != 1 {
		t.Fatalf("compatibility status=%d calls=%d body=%s", rec.Code, controller.getCalls, rec.Body.String())
	}
}

func flowStateCleanupAPIAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: admin}, nil
	}
}

func flowStateCleanupAPIJob() FlowStateCleanupJob {
	identity := sha256.Sum256([]byte("api-state-identity"))
	key := sha256.Sum256([]byte("api-state-key"))
	now := time.Unix(1_000_000, 0).UTC()
	return FlowStateCleanupJob{
		ID: "flowclean_0000000000000001", TenantID: "tenant-a", Status: OperationJobFailed,
		RowVersion: 7, CreatedBy: "user-a", CreatedAt: now, UpdatedAt: now,
		Snapshot: flowcollect.StateCleanupSnapshot{
			ApprovalID: "approval-a", Phase: flowcollect.StateCleanupAwaitingFence,
			Old: flowcollect.StateCleanupCheckpoint{
				Kind: flowcollect.StateCheckpointDecoder, IdentityKey: identity[:], KafkaKey: key[:],
				ExporterID: "exporter-a", CollectorID: "collector-a", RegistryVersion: 20,
				OwnershipEpoch: 5, StateGeneration: 9,
			},
		},
	}
}
