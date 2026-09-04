package watchdog

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

const flowStateCleanupIdempotencyHeader = "Idempotency-Key"

type flowStateCleanupCreateAPIRequest struct {
	TransferID       ID     `json:"transfer_id"`
	StateKind        string `json:"state_kind"`
	StateIdentityKey string `json:"state_identity_key"`
}

type flowStateCleanupJobResponse struct {
	ID                ID                              `json:"id"`
	TenantID          ID                              `json:"tenant_id"`
	Status            OperationJobStatus              `json:"status"`
	Phase             flowcollect.StateCleanupPhase   `json:"phase"`
	ApprovalID        string                          `json:"approval_id"`
	StateKind         flowcollect.StateCheckpointKind `json:"state_kind"`
	StateIdentityKey  string                          `json:"state_identity_key"`
	ExporterID        string                          `json:"exporter_id"`
	OldCollectorID    string                          `json:"old_collector_id"`
	OldPlanRevision   uint64                          `json:"old_plan_revision"`
	OldOwnershipEpoch uint64                          `json:"old_ownership_epoch"`
	OldGeneration     uint64                          `json:"old_generation"`
	AttemptCount      uint32                          `json:"attempt_count"`
	LastErrorCode     string                          `json:"last_error_code,omitempty"`
	LastErrorDetail   string                          `json:"last_error_detail,omitempty"`
	RowVersion        uint64                          `json:"row_version"`
	CreatedBy         ID                              `json:"created_by"`
	CreatedAt         time.Time                       `json:"created_at"`
	StartedAt         time.Time                       `json:"started_at,omitempty"`
	FinishedAt        time.Time                       `json:"finished_at,omitempty"`
	UpdatedAt         time.Time                       `json:"updated_at"`
}

type flowStateCleanupAPI struct {
	controller FlowStateCleanupJobController
}

func registerFlowStateCleanupRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, controller FlowStateCleanupJobController) {
	api := flowStateCleanupAPI{controller: controller}
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	for _, prefix := range []string{"/api/v1/modules/flow", "/api/v1/flow"} {
		mux.Handle("POST "+prefix+"/state-cleanup-jobs", auth(configureTenant(http.HandlerFunc(api.create))))
		mux.Handle("GET "+prefix+"/state-cleanup-jobs/{job_id}", auth(configureTenant(http.HandlerFunc(api.get))))
		mux.Handle("POST "+prefix+"/state-cleanup-jobs/{job_id}/actions/retry", auth(configureTenant(http.HandlerFunc(api.retry))))
	}
}

func (api flowStateCleanupAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	req, err := decodeFlowStateCleanupCreateRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	idempotencyKey := r.Header.Get(flowStateCleanupIdempotencyHeader)
	if idempotencyKey == "" || idempotencyKey != strings.TrimSpace(idempotencyKey) || len(idempotencyKey) > 128 || !isPrintableASCII(idempotencyKey) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Idempotency-Key must be 1 to 128 printable ASCII bytes without spaces", nil)
		return
	}
	identityKey, err := hex.DecodeString(req.StateIdentityKey)
	if err != nil || len(identityKey) != 32 || req.StateIdentityKey != strings.ToLower(req.StateIdentityKey) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "state_identity_key must be 64 lowercase hexadecimal characters", nil)
		return
	}
	job, err := api.controller.Create(r.Context(), auth.TenantID, auth.UserID, FlowStateCleanupCreateRequest{
		TransferID: req.TransferID, Kind: flowcollect.StateCheckpointKind(req.StateKind),
		IdentityKey: identityKey, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeFlowStateCleanupCreateError(w, err)
		return
	}
	writeFlowStateCleanupJob(w, http.StatusCreated, job)
}

func (api flowStateCleanupAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	job, err := api.controller.Get(r.Context(), auth.TenantID, ID(r.PathValue("job_id")))
	if errors.Is(err, sql.ErrNoRows) {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Flow state-cleanup job not found", nil)
		return
	}
	if err != nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Flow state-cleanup jobs are unavailable", nil)
		return
	}
	writeFlowStateCleanupJob(w, http.StatusOK, job)
}

func (api flowStateCleanupAPI) retry(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expectedVersion, err := parseFlowStateCleanupIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		WriteAPIError(w, http.StatusPreconditionRequired, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	job, err := api.controller.Retry(r.Context(), auth.TenantID, ID(r.PathValue("job_id")), expectedVersion, auth.UserID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Flow state-cleanup job not found", nil)
		return
	case errors.Is(err, ErrFlowStateCleanupConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorInvalidRequest, "Flow state-cleanup job version changed", nil)
		return
	case errors.Is(err, ErrFlowStateCleanupRetryNotAllowed):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Flow state-cleanup job is not safely retryable", nil)
		return
	case err != nil:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Flow state-cleanup jobs are unavailable", nil)
		return
	}
	writeFlowStateCleanupJob(w, http.StatusOK, job)
}

func decodeFlowStateCleanupCreateRequest(r *http.Request) (flowStateCleanupCreateAPIRequest, error) {
	defer r.Body.Close()
	var req flowStateCleanupCreateAPIRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return req, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return req, errors.New("request body must contain one JSON object")
		}
		return req, err
	}
	if req.TransferID == "" || len(req.TransferID) > 26 {
		return req, errors.New("transfer_id is required")
	}
	if req.StateKind != string(flowcollect.StateCheckpointDecoder) && req.StateKind != string(flowcollect.StateCheckpointQuality) {
		return req, errors.New("state_kind must be decoder or quality")
	}
	return req, nil
}

func parseFlowStateCleanupIfMatch(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	if len(value) < 3 || value[0] != '"' || value[len(value)-1] != '"' {
		return 0, errors.New("If-Match with the current quoted row_version is required")
	}
	version, err := strconv.ParseUint(value[1:len(value)-1], 10, 64)
	if err != nil || version == 0 {
		return 0, errors.New("If-Match with the current quoted row_version is required")
	}
	return version, nil
}

func writeFlowStateCleanupCreateError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector ownership transfer not found", nil)
	case errors.Is(err, ErrFlowStateCleanupIdempotencyConflict):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Idempotency-Key was already used for another cleanup request", nil)
	case errors.Is(err, ErrFlowStateCleanupCheckpointMissing):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Old checkpoint is not visible at a frozen Kafka boundary", nil)
	case errors.Is(err, ErrFlowStateCleanupCheckpointInvalid):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Old checkpoint does not match the ownership transfer", nil)
	default:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Flow state-cleanup dependencies are unavailable", nil)
	}
}

func writeFlowStateCleanupJob(w http.ResponseWriter, status int, job FlowStateCleanupJob) {
	w.Header().Set("ETag", `"`+strconv.FormatUint(job.RowVersion, 10)+`"`)
	old := job.Snapshot.Old
	WriteAPIJSON(w, status, flowStateCleanupJobResponse{
		ID: job.ID, TenantID: job.TenantID, Status: job.Status, Phase: job.Snapshot.Phase,
		ApprovalID: job.Snapshot.ApprovalID, StateKind: old.Kind,
		StateIdentityKey: hex.EncodeToString(old.IdentityKey), ExporterID: old.ExporterID,
		OldCollectorID: old.CollectorID, OldPlanRevision: old.RegistryVersion,
		OldOwnershipEpoch: old.OwnershipEpoch, OldGeneration: old.StateGeneration,
		AttemptCount: job.AttemptCount, LastErrorCode: job.LastErrorCode,
		LastErrorDetail: job.LastErrorDetail, RowVersion: job.RowVersion,
		CreatedBy: job.CreatedBy, CreatedAt: job.CreatedAt, StartedAt: job.StartedAt,
		FinishedAt: job.FinishedAt, UpdatedAt: job.UpdatedAt,
	})
}
