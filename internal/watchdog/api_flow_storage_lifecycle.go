package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type flowStorageLifecycleAPI struct {
	repo  FlowStorageLifecycleRepository
	audit AuditRepository
	now   func() time.Time
}

func registerFlowStorageLifecycleRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo FlowStorageLifecycleRepository, audit AuditRepository) {
	api := flowStorageLifecycleAPI{repo: repo, audit: audit, now: time.Now}
	view := RequirePermission(ActionView, TenantResource)
	configure := RequirePermission(ActionConfigure, TenantResource)
	operate := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/flow/storage/policies", auth(view(http.HandlerFunc(api.listPolicies))))
	mux.Handle("POST /api/v1/flow/storage/policies", auth(configure(http.HandlerFunc(api.createPolicy))))
	mux.Handle("GET /api/v1/flow/storage/policies/{policy_id}", auth(view(http.HandlerFunc(api.getPolicy))))
	mux.Handle("PATCH /api/v1/flow/storage/policies/{policy_id}", auth(configure(http.HandlerFunc(api.updatePolicy))))
	mux.Handle("DELETE /api/v1/flow/storage/policies/{policy_id}", auth(configure(http.HandlerFunc(api.deletePolicy))))
	mux.Handle("POST /api/v1/flow/storage/policies/{policy_id}/actions/publish", auth(operate(http.HandlerFunc(api.publishPolicy))))
	mux.Handle("GET /api/v1/flow/storage/partitions", auth(view(http.HandlerFunc(api.listPartitions))))
}

func (api flowStorageLifecycleAPI) listPolicies(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	items, err := api.repo.ListFlowStoragePolicies(r.Context(), auth.TenantID)
	if err != nil {
		writeFlowStorageError(w, err)
		return
	}
	if items == nil {
		items = []FlowStoragePolicy{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

func (api flowStorageLifecycleAPI) getPolicy(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetFlowStoragePolicy(r.Context(), auth.TenantID, ID(r.PathValue("policy_id")))
	if err != nil {
		writeFlowStorageError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

type flowStoragePolicyInput struct {
	BootstrapFrom               *time.Time `json:"bootstrap_from"`
	RawRetentionSeconds         *uint64    `json:"raw_retention_seconds"`
	DownsampleResolutionSeconds *uint32    `json:"downsample_resolution_seconds"`
	ArchiveRetentionSeconds     *uint64    `json:"archive_retention_seconds"`
	LateArrivalSeconds          *uint32    `json:"late_arrival_seconds"`
	DeleteGraceSeconds          *uint32    `json:"delete_grace_seconds"`
	MaxPartitionsPerRun         *uint32    `json:"max_partitions_per_run"`
	RawDeleteEnabled            *bool      `json:"raw_delete_enabled"`
}

func (input flowStoragePolicyInput) complete() bool {
	return input.BootstrapFrom != nil && input.RawRetentionSeconds != nil && input.DownsampleResolutionSeconds != nil &&
		input.ArchiveRetentionSeconds != nil && input.LateArrivalSeconds != nil && input.DeleteGraceSeconds != nil &&
		input.MaxPartitionsPerRun != nil && input.RawDeleteEnabled != nil
}

func (input flowStoragePolicyInput) empty() bool {
	return input.BootstrapFrom == nil && input.RawRetentionSeconds == nil && input.DownsampleResolutionSeconds == nil &&
		input.ArchiveRetentionSeconds == nil && input.LateArrivalSeconds == nil && input.DeleteGraceSeconds == nil &&
		input.MaxPartitionsPerRun == nil && input.RawDeleteEnabled == nil
}

func (api flowStorageLifecycleAPI) createPolicy(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	input, ok := decodeFlowStoragePolicyInput(w, r)
	if !ok {
		return
	}
	if !input.complete() {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "all Flow storage policy fields are required", nil)
		return
	}
	item := FlowStoragePolicy{TenantID: auth.TenantID, Status: FlowStoragePolicyDraft, CreatedBy: auth.UserID}
	applyFlowStoragePolicyInput(&item, input)
	created, err := api.repo.CreateFlowStoragePolicyDraft(r.Context(), item)
	if err != nil {
		writeFlowStorageError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow_storage_policy.created", created.ID, map[string]any{"policy_version": created.PolicyVersion})
	w.Header().Set("ETag", quotedRowVersion(created.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api flowStorageLifecycleAPI) updatePolicy(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	item, err := api.repo.GetFlowStoragePolicy(r.Context(), auth.TenantID, ID(r.PathValue("policy_id")))
	if err != nil {
		writeFlowStorageError(w, err)
		return
	}
	input, ok := decodeFlowStoragePolicyInput(w, r)
	if !ok {
		return
	}
	if input.empty() {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "at least one Flow storage policy field is required", nil)
		return
	}
	applyFlowStoragePolicyInput(&item, input)
	updated, err := api.repo.UpdateFlowStoragePolicyDraft(r.Context(), item, expected)
	if err != nil {
		writeFlowStorageError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow_storage_policy.updated", updated.ID, map[string]any{"previous_version": expected, "row_version": updated.RowVersion})
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api flowStorageLifecycleAPI) deletePolicy(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	id := ID(r.PathValue("policy_id"))
	if err := api.repo.DeleteFlowStoragePolicyDraft(r.Context(), auth.TenantID, id, expected); err != nil {
		writeFlowStorageError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow_storage_policy.deleted", id, map[string]any{"row_version": expected})
	w.WriteHeader(http.StatusNoContent)
}

func (api flowStorageLifecycleAPI) publishPolicy(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	now := time.Now()
	if api.now != nil {
		now = api.now()
	}
	id := ID(r.PathValue("policy_id"))
	item, err := api.repo.PublishFlowStoragePolicy(r.Context(), auth.TenantID, id, auth.UserID, expected, now)
	if err != nil {
		writeFlowStorageError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow_storage_policy.published", id, map[string]any{"policy_version": item.PolicyVersion})
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api flowStorageLifecycleAPI) listPartitions(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	for key := range query {
		switch key {
		case "state", "from", "to", "limit", "offset":
		default:
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported query parameter: "+key, nil)
			return
		}
	}
	filter := FlowStoragePartitionFilter{State: strings.TrimSpace(query.Get("state"))}
	var err error
	filter.Limit, err = parsePageInteger(query.Get("limit"), 100, 1, 500)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be between 1 and 500", nil)
		return
	}
	filter.Offset, err = parsePageInteger(query.Get("offset"), 0, 0, int(^uint(0)>>1))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be zero or greater", nil)
		return
	}
	if filter.From, err = parseOptionalFlowStorageDate(query.Get("from")); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if filter.To, err = parseOptionalFlowStorageDate(query.Get("to")); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	items, total, err := api.repo.ListFlowStoragePartitions(r.Context(), auth.TenantID, filter)
	if err != nil {
		writeFlowStorageError(w, err)
		return
	}
	if items == nil {
		items = []FlowStoragePartitionState{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": total, "limit": filter.Limit, "offset": filter.Offset})
}

func decodeFlowStoragePolicyInput(w http.ResponseWriter, r *http.Request) (flowStoragePolicyInput, bool) {
	defer r.Body.Close()
	var input flowStoragePolicyInput
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return input, false
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return input, false
	}
	return input, true
}

func applyFlowStoragePolicyInput(item *FlowStoragePolicy, input flowStoragePolicyInput) {
	if input.BootstrapFrom != nil {
		item.BootstrapFrom = *input.BootstrapFrom
	}
	if input.RawRetentionSeconds != nil {
		item.RawRetentionSeconds = *input.RawRetentionSeconds
	}
	if input.DownsampleResolutionSeconds != nil {
		item.DownsampleResolutionSeconds = *input.DownsampleResolutionSeconds
	}
	if input.ArchiveRetentionSeconds != nil {
		item.ArchiveRetentionSeconds = *input.ArchiveRetentionSeconds
	}
	if input.LateArrivalSeconds != nil {
		item.LateArrivalSeconds = *input.LateArrivalSeconds
	}
	if input.DeleteGraceSeconds != nil {
		item.DeleteGraceSeconds = *input.DeleteGraceSeconds
	}
	if input.MaxPartitionsPerRun != nil {
		item.MaxPartitionsPerRun = *input.MaxPartitionsPerRun
	}
	if input.RawDeleteEnabled != nil {
		item.RawDeleteEnabled = *input.RawDeleteEnabled
	}
}

func parseOptionalFlowStorageDate(raw string) (time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, nil
	}
	value, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, errors.New("from and to must use YYYY-MM-DD")
	}
	return value.UTC(), nil
}

func (api flowStorageLifecycleAPI) recordAudit(ctx context.Context, auth AuthContext, action string, id ID, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: "flow_storage_policy", ResourceID: id, Detail: detail})
}

func writeFlowStorageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Flow storage lifecycle item not found", nil)
	case errors.Is(err, ErrFlowStorageVersionConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), err.Error(), nil)
	case errors.Is(err, ErrFlowStorageTransition), errors.Is(err, ErrFlowStorageRawDeleteLocked):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrFlowStorageInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "Flow storage lifecycle storage is unavailable", map[string]any{"cause": strconv.Quote(err.Error())})
	}
}
