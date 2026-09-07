package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type queryGatewayAPI struct {
	gateway *QueryGateway
	audit   AuditRepository
}

func registerQueryGatewayRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, gateway *QueryGateway, audit AuditRepository) {
	api := queryGatewayAPI{gateway: gateway, audit: audit}
	mux.Handle("POST /api/v1/query", auth(http.HandlerFunc(api.query)))
	mux.Handle("POST /api/v1/flow/query", auth(http.HandlerFunc(api.flowQuery)))
}

func (api queryGatewayAPI) query(w http.ResponseWriter, r *http.Request) {
	api.execute(w, r, "")
}

func (api queryGatewayAPI) flowQuery(w http.ResponseWriter, r *http.Request) {
	api.execute(w, r, FlowTrafficDataset)
}

func (api queryGatewayAPI) execute(w http.ResponseWriter, r *http.Request, fixedDataset string) {
	var request QueryRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxQueryParameterBytes+16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorCode(QueryErrorInvalidRequest), err.Error(), nil)
		return
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorCode(QueryErrorInvalidRequest), err.Error(), nil)
		return
	}
	if fixedDataset != "" {
		if request.Dataset != "" && request.Dataset != fixedDataset {
			WriteAPIError(w, http.StatusBadRequest, APIErrorCode(QueryErrorInvalidRequest), "dataset does not match the Flow query endpoint", nil)
			return
		}
		request.Dataset = fixedDataset
	}
	auth, _ := AuthFromContext(r.Context())
	result, err := api.gateway.Execute(r.Context(), auth, RequestIDFromContext(r.Context()), request)
	if err != nil {
		writeQueryGatewayError(w, err)
		return
	}
	if request.ValueLayer == QueryValueRaw || request.ValueLayer == QueryValueSupplier {
		api.recordAudit(r.Context(), auth, "query.sensitive_viewed", request.Dataset, map[string]any{
			"value_layer": request.ValueLayer, "request_id": result.Meta.RequestID,
		})
	}
	WriteAPIJSON(w, http.StatusOK, result)
}

func (api queryGatewayAPI) recordAudit(ctx context.Context, auth AuthContext, action, dataset string, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: "dataset", ResourceID: ID(dataset), Detail: detail,
	})
}

type queryDatasetPolicyAPI struct {
	repo       QueryDatasetPolicyRepository
	registries *PlatformRegistries
	audit      AuditRepository
}

type queryDatasetPolicyItem struct {
	Dataset    DatasetDescriptor  `json:"dataset"`
	Policy     QueryDatasetPolicy `json:"policy"`
	Configured bool               `json:"configured"`
}

func registerQueryDatasetPolicyRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo QueryDatasetPolicyRepository, registries *PlatformRegistries, audit AuditRepository) {
	api := queryDatasetPolicyAPI{repo: repo, registries: registries, audit: audit}
	admin := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/query-policies", auth(admin(http.HandlerFunc(api.list))))
	mux.Handle("GET /api/v1/query-policies/{dataset_key}", auth(admin(http.HandlerFunc(api.get))))
	mux.Handle("PUT /api/v1/query-policies/{dataset_key}", auth(admin(http.HandlerFunc(api.put))))
	mux.Handle("DELETE /api/v1/query-policies/{dataset_key}", auth(admin(http.HandlerFunc(api.delete))))
}

func (api queryDatasetPolicyAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	stored, err := api.repo.ListQueryDatasetPolicies(r.Context(), auth.TenantID)
	if err != nil {
		writeQueryDatasetPolicyError(w, err)
		return
	}
	byDataset := make(map[string]QueryDatasetPolicy, len(stored))
	for _, policy := range stored {
		byDataset[policy.DatasetKey] = policy
	}
	descriptors := api.registries.Datasets.List()
	items := make([]queryDatasetPolicyItem, 0, len(descriptors))
	for _, descriptor := range descriptors {
		policy, configured := byDataset[descriptor.Key]
		if !configured {
			policy = defaultQueryDatasetPolicy(auth.TenantID, descriptor)
		}
		items = append(items, queryDatasetPolicyItem{Dataset: descriptor, Policy: policy, Configured: configured})
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

func (api queryDatasetPolicyAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	descriptor, ok := api.dataset(w, r)
	if !ok {
		return
	}
	policy, err := api.repo.GetQueryDatasetPolicy(r.Context(), auth.TenantID, descriptor.Key)
	configured := err == nil
	if errors.Is(err, sql.ErrNoRows) {
		policy = defaultQueryDatasetPolicy(auth.TenantID, descriptor)
	} else if err != nil {
		writeQueryDatasetPolicyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(policy.RowVersion))
	WriteAPIJSON(w, http.StatusOK, queryDatasetPolicyItem{Dataset: descriptor, Policy: policy, Configured: configured})
}

type queryDatasetPolicyInput struct {
	Enabled         *bool   `json:"enabled"`
	AllowRaw        *bool   `json:"allow_raw"`
	AllowSupplier   *bool   `json:"allow_supplier"`
	AllowCustomer   *bool   `json:"allow_customer"`
	MaxRangeSeconds *uint32 `json:"max_range_seconds"`
	MaxConcurrent   *uint32 `json:"max_concurrent"`
	MaxResultRows   *uint32 `json:"max_result_rows"`
	QueryTimeoutMS  *uint32 `json:"query_timeout_ms"`
}

func (api queryDatasetPolicyAPI) put(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	descriptor, ok := api.dataset(w, r)
	if !ok {
		return
	}
	expectedVersion, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	var input queryDatasetPolicyInput
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if input.Enabled == nil || input.AllowRaw == nil || input.AllowSupplier == nil || input.AllowCustomer == nil ||
		input.MaxRangeSeconds == nil || input.MaxConcurrent == nil || input.MaxResultRows == nil || input.QueryTimeoutMS == nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "all query policy fields are required", nil)
		return
	}
	if (*input.AllowRaw && !datasetSupportsQueryLayer(descriptor, QueryValueRaw)) ||
		(*input.AllowSupplier && !datasetSupportsQueryLayer(descriptor, QueryValueSupplier)) ||
		(*input.AllowCustomer && !datasetSupportsQueryLayer(descriptor, QueryValueCustomer)) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "policy enables a value layer the dataset does not support", nil)
		return
	}
	policy := QueryDatasetPolicy{
		TenantID: auth.TenantID, DatasetKey: descriptor.Key, UpdatedBy: auth.UserID,
		Enabled: *input.Enabled, AllowRaw: *input.AllowRaw, AllowSupplier: *input.AllowSupplier,
		AllowCustomer: *input.AllowCustomer, MaxRangeSeconds: *input.MaxRangeSeconds,
		MaxConcurrent: *input.MaxConcurrent, MaxResultRows: *input.MaxResultRows,
		QueryTimeoutMS: *input.QueryTimeoutMS,
	}
	if err := validateQueryDatasetPolicy(policy); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	saved, err := api.repo.PutQueryDatasetPolicy(r.Context(), policy, expectedVersion)
	if err != nil {
		writeQueryDatasetPolicyError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "query_policy.updated", descriptor.Key, map[string]any{
		"previous_version": expectedVersion, "row_version": saved.RowVersion,
	})
	w.Header().Set("ETag", quotedRowVersion(saved.RowVersion))
	WriteAPIJSON(w, http.StatusOK, queryDatasetPolicyItem{Dataset: descriptor, Policy: saved, Configured: true})
}

func (api queryDatasetPolicyAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	descriptor, ok := api.dataset(w, r)
	if !ok {
		return
	}
	expectedVersion, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteQueryDatasetPolicy(r.Context(), auth.TenantID, descriptor.Key, expectedVersion); err != nil {
		writeQueryDatasetPolicyError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "query_policy.deleted", descriptor.Key, map[string]any{"row_version": expectedVersion})
	w.WriteHeader(http.StatusNoContent)
}

func (api queryDatasetPolicyAPI) dataset(w http.ResponseWriter, r *http.Request) (DatasetDescriptor, bool) {
	key := strings.TrimSpace(r.PathValue("dataset_key"))
	descriptor, ok := api.registries.Datasets.Get(key)
	if !ok {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Dataset not found", nil)
		return DatasetDescriptor{}, false
	}
	return descriptor, true
}

func (api queryDatasetPolicyAPI) recordAudit(ctx context.Context, auth AuthContext, action, dataset string, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: "query_policy", ResourceID: ID(dataset), Detail: detail,
	})
}

func datasetSupportsQueryLayer(descriptor DatasetDescriptor, layer QueryValueLayer) bool {
	if len(descriptor.ValueLayers) == 0 {
		return layer == QueryValueCustomer
	}
	for _, supported := range descriptor.ValueLayers {
		if supported == layer {
			return true
		}
	}
	return false
}

func writeQueryDatasetPolicyError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Query dataset policy not found", nil)
	case errors.Is(err, ErrQueryDatasetPolicyConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("RESOURCE_VERSION_CONFLICT"), err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "Query policy storage is unavailable", nil)
	}
}

func writeQueryGatewayError(w http.ResponseWriter, err error) {
	var queryErr *QueryGatewayError
	if !errors.As(err, &queryErr) {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorCode(QueryErrorProviderFailure), "Query failed", nil)
		return
	}
	status := http.StatusBadRequest
	switch queryErr.Code {
	case QueryErrorDatasetNotFound:
		status = http.StatusNotFound
	case QueryErrorPermissionDenied:
		status = http.StatusForbidden
	case QueryErrorModuleDisabled, QueryErrorDatasetDisabled, QueryErrorIncomplete:
		status = http.StatusConflict
	case QueryErrorConcurrencyLimit:
		status = http.StatusTooManyRequests
		w.Header().Set("Retry-After", "1")
	case QueryErrorProviderUnavailable, QueryErrorProviderFailure:
		status = http.StatusServiceUnavailable
	case QueryErrorTimeout:
		status = http.StatusGatewayTimeout
	case QueryErrorCanceled:
		status = http.StatusRequestTimeout
	}
	WriteAPIJSON(w, status, apiErrorResponse{Error: APIError{
		Code: APIErrorCode(queryErr.Code), Message: queryErr.Message,
		Retryable: queryErr.Retryable, Details: queryErr.Details,
	}})
}
