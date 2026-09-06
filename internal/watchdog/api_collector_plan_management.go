package watchdog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type collectorPlanCreateAPIRequest struct {
	PlanSchemaVersion uint16          `json:"plan_schema_version"`
	Spec              json.RawMessage `json:"spec"`
	FromConfigVersion uint64          `json:"from_config_version"`
	NotBefore         *time.Time      `json:"not_before"`
	ExpiresAt         *time.Time      `json:"expires_at"`
}

type collectorPlanActivateAPIRequest struct {
	CollectorRowVersion uint64 `json:"collector_row_version"`
}

type collectorPlanRevisionAPIResponse struct {
	ID                      ID                  `json:"id"`
	TenantID                ID                  `json:"tenant_id"`
	CollectorID             ID                  `json:"collector_id"`
	ConfigVersion           uint64              `json:"config_version"`
	PlanSchemaVersion       uint16              `json:"plan_schema_version"`
	Status                  CollectorPlanStatus `json:"status"`
	SpecHash                string              `json:"spec_hash"`
	SigningKeyID            string              `json:"signing_key_id"`
	NotBefore               *time.Time          `json:"not_before,omitempty"`
	ExpiresAt               time.Time           `json:"expires_at"`
	SupersedesConfigVersion uint64              `json:"supersedes_config_version"`
	RowVersion              uint64              `json:"row_version"`
	CreatedAt               time.Time           `json:"created_at"`
	UpdatedAt               time.Time           `json:"updated_at"`
	ActivatedAt             *time.Time          `json:"activated_at,omitempty"`
	RetiredAt               *time.Time          `json:"retired_at,omitempty"`
}

type collectorPlanManagementAPI struct {
	controller CollectorPlanManagementController
}

func registerCollectorPlanManagementRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, controller CollectorPlanManagementController) {
	api := collectorPlanManagementAPI{controller: controller}
	viewTenant := RequirePermission(ActionView, TenantResource)
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	operateTenant := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/collectors/{collector_id}/plan-revisions", auth(viewTenant(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/collectors/{collector_id}/plan-revisions", auth(configureTenant(http.HandlerFunc(api.create))))
	mux.Handle("POST /api/v1/collectors/{collector_id}/plan-revisions/{config_version}/activate", auth(operateTenant(http.HandlerFunc(api.activate))))
}

func (api collectorPlanManagementAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	collectorID := ID(r.PathValue("collector_id"))
	if !validCollectorEvidenceID(collectorID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id is required", nil)
		return
	}
	var body collectorPlanCreateAPIRequest
	if err := decodeCollectorPlanManagementJSON(w, r, &body); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if body.ExpiresAt == nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "expires_at is required", nil)
		return
	}
	request := CollectorPlanCreateRequest{
		TenantID: auth.TenantID, CollectorID: collectorID, ActorID: auth.UserID,
		PlanSchemaVersion: body.PlanSchemaVersion, SpecJSON: body.Spec,
		FromConfigVersion: body.FromConfigVersion, ExpiresAt: body.ExpiresAt.UTC(),
	}
	if body.NotBefore != nil {
		request.NotBefore = body.NotBefore.UTC()
	}
	created, err := api.controller.CreateCollectorPlanRevision(r.Context(), request)
	if err != nil {
		writeCollectorPlanManagementError(w, err)
		return
	}
	writeCollectorPlanRevision(w, http.StatusCreated, created)
}

func (api collectorPlanManagementAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	collectorID := ID(r.PathValue("collector_id"))
	if !validCollectorEvidenceID(collectorID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id is required", nil)
		return
	}
	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 200 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be between 1 and 200", nil)
			return
		}
		limit = parsed
	}
	filter := CollectorPlanPageFilter{Limit: limit}
	if cursor := strings.TrimSpace(r.URL.Query().Get("cursor")); cursor != "" {
		version, err := decodeCollectorPlanCursor(cursor)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "cursor is invalid", nil)
			return
		}
		filter.BeforeVersion = version
	}
	items, next, err := api.controller.ListCollectorPlanRevisions(r.Context(), auth.TenantID, collectorID, filter)
	if err != nil {
		writeCollectorPlanManagementError(w, err)
		return
	}
	responses := make([]collectorPlanRevisionAPIResponse, len(items))
	for i := range items {
		responses[i] = collectorPlanRevisionResponse(items[i])
	}
	response := map[string]any{"items": responses}
	if next != "" {
		response["next_cursor"] = next
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api collectorPlanManagementAPI) activate(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	collectorID := ID(r.PathValue("collector_id"))
	configVersion, err := strconv.ParseUint(r.PathValue("config_version"), 10, 64)
	if !validCollectorEvidenceID(collectorID) || err != nil || configVersion == 0 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id and config_version are required", nil)
		return
	}
	var body collectorPlanActivateAPIRequest
	if err := decodeCollectorPlanManagementJSON(w, r, &body); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if body.CollectorRowVersion == 0 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_row_version is required", nil)
		return
	}
	planRowVersion, err := parseCollectorPrincipalIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		WriteAPIError(w, http.StatusPreconditionRequired, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	activated, err := api.controller.ActivateCollectorPlanRevision(r.Context(), CollectorPlanActivation{
		TenantID: auth.TenantID, CollectorID: collectorID, ConfigVersion: configVersion,
		ExpectedCollectorRowVersion: body.CollectorRowVersion,
		ExpectedPlanRowVersion:      planRowVersion,
		ActorID:                     auth.UserID,
	})
	if err != nil {
		writeCollectorPlanManagementError(w, err)
		return
	}
	writeCollectorPlanRevision(w, http.StatusOK, activated)
}

func decodeCollectorPlanManagementJSON(w http.ResponseWriter, r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, collectorPlanMaxSpecBytes+4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON object")
		}
		return err
	}
	return nil
}

func writeCollectorPlanManagementError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCollectorPlanInvalidRequest):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Collector plan request is invalid", nil)
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector or plan revision not found", nil)
	case errors.Is(err, ErrCollectorPlanConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorInvalidRequest, "Collector or plan revision version changed", nil)
	case errors.Is(err, ErrCollectorPlanInvalidTransition):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector plan transition is not allowed", nil)
	case errors.Is(err, ErrCollectorPlanSigningKeyUnavailable):
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector plan signing is unavailable", nil)
	default:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector plan management is unavailable", nil)
	}
}

func writeCollectorPlanRevision(w http.ResponseWriter, status int, plan CollectorPlanRevision) {
	w.Header().Set("ETag", `"`+strconv.FormatUint(plan.RowVersion, 10)+`"`)
	WriteAPIJSON(w, status, collectorPlanRevisionResponse(plan))
}

func collectorPlanRevisionResponse(plan CollectorPlanRevision) collectorPlanRevisionAPIResponse {
	response := collectorPlanRevisionAPIResponse{
		ID: plan.ID, TenantID: plan.TenantID, CollectorID: plan.CollectorID,
		ConfigVersion: plan.ConfigVersion, PlanSchemaVersion: plan.PlanSchemaVersion,
		Status: plan.Status, SpecHash: plan.SpecHash, SigningKeyID: plan.SigningKeyID,
		ExpiresAt: plan.ExpiresAt, SupersedesConfigVersion: plan.SupersedesConfigVersion,
		RowVersion: plan.RowVersion, CreatedAt: plan.CreatedAt, UpdatedAt: plan.UpdatedAt,
	}
	if !plan.NotBefore.IsZero() {
		value := plan.NotBefore
		response.NotBefore = &value
	}
	if !plan.ActivatedAt.IsZero() {
		value := plan.ActivatedAt
		response.ActivatedAt = &value
	}
	if !plan.RetiredAt.IsZero() {
		value := plan.RetiredAt
		response.RetiredAt = &value
	}
	return response
}
