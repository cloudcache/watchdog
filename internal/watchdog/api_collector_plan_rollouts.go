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

type collectorPlanRolloutCreateAPIRequest struct {
	Selector          CollectorPlanRolloutSelector `json:"selector"`
	PlanSchemaVersion uint16                       `json:"plan_schema_version"`
	Spec              json.RawMessage              `json:"spec"`
	Strategy          CollectorPlanRolloutStrategy `json:"strategy"`
	ExpiresAt         *time.Time                   `json:"expires_at"`
}

type collectorPlanRolloutAPIResponse struct {
	ID                   ID                           `json:"id"`
	TenantID             ID                           `json:"tenant_id"`
	ModuleKey            string                       `json:"module_key"`
	RolloutSchemaVersion uint16                       `json:"rollout_schema_version"`
	Selector             CollectorPlanRolloutSelector `json:"selector"`
	SelectorHash         string                       `json:"selector_hash"`
	SpecHash             string                       `json:"spec_hash"`
	PlanSchemaVersion    uint16                       `json:"plan_schema_version"`
	Strategy             CollectorPlanRolloutStrategy `json:"strategy"`
	StrategyHash         string                       `json:"strategy_hash"`
	Status               CollectorPlanRolloutStatus   `json:"status"`
	ExpiresAt            time.Time                    `json:"expires_at"`
	RowVersion           uint64                       `json:"row_version"`
	CreatedBy            ID                           `json:"created_by"`
	UpdatedBy            ID                           `json:"updated_by"`
	CreatedAt            time.Time                    `json:"created_at"`
	UpdatedAt            time.Time                    `json:"updated_at"`
	PreviewedAt          *time.Time                   `json:"previewed_at,omitempty"`
	CompletedAt          *time.Time                   `json:"completed_at,omitempty"`
}

type collectorPlanRolloutPreviewAPIResponse struct {
	Rollout       collectorPlanRolloutAPIResponse `json:"rollout"`
	MatchedCount  uint64                          `json:"matched_count"`
	EligibleCount uint64                          `json:"eligible_count"`
	SkippedCount  uint64                          `json:"skipped_count"`
	WaveCount     uint32                          `json:"wave_count"`
}

type collectorPlanRolloutTargetAPIResponse struct {
	CollectorID               ID                               `json:"collector_id"`
	CollectorName             string                           `json:"collector_name"`
	AgentType                 string                           `json:"agent_type"`
	CollectorStatus           string                           `json:"collector_status"`
	ObservedHealth            string                           `json:"observed_health"`
	CollectorConfigVersion    uint64                           `json:"collector_config_version"`
	AcknowledgedConfigVersion uint64                           `json:"acknowledged_config_version"`
	LastGoodConfigVersion     uint64                           `json:"last_good_config_version"`
	LastSeenAt                *time.Time                       `json:"last_seen_at,omitempty"`
	Wave                      uint32                           `json:"wave"`
	ConfigVersion             *uint64                          `json:"config_version,omitempty"`
	PriorConfigVersion        uint64                           `json:"prior_config_version"`
	Status                    CollectorPlanRolloutTargetStatus `json:"status"`
	FailureReason             string                           `json:"failure_reason,omitempty"`
	ActivatedAt               *time.Time                       `json:"activated_at,omitempty"`
	ACKedAt                   *time.Time                       `json:"acked_at,omitempty"`
	RowVersion                uint64                           `json:"row_version"`
	CreatedAt                 time.Time                        `json:"created_at"`
	UpdatedAt                 time.Time                        `json:"updated_at"`
}

type collectorPlanRolloutAPI struct {
	controller CollectorPlanRolloutController
}

func registerCollectorPlanRolloutRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, controller CollectorPlanRolloutController) {
	api := collectorPlanRolloutAPI{controller: controller}
	view := RequirePermission(ActionView, TenantResource)
	mux.Handle("GET /api/v1/plan-rollouts", auth(view(http.HandlerFunc(api.list))))
	mux.Handle("GET /api/v1/plan-rollouts/{rollout_id}", auth(view(http.HandlerFunc(api.get))))
	mux.Handle("GET /api/v1/plan-rollouts/{rollout_id}/targets", auth(view(http.HandlerFunc(api.listTargets))))
	mux.Handle("POST /api/v1/plan-rollouts", auth(RequirePermission(ActionConfigure, TenantResource)(http.HandlerFunc(api.create))))
	mux.Handle("POST /api/v1/plan-rollouts/{rollout_id}/preview", auth(RequirePermission(ActionOperate, TenantResource)(http.HandlerFunc(api.preview))))
}

func (api collectorPlanRolloutAPI) list(w http.ResponseWriter, r *http.Request) {
	identity, _ := AuthFromContext(r.Context())
	filter, err := parseCollectorPlanRolloutListFilter(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	items, total, err := api.controller.ListCollectorPlanRollouts(r.Context(), identity.TenantID, filter)
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	responses := make([]collectorPlanRolloutAPIResponse, len(items))
	for index := range items {
		responses[index], err = collectorPlanRolloutResponse(items[index])
		if err != nil {
			writeCollectorPlanRolloutError(w, err)
			return
		}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": responses, "total": total})
}

func (api collectorPlanRolloutAPI) get(w http.ResponseWriter, r *http.Request) {
	identity, _ := AuthFromContext(r.Context())
	rolloutID := ID(r.PathValue("rollout_id"))
	if !validCollectorEvidenceID(rolloutID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "rollout_id is required", nil)
		return
	}
	rollout, summary, err := api.controller.GetCollectorPlanRollout(r.Context(), identity.TenantID, rolloutID)
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	response, err := collectorPlanRolloutResponse(rollout)
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+strconv.FormatUint(rollout.RowVersion, 10)+`"`)
	WriteAPIJSON(w, http.StatusOK, map[string]any{"rollout": response, "summary": summary})
}

func (api collectorPlanRolloutAPI) listTargets(w http.ResponseWriter, r *http.Request) {
	identity, _ := AuthFromContext(r.Context())
	rolloutID := ID(r.PathValue("rollout_id"))
	if !validCollectorEvidenceID(rolloutID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "rollout_id is required", nil)
		return
	}
	filter, err := parseCollectorPlanRolloutTargetListFilter(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	items, total, err := api.controller.ListCollectorPlanRolloutTargets(r.Context(), identity.TenantID, rolloutID, filter)
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	responses := make([]collectorPlanRolloutTargetAPIResponse, len(items))
	for index := range items {
		responses[index] = collectorPlanRolloutTargetResponse(items[index])
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": responses, "total": total})
}

func (api collectorPlanRolloutAPI) create(w http.ResponseWriter, r *http.Request) {
	identity, _ := AuthFromContext(r.Context())
	var body collectorPlanRolloutCreateAPIRequest
	if err := decodeCollectorPlanRolloutJSON(w, r, &body, false); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if body.ExpiresAt == nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "expires_at is required", nil)
		return
	}
	created, err := api.controller.CreateCollectorPlanRollout(r.Context(), CollectorPlanRolloutCreateRequest{
		TenantID: identity.TenantID, ActorID: identity.UserID,
		Selector: body.Selector, SpecJSON: body.Spec,
		PlanSchemaVersion: body.PlanSchemaVersion, Strategy: body.Strategy,
		ExpiresAt: body.ExpiresAt.UTC(),
	})
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	writeCollectorPlanRollout(w, http.StatusCreated, created)
}

func (api collectorPlanRolloutAPI) preview(w http.ResponseWriter, r *http.Request) {
	identity, _ := AuthFromContext(r.Context())
	rolloutID := ID(r.PathValue("rollout_id"))
	if !validCollectorEvidenceID(rolloutID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "rollout_id is required", nil)
		return
	}
	if err := decodeCollectorPlanRolloutJSON(w, r, &struct{}{}, true); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	expectedVersion, err := parseCollectorPrincipalIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		WriteAPIError(w, http.StatusPreconditionRequired, APIErrorCode("precondition_required"), err.Error(), nil)
		return
	}
	preview, err := api.controller.PreviewCollectorPlanRollout(r.Context(), CollectorPlanRolloutPreviewRequest{
		TenantID: identity.TenantID, RolloutID: rolloutID,
		ActorID: identity.UserID, ExpectedRowVersion: expectedVersion,
	})
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	response, err := collectorPlanRolloutResponse(preview.Rollout)
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+strconv.FormatUint(preview.Rollout.RowVersion, 10)+`"`)
	WriteAPIJSON(w, http.StatusOK, collectorPlanRolloutPreviewAPIResponse{
		Rollout: response, MatchedCount: preview.MatchedCount,
		EligibleCount: preview.EligibleCount, SkippedCount: preview.SkippedCount,
		WaveCount: preview.WaveCount,
	})
}

func decodeCollectorPlanRolloutJSON(w http.ResponseWriter, r *http.Request, target any, allowEmpty bool) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, collectorPlanMaxSpecBytes+8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if allowEmpty && errors.Is(err, io.EOF) {
			return nil
		}
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

func parseCollectorPlanRolloutListFilter(r *http.Request) (CollectorPlanRolloutListFilter, error) {
	query := r.URL.Query()
	limit, offset, desc, err := parseCollectorPlanRolloutPage(query.Get("limit"), query.Get("offset"), query.Get("order"))
	if err != nil {
		return CollectorPlanRolloutListFilter{}, err
	}
	return normalizeCollectorPlanRolloutListFilter(CollectorPlanRolloutListFilter{
		Query: query.Get("q"), ModuleKey: query.Get("module_key"),
		Status: CollectorPlanRolloutStatus(query.Get("status")), Sort: query.Get("sort"),
		Desc: desc, Limit: limit, Offset: offset,
	})
}

func parseCollectorPlanRolloutTargetListFilter(r *http.Request) (CollectorPlanRolloutTargetListFilter, error) {
	query := r.URL.Query()
	limit, offset, desc, err := parseCollectorPlanRolloutPage(query.Get("limit"), query.Get("offset"), query.Get("order"))
	if err != nil {
		return CollectorPlanRolloutTargetListFilter{}, err
	}
	var wave *uint32
	if raw := strings.ToLower(strings.TrimSpace(query.Get("wave"))); raw != "" {
		value := uint64(0)
		switch raw {
		case "canary":
		case "skipped":
			value = uint64(CollectorPlanRolloutSkippedWave)
		default:
			value, err = strconv.ParseUint(raw, 10, 32)
			if err != nil {
				return CollectorPlanRolloutTargetListFilter{}, errors.New("wave must be canary, skipped, or an unsigned integer")
			}
		}
		parsed := uint32(value)
		wave = &parsed
	}
	return normalizeCollectorPlanRolloutTargetListFilter(CollectorPlanRolloutTargetListFilter{
		Query: query.Get("q"), Status: CollectorPlanRolloutTargetStatus(query.Get("status")),
		Health: query.Get("health"), Wave: wave, Sort: query.Get("sort"),
		Desc: desc, Limit: limit, Offset: offset,
	})
}

func parseCollectorPlanRolloutPage(rawLimit, rawOffset, order string) (int, int, bool, error) {
	limit, offset := 50, 0
	var err error
	if rawLimit = strings.TrimSpace(rawLimit); rawLimit != "" {
		limit, err = strconv.Atoi(rawLimit)
		if err != nil {
			return 0, 0, false, errors.New("limit must be an integer")
		}
	}
	if rawOffset = strings.TrimSpace(rawOffset); rawOffset != "" {
		offset, err = strconv.Atoi(rawOffset)
		if err != nil {
			return 0, 0, false, errors.New("offset must be an integer")
		}
	}
	order = strings.ToLower(strings.TrimSpace(order))
	if order != "" && order != "asc" && order != "desc" {
		return 0, 0, false, errors.New("order must be asc or desc")
	}
	return limit, offset, order == "desc", nil
}

func writeCollectorPlanRollout(w http.ResponseWriter, status int, rollout CollectorPlanRollout) {
	response, err := collectorPlanRolloutResponse(rollout)
	if err != nil {
		writeCollectorPlanRolloutError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+strconv.FormatUint(rollout.RowVersion, 10)+`"`)
	WriteAPIJSON(w, status, response)
}

func collectorPlanRolloutResponse(rollout CollectorPlanRollout) (collectorPlanRolloutAPIResponse, error) {
	var selector CollectorPlanRolloutSelector
	var strategy CollectorPlanRolloutStrategy
	if err := json.Unmarshal(rollout.SelectorJSON, &selector); err != nil {
		return collectorPlanRolloutAPIResponse{}, err
	}
	if err := json.Unmarshal(rollout.StrategyJSON, &strategy); err != nil {
		return collectorPlanRolloutAPIResponse{}, err
	}
	response := collectorPlanRolloutAPIResponse{
		ID: rollout.ID, TenantID: rollout.TenantID, ModuleKey: rollout.ModuleKey,
		RolloutSchemaVersion: rollout.RolloutSchemaVersion,
		Selector:             selector, SelectorHash: rollout.SelectorHash, SpecHash: rollout.SpecHash,
		PlanSchemaVersion: rollout.PlanSchemaVersion, Strategy: strategy,
		StrategyHash: rollout.StrategyHash, Status: rollout.Status,
		ExpiresAt: rollout.ExpiresAt, RowVersion: rollout.RowVersion,
		CreatedBy: rollout.CreatedBy, UpdatedBy: rollout.UpdatedBy,
		CreatedAt: rollout.CreatedAt, UpdatedAt: rollout.UpdatedAt,
	}
	if !rollout.PreviewedAt.IsZero() {
		value := rollout.PreviewedAt
		response.PreviewedAt = &value
	}
	if !rollout.CompletedAt.IsZero() {
		value := rollout.CompletedAt
		response.CompletedAt = &value
	}
	return response, nil
}

func collectorPlanRolloutTargetResponse(target CollectorPlanRolloutTarget) collectorPlanRolloutTargetAPIResponse {
	response := collectorPlanRolloutTargetAPIResponse{
		CollectorID: target.CollectorID, CollectorName: target.CollectorName,
		AgentType: target.AgentType, CollectorStatus: target.CollectorStatus,
		ObservedHealth:            target.ObservedHealth,
		CollectorConfigVersion:    target.CollectorConfigVersion,
		AcknowledgedConfigVersion: target.AcknowledgedConfigVersion,
		LastGoodConfigVersion:     target.LastGoodConfigVersion,
		Wave:                      target.Wave, PriorConfigVersion: target.PriorConfigVersion,
		Status: target.Status, FailureReason: target.FailureReason,
		RowVersion: target.RowVersion, CreatedAt: target.CreatedAt, UpdatedAt: target.UpdatedAt,
	}
	if !target.LastSeenAt.IsZero() {
		value := target.LastSeenAt
		response.LastSeenAt = &value
	}
	if target.ConfigVersion != 0 {
		value := target.ConfigVersion
		response.ConfigVersion = &value
	}
	if !target.ActivatedAt.IsZero() {
		value := target.ActivatedAt
		response.ActivatedAt = &value
	}
	if !target.ACKedAt.IsZero() {
		value := target.ACKedAt
		response.ACKedAt = &value
	}
	return response
}

func writeCollectorPlanRolloutError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCollectorPlanRolloutInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Collector plan rollout request is invalid", nil)
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector plan rollout not found", nil)
	case errors.Is(err, ErrCollectorPlanRolloutConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), "Collector plan rollout changed since it was read", nil)
	case errors.Is(err, ErrCollectorPlanRolloutEmpty):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector plan rollout selector matched no collectors", nil)
	case errors.Is(err, ErrCollectorPlanInvalidTransition):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector plan rollout transition is not allowed", nil)
	default:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector plan rollout management is unavailable", nil)
	}
}
