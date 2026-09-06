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

type operationJobScheduleAPI struct {
	repo  OperationJobScheduleRepository
	audit AuditRepository
}

func registerOperationJobScheduleRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo OperationJobScheduleRepository, audit AuditRepository) {
	api := operationJobScheduleAPI{repo: repo, audit: audit}
	view := RequirePermission(ActionView, TenantResource)
	admin := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/operation-job-schedules", auth(view(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/operation-job-schedules", auth(admin(http.HandlerFunc(api.create))))
	mux.Handle("GET /api/v1/operation-job-schedules/{schedule_id}", auth(view(http.HandlerFunc(api.get))))
	mux.Handle("PATCH /api/v1/operation-job-schedules/{schedule_id}", auth(admin(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/operation-job-schedules/{schedule_id}", auth(admin(http.HandlerFunc(api.delete))))
}

func (api operationJobScheduleAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter, ok := parseOperationJobScheduleFilter(w, r)
	if !ok {
		return
	}
	items, total, err := api.repo.ListOperationJobSchedules(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": total, "limit": filter.Limit, "offset": filter.Offset})
}

func (api operationJobScheduleAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetOperationJobSchedule(r.Context(), auth.TenantID, ID(r.PathValue("schedule_id")))
	if err != nil {
		writeOperationJobScheduleError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api operationJobScheduleAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	input, ok := decodeOperationJobScheduleInput(w, r)
	if !ok {
		return
	}
	item := OperationJobSchedule{
		TenantID: auth.TenantID, ScopeType: OperationJobScopeTenant, CreatedBy: auth.UserID,
		Enabled: true, MaxInflight: 1,
	}
	applyOperationJobScheduleInput(&item, input)
	item, err := normalizeOperationJobSchedule(item, time.Now())
	if err != nil {
		writeOperationJobScheduleError(w, err)
		return
	}
	created, err := api.repo.CreateOperationJobSchedule(r.Context(), item)
	if err != nil {
		writeOperationJobScheduleError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "operation_job_schedule.created", created.ID, map[string]any{"job_type": created.JobType, "partition_key": created.PartitionKey})
	w.Header().Set("ETag", quotedRowVersion(created.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api operationJobScheduleAPI) patch(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	scheduleID := ID(r.PathValue("schedule_id"))
	item, err := api.repo.GetOperationJobSchedule(r.Context(), auth.TenantID, scheduleID)
	if err != nil {
		writeOperationJobScheduleError(w, err)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	input, ok := decodeOperationJobScheduleInput(w, r)
	if !ok {
		return
	}
	if input.empty() {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "at least one schedule field is required", nil)
		return
	}
	applyOperationJobScheduleInput(&item, input)
	item, err = normalizeOperationJobSchedule(item, time.Now())
	if err != nil {
		writeOperationJobScheduleError(w, err)
		return
	}
	updated, err := api.repo.UpdateOperationJobSchedule(r.Context(), item, expected)
	if err != nil {
		writeOperationJobScheduleError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "operation_job_schedule.updated", updated.ID, map[string]any{"previous_version": expected, "row_version": updated.RowVersion})
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api operationJobScheduleAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	scheduleID := ID(r.PathValue("schedule_id"))
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteOperationJobSchedule(r.Context(), auth.TenantID, scheduleID, expected); err != nil {
		writeOperationJobScheduleError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "operation_job_schedule.deleted", scheduleID, map[string]any{"row_version": expected})
	w.WriteHeader(http.StatusNoContent)
}

type operationJobScheduleInput struct {
	Name           *string          `json:"name"`
	JobType        *string          `json:"job_type"`
	PartitionKey   *string          `json:"partition_key"`
	CronExpression *string          `json:"cron_expression"`
	Timezone       *string          `json:"timezone"`
	Payload        *json.RawMessage `json:"payload"`
	Enabled        *bool            `json:"enabled"`
	MaxInflight    *uint32          `json:"max_inflight"`
}

func (input operationJobScheduleInput) empty() bool {
	return input.Name == nil && input.JobType == nil && input.PartitionKey == nil &&
		input.CronExpression == nil && input.Timezone == nil && input.Payload == nil &&
		input.Enabled == nil && input.MaxInflight == nil
}

func decodeOperationJobScheduleInput(w http.ResponseWriter, r *http.Request) (operationJobScheduleInput, bool) {
	defer r.Body.Close()
	var input operationJobScheduleInput
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, operationJobSchedulePayloadMax+4096))
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

func applyOperationJobScheduleInput(item *OperationJobSchedule, input operationJobScheduleInput) {
	if input.Name != nil {
		item.Name = *input.Name
	}
	if input.JobType != nil {
		item.JobType = *input.JobType
	}
	if input.PartitionKey != nil {
		item.PartitionKey = *input.PartitionKey
	}
	if input.CronExpression != nil {
		item.CronExpression = *input.CronExpression
	}
	if input.Timezone != nil {
		item.Timezone = *input.Timezone
	}
	if input.Payload != nil {
		item.PayloadJSON = *input.Payload
	}
	if input.Enabled != nil {
		item.Enabled = *input.Enabled
	}
	if input.MaxInflight != nil {
		item.MaxInflight = *input.MaxInflight
	}
}

func parseOperationJobScheduleFilter(w http.ResponseWriter, r *http.Request) (OperationJobScheduleFilter, bool) {
	query := r.URL.Query()
	filter := OperationJobScheduleFilter{
		Search: query.Get("q"), JobType: query.Get("job_type"), Sort: query.Get("sort"),
		Desc: strings.EqualFold(strings.TrimSpace(query.Get("order")), "desc"),
	}
	if order := strings.TrimSpace(query.Get("order")); order != "" && !strings.EqualFold(order, "asc") && !strings.EqualFold(order, "desc") {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "order must be asc or desc", nil)
		return filter, false
	}
	if raw, exists := query["enabled"]; exists {
		if len(raw) != 1 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "enabled must have one boolean value", nil)
			return filter, false
		}
		value, err := strconv.ParseBool(raw[0])
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "enabled must be true or false", nil)
			return filter, false
		}
		filter.Enabled = &value
	}
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return filter, false
		}
		filter.Limit = value
	}
	if raw := strings.TrimSpace(query.Get("offset")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be a non-negative integer", nil)
			return filter, false
		}
		filter.Offset = value
	}
	filter, err := normalizeOperationJobScheduleFilter(filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return filter, false
	}
	return filter, true
}

func writeOperationJobScheduleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Operation job schedule not found", nil)
	case errors.Is(err, ErrOperationJobScheduleConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), err.Error(), nil)
	case errors.Is(err, ErrOperationJobScheduleExists):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	}
}

func (api operationJobScheduleAPI) recordAudit(ctx context.Context, auth AuthContext, action string, scheduleID ID, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: "operation_job_schedule", ResourceID: scheduleID, Detail: detail,
	})
}
