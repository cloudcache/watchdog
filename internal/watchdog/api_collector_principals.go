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

type collectorPrincipalGrantAPIRequest struct {
	Provider              string `json:"provider"`
	ACLPropagationDelayMS uint64 `json:"acl_propagation_delay_ms"`
}

type collectorPrincipalResponse struct {
	ID                    ID        `json:"id"`
	TenantID              ID        `json:"tenant_id"`
	CollectorID           ID        `json:"collector_id"`
	ServiceType           string    `json:"service_type"`
	PrincipalRef          string    `json:"principal_ref"`
	Provider              string    `json:"provider"`
	Status                string    `json:"status"`
	ACLPropagationDelayMS uint64    `json:"acl_propagation_delay_ms"`
	RowVersion            uint64    `json:"row_version"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type collectorPrincipalAPI struct {
	controller CollectorPrincipalController
}

func registerCollectorPrincipalRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, controller CollectorPrincipalController) {
	api := collectorPrincipalAPI{controller: controller}
	operateTenant := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("POST /api/v1/collectors/{collector_id}/service-principals", auth(operateTenant(http.HandlerFunc(api.grant))))
	mux.Handle("POST /api/v1/collectors/{collector_id}/service-principals/{principal_id}/actions/revoke-write", auth(operateTenant(http.HandlerFunc(api.revokeWrite))))
}

func (api collectorPrincipalAPI) grant(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	collectorID := ID(r.PathValue("collector_id"))
	if !validCollectorEvidenceID(collectorID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id is required", nil)
		return
	}
	var req collectorPrincipalGrantAPIRequest
	if err := decodeCollectorPrincipalJSON(r, &req, false); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	idempotencyKey := r.Header.Get(flowStateCleanupIdempotencyHeader)
	if idempotencyKey == "" || len(idempotencyKey) > 128 || !isPrintableASCII(idempotencyKey) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Idempotency-Key must be 1 to 128 non-space printable ASCII bytes", nil)
		return
	}
	if req.Provider == "" || len(req.Provider) > 64 || !isPrintableASCII(req.Provider) || req.ACLPropagationDelayMS > uint64((24*time.Hour)/time.Millisecond) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "provider and a bounded acl_propagation_delay_ms are required", nil)
		return
	}
	principal, err := api.controller.Grant(r.Context(), auth.TenantID, collectorID, auth.UserID, CollectorPrincipalGrantRequest{
		Provider: req.Provider, IdempotencyKey: idempotencyKey,
		ACLPropagationDelay: time.Duration(req.ACLPropagationDelayMS) * time.Millisecond,
	})
	if err != nil {
		writeCollectorPrincipalError(w, err, false)
		return
	}
	writeCollectorPrincipal(w, http.StatusCreated, principal)
}

func (api collectorPrincipalAPI) revokeWrite(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	collectorID, principalID := ID(r.PathValue("collector_id")), ID(r.PathValue("principal_id"))
	if !validCollectorEvidenceID(collectorID) || !validCollectorEvidenceID(principalID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id and principal_id are required", nil)
		return
	}
	if err := decodeCollectorPrincipalJSON(r, &struct{}{}, true); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	expectedVersion, err := parseCollectorPrincipalIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		WriteAPIError(w, http.StatusPreconditionRequired, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	principal, err := api.controller.RevokeWrite(r.Context(), auth.TenantID, collectorID, principalID, auth.UserID, expectedVersion)
	if err != nil {
		writeCollectorPrincipalError(w, err, true)
		return
	}
	writeCollectorPrincipal(w, http.StatusOK, principal)
}

func decodeCollectorPrincipalJSON(r *http.Request, target any, allowEmpty bool) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if allowEmpty && errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain one JSON object")
		}
		return err
	}
	return nil
}

func parseCollectorPrincipalIfMatch(value string) (uint64, error) {
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

func writeCollectorPrincipalError(w http.ResponseWriter, err error, revocation bool) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector or service principal not found", nil)
	case errors.Is(err, ErrCollectorPrincipalVersionConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorInvalidRequest, "Collector service principal version changed", nil)
	case errors.Is(err, ErrCollectorEvidenceConflict):
		message := "Idempotency-Key was already used for another principal grant"
		if revocation {
			message = "Collector service principal cannot be revoked from its current state"
		}
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, message, nil)
	case errors.Is(err, ErrCollectorPrincipalProviderNotConfigured):
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector principal provider is unavailable", nil)
	default:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector principal operation is unavailable", nil)
	}
}

func writeCollectorPrincipal(w http.ResponseWriter, status int, principal CollectorServicePrincipal) {
	w.Header().Set("ETag", `"`+strconv.FormatUint(principal.RowVersion, 10)+`"`)
	WriteAPIJSON(w, status, collectorPrincipalResponse{
		ID: principal.ID, TenantID: principal.TenantID, CollectorID: principal.CollectorID,
		ServiceType: principal.ServiceType, PrincipalRef: principal.PrincipalRef,
		Provider: principal.Provider, Status: principal.Status,
		ACLPropagationDelayMS: uint64(principal.ACLPropagationDelay / time.Millisecond), RowVersion: principal.RowVersion,
		CreatedAt: principal.CreatedAt, UpdatedAt: principal.UpdatedAt,
	})
}
