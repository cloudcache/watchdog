package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

// PLAT-03C2 credential rotation API. All routes require the tenant operate
// action plus If-Match with the collector's current quoted row_version, so a
// concurrent management write turns into a 412 instead of a lost update.
// Rotation returns the new cleartext token exactly once; only hashes persist.

type collectorCredentialAPI struct {
	repo  CollectorCredentialRepository
	audit AuditRepository
}

func registerCollectorCredentialRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo CollectorCredentialRepository, audit AuditRepository) {
	api := collectorCredentialAPI{repo: repo, audit: audit}
	operateTenant := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/collectors/{collector_id}/credentials", auth(operateTenant(http.HandlerFunc(api.get))))
	mux.Handle("POST /api/v1/collectors/{collector_id}/credentials/actions/rotate", auth(operateTenant(http.HandlerFunc(api.rotate))))
	mux.Handle("POST /api/v1/collectors/{collector_id}/credentials/actions/commit", auth(operateTenant(http.HandlerFunc(api.commit))))
	mux.Handle("POST /api/v1/collectors/{collector_id}/credentials/actions/abort", auth(operateTenant(http.HandlerFunc(api.abort))))
	mux.Handle("POST /api/v1/collectors/{collector_id}/credentials/actions/revoke", auth(operateTenant(http.HandlerFunc(api.revoke))))
}

func (api collectorCredentialAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	state, err := api.repo.GetCollectorCredentialState(r.Context(), auth.TenantID, ID(r.PathValue("collector_id")))
	if err != nil {
		writeCollectorCredentialError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(state.RowVersion))
	response := map[string]any{
		"collector_id": state.CollectorID,
		"auth_type":    state.AuthType,
		"status":       state.Status,
		"has_pending":  state.HasPending,
		"row_version":  state.RowVersion,
	}
	if state.HasPending {
		response["pending_expires_at"] = state.ExpiresAt
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

type collectorRotateRequest struct {
	TTLSeconds             uint64 `json:"ttl_seconds"`
	CertificateFingerprint string `json:"certificate_fingerprint"`
}

func (api collectorCredentialAPI) rotate(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	collectorID := ID(r.PathValue("collector_id"))
	expectedVersion, err := parseCollectorPrincipalIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	var req collectorRotateRequest
	if body, readErr := io.ReadAll(io.LimitReader(r.Body, 1<<16)); readErr == nil && len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid JSON body", nil)
			return
		}
	}
	ttl := collectorRotationDefaultTTL
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
		if ttl > collectorRotationMaxTTL {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "ttl_seconds exceeds the 7-day rotation window limit", nil)
			return
		}
	}
	state, err := api.repo.GetCollectorCredentialState(r.Context(), auth.TenantID, collectorID)
	if err != nil {
		writeCollectorCredentialError(w, err)
		return
	}

	rotation := CollectorCredentialRotation{CollectorID: collectorID, AuthType: state.AuthType}
	var pendingTokenHash, pendingFingerprint string
	switch state.AuthType {
	case "token":
		if req.CertificateFingerprint != "" {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector authenticates with a token; certificate_fingerprint is not accepted", nil)
			return
		}
		token, err := NewCollectorToken()
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "failed to generate token", nil)
			return
		}
		rotation.Token = token
		pendingTokenHash = NewAgentTokenHash(token)
		if pendingTokenHash == "" {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "failed to hash token", nil)
			return
		}
	case "mtls":
		if !validCollectorCertificateFingerprint(req.CertificateFingerprint) {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, `certificate_fingerprint must be "sha256:" plus 64 lowercase hex characters`, nil)
			return
		}
		pendingFingerprint = req.CertificateFingerprint
	default:
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "collector auth type does not support rotation", nil)
		return
	}

	expiresAt := time.Now().UTC().Add(ttl)
	if err := api.repo.StageCollectorCredential(r.Context(), auth.TenantID, collectorID, expectedVersion, pendingTokenHash, pendingFingerprint, expiresAt, auth.UserID); err != nil {
		writeCollectorCredentialError(w, err)
		return
	}
	rotation.ExpiresAt = expiresAt
	rotation.RowVersion = expectedVersion + 1
	api.recordAudit(r.Context(), auth, "collector.credentials.rotate", collectorID, map[string]any{
		"auth_type": state.AuthType, "expires_at": expiresAt,
	})
	w.Header().Set("ETag", quotedRowVersion(rotation.RowVersion))
	WriteAPIJSON(w, http.StatusOK, rotation)
}

func (api collectorCredentialAPI) commit(w http.ResponseWriter, r *http.Request) {
	api.transition(w, r, "collector.credentials.commit", api.repo.CommitCollectorCredential)
}

func (api collectorCredentialAPI) abort(w http.ResponseWriter, r *http.Request) {
	api.transition(w, r, "collector.credentials.abort", api.repo.AbortCollectorCredential)
}

func (api collectorCredentialAPI) revoke(w http.ResponseWriter, r *http.Request) {
	api.transition(w, r, "collector.credentials.revoke", api.repo.RevokeCollectorCredential)
}

func (api collectorCredentialAPI) transition(w http.ResponseWriter, r *http.Request, action string, apply func(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, actor ID) error) {
	auth, _ := AuthFromContext(r.Context())
	collectorID := ID(r.PathValue("collector_id"))
	expectedVersion, err := parseCollectorPrincipalIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := apply(r.Context(), auth.TenantID, collectorID, expectedVersion, auth.UserID); err != nil {
		writeCollectorCredentialError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, action, collectorID, nil)
	w.Header().Set("ETag", quotedRowVersion(expectedVersion+1))
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true, "row_version": expectedVersion + 1})
}

func (api collectorCredentialAPI) recordAudit(ctx context.Context, auth AuthContext, action string, collectorID ID, details map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: "collector", ResourceID: collectorID, Detail: details,
	})
}

func quotedRowVersion(version uint64) string {
	return `"` + strconv.FormatUint(version, 10) + `"`
}

func writeCollectorCredentialError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector not found", nil)
	case errors.Is(err, ErrCollectorNoPendingRotation):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector has no unexpired pending credential", nil)
	case errors.Is(err, ErrCollectorCredentialConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), "Collector changed since it was read; re-read and retry", nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
	}
}
