package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// PLAT-03C2 enrollment API. Admin routes mint and manage single-use secrets;
// the machine route is unauthenticated (possession of an unexpired secret is
// the credential) and answers every failure identically so callers cannot
// probe which part was wrong.

type collectorEnrollmentAPI struct {
	repo  CollectorEnrollmentRepository
	audit AuditRepository
}

func registerCollectorEnrollmentRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo CollectorEnrollmentRepository, audit AuditRepository) {
	api := collectorEnrollmentAPI{repo: repo, audit: audit}
	operateTenant := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/collector-enrollment-secrets", auth(operateTenant(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/collector-enrollment-secrets", auth(operateTenant(http.HandlerFunc(api.create))))
	mux.Handle("DELETE /api/v1/collector-enrollment-secrets/{secret_id}", auth(operateTenant(http.HandlerFunc(api.revoke))))
	mux.Handle("POST /api/v1/collectors/enroll", http.HandlerFunc(api.enroll))
}

func (api collectorEnrollmentAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	secrets, err := api.repo.ListEnrollmentSecrets(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": secrets})
}

type enrollmentSecretRequest struct {
	CollectorName string `json:"collector_name"`
	ModuleKey     string `json:"module_key"`
	AgentType     string `json:"agent_type"`
	Mode          string `json:"mode"`
	TTLSeconds    uint64 `json:"ttl_seconds"`
}

func (api collectorEnrollmentAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var req enrollmentSecretRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	req.CollectorName = strings.TrimSpace(req.CollectorName)
	req.ModuleKey = strings.TrimSpace(strings.ToLower(req.ModuleKey))
	req.AgentType = strings.TrimSpace(strings.ToLower(req.AgentType))
	if req.Mode == "" {
		req.Mode = "listen"
	}
	if req.CollectorName == "" || req.ModuleKey == "" || req.AgentType == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_name, module_key and agent_type are required", nil)
		return
	}
	if req.Mode != "push" && req.Mode != "pull" && req.Mode != "listen" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "mode must be push, pull or listen", nil)
		return
	}
	ttl := enrollmentSecretDefaultTTL
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
		if ttl > enrollmentSecretMaxTTL {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "ttl_seconds exceeds the 24-hour enrollment limit", nil)
			return
		}
	}
	secretValue, err := NewCollectorToken()
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "failed to generate secret", nil)
		return
	}
	secretValue = "wde_" + strings.TrimPrefix(secretValue, "wdc_")
	secretHash := NewAgentTokenHash(secretValue)
	if secretHash == "" {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "failed to hash secret", nil)
		return
	}
	created, err := api.repo.CreateEnrollmentSecret(r.Context(), CollectorEnrollmentSecret{
		TenantID:      auth.TenantID,
		ModuleKey:     req.ModuleKey,
		AgentType:     req.AgentType,
		Mode:          req.Mode,
		CollectorName: req.CollectorName,
		ExpiresAt:     time.Now().UTC().Add(ttl),
		CreatedBy:     auth.UserID,
	}, secretHash)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	created.Secret = secretValue
	api.recordAudit(r.Context(), auth, "collector.enrollment_secret.create", created.ID, map[string]any{
		"collector_name": created.CollectorName, "module_key": created.ModuleKey, "expires_at": created.ExpiresAt,
	})
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api collectorEnrollmentAPI) revoke(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	secretID := ID(r.PathValue("secret_id"))
	if err := api.repo.DeleteEnrollmentSecret(r.Context(), auth.TenantID, secretID); err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Enrollment secret not found or already used", nil)
		return
	}
	api.recordAudit(r.Context(), auth, "collector.enrollment_secret.revoke", secretID, nil)
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

type enrollRequest struct {
	SecretID    ID                              `json:"secret_id"`
	Secret      string                          `json:"secret"`
	Declaration *CollectorEnrollmentDeclaration `json:"declaration"`
}

func (api collectorEnrollmentAPI) enroll(w http.ResponseWriter, r *http.Request) {
	var req enrollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<14)).Decode(&req); err != nil {
		writeEnrollmentRejected(w)
		return
	}
	req.Secret = strings.TrimSpace(req.Secret)
	if req.SecretID == "" || len(req.SecretID) > 26 || req.Secret == "" || len(req.Secret) > 256 {
		writeEnrollmentRejected(w)
		return
	}
	// Declaration shape is validated before the secret is consulted, so a 400
	// here reveals nothing about the secret and a bad declaration cannot burn
	// a valid secret.
	if req.Declaration != nil {
		if err := validateCollectorEnrollmentDeclaration(*req.Declaration); err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
	}
	token, err := NewCollectorToken()
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "enrollment failed", nil)
		return
	}
	tokenHash := NewAgentTokenHash(token)
	result, err := api.repo.ConsumeEnrollmentSecret(r.Context(), req.SecretID, func(secretHash string) bool {
		return AgentTokenMatches(req.Secret, secretHash)
	}, tokenHash, req.Declaration)
	if err != nil {
		if errors.Is(err, ErrEnrollmentSecretInvalid) {
			writeEnrollmentRejected(w)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "enrollment failed", nil)
		return
	}
	result.Token = token
	api.recordEnrollAudit(r.Context(), result)
	WriteAPIJSON(w, http.StatusCreated, result)
}

func writeEnrollmentRejected(w http.ResponseWriter) {
	WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Enrollment rejected", nil)
}

func (api collectorEnrollmentAPI) recordAudit(ctx context.Context, auth AuthContext, action string, secretID ID, details map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: "collector", ResourceID: secretID, Detail: details,
	})
}

func (api collectorEnrollmentAPI) recordEnrollAudit(ctx context.Context, result CollectorEnrollmentResult) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: result.TenantID, ActorID: "system:enrollment", Action: "collector.enroll",
		ResourceType: "collector", ResourceID: result.CollectorID,
		Detail: map[string]any{"module_key": result.ModuleKey, "agent_type": result.AgentType},
	})
}
