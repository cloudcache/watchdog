package watchdog

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type agentRegistryAPI struct {
	repo AgentRepository
}

type agentRegistryResponse struct {
	ID           ID
	TenantID     ID
	TargetID     ID
	AgentType    AgentType
	Mode         AgentMode
	Endpoint     string
	Status       string
	LastSeen     time.Time
	LastRun      time.Time
	LastSuccess  time.Time
	LastError    string
	RunCount     uint64
	FailureCount uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type agentRunHistoryResponse struct {
	ID         ID
	TenantID   ID
	AgentID    ID
	TargetID   ID
	Status     AgentRunStatus
	Error      string
	Seen       bool
	StartedAt  time.Time
	EndedAt    time.Time
	DurationMS uint64
	CreatedAt  time.Time
}

type agentRegistryRequest struct {
	ID        ID
	TargetID  ID
	AgentType AgentType
	Mode      AgentMode
	Endpoint  string
	Token     string
	Status    string
}

type agentRegistryPatchRequest struct {
	ID        *ID
	TargetID  *ID
	AgentType *AgentType
	Mode      *AgentMode
	Endpoint  *string
	Token     *string
	Status    *string
}

func registerAgentRegistryRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo AgentRepository) {
	api := agentRegistryAPI{repo: repo}
	viewTenant := RequirePermission(ActionView, TenantResource)
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/agent-registry", auth(viewTenant(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/agent-registry", auth(configureTenant(http.HandlerFunc(api.create))))
	mux.Handle("GET /api/v1/agent-registry/{agent_id}", auth(viewTenant(http.HandlerFunc(api.get))))
	mux.Handle("GET /api/v1/agent-registry/{agent_id}/runs", auth(viewTenant(http.HandlerFunc(api.listRuns))))
	mux.Handle("PATCH /api/v1/agent-registry/{agent_id}", auth(configureTenant(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/agent-registry/{agent_id}", auth(configureTenant(http.HandlerFunc(api.delete))))
}

func (api agentRegistryAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	agents, err := api.repo.ListAgents(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	visible := make([]agentRegistryResponse, 0, len(agents))
	for _, agent := range agents {
		if canAccessTarget(auth, agent.TargetID, ActionView) {
			visible = append(visible, agentRegistryDTO(agent))
		}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": visible})
}

func (api agentRegistryAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	agent, err := api.repo.GetAgent(r.Context(), ID(r.PathValue("agent_id")))
	if err != nil || agent.TenantID != auth.TenantID {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Agent not found", nil)
		return
	}
	if !canAccessTarget(auth, agent.TargetID, ActionView) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, agentRegistryDTO(agent))
}

func (api agentRegistryAPI) listRuns(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	agent, err := api.repo.GetAgent(r.Context(), ID(r.PathValue("agent_id")))
	if err != nil || agent.TenantID != auth.TenantID {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Agent not found", nil)
		return
	}
	if !canAccessTarget(auth, agent.TargetID, ActionView) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	limit := 100
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be positive", nil)
			return
		}
		limit = parsed
	}
	runs, err := api.repo.ListAgentRuns(r.Context(), auth.TenantID, agent.ID, limit)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	items := make([]agentRunHistoryResponse, 0, len(runs))
	for _, run := range runs {
		items = append(items, agentRunHistoryResponse(run))
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api agentRegistryAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	req, err := decodeAgentRegistryRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if strings.TrimSpace(req.Token) == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "agent token is required", nil)
		return
	}
	if !canAccessTarget(auth, req.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	agent := NormalizeAgentConfig(SNMPAgentConfig{
		ID:        req.ID,
		TenantID:  auth.TenantID,
		TargetID:  req.TargetID,
		AgentType: req.AgentType,
		Mode:      req.Mode,
		Endpoint:  req.Endpoint,
		TokenHash: NewAgentTokenHash(req.Token),
		Status:    req.Status,
	})
	if err := ValidateAgentConfig(agent); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	created, err := api.repo.UpsertAgent(r.Context(), agent)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, agentRegistryDTO(created))
}

func (api agentRegistryAPI) patch(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	existing, err := api.repo.GetAgent(r.Context(), ID(r.PathValue("agent_id")))
	if err != nil || existing.TenantID != auth.TenantID {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Agent not found", nil)
		return
	}
	if !canAccessTarget(auth, existing.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	req, err := decodeAgentRegistryPatchRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if req.ID != nil && ID(strings.TrimSpace(string(*req.ID))) != existing.ID {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "agent id must match the request path", nil)
		return
	}
	targetID := existing.TargetID
	if req.TargetID != nil {
		targetID = ID(strings.TrimSpace(string(*req.TargetID)))
	}
	if targetID != existing.TargetID && !canAccessTarget(auth, targetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	tokenHash := existing.TokenHash
	if req.Token != nil && strings.TrimSpace(*req.Token) != "" {
		tokenHash = NewAgentTokenHash(*req.Token)
	}
	agentType := existing.AgentType
	if req.AgentType != nil {
		agentType = *req.AgentType
	}
	mode := existing.Mode
	if req.Mode != nil {
		mode = *req.Mode
	}
	endpoint := existing.Endpoint
	if req.Endpoint != nil {
		endpoint = *req.Endpoint
	}
	status := existing.Status
	if req.Status != nil {
		status = *req.Status
	}
	agent := NormalizeAgentConfig(SNMPAgentConfig{
		ID:        existing.ID,
		TenantID:  auth.TenantID,
		TargetID:  targetID,
		AgentType: agentType,
		Mode:      mode,
		Endpoint:  endpoint,
		TokenHash: tokenHash,
		Status:    status,
	})
	if err := ValidateAgentConfig(agent); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	updated, err := api.repo.UpsertAgent(r.Context(), agent)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, agentRegistryDTO(updated))
}

func (api agentRegistryAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	existing, err := api.repo.GetAgent(r.Context(), ID(r.PathValue("agent_id")))
	if err != nil || existing.TenantID != auth.TenantID {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Agent not found", nil)
		return
	}
	if !canAccessTarget(auth, existing.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if err := api.repo.DeleteAgent(r.Context(), auth.TenantID, existing.ID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeAgentRegistryRequest(r *http.Request) (agentRegistryRequest, error) {
	var req agentRegistryRequest
	if err := decodeStrictAgentJSON(r, &req); err != nil {
		return req, err
	}
	if req.ID == "" {
		return req, errors.New("agent id is required")
	}
	if req.TargetID == "" {
		return req, errors.New("target id is required")
	}
	normalized := NormalizeAgentConfig(SNMPAgentConfig{
		ID:        req.ID,
		TargetID:  req.TargetID,
		AgentType: req.AgentType,
		Mode:      req.Mode,
		Endpoint:  req.Endpoint,
		Status:    req.Status,
	})
	req.ID = normalized.ID
	req.TargetID = normalized.TargetID
	req.AgentType = normalized.AgentType
	req.Mode = normalized.Mode
	req.Endpoint = normalized.Endpoint
	req.Status = normalized.Status
	return req, nil
}

func decodeAgentRegistryPatchRequest(r *http.Request) (agentRegistryPatchRequest, error) {
	var req agentRegistryPatchRequest
	if err := decodeStrictAgentJSON(r, &req); err != nil {
		return req, err
	}
	return req, nil
}

func decodeStrictAgentJSON(r *http.Request, out any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
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

func agentRegistryDTO(agent SNMPAgentConfig) agentRegistryResponse {
	agent = NormalizeAgentConfig(agent)
	return agentRegistryResponse{
		ID:           agent.ID,
		TenantID:     agent.TenantID,
		TargetID:     agent.TargetID,
		AgentType:    agent.AgentType,
		Mode:         agent.Mode,
		Endpoint:     agent.Endpoint,
		Status:       agent.Status,
		LastSeen:     agent.LastSeen,
		LastRun:      agent.LastRun,
		LastSuccess:  agent.LastSuccess,
		LastError:    agent.LastError,
		RunCount:     agent.RunCount,
		FailureCount: agent.FailureCount,
		CreatedAt:    agent.CreatedAt,
		UpdatedAt:    agent.UpdatedAt,
	}
}
