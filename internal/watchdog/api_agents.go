package watchdog

import (
	"encoding/json"
	"net/http"
)

type agentAPI struct {
	plans AgentPlanService
}

func registerAgentRoutes(mux *http.ServeMux, plans AgentPlanService) {
	api := agentAPI{plans: plans}
	mux.HandleFunc("POST /api/v1/agents/{agent_id}/heartbeat", api.heartbeat)
	mux.HandleFunc("POST /api/v1/agents/{agent_id}/status", api.status)
	mux.HandleFunc("POST /api/v1/agents/{agent_id}/errors", api.errors)
	mux.HandleFunc("GET /api/v1/agents/{agent_id}/system/plan", api.systemPlan)
	mux.HandleFunc("POST /api/v1/agents/{agent_id}/system/samples", api.systemPush)
	mux.HandleFunc("GET /api/v1/system-agents/{agent_id}/plan", api.systemPlan)
	mux.HandleFunc("POST /api/v1/system-agents/{agent_id}/samples", api.systemPush)
}

func (api agentAPI) systemPlan(w http.ResponseWriter, r *http.Request) {
	token := agentTokenFromRequest(r)
	if token == "" {
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Agent token is required", nil)
		return
	}
	plan, err := api.plans.BuildSystemAgentPlan(r.Context(), ID(r.PathValue("agent_id")), token)
	if err != nil {
		status := http.StatusBadRequest
		code := APIErrorInvalidRequest
		if err.Error() == "invalid agent token" {
			status = http.StatusUnauthorized
			code = APIErrorUnauthorized
		}
		WriteAPIError(w, status, code, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, plan)
}

func (api agentAPI) heartbeat(w http.ResponseWriter, r *http.Request) {
	token := agentTokenFromRequest(r)
	if token == "" {
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Agent token is required", nil)
		return
	}
	if err := api.plans.MarkHeartbeat(r.Context(), ID(r.PathValue("agent_id")), token); err != nil {
		status := http.StatusBadRequest
		code := APIErrorInvalidRequest
		if err.Error() == "invalid agent token" {
			status = http.StatusUnauthorized
			code = APIErrorUnauthorized
		}
		WriteAPIError(w, status, code, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (api agentAPI) status(w http.ResponseWriter, r *http.Request) {
	token := agentTokenFromRequest(r)
	if token == "" {
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Agent token is required", nil)
		return
	}
	defer r.Body.Close()
	var report AgentRunReport
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.plans.ReportStatus(r.Context(), ID(r.PathValue("agent_id")), token, report); err != nil {
		status := http.StatusBadRequest
		code := APIErrorInvalidRequest
		if err.Error() == "invalid agent token" {
			status = http.StatusUnauthorized
			code = APIErrorUnauthorized
		}
		WriteAPIError(w, status, code, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (api agentAPI) errors(w http.ResponseWriter, r *http.Request) {
	token := agentTokenFromRequest(r)
	if token == "" {
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Agent token is required", nil)
		return
	}
	defer r.Body.Close()
	var report AgentRunReport
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	report.Status = AgentRunFailure
	if err := api.plans.ReportStatus(r.Context(), ID(r.PathValue("agent_id")), token, report); err != nil {
		status := http.StatusBadRequest
		code := APIErrorInvalidRequest
		if err.Error() == "invalid agent token" {
			status = http.StatusUnauthorized
			code = APIErrorUnauthorized
		}
		WriteAPIError(w, status, code, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (api agentAPI) systemPush(w http.ResponseWriter, r *http.Request) {
	token := agentTokenFromRequest(r)
	if token == "" {
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Agent token is required", nil)
		return
	}
	defer r.Body.Close()
	var batch SystemSampleBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.plans.ImportSystemPush(r.Context(), ID(r.PathValue("agent_id")), token, batch); err != nil {
		status := http.StatusBadRequest
		code := APIErrorInvalidRequest
		if err.Error() == "invalid agent token" {
			status = http.StatusUnauthorized
			code = APIErrorUnauthorized
		}
		WriteAPIError(w, status, code, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func agentTokenFromRequest(r *http.Request) string {
	if token := r.Header.Get("X-Watchdog-Agent-Token"); token != "" {
		return token
	}
	const prefix = "Bearer "
	value := r.Header.Get("Authorization")
	if len(value) > len(prefix) && value[:len(prefix)] == prefix {
		return value[len(prefix):]
	}
	return ""
}
