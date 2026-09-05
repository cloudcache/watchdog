package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
)

type collectorPlanAckAPIRequest struct {
	ConfigVersion   uint64 `json:"config_version"`
	SpecHash        string `json:"spec_hash"`
	BootID          string `json:"boot_id"`
	SoftwareVersion string `json:"software_version"`
}

type collectorPlanFailureAPIRequest struct {
	FailedConfigVersion uint64 `json:"failed_config_version"`
	BootID              string `json:"boot_id"`
	SoftwareVersion     string `json:"software_version"`
	Stage               string `json:"stage"`
	Code                string `json:"code"`
	Detail              string `json:"detail"`
}

type collectorHeartbeatAPIRequest struct {
	SchemaVersion uint16                           `json:"schema_version"`
	Kind          string                           `json:"kind"`
	Runtime       *CollectorRuntimeHeartbeatReport `json:"runtime,omitempty"`
	PlanFailure   *collectorPlanFailureAPIRequest  `json:"plan_failure,omitempty"`
}

type collectorPlanDeliveryAPI struct {
	controller CollectorPlanDeliveryController
}

func registerCollectorPlanDeliveryRoutes(mux *http.ServeMux, controller CollectorPlanDeliveryController) {
	api := collectorPlanDeliveryAPI{controller: controller}
	mux.HandleFunc("GET /api/v1/collectors/{collector_id}/plan", api.fetch)
	mux.HandleFunc("POST /api/v1/collectors/{collector_id}/plan-ack", api.acknowledge)
	mux.HandleFunc("POST /api/v1/collectors/{collector_id}/heartbeat", api.heartbeat)
}

func (api collectorPlanDeliveryAPI) fetch(w http.ResponseWriter, r *http.Request) {
	collectorID := ID(r.PathValue("collector_id"))
	if !validCollectorEvidenceID(collectorID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id is required", nil)
		return
	}
	credential, err := collectorMachineCredentialFromRequest(r)
	if err != nil {
		writeCollectorPlanDeliveryError(w, err)
		return
	}
	delivery, err := api.controller.Fetch(r.Context(), collectorID, credential)
	if err != nil {
		writeCollectorPlanDeliveryError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", delivery.ETag)
	w.Header().Set("X-Watchdog-Plan-Version", strconv.FormatUint(delivery.ConfigVersion, 10))
	w.Header().Set("X-Watchdog-Plan-Expires", delivery.ExpiresAt.UTC().Format(http.TimeFormat))
	if r.Header.Get("If-None-Match") == delivery.ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(delivery.Envelope)
}

func (api collectorPlanDeliveryAPI) acknowledge(w http.ResponseWriter, r *http.Request) {
	collectorID := ID(r.PathValue("collector_id"))
	credential, ok := collectorPlanAPIIdentity(w, r, collectorID)
	if !ok {
		return
	}
	var req collectorPlanAckAPIRequest
	if err := decodeCollectorEvidenceJSON(r, &req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	acknowledgement := CollectorPlanAcknowledgement{
		ConfigVersion: req.ConfigVersion, SpecHash: req.SpecHash,
		BootID: req.BootID, SoftwareVersion: req.SoftwareVersion,
	}
	if err := validateCollectorPlanAcknowledgementReport(acknowledgement); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.controller.Acknowledge(r.Context(), collectorID, credential, acknowledgement); err != nil {
		writeCollectorPlanDeliveryError(w, err)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (api collectorPlanDeliveryAPI) heartbeat(w http.ResponseWriter, r *http.Request) {
	collectorID := ID(r.PathValue("collector_id"))
	credential, ok := collectorPlanAPIIdentity(w, r, collectorID)
	if !ok {
		return
	}
	var req collectorHeartbeatAPIRequest
	if err := decodeCollectorEvidenceJSON(r, &req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if req.SchemaVersion != CollectorHeartbeatEnvelopeSchemaVersion {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "heartbeat envelope schema is unsupported", nil)
		return
	}
	switch req.Kind {
	case "runtime":
		if req.Runtime == nil || req.PlanFailure != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "runtime heartbeat payload is required", nil)
			return
		}
		if err := validateCollectorRuntimeHeartbeatReport(*req.Runtime); err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		if err := api.controller.ReportRuntime(r.Context(), collectorID, credential, *req.Runtime); err != nil {
			writeCollectorPlanDeliveryError(w, err)
			return
		}
	case "plan_failure":
		if req.PlanFailure == nil || req.Runtime != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "plan failure heartbeat payload is required", nil)
			return
		}
		if err := api.reportFailure(r, collectorID, credential, *req.PlanFailure); err != nil {
			if errors.Is(err, errCollectorHeartbeatInvalid) {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			} else {
				writeCollectorPlanDeliveryError(w, err)
			}
			return
		}
	default:
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "heartbeat kind is unsupported", nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

var errCollectorHeartbeatInvalid = errors.New("collector heartbeat payload is invalid")

func (api collectorPlanDeliveryAPI) reportFailure(r *http.Request, collectorID ID, credential CollectorMachineCredential, req collectorPlanFailureAPIRequest) error {
	report := CollectorPlanFailureReport{
		FailedConfigVersion: req.FailedConfigVersion, BootID: req.BootID,
		SoftwareVersion: req.SoftwareVersion, Stage: req.Stage, Code: req.Code, Detail: req.Detail,
	}
	if err := validateCollectorPlanFailureReport(report); err != nil {
		return errors.Join(errCollectorHeartbeatInvalid, err)
	}
	return api.controller.ReportFailure(r.Context(), collectorID, credential, report)
}

func collectorPlanAPIIdentity(w http.ResponseWriter, r *http.Request, collectorID ID) (CollectorMachineCredential, bool) {
	if !validCollectorEvidenceID(collectorID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id is required", nil)
		return CollectorMachineCredential{}, false
	}
	credential, err := collectorMachineCredentialFromRequest(r)
	if err != nil {
		writeCollectorPlanDeliveryError(w, err)
		return CollectorMachineCredential{}, false
	}
	return credential, true
}

func writeCollectorPlanDeliveryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCollectorMachineUnauthorized):
		w.Header().Set("WWW-Authenticate", "Watchdog-Collector")
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Collector authentication failed", nil)
	case errors.Is(err, ErrCollectorPlanUnavailable), errors.Is(err, ErrCollectorPlanInvalidTransition), errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector active plan is unavailable or no longer applicable", nil)
	case errors.Is(err, ErrCollectorHeartbeatConflict):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector heartbeat is stale or conflicts with stored runtime state", nil)
	case errors.Is(err, ErrCollectorHeartbeatFenced):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorInvalidRequest, "Collector process incarnation is fenced", nil)
	default:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector plan service is unavailable", nil)
	}
}
