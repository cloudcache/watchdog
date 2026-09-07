package watchdog

import (
	"errors"
	"net/http"
	"strconv"
)

type collectorPlanTrustAPI struct {
	controller   CollectorPlanTrustBundleController
	principal    string
	authenticate string
	idParameter  string
}

func registerCollectorPlanTrustRoutes(mux *http.ServeMux, controller CollectorPlanTrustBundleController) {
	api := collectorPlanTrustAPI{controller: controller, principal: "Collector", authenticate: "Watchdog-Collector", idParameter: "collector_id"}
	mux.HandleFunc("GET /api/v1/collectors/{collector_id}/trust-bundle", api.fetch)
}

func registerFlowWorkerTrustRoutes(mux *http.ServeMux, controller CollectorPlanTrustBundleController) {
	api := collectorPlanTrustAPI{controller: controller, principal: "Flow worker", authenticate: "Watchdog-Flow-Worker", idParameter: "worker_id"}
	mux.HandleFunc("GET /api/v1/flow-workers/{worker_id}/trust-bundle", api.fetch)
}

func (api collectorPlanTrustAPI) fetch(w http.ResponseWriter, r *http.Request) {
	collectorID := ID(r.PathValue(api.idParameter))
	if !validCollectorEvidenceID(collectorID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, api.idParameter+" is required", nil)
		return
	}
	credential, err := collectorMachineCredentialFromRequest(r)
	if err != nil {
		api.writeError(w, err)
		return
	}
	delivery, err := api.controller.FetchTrustBundle(r.Context(), collectorID, credential)
	if err != nil {
		api.writeError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", delivery.ETag)
	w.Header().Set("X-Watchdog-Trust-Generation", strconv.FormatUint(delivery.Generation, 10))
	w.Header().Set("X-Watchdog-Trust-Checksum", delivery.Checksum)
	if r.Header.Get("If-None-Match") == delivery.ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(delivery.Payload)
}

func (api collectorPlanTrustAPI) writeError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrCollectorMachineUnauthorized) {
		w.Header().Set("WWW-Authenticate", api.authenticate)
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, api.principal+" authentication failed", nil)
		return
	}
	WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, api.principal+" trust bundle is unavailable", nil)
}
