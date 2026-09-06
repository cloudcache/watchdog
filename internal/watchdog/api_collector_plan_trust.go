package watchdog

import (
	"errors"
	"net/http"
	"strconv"
)

type collectorPlanTrustAPI struct {
	controller CollectorPlanTrustBundleController
}

func registerCollectorPlanTrustRoutes(mux *http.ServeMux, controller CollectorPlanTrustBundleController) {
	api := collectorPlanTrustAPI{controller: controller}
	mux.HandleFunc("GET /api/v1/collectors/{collector_id}/trust-bundle", api.fetch)
}

func (api collectorPlanTrustAPI) fetch(w http.ResponseWriter, r *http.Request) {
	collectorID := ID(r.PathValue("collector_id"))
	if !validCollectorEvidenceID(collectorID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id is required", nil)
		return
	}
	credential, err := collectorMachineCredentialFromRequest(r)
	if err != nil {
		writeCollectorPlanTrustError(w, err)
		return
	}
	delivery, err := api.controller.FetchTrustBundle(r.Context(), collectorID, credential)
	if err != nil {
		writeCollectorPlanTrustError(w, err)
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

func writeCollectorPlanTrustError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrCollectorMachineUnauthorized) {
		w.Header().Set("WWW-Authenticate", "Watchdog-Collector")
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Collector authentication failed", nil)
		return
	}
	WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector plan trust bundle is unavailable", nil)
}
