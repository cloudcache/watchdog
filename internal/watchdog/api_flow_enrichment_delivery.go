package watchdog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
)

type flowEnrichmentAckAPIRequest struct {
	State                  string `json:"state"`
	BootID                 string `json:"boot_id"`
	SoftwareVersion        string `json:"software_version"`
	DimensionSnapshotID    ID     `json:"dimension_snapshot_id"`
	DimensionVersion       uint64 `json:"dimension_version"`
	DimensionChecksum      string `json:"dimension_checksum"`
	ClassificationVersion  uint32 `json:"classification_version"`
	ClassificationChecksum string `json:"classification_checksum"`
	FailureStage           string `json:"failure_stage,omitempty"`
	FailureCode            string `json:"failure_code,omitempty"`
	FailureMessage         string `json:"failure_message,omitempty"`
}

type flowEnrichmentDeliveryAPI struct {
	controller FlowEnrichmentDeliveryController
}

func registerFlowEnrichmentDeliveryRoutes(mux *http.ServeMux, controller FlowEnrichmentDeliveryController) {
	api := flowEnrichmentDeliveryAPI{controller: controller}
	mux.HandleFunc("GET /api/v1/flow-workers/{worker_id}/enrichment-publications", api.fetchDesired)
	mux.HandleFunc("GET /api/v1/flow-workers/{worker_id}/enrichment-publications/{publication_id}/objects/{kind}", api.fetchObject)
	mux.HandleFunc("POST /api/v1/flow-workers/{worker_id}/enrichment-publications/{publication_id}/ack", api.acknowledge)
}

func (api flowEnrichmentDeliveryAPI) fetchDesired(w http.ResponseWriter, r *http.Request) {
	workerID, credential, ok := flowWorkerAPIIdentity(w, r)
	if !ok {
		return
	}
	afterVersion, limit, err := parseFlowEnrichmentDeliveryQuery(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	page, err := api.controller.FetchDesired(r.Context(), workerID, credential, afterVersion, limit)
	if err != nil {
		writeFlowEnrichmentDeliveryError(w, err)
		return
	}
	items := make([]json.RawMessage, 0, len(page.Items))
	for _, item := range page.Items {
		items = append(items, json.RawMessage(item.Envelope))
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Watchdog-Next-Classification-Version", strconv.FormatUint(uint64(page.NextVersion), 10))
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"items": items, "next_version": page.NextVersion, "has_more": page.HasMore,
	})
}

func (api flowEnrichmentDeliveryAPI) fetchObject(w http.ResponseWriter, r *http.Request) {
	workerID, credential, ok := flowWorkerAPIIdentity(w, r)
	if !ok {
		return
	}
	delivery, err := api.controller.ResolveObject(r.Context(), workerID, credential, ID(r.PathValue("publication_id")), r.PathValue("kind"))
	if err != nil {
		writeFlowEnrichmentDeliveryError(w, err)
		return
	}
	file, err := os.Open(delivery.Path)
	if err != nil {
		writeFlowEnrichmentDeliveryError(w, errors.Join(ErrFlowEnrichmentObjectMissing, err))
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != delivery.Size {
		writeFlowEnrichmentDeliveryError(w, errors.Join(ErrFlowEnrichmentObjectMissing, err))
		return
	}
	etag := fmt.Sprintf(`"%s"`, delivery.Checksum)
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	w.Header().Set("Content-Type", delivery.ContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Watchdog-Object-Checksum", delivery.Checksum)
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	http.ServeContent(w, r, flowEnrichmentObjectBaseName(delivery), info.ModTime(), file)
}

func (api flowEnrichmentDeliveryAPI) acknowledge(w http.ResponseWriter, r *http.Request) {
	workerID, credential, ok := flowWorkerAPIIdentity(w, r)
	if !ok {
		return
	}
	publicationID := ID(r.PathValue("publication_id"))
	if !validCollectorEvidenceID(publicationID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "publication_id is required", nil)
		return
	}
	var request flowEnrichmentAckAPIRequest
	if err := decodeCollectorEvidenceJSON(r, &request); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	report := FlowEnrichmentAcknowledgementReport{
		State: request.State, BootID: request.BootID, SoftwareVersion: request.SoftwareVersion,
		DimensionSnapshotID: request.DimensionSnapshotID, DimensionVersion: request.DimensionVersion,
		DimensionChecksum: request.DimensionChecksum, ClassificationVersion: request.ClassificationVersion,
		ClassificationChecksum: request.ClassificationChecksum, FailureStage: request.FailureStage,
		FailureCode: request.FailureCode, FailureMessage: request.FailureMessage,
	}
	if err := validateFlowEnrichmentAcknowledgementReport(report); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.controller.Acknowledge(r.Context(), workerID, credential, publicationID, report); err != nil {
		writeFlowEnrichmentDeliveryError(w, err)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func flowWorkerAPIIdentity(w http.ResponseWriter, r *http.Request) (ID, CollectorMachineCredential, bool) {
	workerID := ID(r.PathValue("worker_id"))
	if !validCollectorEvidenceID(workerID) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "worker_id is required", nil)
		return "", CollectorMachineCredential{}, false
	}
	credential, err := collectorMachineCredentialFromRequest(r)
	if err != nil {
		writeFlowEnrichmentDeliveryError(w, err)
		return "", CollectorMachineCredential{}, false
	}
	return workerID, credential, true
}

func parseFlowEnrichmentDeliveryQuery(r *http.Request) (uint32, int, error) {
	values := r.URL.Query()
	for key, entries := range values {
		if (key != "after_version" && key != "limit") || len(entries) != 1 {
			return 0, 0, ErrFlowEnrichmentDeliveryInvalid
		}
	}
	var afterVersion uint64
	var err error
	if value := values.Get("after_version"); value != "" {
		afterVersion, err = strconv.ParseUint(value, 10, 32)
		if err != nil {
			return 0, 0, ErrFlowEnrichmentDeliveryInvalid
		}
	}
	limit := 20
	if value := values.Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > flowEnrichmentPublicationPageLimit {
			return 0, 0, ErrFlowEnrichmentDeliveryInvalid
		}
	}
	return uint32(afterVersion), limit, nil
}

func writeFlowEnrichmentDeliveryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCollectorMachineUnauthorized):
		w.Header().Set("WWW-Authenticate", "Watchdog-Flow-Worker")
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Flow worker authentication failed", nil)
	case errors.Is(err, ErrFlowEnrichmentDeliveryInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Flow enrichment delivery request is invalid", nil)
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Flow enrichment publication was not found", nil)
	case errors.Is(err, ErrFlowEnrichmentAckConflict):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Flow enrichment acknowledgement does not match the publication", nil)
	case errors.Is(err, ErrFlowEnrichmentObjectMissing):
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Flow enrichment object is unavailable", nil)
	default:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Flow enrichment delivery service is unavailable", nil)
	}
}
