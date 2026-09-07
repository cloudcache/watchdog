package watchdog

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

func (api exportAPI) createFlowDetail(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	input, err := decodeFlowDetailExportCreateRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	layer := QueryValueLayer(input.Query.View)
	access := exportAccessRequest(auth, ExportTask{DatasetKey: FlowRecordDetailDataset, ValueLayer: layer})
	if (layer == QueryValueRaw || layer == QueryValueSupplier) &&
		(!queryLayerAuthorized(auth, layer) || !CanCreateExportLayer(access, layer, auth.Grants, auth.IsAdmin)) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Flow detail view and export permissions are required", nil)
		return
	}
	targetIDs, deviceIDs, exporterIDs := detailResourceFilters(input.Query.Filters, input.Query.ColumnFilters)
	if err := authorizeFlowResourceFilters(r.Context(), auth, api.network, targetIDs, deviceIDs, exporterIDs); err != nil {
		writeQueryGatewayError(w, err)
		return
	}
	now := time.Now().UTC()
	if api.now != nil {
		now = api.now().UTC()
	}
	task, err := prepareFlowDetailExportTask(r.Context(), api.gateway, auth, input, now)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	task.ID, err = newExportTaskID()
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	created, err := api.repo.CreateExportTask(r.Context(), task)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	api.recordAudit(r.Context(), auth, "flow.records.export.created", created.ID, map[string]any{
		"dataset_key": created.DatasetKey, "value_layer": created.ValueLayer, "format": created.Format,
		"query_hash": created.QueryHash, "operation_job_id": created.OperationJobID,
	})
	WriteAPIJSON(w, http.StatusCreated, created)
}

func decodeFlowDetailExportCreateRequest(r *http.Request) (flowDetailExportCreateRequest, error) {
	defer r.Body.Close()
	var input flowDetailExportCreateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return input, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return input, errors.New("request must contain exactly one JSON object")
		}
		return input, err
	}
	return input, nil
}
