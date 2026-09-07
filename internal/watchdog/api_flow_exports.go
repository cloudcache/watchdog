package watchdog

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

type flowExportCreateRequest struct {
	Query            QueryRequest `json:"query"`
	Format           ExportFormat `json:"format"`
	RetentionSeconds uint32       `json:"retention_seconds,omitempty"`
}

func (api exportAPI) createFlow(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	input, err := decodeFlowExportCreateRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	access := exportAccessRequest(auth, ExportTask{DatasetKey: FlowTrafficDataset})
	if !queryLayerAuthorized(auth, input.Query.ValueLayer) ||
		!CanCreateExportLayer(access, input.Query.ValueLayer, auth.Grants, auth.IsAdmin) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Flow export permission denied", nil)
		return
	}
	task, err := prepareFlowExportExecutionTask(r.Context(), api.gateway, auth, input.Query, input.Format, input.RetentionSeconds)
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
	api.recordAudit(r.Context(), auth, "flow.export.created", created.ID, map[string]any{
		"dataset_key": created.DatasetKey, "value_layer": created.ValueLayer, "format": created.Format,
		"query_hash": created.QueryHash, "operation_job_id": created.OperationJobID,
	})
	WriteAPIJSON(w, http.StatusCreated, created)
}

func decodeFlowExportCreateRequest(r *http.Request) (flowExportCreateRequest, error) {
	defer r.Body.Close()
	var input flowExportCreateRequest
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
