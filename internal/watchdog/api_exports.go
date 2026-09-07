package watchdog

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var errExportPortTargetMismatch = errors.New("target_id does not match export port")

type exportAPI struct {
	repo    ExportRepository
	files   ExportFileReader
	network NetworkRepository
	audit   AuditRepository
	jobs    OperationJobRepository
	gateway *QueryGateway
	metric  string
	step    time.Duration
}

func (api exportAPI) recordAudit(ctx context.Context, auth AuthContext, action string, exportID ID, details map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: ResourceExportTask, ResourceID: exportID, Detail: details,
	})
}

func registerExportRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo ExportRepository, files ExportFileReader, network NetworkRepository, audit AuditRepository, jobs OperationJobRepository, gateway *QueryGateway, metric string, collectionStep time.Duration) {
	api := exportAPI{repo: repo, files: files, network: network, audit: audit, jobs: jobs, gateway: gateway, metric: metric, step: collectionStep}
	mux.Handle("POST /api/v1/exports", auth(http.HandlerFunc(api.create)))
	mux.Handle("POST /api/v1/flow/exports", auth(http.HandlerFunc(api.createFlow)))
	mux.Handle("GET /api/v1/exports", auth(http.HandlerFunc(api.list)))
	mux.Handle("GET /api/v1/exports/{export_id}", auth(http.HandlerFunc(api.get)))
	mux.Handle("POST /api/v1/exports/{export_id}/cancel", auth(http.HandlerFunc(api.cancel)))
	mux.Handle("POST /api/v1/exports/{export_id}/retry", auth(http.HandlerFunc(api.retry)))
	mux.Handle("GET /api/v1/exports/{export_id}/download", auth(http.HandlerFunc(api.download)))
	mux.Handle("DELETE /api/v1/exports/{export_id}", auth(http.HandlerFunc(api.delete)))
}

func (api exportAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	task, err := decodeExportTaskRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if task.ID == "" {
		var err error
		task.ID, err = newExportTaskID()
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
	}
	task.TenantID = auth.TenantID
	task.CreatedBy = auth.UserID
	task, err = api.resolveExportResource(r, auth, task)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	task, err = NormalizeExportRange(task, time.Now().UTC(), time.UTC)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	task = normalizeExportTask(task)
	if err := ValidateExportRequest(ExportRequestValidation{
		Task:    task,
		Access:  exportAccessRequest(auth, task),
		Grants:  auth.Grants,
		IsAdmin: auth.IsAdmin,
	}); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	task, err = prepareExportExecutionTask(r.Context(), api.gateway, api.network, auth, api.metric, api.step, task)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	created, err := api.repo.CreateExportTask(r.Context(), task)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	api.recordAudit(r.Context(), auth, "export.created", created.ID, map[string]any{
		"dataset_key": created.DatasetKey, "value_layer": created.ValueLayer, "format": created.Format,
		"query_hash": created.QueryHash, "operation_job_id": created.OperationJobID,
	})
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api exportAPI) resolveExportResource(r *http.Request, auth AuthContext, task ExportTask) (ExportTask, error) {
	if api.network == nil || task.PortID == "" {
		return task, nil
	}
	port, err := api.network.GetPort(r.Context(), auth.TenantID, task.PortID)
	if err != nil {
		return ExportTask{}, err
	}
	device, err := api.network.GetDevice(r.Context(), auth.TenantID, port.DeviceID)
	if err != nil {
		return ExportTask{}, err
	}
	if task.TargetID != "" && task.TargetID != device.TargetID {
		return ExportTask{}, errExportPortTargetMismatch
	}
	task.TargetID = device.TargetID
	return task, nil
}

func (api exportAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	createdBy := auth.UserID
	if auth.IsAdmin && r.URL.Query().Get("scope") == "tenant" {
		createdBy = ""
	}
	if pageRepo, ok := api.repo.(ExportPageRepository); ok {
		filter, err := parseExportTaskListFilter(r)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		filter.CreatedBy = createdBy
		tasks, total, err := pageRepo.ListExportTasksPage(r.Context(), auth.TenantID, filter)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		WriteAPIJSON(w, http.StatusOK, map[string]any{"items": tasks, "total": total, "limit": filter.Limit, "offset": filter.Offset})
		return
	}
	tasks, err := api.repo.ListExportTasks(r.Context(), auth.TenantID, createdBy)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": tasks})
}

func parseExportTaskListFilter(r *http.Request) (ExportTaskListFilter, error) {
	query := r.URL.Query()
	filter := ExportTaskListFilter{
		Search: strings.TrimSpace(query.Get("q")), Status: ExportStatus(query.Get("status")),
		ValueLayer: QueryValueLayer(query.Get("value_layer")), Format: ExportFormat(query.Get("format")),
		SortBy: strings.TrimSpace(query.Get("sort_by")), SortDirection: strings.TrimSpace(query.Get("sort_direction")),
	}
	var err error
	if filter.Limit, err = parseExportListInteger(query.Get("limit"), 25); err != nil {
		return ExportTaskListFilter{}, err
	}
	if filter.Offset, err = parseExportListInteger(query.Get("offset"), 0); err != nil {
		return ExportTaskListFilter{}, err
	}
	if err := validateExportTaskListFilter(&filter); err != nil {
		return ExportTaskListFilter{}, err
	}
	return filter, nil
}

func parseExportListInteger(raw string, defaultValue int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("export list limit and offset must be integers")
	}
	return value, nil
}

func (api exportAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	task, err := api.repo.GetExportTask(r.Context(), auth.TenantID, ID(r.PathValue("export_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export not found", nil)
		return
	}
	if !auth.IsAdmin && task.CreatedBy != auth.UserID {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, task)
}

func (api exportAPI) retry(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	task, err := api.repo.GetExportTask(r.Context(), auth.TenantID, ID(r.PathValue("export_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export not found", nil)
		return
	}
	if !api.authorizeCurrent(auth, task) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if task.Status != ExportStatusFailed && task.Status != ExportStatusCanceled {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Only failed or canceled exports can be retried", nil)
		return
	}
	if err := api.repo.RetryExportTask(r.Context(), auth.TenantID, task.ID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	task, err = api.repo.GetExportTask(r.Context(), auth.TenantID, task.ID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export not found", nil)
		return
	}
	api.recordAudit(r.Context(), auth, "export.retried", task.ID, map[string]any{"operation_job_id": task.OperationJobID})
	WriteAPIJSON(w, http.StatusOK, task)
}

func (api exportAPI) cancel(w http.ResponseWriter, r *http.Request) {
	if api.jobs == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Export cancellation is not configured", nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	task, err := api.repo.GetExportTask(r.Context(), auth.TenantID, ID(r.PathValue("export_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export not found", nil)
		return
	}
	if !api.authorizeCurrent(auth, task) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if task.ContractVersion != ExportExecutionContractVersion || task.OperationJobID == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Legacy exports cannot be canceled", nil)
		return
	}
	if err := api.jobs.RequestOperationJobCancel(r.Context(), auth.TenantID, task.OperationJobID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	api.recordAudit(r.Context(), auth, "export.cancel_requested", task.ID, map[string]any{"operation_job_id": task.OperationJobID})
	updated, err := api.repo.GetExportTask(r.Context(), auth.TenantID, task.ID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api exportAPI) delete(w http.ResponseWriter, r *http.Request) {
	if api.jobs == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Export deletion is not configured", nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	task, err := api.repo.GetExportTask(r.Context(), auth.TenantID, ID(r.PathValue("export_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export not found", nil)
		return
	}
	if !api.authorizeCurrent(auth, task) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if task.Status != ExportStatusComplete && task.Status != ExportStatusFailed && task.Status != ExportStatusCanceled {
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Cancel the export before deleting it", nil)
		return
	}
	payload, requestHash, err := EncodeExportDeletePayload(task)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	job, err := api.jobs.EnqueueOperationJob(r.Context(), OperationJob{
		TenantID: auth.TenantID, ScopeType: OperationJobScopeTenant, JobType: ExportDeleteJobType,
		IdempotencyKey: "export-delete:" + string(task.ID), RequestHash: requestHash,
		CheckpointJSON: payload, CreatedBy: auth.UserID,
	})
	if err != nil {
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	api.recordAudit(r.Context(), auth, "export.delete_requested", task.ID, map[string]any{"operation_job_id": job.ID})
	WriteAPIJSON(w, http.StatusAccepted, job)
}

func (api exportAPI) download(w http.ResponseWriter, r *http.Request) {
	if api.files == nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export file store is not configured", nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	task, err := api.repo.GetExportTask(r.Context(), auth.TenantID, ID(r.PathValue("export_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export not found", nil)
		return
	}
	if !api.authorizeCurrent(auth, task) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if task.FileRef == "" || task.Status != ExportStatusComplete {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Export file is not ready", nil)
		return
	}
	if !task.ExpiresAt.IsZero() && time.Now().After(task.ExpiresAt) {
		WriteAPIError(w, http.StatusGone, APIErrorCode("export_expired"), "Export file has expired", nil)
		return
	}
	data, contentType, err := api.files.ReadExport(r.Context(), task.FileRef)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export file not found", nil)
		return
	}
	checksum := sha256.Sum256(data)
	actualChecksum := hex.EncodeToString(checksum[:])
	if task.ContractVersion == ExportExecutionContractVersion {
		if task.SizeBytes != int64(len(data)) || task.Checksum == "" || !strings.EqualFold(task.Checksum, actualChecksum) {
			WriteAPIError(w, http.StatusConflict, APIErrorCode("export_integrity_failed"), "Export artifact integrity check failed", nil)
			return
		}
		if task.ArtifactSchemaVersion == 0 || task.ContentType == "" || (contentType != "" && contentType != task.ContentType) {
			WriteAPIError(w, http.StatusConflict, APIErrorCode("export_metadata_invalid"), "Export artifact metadata is invalid", nil)
			return
		}
		contentType = task.ContentType
	} else {
		if task.Checksum != "" && !strings.EqualFold(task.Checksum, actualChecksum) {
			WriteAPIError(w, http.StatusConflict, APIErrorCode("export_integrity_failed"), "Export artifact integrity check failed", nil)
			return
		}
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	// Record who downloaded which export (download audit trail).
	api.recordAudit(r.Context(), auth, "export.downloaded", task.ID, nil)
	w.Header().Set("Content-Type", contentType)
	extension := string(task.Format)
	if extension != string(ExportFormatCSV) && extension != string(ExportFormatParquet) {
		extension = "bin"
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.%s"`, task.ID, extension))
	if task.Checksum != "" {
		w.Header().Set("X-Checksum-SHA256", task.Checksum)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func decodeExportTaskRequest(r *http.Request) (ExportTask, error) {
	defer r.Body.Close()
	var task ExportTask
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&task); err != nil {
		return ExportTask{}, err
	}
	if err := ensureExportJSONEOF(decoder); err != nil {
		return ExportTask{}, err
	}
	return task, nil
}

func ensureExportJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain exactly one JSON object")
		}
		return err
	}
	return nil
}

func (api exportAPI) authorizeCurrent(auth AuthContext, task ExportTask) bool {
	if !auth.IsAdmin && task.CreatedBy != auth.UserID {
		return false
	}
	if task.ValueLayer == "" {
		task.ValueLayer = inferExportValueLayer(task)
	}
	return canExecuteExportTask(auth, task)
}

func newExportTaskID() (ID, error) {
	var data [8]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return ID("export_" + hex.EncodeToString(data[:])), nil
}

func exportAccessRequest(auth AuthContext, task ExportTask) AccessRequest {
	resource := ResourceRef{Type: ResourceTarget, ID: task.TargetID}
	if task.DatasetKey == FlowTrafficDataset || task.DatasetKey == FlowVPNFindingsDataset {
		resource = ResourceRef{Type: ResourceTenant, ID: auth.TenantID}
	} else if task.PortID != "" {
		resource = ResourceRef{Type: ResourcePort, ID: task.PortID, ParentID: task.TargetID}
	}
	return AccessRequest{
		TenantID: auth.TenantID,
		UserID:   auth.UserID,
		RoleIDs:  auth.RoleIDs,
		Action:   ActionExport,
		Resource: resource,
	}
}
