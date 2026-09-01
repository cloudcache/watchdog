package watchdog

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

var errExportPortTargetMismatch = errors.New("target_id does not match export port")

type exportAPI struct {
	repo    ExportRepository
	files   ExportFileReader
	network NetworkRepository
}

func registerExportRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo ExportRepository, files ExportFileReader, network NetworkRepository) {
	api := exportAPI{repo: repo, files: files, network: network}
	mux.Handle("POST /api/v1/exports", auth(http.HandlerFunc(api.create)))
	mux.Handle("GET /api/v1/exports", auth(http.HandlerFunc(api.list)))
	mux.Handle("GET /api/v1/exports/{export_id}", auth(http.HandlerFunc(api.get)))
	mux.Handle("POST /api/v1/exports/{export_id}/retry", auth(http.HandlerFunc(api.retry)))
	mux.Handle("GET /api/v1/exports/{export_id}/download", auth(http.HandlerFunc(api.download)))
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
	if err := ValidateExportRequest(ExportRequestValidation{
		Task:    task,
		Access:  exportAccessRequest(auth, task),
		Grants:  auth.Grants,
		IsAdmin: auth.IsAdmin,
	}); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	created, err := api.repo.CreateExportTask(r.Context(), task)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
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
	tasks, err := api.repo.ListExportTasks(r.Context(), auth.TenantID, createdBy)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": tasks})
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
	if !auth.IsAdmin && task.CreatedBy != auth.UserID {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if task.Status != ExportStatusFailed {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Only failed exports can be retried", nil)
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
	WriteAPIJSON(w, http.StatusOK, task)
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
	if !auth.IsAdmin && task.CreatedBy != auth.UserID {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if task.FileRef == "" || task.Status != ExportStatusComplete {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Export file is not ready", nil)
		return
	}
	data, contentType, err := api.files.ReadExport(r.Context(), task.FileRef)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Export file not found", nil)
		return
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", `attachment; filename="`+string(task.ID)+`.csv"`)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func decodeExportTaskRequest(r *http.Request) (ExportTask, error) {
	defer r.Body.Close()
	var task ExportTask
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		return ExportTask{}, err
	}
	return task, nil
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
	if task.PortID != "" {
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
