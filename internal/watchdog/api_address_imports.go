package watchdog

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

type addressImportAPI struct {
	repo           AddressImportRepository
	artifacts      AddressArtifactStore
	jobs           OperationJobRepository
	maxUploadBytes int64
}

func registerAddressImportRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo AddressImportRepository, artifacts AddressArtifactStore, jobs OperationJobRepository, maxUploadBytes int64) {
	api := addressImportAPI{repo: repo, artifacts: artifacts, jobs: jobs, maxUploadBytes: maxUploadBytes}
	viewTenant := RequirePermission(ActionView, TenantResource)
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/address-imports", auth(viewTenant(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/address-imports", auth(configureTenant(http.HandlerFunc(api.upload))))
	mux.Handle("GET /api/v1/address-imports/{import_id}", auth(viewTenant(http.HandlerFunc(api.get))))
	mux.Handle("GET /api/v1/address-imports/{import_id}/prefixes", auth(viewTenant(http.HandlerFunc(api.listPrefixes))))
	mux.Handle("GET /api/v1/address-imports/{import_id}/lookup", auth(viewTenant(http.HandlerFunc(api.lookupPrefix))))
	mux.Handle("POST /api/v1/address-imports/{import_id}/actions/activate", auth(configureTenant(http.HandlerFunc(api.activate))))
	mux.Handle("GET /api/v1/address-import-slots/{source_slot}", auth(viewTenant(http.HandlerFunc(api.getSlot))))
}

func (api addressImportAPI) listPrefixes(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	importID := ID(r.PathValue("import_id"))
	if !api.addressImportExists(w, r, auth.TenantID, importID) {
		return
	}
	query := r.URL.Query()
	filter := AddressBasePrefixFilter{
		CountryCode: strings.TrimSpace(query.Get("country_code")),
		Operator:    strings.TrimSpace(query.Get("operator")),
		Search:      strings.TrimSpace(query.Get("q")),
		Cursor:      strings.TrimSpace(query.Get("cursor")),
	}
	if len(filter.CountryCode) > 2 || len(filter.Operator) > 255 || len(filter.Search) > 255 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "address prefix filter is too long", nil)
		return
	}
	if raw := strings.TrimSpace(query.Get("family")); raw != "" {
		family, err := strconv.ParseUint(raw, 10, 8)
		if err != nil || (family != 4 && family != 6) {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "family must be 4 or 6", nil)
			return
		}
		filter.Family = uint8(family)
	}
	if raw := strings.TrimSpace(query.Get("asn")); raw != "" {
		asn, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "asn must be an unsigned 32-bit integer", nil)
			return
		}
		value := uint32(asn)
		filter.ASN = &value
	}
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	items, cursor, err := api.repo.ListAddressBasePrefixes(r.Context(), auth.TenantID, importID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if items == nil {
		items = []AddressBasePrefix{}
	}
	response := map[string]any{"items": items}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api addressImportAPI) lookupPrefix(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	importID := ID(r.PathValue("import_id"))
	if !api.addressImportExists(w, r, auth.TenantID, importID) {
		return
	}
	value := strings.TrimSpace(r.URL.Query().Get("ip"))
	if value == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "ip is required", nil)
		return
	}
	limit := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		limit = parsed
	}
	items, err := api.repo.LookupAddressBasePrefixes(r.Context(), auth.TenantID, importID, value, limit)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if items == nil {
		items = []AddressBasePrefix{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api addressImportAPI) addressImportExists(w http.ResponseWriter, r *http.Request, tenantID, importID ID) bool {
	_, err := api.repo.GetAddressImport(r.Context(), tenantID, importID)
	if err == nil {
		return true
	}
	if errors.Is(err, sql.ErrNoRows) {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address import not found", nil)
		return false
	}
	WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
	return false
}

func (api addressImportAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	filter := AddressImportListFilter{
		SourceSlot: strings.TrimSpace(query.Get("source_slot")),
		Status:     strings.TrimSpace(query.Get("status")),
		Cursor:     strings.TrimSpace(query.Get("cursor")),
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	items, cursor, err := api.repo.ListAddressImports(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if items == nil {
		items = []AddressImport{}
	}
	response := map[string]any{"items": items}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api addressImportAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetAddressImport(r.Context(), auth.TenantID, ID(r.PathValue("import_id")))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address import not found", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api addressImportAPI) getSlot(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	slot, err := api.repo.GetAddressImportSlot(r.Context(), auth.TenantID, strings.TrimSpace(r.PathValue("source_slot")))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address import slot is not active", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(slot.RowVersion))
	WriteAPIJSON(w, http.StatusOK, slot)
}

func (api addressImportAPI) upload(w http.ResponseWriter, r *http.Request) {
	if api.artifacts == nil || api.jobs == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Address import service is not configured", nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	importID, err := newIdentityID()
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "Failed to allocate address import", nil)
		return
	}
	maxUpload := api.maxUploadBytes
	if maxUpload <= 0 {
		maxUpload = DefaultAddressArtifactMaxBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxUpload+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "Content-Type must be multipart/form-data", nil)
		return
	}
	var sourceSlot, language, originalName string
	var artifact AddressArtifact
	retained := false
	defer func() {
		if artifact.Ref != "" && !retained {
			_ = api.artifacts.RemoveAddressArtifact(artifact.Ref)
		}
	}()
	for {
		part, nextErr := reader.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, nextErr.Error(), nil)
			return
		}
		name := part.FormName()
		switch name {
		case "file":
			if artifact.Ref != "" || part.FileName() == "" {
				part.Close()
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "exactly one address database file is required", nil)
				return
			}
			originalName = filepath.Base(strings.TrimSpace(part.FileName()))
			if originalName == "" || len(originalName) > 255 {
				part.Close()
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "address database filename is invalid", nil)
				return
			}
			artifact, err = api.artifacts.SaveAddressArtifact(r.Context(), auth.TenantID, importID, originalName, part)
		case "source_slot":
			sourceSlot, err = readAddressImportFormValue(part)
		case "language":
			language, err = readAddressImportFormValue(part)
		default:
			err = errors.New("unknown multipart field " + name)
		}
		part.Close()
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
	}
	if artifact.Ref == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "address database file is required", nil)
		return
	}
	sourceSlot = strings.TrimSpace(sourceSlot)
	language = strings.TrimSpace(language)
	if !validAddressImportSlot(sourceSlot) || len(language) > 16 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "source_slot or language is invalid", nil)
		return
	}
	item, err := api.repo.CreateAddressImport(r.Context(), AddressImport{
		ID: importID, TenantID: auth.TenantID, SourceSlot: sourceSlot, Format: artifact.Format,
		OriginalName: originalName, ArtifactRef: artifact.Ref, ChecksumSHA256: artifact.ChecksumSHA256,
		SizeBytes: artifact.SizeBytes, Status: AddressImportStatusQueued, CreatedBy: auth.UserID,
	})
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	retained = true
	checkpoint, err := EncodeAddressImportJobPayload(item.ID, language, 0)
	if err != nil {
		_ = api.repo.FailAddressImport(r.Context(), auth.TenantID, item.ID, "ENQUEUE_FAILED", err.Error())
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "Failed to encode address import job", nil)
		return
	}
	hash := sha256.Sum256(checkpoint)
	job, err := api.jobs.EnqueueOperationJob(r.Context(), OperationJob{
		TenantID: auth.TenantID, JobType: AddressImportJobType,
		IdempotencyKey: "address-import:" + string(item.ID), RequestHash: hex.EncodeToString(hash[:]),
		CheckpointJSON: checkpoint, CreatedBy: auth.UserID,
	})
	if err != nil {
		_ = api.repo.FailAddressImport(r.Context(), auth.TenantID, item.ID, "ENQUEUE_FAILED", err.Error())
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "Failed to enqueue address import", nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]any{"import": item, "job": job})
}

func readAddressImportFormValue(part *multipart.Part) (string, error) {
	data, err := io.ReadAll(io.LimitReader(part, 4_097))
	if err != nil {
		return "", err
	}
	if len(data) > 4_096 {
		return "", errors.New("multipart field exceeds 4096 bytes")
	}
	return string(data), nil
}

func (api addressImportAPI) activate(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expectedVersion, ok := parseUserPreferencesIfMatch(w, r)
	if !ok {
		return
	}
	slot, err := api.repo.ActivateAddressImport(r.Context(), auth.TenantID, ID(r.PathValue("import_id")), auth.UserID, expectedVersion)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address import not found", nil)
		case errors.Is(err, ErrAddressImportVersionConflict):
			WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), "Active address import changed since it was read", nil)
		case errors.Is(err, ErrAddressImportNotWritable):
			WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Only a ready address import can be activated", nil)
		default:
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		}
		return
	}
	w.Header().Set("ETag", quotedRowVersion(slot.RowVersion))
	WriteAPIJSON(w, http.StatusOK, slot)
}
