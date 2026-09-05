package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
)

type targetAPI struct {
	repo          TargetRepository
	seriesCleaner SeriesCleaner
	network       NetworkRepository
	discoveryJobs DiscoveryJobRepository
	snmp          SNMPRepository
	deletePreview TargetDeletePreviewRepository
	operationJobs OperationJobRepository
}

type targetRequest struct {
	ID            ID                `json:"id"`
	Name          string            `json:"name"`
	Kind          TargetKind        `json:"kind"`
	Host          string            `json:"host"`
	Status        string            `json:"status"`
	Labels        map[string]string `json:"labels"`
	SNMPProfileID ID                `json:"snmp_profile_id"`
	SNMPPort      uint16            `json:"snmp_port"`
	SNMPSecurity  map[string]string `json:"snmp_security"`
}

func registerTargetRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo TargetRepository, cleaner SeriesCleaner, network NetworkRepository, jobs DiscoveryJobRepository, snmp SNMPRepository, deletePreview TargetDeletePreviewRepository, operationJobs OperationJobRepository) {
	api := targetAPI{repo: repo, seriesCleaner: cleaner, network: network, discoveryJobs: jobs, snmp: snmp, deletePreview: deletePreview, operationJobs: operationJobs}
	mux.Handle("GET /api/v1/targets", auth(RequirePermission(ActionView, TenantResource)(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/targets", auth(RequirePermission(ActionConfigure, TenantResource)(http.HandlerFunc(api.create))))
	mux.Handle("GET /api/v1/targets/{target_id}", auth(RequirePermission(ActionView, targetResourceFromPath)(http.HandlerFunc(api.get))))
	mux.Handle("PATCH /api/v1/targets/{target_id}", auth(RequirePermission(ActionConfigure, targetResourceFromPath)(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/targets/{target_id}", auth(RequirePermission(ActionConfigure, targetResourceFromPath)(http.HandlerFunc(api.delete))))
	mux.Handle("GET /api/v1/targets/{target_id}/delete-preview", auth(RequirePermission(ActionConfigure, targetResourceFromPath)(http.HandlerFunc(api.previewDelete))))
}

func (api targetAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	targets, err := api.repo.ListTargets(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	visible := make([]Target, 0, len(targets))
	for _, target := range targets {
		if canListTarget(auth, target.ID) {
			visible = append(visible, target)
		}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": visible})
}

func (api targetAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	target, err := api.repo.GetTarget(r.Context(), auth.TenantID, ID(r.PathValue("target_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Target not found", nil)
		return
	}
	SetEntityETag(w, target.UpdatedAt)
	WriteAPIJSON(w, http.StatusOK, target)
}

func (api targetAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	req, err := decodeTargetRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	target := req.target()
	target.TenantID = auth.TenantID
	if target.Kind == TargetKindNetwork {
		target.Status = "pending"
	}
	// The API owns resource identity. Names are presentation only; a normalized
	// tenant/kind/host key gives retries the same bounded database ID.
	target.ID = stableID("target", string(auth.TenantID), string(target.Kind), target.Host)

	if existing, duplicateErr := api.findTargetByHost(r.Context(), target); duplicateErr != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, duplicateErr.Error(), nil)
		return
	} else if existing.ID != "" {
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "A target with this host already exists", map[string]any{"target_id": existing.ID})
		return
	}

	var created Target
	if target.Kind == TargetKindNetwork && api.network != nil {
		profileID, profileErr := api.resolveSNMPProfileID(r.Context(), auth.TenantID, req.SNMPProfileID)
		if profileErr != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, profileErr.Error(), nil)
			return
		}
		device := NetworkDevice{
			ID:            stableID("device", string(auth.TenantID), string(target.ID)),
			TenantID:      auth.TenantID,
			TargetID:      target.ID,
			SNMPProfileID: profileID,
			SNMPPort:      normalizeSNMPPort(req.SNMPPort),
			SNMPSecurity:  req.SNMPSecurity,
		}
		created, _, err = api.createNetworkTarget(r.Context(), target, device)
		if err == nil && api.discoveryJobs != nil && profileID != "" {
			_ = api.discoveryJobs.EnqueueDiscoveryJob(r.Context(), auth.TenantID, device.ID, "device_created")
		}
	} else {
		created, err = api.repo.CreateTarget(r.Context(), target)
	}
	if err != nil {
		status := http.StatusBadRequest
		message := err.Error()
		if errors.Is(err, ErrTargetHostExists) {
			status = http.StatusConflict
			message = "A target with this host already exists"
		}
		WriteAPIError(w, status, APIErrorInvalidRequest, message, nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api targetAPI) patch(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	existing, err := api.repo.GetTarget(r.Context(), auth.TenantID, ID(r.PathValue("target_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Target not found", nil)
		return
	}
	if !CheckIfMatch(w, r, existing.UpdatedAt) {
		return
	}
	req, err := decodeTargetRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	target := req.target()
	target.ID = existing.ID
	target.TenantID = auth.TenantID
	updated, err := api.repo.UpdateTarget(r.Context(), target)
	if err != nil {
		status := http.StatusBadRequest
		message := err.Error()
		if errors.Is(err, ErrTargetHostExists) {
			status = http.StatusConflict
			message = "A target with this host already exists"
		}
		WriteAPIError(w, status, APIErrorInvalidRequest, message, nil)
		return
	}
	if api.discoveryJobs != nil && api.network != nil && existing.Host != updated.Host {
		if device, derr := api.network.GetDeviceByTarget(r.Context(), auth.TenantID, updated.ID); derr == nil {
			_ = api.discoveryJobs.EnqueueDiscoveryJob(r.Context(), auth.TenantID, device.ID, "target_host_changed")
		}
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api targetAPI) previewDelete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if api.deletePreview == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Delete preview is not available", nil)
		return
	}
	preview, err := api.deletePreview.PreviewTargetDelete(r.Context(), auth.TenantID, ID(r.PathValue("target_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Target not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, preview)
}

func (api targetAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	targetID := ID(r.PathValue("target_id"))
	if current, err := api.repo.GetTarget(r.Context(), auth.TenantID, targetID); err == nil {
		if !CheckIfMatch(w, r, current.UpdatedAt) {
			return
		}
	} else {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Target not found", nil)
		return
	}
	// With a job repository configured, deletion runs asynchronously: the
	// handler only enqueues (202 + job id) and the worker performs the
	// cascade, so a large fan-out cannot stall or die inside the request.
	if api.operationJobs != nil {
		payload, err := EncodeTargetDeletePayload(targetID)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		digest := sha256.Sum256(payload)
		job, err := api.operationJobs.EnqueueOperationJob(r.Context(), OperationJob{
			TenantID:       auth.TenantID,
			JobType:        TargetDeleteJobType,
			IdempotencyKey: "target_delete:" + string(targetID),
			RequestHash:    hex.EncodeToString(digest[:]),
			CheckpointJSON: payload,
			CreatedBy:      auth.UserID,
		})
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		WriteAPIJSON(w, http.StatusAccepted, map[string]any{
			"job_id": job.ID, "status": job.Status,
			"status_url": "/api/v1/operation-jobs/" + string(job.ID),
		})
		return
	}
	if err := api.repo.DeleteTarget(r.Context(), auth.TenantID, targetID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.seriesCleaner != nil {
		_ = api.seriesCleaner.DeleteSeries(r.Context(), []string{
			`{target_id="` + string(targetID) + `"}`,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeTargetRequest(r *http.Request) (targetRequest, error) {
	defer r.Body.Close()
	var req targetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return targetRequest{}, err
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Host = normalizeTargetHost(req.Host)
	if req.Host == "" {
		return targetRequest{}, errors.New("target host is required")
	}
	if req.Name == "" {
		req.Name = req.Host
	}
	if req.Kind != TargetKindSystem && req.Kind != TargetKindNetwork {
		return targetRequest{}, errors.New("target kind must be system or network")
	}
	return req, nil
}

func normalizeTargetHost(value string) string {
	value = strings.TrimSpace(value)
	if ip := net.ParseIP(strings.Trim(value, "[]")); ip != nil {
		return ip.String()
	}
	return strings.ToLower(strings.TrimSuffix(value, "."))
}

func (req targetRequest) target() Target {
	return Target{
		ID: req.ID, Name: req.Name, Kind: req.Kind, Host: req.Host,
		Status: req.Status, Labels: req.Labels,
	}
}

func (api targetAPI) findTargetByHost(ctx context.Context, target Target) (Target, error) {
	targets, err := api.repo.ListTargets(ctx, target.TenantID)
	if err != nil {
		return Target{}, err
	}
	for _, existing := range targets {
		if existing.Kind == target.Kind && strings.EqualFold(strings.TrimSpace(existing.Host), target.Host) {
			return existing, nil
		}
	}
	return Target{}, nil
}

func (api targetAPI) resolveSNMPProfileID(ctx context.Context, tenantID, requested ID) (ID, error) {
	if api.snmp == nil {
		return requested, nil
	}
	if requested != "" {
		if _, err := api.snmp.GetSNMPProfile(ctx, tenantID, requested); err != nil {
			return "", errors.New("selected SNMP profile does not exist")
		}
		return requested, nil
	}
	profiles, err := api.snmp.ListSNMPProfiles(ctx, tenantID)
	if err != nil {
		return "", err
	}
	switch len(profiles) {
	case 0:
		return "", errors.New("create an SNMP profile before adding a network device")
	case 1:
		return profiles[0].ID, nil
	default:
		return "", errors.New("select an SNMP profile")
	}
}

func (api targetAPI) createNetworkTarget(ctx context.Context, target Target, device NetworkDevice) (Target, NetworkDevice, error) {
	if provisioner, ok := api.repo.(NetworkTargetProvisioner); ok {
		return provisioner.CreateNetworkTarget(ctx, target, device)
	}
	created, err := api.repo.CreateTarget(ctx, target)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	createdDevice, err := api.network.UpsertDevice(ctx, device)
	if err != nil {
		_ = api.repo.DeleteTarget(ctx, target.TenantID, target.ID)
		return Target{}, NetworkDevice{}, err
	}
	return created, createdDevice, nil
}

func promoteDiscoveredTargetName(ctx context.Context, repo TargetRepository, tenantID, targetID ID, sysName string) error {
	sysName = strings.TrimSpace(sysName)
	if repo == nil || sysName == "" {
		return nil
	}
	target, err := repo.GetTarget(ctx, tenantID, targetID)
	if err != nil {
		return err
	}
	// Host is the automatic placeholder. Any other name is an explicit user
	// choice and must not be overwritten by a later discovery run.
	if target.Name != "" && target.Name != target.Host {
		return nil
	}
	target.Name = sysName
	_, err = repo.UpdateTarget(ctx, target)
	return err
}

func targetResourceFromPath(r *http.Request, _ AuthContext) (ResourceRef, error) {
	targetID := ID(r.PathValue("target_id"))
	if targetID == "" {
		return ResourceRef{}, errors.New("target id is required")
	}
	return ResourceRef{Type: ResourceTarget, ID: targetID}, nil
}

func canListTarget(auth AuthContext, targetID ID) bool {
	if auth.IsAdmin {
		return true
	}
	for _, grant := range auth.Grants {
		if grant.TenantID != auth.TenantID || grant.ResourceType != ResourceTarget || grant.ResourceID != targetID {
			continue
		}
		if !subjectMatches(AccessRequest{UserID: auth.UserID, RoleIDs: auth.RoleIDs}, grant) {
			continue
		}
		if actionAllowed(ActionView, grant.Actions) {
			return true
		}
	}
	return false
}
