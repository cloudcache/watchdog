package watchdog

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

type snmpAPI struct {
	repo SNMPRepository
}

func registerSNMPRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo SNMPRepository) {
	api := snmpAPI{repo: repo}
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/snmp/profiles", auth(configureTenant(http.HandlerFunc(api.listProfiles))))
	mux.Handle("POST /api/v1/snmp/profiles", auth(configureTenant(http.HandlerFunc(api.createProfile))))
	mux.Handle("GET /api/v1/snmp/profiles/{profile_id}", auth(configureTenant(http.HandlerFunc(api.getProfile))))
	mux.Handle("PATCH /api/v1/snmp/profiles/{profile_id}", auth(configureTenant(http.HandlerFunc(api.patchProfile))))
	mux.Handle("DELETE /api/v1/snmp/profiles/{profile_id}", auth(configureTenant(http.HandlerFunc(api.deleteProfile))))
	mux.Handle("GET /api/v1/snmp/mib-modules", auth(configureTenant(http.HandlerFunc(api.listMIBModules))))
	mux.Handle("PUT /api/v1/snmp/mib-modules", auth(configureTenant(http.HandlerFunc(api.putMIBModule))))
	mux.Handle("DELETE /api/v1/snmp/mib-modules/{module_id}", auth(configureTenant(http.HandlerFunc(api.deleteMIBModule))))
}

func (api snmpAPI) listProfiles(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	profiles, err := api.repo.ListSNMPProfiles(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": profiles})
}

func (api snmpAPI) getProfile(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	profile, err := api.repo.GetSNMPProfile(r.Context(), auth.TenantID, ID(r.PathValue("profile_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "SNMP profile not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, profile)
}

func (api snmpAPI) createProfile(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	profile, err := decodeSNMPProfileRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	profile.TenantID = auth.TenantID
	saved, err := api.repo.UpsertSNMPProfile(r.Context(), profile)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, saved)
}

func (api snmpAPI) patchProfile(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	profile, err := decodeSNMPProfileRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	profile.ID = ID(r.PathValue("profile_id"))
	profile.TenantID = auth.TenantID
	saved, err := api.repo.UpsertSNMPProfile(r.Context(), profile)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, saved)
}

func (api snmpAPI) deleteProfile(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if err := api.repo.DeleteSNMPProfile(r.Context(), auth.TenantID, ID(r.PathValue("profile_id"))); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (api snmpAPI) listMIBModules(w http.ResponseWriter, r *http.Request) {
	modules, err := api.repo.ListMIBModules(r.Context())
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": modules})
}

func (api snmpAPI) putMIBModule(w http.ResponseWriter, r *http.Request) {
	module, err := decodeMIBModuleRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	saved, err := api.repo.UpsertMIBModule(r.Context(), module)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, saved)
}

func (api snmpAPI) deleteMIBModule(w http.ResponseWriter, r *http.Request) {
	if err := api.repo.DeleteMIBModule(r.Context(), ID(r.PathValue("module_id"))); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

func decodeSNMPProfileRequest(r *http.Request) (SNMPProfile, error) {
	defer r.Body.Close()
	var profile SNMPProfile
	if err := json.NewDecoder(r.Body).Decode(&profile); err != nil {
		return SNMPProfile{}, err
	}
	if profile.ID == "" {
		return SNMPProfile{}, errors.New("snmp profile id is required")
	}
	if profile.Name == "" {
		return SNMPProfile{}, errors.New("snmp profile name is required")
	}
	if profile.Version == "" {
		profile.Version = SNMPVersion2c
	}
	if profile.Version != SNMPVersion2c && profile.Version != SNMPVersion3 {
		return SNMPProfile{}, errors.New("snmp profile version must be 2c or 3")
	}
	if profile.Security == nil {
		profile.Security = map[string]string{}
	}
	if profile.Timeout <= 0 {
		profile.Timeout = 5 * time.Second
	}
	return profile, nil
}

func decodeMIBModuleRequest(r *http.Request) (MIBModule, error) {
	defer r.Body.Close()
	var module MIBModule
	if err := json.NewDecoder(r.Body).Decode(&module); err != nil {
		return MIBModule{}, err
	}
	if module.Name == "" {
		return MIBModule{}, errors.New("mib module name is required")
	}
	if module.Source == "" {
		module.Source = "librenms"
	}
	if module.Checksum == "" {
		return MIBModule{}, errors.New("mib module checksum is required")
	}
	return module, nil
}
