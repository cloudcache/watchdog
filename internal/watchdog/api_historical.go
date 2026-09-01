package watchdog

import (
	"encoding/json"
	"net/http"
)

func registerHistoricalRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler) {
	adminTenant := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("POST /api/v1/historical/preview", auth(adminTenant(http.HandlerFunc(handleHistoricalPreview))))
	mux.Handle("POST /api/v1/historical/archive", auth(adminTenant(http.HandlerFunc(handleHistoricalArchive))))
	mux.Handle("POST /api/v1/historical/delete", auth(adminTenant(http.HandlerFunc(handleHistoricalDelete))))
}

func handleHistoricalPreview(w http.ResponseWriter, r *http.Request) {
	handleHistoricalAction(w, r, HistoricalActionPreview)
}

func handleHistoricalArchive(w http.ResponseWriter, r *http.Request) {
	handleHistoricalAction(w, r, HistoricalActionArchive)
}

func handleHistoricalDelete(w http.ResponseWriter, r *http.Request) {
	handleHistoricalAction(w, r, HistoricalActionDelete)
}

func handleHistoricalAction(w http.ResponseWriter, r *http.Request, action HistoricalAction) {
	auth, _ := AuthFromContext(r.Context())
	req, err := decodeHistoricalDataRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	req.TenantID = auth.TenantID
	req.Action = action
	if err := ValidateHistoricalDataRequest(req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if action == HistoricalActionPreview {
		preview, err := PreviewHistoricalData(req, parseDurationSeconds(r.URL.Query().Get("sample_step")), 0)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		WriteAPIJSON(w, http.StatusOK, preview)
		return
	}
	operation, err := CreateHistoricalDataOperation(ID(r.URL.Query().Get("operation_id")), req)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, operation)
}

func decodeHistoricalDataRequest(r *http.Request) (HistoricalDataRequest, error) {
	defer r.Body.Close()
	var req HistoricalDataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return HistoricalDataRequest{}, err
	}
	return req, nil
}
