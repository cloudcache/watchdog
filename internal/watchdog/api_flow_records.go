package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowDetailRunner interface {
	Run(context.Context, flowquery.CompiledDetail) (flowquery.DetailResult, error)
}

type flowRecordAPI struct {
	runner  flowDetailRunner
	network NetworkRepository
	audit   AuditRepository
	now     func() time.Time
}

func registerFlowRecordRoutes(
	mux *http.ServeMux,
	auth func(http.Handler) http.Handler,
	runner flowDetailRunner,
	network NetworkRepository,
	audit AuditRepository,
	now func() time.Time,
) {
	api := flowRecordAPI{runner: runner, network: network, audit: audit, now: now}
	mux.Handle("GET /api/v1/flow/records/capabilities", auth(http.HandlerFunc(api.capabilities)))
	mux.Handle("POST /api/v1/flow/records/search", auth(http.HandlerFunc(api.search)))
}

func (api flowRecordAPI) capabilities(w http.ResponseWriter, _ *http.Request) {
	capabilities := flowquery.DetailCapabilities()
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": capabilities, "total": len(capabilities)})
}

func (api flowRecordAPI) search(w http.ResponseWriter, r *http.Request) {
	var input flowquery.DetailRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	layer := QueryValueLayer(input.View)
	view, err := flowView(layer)
	if err != nil {
		writeQueryGatewayError(w, err)
		return
	}
	if !queryLayerAuthorized(auth, layer) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Flow detail view is not permitted", nil)
		return
	}
	if err := authorizeFlowResourceFilters(r.Context(), auth, api.network, input.Filters.TargetIDs, input.Filters.DeviceIDs, input.Filters.ExporterIDs); err != nil {
		writeQueryGatewayError(w, err)
		return
	}
	input.View = view
	compiled, err := flowquery.CompileDetail(flowquery.Scope{
		TenantID: string(auth.TenantID), AllowedViews: []flowquery.View{view},
	}, input, api.currentTime())
	if err != nil {
		writeFlowExecutionError(w, err)
		return
	}
	result, err := api.runner.Run(r.Context(), compiled)
	if err != nil {
		writeFlowExecutionError(w, err)
		return
	}
	(queryGatewayAPI{audit: api.audit}).recordAudit(r.Context(), auth, "query.sensitive_viewed", "flow.records", map[string]any{
		"value_layer": input.View, "endpoint": input.Endpoint, "has_more": result.HasMore,
	})
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": map[string]any{
			"sort": "event_time:desc,record_id:desc", "page_size": input.Limit,
		},
	})
}

func (api flowRecordAPI) currentTime() time.Time {
	if api.now != nil {
		return api.now().UTC()
	}
	return time.Now().UTC()
}

func writeFlowExecutionError(w http.ResponseWriter, err error) {
	mapped := mapFlowQueryError(err)
	if _, ok := mapped.(*QueryGatewayError); !ok {
		mapped = &QueryGatewayError{Code: QueryErrorProviderFailure, Message: "Flow query failed", Cause: err}
	}
	writeQueryGatewayError(w, mapped)
}
