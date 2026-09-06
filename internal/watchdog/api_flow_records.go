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
	RunFacet(context.Context, flowquery.CompiledDetailFacet) (flowquery.DetailFacetResult, error)
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
	mux.Handle("POST /api/v1/flow/records/facets", auth(http.HandlerFunc(api.facets)))
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
	targetIDs, deviceIDs, exporterIDs := detailResourceFilters(input.Filters, input.ColumnFilters)
	if err := authorizeFlowResourceFilters(r.Context(), auth, api.network, targetIDs, deviceIDs, exporterIDs); err != nil {
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
		"sort_field": compiled.Sort.Field, "sort_direction": compiled.Sort.Direction,
	})
	sortDescription := compiled.Sort.Field + ":" + compiled.Sort.Direction
	if compiled.Sort.Field != "event_time" {
		sortDescription += ",event_time:" + compiled.Sort.Direction
	}
	sortDescription += ",record_id:" + compiled.Sort.Direction
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": map[string]any{
			"sort": sortDescription, "page_size": input.Limit,
		},
	})
}

func (api flowRecordAPI) facets(w http.ResponseWriter, r *http.Request) {
	var input flowquery.DetailFacetRequest
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
	targetIDs, deviceIDs, exporterIDs := detailResourceFilters(input.Filters, input.ColumnFilters)
	if err := authorizeFlowResourceFilters(r.Context(), auth, api.network, targetIDs, deviceIDs, exporterIDs); err != nil {
		writeQueryGatewayError(w, err)
		return
	}
	input.View = view
	compiled, err := flowquery.CompileDetailFacet(flowquery.Scope{
		TenantID: string(auth.TenantID), AllowedViews: []flowquery.View{view},
	}, input, api.currentTime())
	if err != nil {
		writeFlowExecutionError(w, err)
		return
	}
	result, err := api.runner.RunFacet(r.Context(), compiled)
	if err != nil {
		writeFlowExecutionError(w, err)
		return
	}
	(queryGatewayAPI{audit: api.audit}).recordAudit(r.Context(), auth, "query.sensitive_viewed", "flow.record_facets", map[string]any{
		"value_layer": input.View, "endpoint": input.Endpoint, "field": input.Field, "result_count": len(result.Items),
	})
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": map[string]any{"limit": input.Limit},
	})
}

func detailResourceFilters(filters flowquery.DetailFilters, columns []flowquery.DetailColumnFilter) ([]string, []string, []string) {
	targetIDs := append([]string(nil), filters.TargetIDs...)
	deviceIDs := append([]string(nil), filters.DeviceIDs...)
	exporterIDs := append([]string(nil), filters.ExporterIDs...)
	for _, filter := range columns {
		switch filter.Field {
		case string(flowquery.DetailFieldTargetID):
			targetIDs = append(targetIDs, filter.Values...)
		case string(flowquery.DetailFieldDeviceID):
			deviceIDs = append(deviceIDs, filter.Values...)
		case string(flowquery.DetailFieldExporterID):
			exporterIDs = append(exporterIDs, filter.Values...)
		}
	}
	return targetIDs, deviceIDs, exporterIDs
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
