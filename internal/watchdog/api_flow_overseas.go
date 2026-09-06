package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowOverseasRunner interface {
	Run(context.Context, flowquery.CompiledOverseas) (flowquery.OverseasResult, error)
}

type flowOverseasAPI struct {
	runner  flowOverseasRunner
	network NetworkRepository
	audit   AuditRepository
	now     func() time.Time
}

func registerFlowOverseasRoutes(
	mux *http.ServeMux,
	auth func(http.Handler) http.Handler,
	runner flowOverseasRunner,
	network NetworkRepository,
	audit AuditRepository,
	now func() time.Time,
) {
	api := flowOverseasAPI{runner: runner, network: network, audit: audit, now: now}
	mux.Handle("POST /api/v1/flow/overseas/query", auth(http.HandlerFunc(api.query)))
}

func (api flowOverseasAPI) query(w http.ResponseWriter, r *http.Request) {
	var input flowquery.OverseasRequest
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
	if view != flowquery.ViewCustomer || !queryLayerAuthorized(auth, layer) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Flow overseas view is not permitted", nil)
		return
	}
	if err := authorizeFlowResourceFilters(r.Context(), auth, api.network, input.Filters.TargetIDs, input.Filters.DeviceIDs, input.Filters.ExporterIDs); err != nil {
		writeQueryGatewayError(w, err)
		return
	}
	input.View = view
	compiled, err := flowquery.CompileOverseas(flowquery.Scope{
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
	(queryGatewayAPI{audit: api.audit}).recordAudit(r.Context(), auth, "query.executed", FlowTrafficDataset, map[string]any{
		"surface": "overseas", "bucket": input.Bucket, "geo_level": input.GeoLevel,
	})
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"data": result,
		"meta": map[string]any{
			"source":       input.Bucket,
			"step_seconds": uint32(compiled.BucketDuration / time.Second),
			"sort":         "bucket:asc,kind:asc,direction:asc,value:desc",
		},
	})
}

func (api flowOverseasAPI) currentTime() time.Time {
	if api.now != nil {
		return api.now().UTC()
	}
	return time.Now().UTC()
}
