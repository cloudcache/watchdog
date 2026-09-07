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
	runner           flowOverseasRunner
	network          NetworkRepository
	storageLifecycle FlowStorageArchiveBoundaryRepository
	audit            AuditRepository
	geo              *FlowGeoService
	now              func() time.Time
}

func registerFlowOverseasRoutes(
	mux *http.ServeMux,
	auth func(http.Handler) http.Handler,
	runner flowOverseasRunner,
	network NetworkRepository,
	storageLifecycle FlowStorageArchiveBoundaryRepository,
	audit AuditRepository,
	geo *FlowGeoService,
	now func() time.Time,
) {
	api := flowOverseasAPI{runner: runner, network: network, storageLifecycle: storageLifecycle, audit: audit, geo: geo, now: now}
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
	if api.storageLifecycle != nil {
		input.StorageV2 = true
		input.ArchiveThrough = input.From.UTC()
		if input.Bucket == flowquery.BucketOneHour {
			boundary, boundaryErr := api.storageLifecycle.FlowStorageArchiveThrough(r.Context(), auth.TenantID, input.From, input.To)
			if boundaryErr != nil {
				writeFlowExecutionError(w, boundaryErr)
				return
			}
			input.ArchiveThrough = boundary
		}
	}
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
			"source":          input.Bucket,
			"step_seconds":    uint32(compiled.BucketDuration / time.Second),
			"sort":            "bucket:asc,kind:asc,direction:asc,value:desc",
			"geo_labels":      api.geoLabels(result),
			"uses_raw":        compiled.UsesRawFacts,
			"archive_through": compiled.ArchiveThrough,
		},
	})
}

func (api flowOverseasAPI) geoLabels(result flowquery.OverseasResult) map[string]FlowGeoLabel {
	labels := make(map[string]FlowGeoLabel)
	for _, point := range result.Points {
		if point.Kind != flowquery.OverseasRowGeo || point.GeoValue == "" || point.Other {
			continue
		}
		key := point.GeoVersion + ":" + point.GeoValue
		if _, exists := labels[key]; exists {
			continue
		}
		if label, ok := api.geo.Label(point.GeoVersion, point.GeoValue); ok {
			labels[key] = label
		}
	}
	return labels
}

func (api flowOverseasAPI) currentTime() time.Time {
	if api.now != nil {
		return api.now().UTC()
	}
	return time.Now().UTC()
}
