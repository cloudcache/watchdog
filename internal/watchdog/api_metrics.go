package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func registerMetricsRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, service MetricsService, network NetworkRepository, gateway *QueryGateway, audit AuditRepository) {
	api := metricsAPI{service: service, network: network, gateway: gateway, audit: audit}
	handler := http.HandlerFunc(api.query)
	mux.Handle("GET /api/v1/metrics/catalog", auth(http.HandlerFunc(api.catalog)))
	mux.Handle("GET /api/v1/metrics/realtime", auth(handler))
	mux.Handle("GET /api/v1/metrics/query", auth(handler))
	mux.Handle("GET /api/v1/metrics/range", auth(handler))
	mux.Handle("GET /api/v1/metrics/aggregate", auth(http.HandlerFunc(api.aggregate)))
	mux.Handle("GET /api/v1/metrics/vmquery", auth(http.HandlerFunc(api.vmQuery)))
}

type metricsAPI struct {
	service MetricsService
	network NetworkRepository
	gateway *QueryGateway
	audit   AuditRepository
}

func (api metricsAPI) catalog(w http.ResponseWriter, _ *http.Request) {
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": MetricCatalog})
}

func (api metricsAPI) query(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	req, err := parseMetricsQueryRequest(r, auth)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	req, err = api.resolveMetricsResource(r.Context(), auth, req)
	if err != nil {
		status := http.StatusBadRequest
		code := APIErrorInvalidRequest
		if errors.Is(err, errMetricsResourceNotFound) {
			status = http.StatusNotFound
			code = APIErrorNotFound
		}
		WriteAPIError(w, status, code, err.Error(), nil)
		return
	}
	req = normalizeMetricsQueryTime(req)
	if err := ValidateMetricsQuery(req, auth.IsAdmin); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if !canAccessMetrics(auth, req) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	// per_port=1 returns one series per port of the device (each corrected
	// with its own policy) — the ports table needs current values for every
	// interface in a single query.
	perPort := r.URL.Query().Get("per_port") == "1"
	if api.gateway != nil {
		result, err := api.queryThroughGateway(r.Context(), auth, req, perPort)
		if err != nil {
			writeQueryGatewayError(w, err)
			return
		}
		api.auditSensitiveGatewayQuery(r.Context(), auth, result, req.Metric, r.URL.Path)
		WriteAPIJSONRaw(w, http.StatusOK, result.Data)
		return
	}
	if isSNMPTrafficBpsMetric(req.Metric) && !perPort && (req.TrafficView == TrafficViewSupplier || req.TrafficView == TrafficViewCustomer) && req.PortID == "" {
		api.queryTrafficViewBySide(w, r, auth, req)
		return
	}
	if isSNMPTrafficBpsMetric(req.Metric) && req.ValueMode != MetricValueRaw && req.PortID == "" && !(perPort && req.DeviceID != "") {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "corrected traffic values require port_id or aggregate graph scope", nil)
		return
	}
	selector := metricsSelector(req)
	if api.service.Client == nil {
		WriteAPIJSON(w, http.StatusOK, map[string]any{"query": req, "selector": selector})
		return
	}
	response, err := api.service.QueryRange(r.Context(), req, selector, auth.IsAdmin)
	if err != nil {
		if errors.Is(err, ErrVictoriaMetricsUnavailable) {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "VictoriaMetrics is unavailable; check victoriametrics.base_url or start VictoriaMetrics", map[string]any{
				"selector": selector,
			})
			return
		}
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if isSNMPTrafficBpsMetric(req.Metric) {
		response = applyCounterRateToVMResponse(response)
		// Correction policies describe traffic; other port-scoped metrics
		// (optics, status, error counters) must pass through untouched.
		if req.PortID != "" {
			policy := loadPortPolicy(r.Context(), api.network, auth.TenantID, req.PortID)
			if transformed, terr := TransformVMRangeValues(response, req.ValueMode, policy, nil); terr == nil {
				response = transformed
			}
		} else if perPort && req.DeviceID != "" && req.ValueMode != MetricValueRaw {
			response = transformVMRangePerPort(r.Context(), api.network, auth.TenantID, req.DeviceID, response)
		}
	}
	response = applyTrafficViewResponse(req, response)
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api metricsAPI) aggregate(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	req, err := parseMetricsAggregateRequest(r, auth)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	req.Query = normalizeMetricsQueryTime(req.Query)
	if err := ValidateMetricsQuery(req.Query, auth.IsAdmin); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.authorizeAggregate(r.Context(), auth, req); err != nil {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, err.Error(), nil)
		return
	}
	if api.gateway != nil {
		result, err := api.aggregateThroughGateway(r.Context(), auth, req)
		if err != nil {
			writeQueryGatewayError(w, err)
			return
		}
		api.auditSensitiveGatewayQuery(r.Context(), auth, result, req.Query.Metric, r.URL.Path)
		WriteAPIJSONRaw(w, http.StatusOK, result.Data)
		return
	}
	selector := aggregateMetricsSelector(req)
	if api.service.Client == nil {
		WriteAPIJSON(w, http.StatusOK, map[string]any{"query": req, "selector": selector})
		return
	}
	response, err := aggregatePortSeries(r.Context(), api.network, api.service, req)
	if err != nil {
		if errors.Is(err, ErrVictoriaMetricsUnavailable) {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "VictoriaMetrics is unavailable; check victoriametrics.base_url or start VictoriaMetrics", map[string]any{
				"selector": selector,
			})
			return
		}
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	response = applyTrafficViewResponse(req.Query, response)
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api metricsAPI) queryThroughGateway(ctx context.Context, auth AuthContext, req MetricsQueryRequest, perPort bool) (QueryResult, error) {
	dataset, err := api.metricsDataset(req.Metric)
	if err != nil {
		return QueryResult{}, err
	}
	mode := valueModeOrDefault(req.ValueMode)
	view := normalizeTrafficView(req.TrafficView)
	if view == "" {
		// The legacy default had no traffic re-bucketing. "raw" here names
		// that view granularity; raw value access remains controlled by mode.
		view = TrafficViewRaw
	}
	parameters, err := json.Marshal(victoriaMetricsQueryParameters{
		Metric: req.Metric, TargetID: req.TargetID, DeviceID: req.DeviceID, PortID: req.PortID,
		Function: req.Func, ValueMode: mode, TrafficView: view, PerPort: perPort,
	})
	if err != nil {
		return QueryResult{}, err
	}
	result, err := api.gateway.executeCompatibility(ctx, auth, RequestIDFromContext(ctx), QueryRequest{
		Dataset: dataset.Key, From: req.Start, To: req.End, StepSeconds: uint32(req.Step / time.Second),
		ValueLayer: legacyMetricsValueLayer(mode, view), Parameters: parameters,
	})
	if err != nil {
		return QueryResult{}, err
	}
	// The provider already serialized the VictoriaMetrics response; forward the
	// bytes verbatim (WriteAPIJSONRaw) instead of decoding into a struct only to
	// re-encode it. json.Valid keeps the original guard against a provider whose
	// output is not usable JSON, without the per-datapoint decode+encode cost.
	if !json.Valid(result.Data) {
		return QueryResult{}, &QueryGatewayError{Code: QueryErrorProviderFailure, Message: "query provider returned an incompatible metrics response"}
	}
	return result, nil
}

func (api metricsAPI) aggregateThroughGateway(ctx context.Context, auth AuthContext, req MetricsAggregateRequest) (QueryResult, error) {
	dataset, err := api.metricsDataset(req.Query.Metric)
	if err != nil {
		return QueryResult{}, err
	}
	mode := valueModeOrDefault(req.Query.ValueMode)
	view := normalizeTrafficView(req.Query.TrafficView)
	if view == "" {
		view = TrafficViewRaw
	}
	parameters, err := json.Marshal(victoriaMetricsQueryParameters{
		Metric: req.Query.Metric, TargetIDs: req.TargetIDs, PortIDs: req.PortIDs,
		Function: req.Query.Func, Aggregation: req.Method, ValueMode: mode, TrafficView: view,
	})
	if err != nil {
		return QueryResult{}, err
	}
	result, err := api.gateway.executeCompatibility(ctx, auth, RequestIDFromContext(ctx), QueryRequest{
		Dataset: dataset.Key, From: req.Query.Start, To: req.Query.End, StepSeconds: uint32(req.Query.Step / time.Second),
		ValueLayer: legacyMetricsValueLayer(mode, view), Parameters: parameters,
	})
	if err != nil {
		return QueryResult{}, err
	}
	// See queryThroughGateway: forward the provider's already-serialized bytes.
	if !json.Valid(result.Data) {
		return QueryResult{}, &QueryGatewayError{Code: QueryErrorProviderFailure, Message: "query provider returned an incompatible metrics response"}
	}
	return result, nil
}

func (api metricsAPI) metricsDataset(metric string) (DatasetDescriptor, error) {
	if api.gateway == nil || api.gateway.Datasets == nil {
		return DatasetDescriptor{}, &QueryGatewayError{Code: QueryErrorDatasetNotFound, Message: "metrics dataset is not registered"}
	}
	var found DatasetDescriptor
	for _, dataset := range api.gateway.Datasets.List() {
		for _, candidate := range dataset.Metrics {
			if candidate != metric {
				continue
			}
			if found.Key != "" {
				return DatasetDescriptor{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "metric is registered by multiple datasets"}
			}
			found = dataset
		}
	}
	if found.Key == "" {
		return DatasetDescriptor{}, &QueryGatewayError{Code: QueryErrorDatasetNotFound, Message: "metrics dataset is not registered"}
	}
	return found, nil
}

func legacyMetricsValueLayer(mode MetricValueMode, view TrafficViewMode) QueryValueLayer {
	if mode == MetricValueRaw || mode == MetricValueBoth {
		return QueryValueRaw
	}
	if view == TrafficViewSupplier {
		return QueryValueSupplier
	}
	return QueryValueCustomer
}

func (api metricsAPI) auditSensitiveGatewayQuery(ctx context.Context, auth AuthContext, result QueryResult, metric, endpoint string) {
	if result.Meta.ValueLayer != QueryValueRaw && result.Meta.ValueLayer != QueryValueSupplier {
		return
	}
	dataset, err := api.metricsDataset(metric)
	if err != nil {
		return
	}
	(queryGatewayAPI{audit: api.audit}).recordAudit(ctx, auth, "query.sensitive_viewed", dataset.Key, map[string]any{
		"value_layer": result.Meta.ValueLayer, "query_hash": result.Meta.QueryHash, "compatibility_endpoint": endpoint,
	})
}

var errMetricsResourceNotFound = errors.New("metrics resource not found")

func (api metricsAPI) resolveMetricsResource(ctx context.Context, auth AuthContext, req MetricsQueryRequest) (MetricsQueryRequest, error) {
	if api.network == nil {
		return req, nil
	}
	if req.PortID != "" {
		port, err := api.network.GetPort(ctx, auth.TenantID, req.PortID)
		if err != nil {
			return MetricsQueryRequest{}, errMetricsResourceNotFound
		}
		device, err := api.network.GetDevice(ctx, auth.TenantID, port.DeviceID)
		if err != nil {
			return MetricsQueryRequest{}, errMetricsResourceNotFound
		}
		if req.DeviceID != "" && req.DeviceID != device.ID {
			return MetricsQueryRequest{}, errors.New("device_id does not match port")
		}
		if req.TargetID != "" && req.TargetID != device.TargetID {
			return MetricsQueryRequest{}, errors.New("target_id does not match port")
		}
		req.DeviceID = device.ID
		req.TargetID = device.TargetID
		req.PortIfIndex = port.IfIndex
		return req, nil
	}
	if req.DeviceID != "" && req.TargetID == "" {
		device, err := api.network.GetDevice(ctx, auth.TenantID, req.DeviceID)
		if err != nil {
			return MetricsQueryRequest{}, errMetricsResourceNotFound
		}
		req.TargetID = device.TargetID
	}
	if req.TargetID != "" && req.DeviceID == "" {
		devices, err := api.network.ListDevices(ctx, auth.TenantID)
		if err != nil {
			return MetricsQueryRequest{}, err
		}
		for _, device := range devices {
			if device.TargetID == req.TargetID {
				req.DeviceID = device.ID
				break
			}
		}
	}
	return req, nil
}

func parseMetricsQueryRequest(r *http.Request, auth AuthContext) (MetricsQueryRequest, error) {
	values := r.URL.Query()
	req := MetricsQueryRequest{
		TenantID:       auth.TenantID,
		TargetID:       ID(values.Get("target_id")),
		PortID:         ID(values.Get("port_id")),
		DeviceID:       ID(values.Get("device_id")),
		Metric:         values.Get("metric"),
		TimeMode:       TimeMode(values.Get("time_mode")),
		ValueMode:      MetricValueMode(values.Get("value_mode")),
		TrafficView:    normalizeTrafficView(TrafficViewMode(values.Get("traffic_view"))),
		Step:           parseDurationSeconds(values.Get("step")),
		CollectionStep: parseDurationSeconds(values.Get("collection_step")),
		Window:         FixedTimeWindow(values.Get("window")),
		MaxDataPoints:  parsePositiveInt(values.Get("max_data_points")),
		Func:           values.Get("func"),
	}
	if req.TimeMode == "" && r.URL.Path == "/api/v1/metrics/realtime" {
		req.TimeMode = TimeModeRealtime
	}
	if req.Metric == "" {
		return MetricsQueryRequest{}, errors.New("metric is required")
	}
	if req.TargetID == "" && req.PortID == "" {
		return MetricsQueryRequest{}, errors.New("target_id or port_id is required")
	}
	if start := values.Get("start"); start != "" {
		parsed, err := time.Parse(time.RFC3339, start)
		if err != nil {
			return MetricsQueryRequest{}, errors.New("invalid start")
		}
		req.Start = parsed
	}
	if end := values.Get("end"); end != "" {
		parsed, err := time.Parse(time.RFC3339, end)
		if err != nil {
			return MetricsQueryRequest{}, errors.New("invalid end")
		}
		req.End = parsed
	}
	return req, nil
}

func normalizeMetricsQueryTime(req MetricsQueryRequest) MetricsQueryRequest {
	if req.TimeMode == TimeModeRealtime {
		now := time.Now().UTC()
		req.End = now
		req.Start = now.Add(-10 * time.Minute)
		if req.Step == 0 {
			req.Step = 10 * time.Second
		}
		return req
	}
	if req.TimeMode == TimeModeFixed {
		duration, _, err := fixedWindowDuration(req.Window)
		if err != nil {
			return req
		}
		now := time.Now().UTC()
		req.Start = now.Add(-duration)
		req.End = now
		if req.Step == 0 {
			req.Step = AutoQueryStep(duration, req.MaxDataPoints)
		}
	}
	if req.TimeMode == TimeModeCustom && req.Step == 0 && req.End.After(req.Start) {
		req.Step = AutoQueryStep(req.End.Sub(req.Start), req.MaxDataPoints)
	}
	req = applyTrafficViewStep(req)
	req = applyRateWindow(req)
	// Traffic counters are collected once a minute; querying them at a finer
	// step yields staleness-filled duplicates whose deltas are zero.
	if isSNMPTrafficBpsMetric(req.Metric) && req.Step > 0 && req.Step < time.Minute {
		req.Step = time.Minute
	}
	return req
}

func applyTrafficViewStep(req MetricsQueryRequest) MetricsQueryRequest {
	if !isSNMPTrafficMetric(req.Metric) {
		return req
	}
	step := trafficViewQueryStep(req.TrafficView)
	if step <= 0 {
		return req
	}
	if req.TimeMode == TimeModeRealtime && step > time.Minute {
		step = time.Minute
	}
	if req.TimeMode != TimeModeRealtime && req.End.After(req.Start) {
		if min := minStepForRange(req.End.Sub(req.Start), maxVMPointsPerSeries); min > step {
			step = min
		}
	}
	req.Step = step
	return req
}

// applyRateWindow derives a Prometheus range window for rate() from the query
// step (>= 2 samples, floored at 1m) so counters render as per-second rates.
func applyRateWindow(req MetricsQueryRequest) MetricsQueryRequest {
	if req.Func != "rate" {
		return req
	}
	if req.RateWindow != "" {
		return req
	}
	window := req.Step * 2
	if window < time.Minute {
		window = time.Minute
	}
	req.RateWindow = prometheusRangeWindow(window)
	return req
}

func prometheusRangeWindow(d time.Duration) string {
	switch {
	case d >= 24*time.Hour:
		return "24h"
	case d >= time.Hour:
		return strconv.Itoa(int(d/time.Hour)) + "h"
	default:
		return strconv.Itoa(int((d+time.Minute-1)/time.Minute)) + "m"
	}
}

func parseDurationSeconds(value string) time.Duration {
	if value == "" {
		return 0
	}
	seconds, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func parsePositiveInt(value string) int {
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func metricsSelector(req MetricsQueryRequest) string {
	if selector := snmpCollectorTrafficSelector(req); selector != "" {
		return selector
	}
	labels := []string{labelMatcher("tenant_id", string(req.TenantID))}
	if req.TargetID != "" {
		labels = append(labels, labelMatcher("target_id", string(req.TargetID)))
	}
	if req.DeviceID != "" {
		labels = append(labels, labelMatcher("device_id", string(req.DeviceID)))
	}
	if req.PortID != "" {
		labels = append(labels, labelMatcher("port_id", string(req.PortID)))
	}
	expr := req.Metric + "{" + strings.Join(labels, ",") + "}"
	return wrapQueryFunc(expr, req.Func, req.RateWindow)
}

func snmpCollectorTrafficSelector(req MetricsQueryRequest) string {
	direction := SNMPTrafficDirection("")
	switch req.Metric {
	case MetricSNMPIfInBps:
		direction = SNMPTrafficIn
	case MetricSNMPIfOutBps:
		direction = SNMPTrafficOut
	default:
		return ""
	}
	metric, err := rawOctetsMetricForTrafficDirection(direction)
	if err != nil {
		return ""
	}
	labels := []string{labelMatcher("tenant_id", string(req.TenantID))}
	if req.TargetID != "" {
		labels = append(labels, labelMatcher("target_id", string(req.TargetID)))
	}
	if req.DeviceID != "" {
		labels = append(labels, labelMatcher("device_id", string(req.DeviceID)))
	}
	if req.PortID != "" {
		labels = append(labels, labelMatcher("port_id", string(req.PortID)))
	}
	if req.PortIfIndex != 0 {
		labels = append(labels, labelMatcher("if_index", strconv.FormatUint(req.PortIfIndex, 10)))
	}
	return metric + "{" + strings.Join(labels, ",") + "}"
}

func isSNMPTrafficBpsMetric(metric string) bool {
	return metric == MetricSNMPIfInBps || metric == MetricSNMPIfOutBps
}

func applyCounterRateToVMResponse(response VictoriaMetricsResponse) VictoriaMetricsResponse {
	response = cloneVMResponse(response)
	for i := range response.Data.Result {
		rate := ComputeCounterRate(response.Data.Result[i].Values, CounterRateOptions{CounterBits: 64, ToBits: true})
		response.Data.Result[i].Values = rate
	}
	return response
}

func (api metricsAPI) queryTrafficViewBySide(w http.ResponseWriter, r *http.Request, auth AuthContext, req MetricsQueryRequest) {
	if api.network == nil || req.DeviceID == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "traffic_view by side requires device_id scope", nil)
		return
	}
	side := PortSideCustomer
	if req.TrafficView == TrafficViewSupplier {
		side = PortSideProvider
	}
	ports, err := api.network.ListPorts(r.Context(), auth.TenantID, req.DeviceID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	// Side totals sum physical interfaces only; virtual/aggregate interfaces
	// would double-count traffic already counted on their members.
	ports = aggregatablePorts(ports)
	var portIDs []ID
	for _, port := range ports {
		policy, perr := api.network.GetPortPolicy(r.Context(), auth.TenantID, port.ID)
		if perr != nil {
			continue
		}
		if policy.SideType == side {
			portIDs = append(portIDs, port.ID)
		}
	}
	if len(portIDs) == 0 {
		WriteAPIJSON(w, http.StatusOK, VictoriaMetricsResponse{Status: "success"})
		return
	}
	mode := req.ValueMode
	if mode == "" {
		mode = MetricValueCorrected
	}
	policies, _ := loadPortPolicies(r.Context(), api.network, auth.TenantID, portIDs)
	resp, err := aggregateResolvedPortSeries(r.Context(), api.network, api.service, auth.TenantID, portIDs, req.Metric, "sum", mode, req.Start, req.End, req.Step, policies)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	resp = applyTrafficViewResponse(req, resp)
	WriteAPIJSON(w, http.StatusOK, resp)
}

// wrapQueryFunc wraps a selector expression for the requested function (rate).
func wrapQueryFunc(expr, fn, window string) string {
	if fn == "rate" {
		if window == "" {
			window = "5m"
		}
		return "rate(" + expr + "[" + window + "])"
	}
	return expr
}

func canAccessMetrics(auth AuthContext, req MetricsQueryRequest) bool {
	if auth.IsAdmin {
		return true
	}
	resource := ResourceRef{Type: ResourceTarget, ID: req.TargetID}
	if req.PortID != "" {
		resource = ResourceRef{Type: ResourcePort, ID: req.PortID, ParentID: req.TargetID}
	}
	return HasPermission(AccessRequest{
		TenantID: auth.TenantID,
		UserID:   auth.UserID,
		RoleIDs:  auth.RoleIDs,
		Action:   ActionView,
		Resource: resource,
	}, auth.Grants)
}

type MetricsAggregateRequest struct {
	Query     MetricsQueryRequest
	TargetIDs []ID
	PortIDs   []ID
	Method    string
}

func parseMetricsAggregateRequest(r *http.Request, auth AuthContext) (MetricsAggregateRequest, error) {
	values := r.URL.Query()
	query := MetricsQueryRequest{
		TenantID:       auth.TenantID,
		Metric:         values.Get("metric"),
		TimeMode:       TimeMode(values.Get("time_mode")),
		ValueMode:      MetricValueMode(values.Get("value_mode")),
		TrafficView:    normalizeTrafficView(TrafficViewMode(values.Get("traffic_view"))),
		Step:           parseDurationSeconds(values.Get("step")),
		CollectionStep: parseDurationSeconds(values.Get("collection_step")),
		Window:         FixedTimeWindow(values.Get("window")),
		MaxDataPoints:  parsePositiveInt(values.Get("max_data_points")),
		Func:           values.Get("func"),
	}
	if start := values.Get("start"); start != "" {
		parsed, err := time.Parse(time.RFC3339, start)
		if err != nil {
			return MetricsAggregateRequest{}, errors.New("invalid start")
		}
		query.Start = parsed
	}
	if end := values.Get("end"); end != "" {
		parsed, err := time.Parse(time.RFC3339, end)
		if err != nil {
			return MetricsAggregateRequest{}, errors.New("invalid end")
		}
		query.End = parsed
	}
	req := MetricsAggregateRequest{
		Query:     query,
		TargetIDs: splitIDs(values.Get("target_ids")),
		PortIDs:   splitIDs(values.Get("port_ids")),
		Method:    values.Get("aggregate"),
	}
	if req.Method == "" {
		req.Method = "sum"
	}
	if !isSupportedPromAggregate(req.Method) {
		return MetricsAggregateRequest{}, errors.New("unsupported aggregate")
	}
	if req.Query.Metric == "" {
		return MetricsAggregateRequest{}, errors.New("metric is required")
	}
	if len(req.TargetIDs) == 0 && len(req.PortIDs) == 0 {
		return MetricsAggregateRequest{}, errors.New("target_ids or port_ids are required")
	}
	if len(req.TargetIDs) > 0 {
		req.Query.TargetID = req.TargetIDs[0]
	}
	if len(req.PortIDs) > 0 {
		req.Query.PortID = req.PortIDs[0]
	}
	return req, nil
}

func (api metricsAPI) authorizeAggregate(ctx context.Context, auth AuthContext, req MetricsAggregateRequest) error {
	if auth.IsAdmin {
		return nil
	}
	for _, targetID := range req.TargetIDs {
		if !canAccessMetrics(auth, MetricsQueryRequest{TenantID: auth.TenantID, TargetID: targetID}) {
			return errors.New("Permission denied")
		}
	}
	for _, portID := range req.PortIDs {
		query := MetricsQueryRequest{TenantID: auth.TenantID, PortID: portID}
		resolved, err := api.resolveMetricsResource(ctx, auth, query)
		if err != nil {
			return errors.New("Permission denied")
		}
		if !canAccessMetrics(auth, resolved) {
			return errors.New("Permission denied")
		}
	}
	return nil
}

func aggregateMetricsSelector(req MetricsAggregateRequest) string {
	labels := []string{labelMatcher("tenant_id", string(req.Query.TenantID))}
	if len(req.PortIDs) > 0 {
		labels = append(labels, regexLabelMatcher("port_id", req.PortIDs))
	} else if len(req.TargetIDs) > 0 {
		labels = append(labels, regexLabelMatcher("target_id", req.TargetIDs))
	}
	inner := req.Query.Metric + "{" + strings.Join(labels, ",") + "}"
	expr := wrapQueryFunc(inner, req.Query.Func, req.Query.RateWindow)
	return req.Method + "(" + expr + ") and (count(" + inner + ") > 0)"
}

func splitIDs(value string) []ID {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	ids := make([]ID, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			ids = append(ids, ID(part))
		}
	}
	return ids
}

func isSupportedPromAggregate(value string) bool {
	switch value {
	case "sum", "avg", "max", "min", "count":
		return true
	default:
		return false
	}
}

func regexLabelMatcher(label string, ids []ID) string {
	values := make([]string, 0, len(ids))
	for _, id := range ids {
		values = append(values, regexpQuoteMeta(string(id)))
	}
	return label + `=~"` + strings.Join(values, "|") + `"`
}

func regexpQuoteMeta(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `.`, `\.`, `+`, `\+`, `*`, `\*`, `?`, `\?`, `(`, `\(`, `)`, `\)`, `[`, `\[`, `]`, `\]`, `{`, `\{`, `}`, `\}`, `|`, `\|`, `^`, `\^`, `$`, `\$`)
	return replacer.Replace(value)
}

func (api metricsAPI) vmQuery(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if !auth.IsAdmin {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "admin permission required", nil)
		return
	}
	query := r.URL.Query().Get("query")
	if query == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "query is required", nil)
		return
	}
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	step := r.URL.Query().Get("step")
	if step == "" {
		step = "60"
	}
	if api.service.Client == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "VictoriaMetrics is not configured", nil)
		return
	}
	stepDur, err := time.ParseDuration(step + "s")
	if err != nil {
		stepDur = time.Minute
	}
	resp, err := api.service.Client.QueryRange(r.Context(), RangeQuery{
		Query: query,
		Start: parseTimeRFC3339(start),
		End:   parseTimeRFC3339(end),
		Step:  stepDur,
	})
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.audit != nil {
		sum := sha256.Sum256([]byte(query))
		_ = api.audit.CreateAuditLog(r.Context(), AuditLog{
			TenantID: auth.TenantID, ActorID: auth.UserID, Action: "metrics.vmquery.sensitive_viewed",
			ResourceType: "dataset", ResourceID: "victoriametrics.raw_query",
			Detail: map[string]any{
				"query_hash": hex.EncodeToString(sum[:]), "start": start, "end": end, "step_seconds": uint64(stepDur / time.Second),
			},
		})
	}
	WriteAPIJSON(w, http.StatusOK, resp)
}

func parseTimeRFC3339(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Now().UTC().Add(-time.Hour)
	}
	return t
}
