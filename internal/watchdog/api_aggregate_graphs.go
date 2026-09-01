package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type aggregateGraphAPI struct {
	repo    AggregateGraphRepository
	network NetworkRepository
	metrics MetricsService
}

func registerAggregateGraphRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo AggregateGraphRepository, network NetworkRepository, metrics MetricsService) {
	api := aggregateGraphAPI{repo: repo, network: network, metrics: metrics}
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	viewTenant := RequirePermission(ActionView, TenantResource)
	mux.Handle("GET /api/v1/aggregate-graphs", auth(viewTenant(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/aggregate-graphs", auth(configureTenant(http.HandlerFunc(api.create))))
	mux.Handle("GET /api/v1/aggregate-graphs/{aggregate_graph_id}", auth(viewTenant(http.HandlerFunc(api.get))))
	mux.Handle("PATCH /api/v1/aggregate-graphs/{aggregate_graph_id}", auth(configureTenant(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/aggregate-graphs/{aggregate_graph_id}", auth(configureTenant(http.HandlerFunc(api.delete))))
	mux.Handle("GET /api/v1/aggregate-graphs/{aggregate_graph_id}/ports", auth(viewTenant(http.HandlerFunc(api.listPorts))))
	mux.Handle("PUT /api/v1/aggregate-graphs/{aggregate_graph_id}/ports", auth(configureTenant(http.HandlerFunc(api.replacePorts))))
	mux.Handle("GET /api/v1/aggregate-graphs/{aggregate_graph_id}/items", auth(viewTenant(http.HandlerFunc(api.listItems))))
	mux.Handle("PUT /api/v1/aggregate-graphs/{aggregate_graph_id}/items", auth(configureTenant(http.HandlerFunc(api.replaceItems))))
	mux.Handle("GET /api/v1/aggregate-graphs/{aggregate_graph_id}/series", auth(viewTenant(http.HandlerFunc(api.series))))
	mux.Handle("GET /api/v1/aggregate-graphs/{aggregate_graph_id}/data", auth(viewTenant(http.HandlerFunc(api.storedData))))
	mux.Handle("GET /api/v1/aggregate-graphs/{aggregate_graph_id}/summary", auth(viewTenant(http.HandlerFunc(api.summary))))
}

func (api aggregateGraphAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	graphs, err := api.repo.ListAggregateGraphs(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	// The library view groups graphs by device and shows what each graph
	// plots, so each item carries a membership summary alongside the graph.
	portDeviceIDs := map[ID]ID{}
	deviceNames := map[ID]string{}
	items := make([]map[string]any, 0, len(graphs))
	for _, graph := range graphs {
		items = append(items, api.graphListItem(r.Context(), auth.TenantID, graph, portDeviceIDs, deviceNames))
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api aggregateGraphAPI) graphListItem(ctx context.Context, tenantID ID, graph AggregateGraph, portDeviceIDs map[ID]ID, deviceNames map[ID]string) map[string]any {
	item := map[string]any{
		"ID":          graph.ID,
		"Name":        graph.Name,
		"Aggregation": graph.Aggregation,
		"ValueMode":   graph.ValueMode,
		"Unit":        graph.Unit,
		"Description": graph.Description,
		"CreatedAt":   graph.CreatedAt,
		"UpdatedAt":   graph.UpdatedAt,
		"PortCount":   0,
		"Metrics":     []string{},
		"Devices":     []map[string]string{},
	}
	if graphItems, err := api.repo.ListAggregateGraphItems(ctx, tenantID, graph.ID); err == nil {
		metrics := make([]string, 0, len(graphItems))
		seen := map[string]bool{}
		for _, graphItem := range graphItems {
			if graphItem.Metric != "" && !seen[graphItem.Metric] {
				seen[graphItem.Metric] = true
				metrics = append(metrics, graphItem.Metric)
			}
		}
		item["Metrics"] = metrics
	}
	ports, err := api.repo.ListAggregateGraphPorts(ctx, tenantID, graph.ID)
	if err != nil || api.network == nil {
		return item
	}
	item["PortCount"] = len(ports)
	devices := make([]map[string]string, 0, 2)
	seenDevices := map[ID]bool{}
	for _, link := range ports {
		if link.PortID == "" {
			continue
		}
		deviceID, ok := portDeviceIDs[link.PortID]
		if !ok {
			port, perr := api.network.GetPort(ctx, tenantID, link.PortID)
			if perr != nil {
				continue
			}
			deviceID = port.DeviceID
			portDeviceIDs[link.PortID] = deviceID
		}
		if deviceID == "" || seenDevices[deviceID] {
			continue
		}
		seenDevices[deviceID] = true
		name, ok := deviceNames[deviceID]
		if !ok {
			if device, derr := api.network.GetDevice(ctx, tenantID, deviceID); derr == nil {
				name = firstNonEmptySNMPString(device.SysName, device.Model, string(device.ID))
			} else {
				name = string(deviceID)
			}
			deviceNames[deviceID] = name
		}
		devices = append(devices, map[string]string{"ID": string(deviceID), "Name": name})
	}
	item["Devices"] = devices
	return item
}

func (api aggregateGraphAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	graph, err := api.repo.GetAggregateGraph(r.Context(), auth.TenantID, ID(r.PathValue("aggregate_graph_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Aggregate graph not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, graph)
}

func (api aggregateGraphAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	graph, err := decodeAggregateGraphRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := authorizeAggregateValueMode(auth, graph.ValueMode); err != nil {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, err.Error(), nil)
		return
	}
	graph.TenantID = auth.TenantID
	created, err := api.repo.CreateAggregateGraph(r.Context(), graph)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api aggregateGraphAPI) patch(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	graph, err := decodeAggregateGraphRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := authorizeAggregateValueMode(auth, graph.ValueMode); err != nil {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, err.Error(), nil)
		return
	}
	graph.ID = ID(r.PathValue("aggregate_graph_id"))
	graph.TenantID = auth.TenantID
	updated, err := api.repo.UpdateAggregateGraph(r.Context(), graph)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api aggregateGraphAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if err := api.repo.DeleteAggregateGraph(r.Context(), auth.TenantID, ID(r.PathValue("aggregate_graph_id"))); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api aggregateGraphAPI) listItems(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	items, err := api.repo.ListAggregateGraphItems(r.Context(), auth.TenantID, ID(r.PathValue("aggregate_graph_id")))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api aggregateGraphAPI) replaceItems(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	items, err := decodeAggregateGraphItemsRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	graphID := ID(r.PathValue("aggregate_graph_id"))
	for i := range items {
		if items[i].Metric == "" {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "aggregate graph item metric is required", nil)
			return
		}
		if items[i].Direction != "" && !isSupportedItemDirection(items[i].Direction) {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported item direction", nil)
			return
		}
		items[i].GraphID = graphID
		items[i].TenantID = auth.TenantID
	}
	if err := api.repo.ReplaceAggregateGraphItems(r.Context(), auth.TenantID, graphID, items); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (api aggregateGraphAPI) listPorts(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	ports, err := api.repo.ListAggregateGraphPorts(r.Context(), auth.TenantID, ID(r.PathValue("aggregate_graph_id")))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": ports})
}

func (api aggregateGraphAPI) replacePorts(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	ports, err := decodeAggregateGraphPortsRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	graphID := ID(r.PathValue("aggregate_graph_id"))
	for i := range ports {
		if ports[i].PortID == "" {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "aggregate graph port id is required", nil)
			return
		}
		if api.network != nil {
			if _, err := api.network.GetPort(r.Context(), auth.TenantID, ports[i].PortID); err != nil {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "aggregate graph port must be a discovered network port", nil)
				return
			}
		}
		ports[i].AggregateGraphID = graphID
		ports[i].TenantID = auth.TenantID
	}
	if err := api.repo.ReplaceAggregateGraphPorts(r.Context(), auth.TenantID, graphID, ports); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (api aggregateGraphAPI) series(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	graphID := ID(r.PathValue("aggregate_graph_id"))
	graph, err := api.repo.GetAggregateGraph(r.Context(), auth.TenantID, graphID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Aggregate graph not found", nil)
		return
	}
	portLinks, err := api.repo.ListAggregateGraphPorts(r.Context(), auth.TenantID, graphID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	portIDs := make([]ID, 0, len(portLinks))
	for _, link := range portLinks {
		if link.PortID != "" {
			portIDs = append(portIDs, link.PortID)
		}
	}
	query := parseAggregateGraphSeriesQuery(r)
	query.TenantID = auth.TenantID
	if query.ValueMode == "" {
		query.ValueMode = graph.ValueMode
	}
	items, err := api.repo.ListAggregateGraphItems(r.Context(), auth.TenantID, graphID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if len(items) == 0 {
		WriteAPIJSON(w, http.StatusOK, victoriaMetricsResponse(nil))
		return
	}
	// Resolve window/time_mode into concrete start/end/step BEFORE querying;
	// the raw query carries zero times and step, which the per-port series
	// path would reject ("unsupported query step") on every request.
	query.Metric = items[0].Metric
	query = normalizeMetricsQueryTime(query)
	query.Metric = ""
	validationQuery := query
	validationQuery.Metric = items[0].Metric
	if err := ValidateMetricsQuery(validationQuery, auth.IsAdmin); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	authReq := MetricsAggregateRequest{Query: query, PortIDs: portIDs, Method: string(graph.Aggregation)}
	if err := api.authorizeSeries(r.Context(), auth, authReq); err != nil {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, err.Error(), nil)
		return
	}
	if api.metrics.Client == nil {
		WriteAPIJSON(w, http.StatusOK, map[string]any{"graph": graph, "items": items, "port_ids": portIDs})
		return
	}
	splitSide := r.URL.Query().Has("split_side_type")
	response, err := buildAggregateGraphSeries(r.Context(), api.network, api.metrics, graph, items, portIDs, query, splitSide)
	if err != nil {
		if errors.Is(err, ErrVictoriaMetricsUnavailable) {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "VictoriaMetrics is unavailable", nil)
			return
		}
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api aggregateGraphAPI) storedData(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	graphID := ID(r.PathValue("aggregate_graph_id"))
	start, end, ok := parseAggregateGraphRange(r)
	if !ok {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid start/end time", nil)
		return
	}
	points, err := api.repo.ListAggregateGraphData(r.Context(), auth.TenantID, graphID, start, end)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": points})
}

func (api aggregateGraphAPI) summary(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	graphID := ID(r.PathValue("aggregate_graph_id"))
	start, end, ok := parseAggregateGraphRange(r)
	if !ok {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid start/end time", nil)
		return
	}
	points, err := api.repo.ListAggregateGraphData(r.Context(), auth.TenantID, graphID, start, end)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	// Combine all items (e.g. in+out) at each timestamp into one traffic series.
	combined := map[time.Time]float64{}
	for _, point := range points {
		combined[point.Timestamp] += point.Value
	}
	samples := make([]Sample, 0, len(combined))
	for ts, value := range combined {
		samples = append(samples, Sample{Time: ts, Value: value})
	}
	sortSamples(samples)
	summary := aggregateGraphSummaryFromSamples(samples)
	WriteAPIJSON(w, http.StatusOK, summary)
}

func parseAggregateGraphRange(r *http.Request) (time.Time, time.Time, bool) {
	values := r.URL.Query()
	end := time.Now().UTC()
	start := end.Add(-24 * time.Hour)
	ok := true
	if s := values.Get("start"); s != "" {
		parsed, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return time.Time{}, time.Time{}, false
		}
		start = parsed
	}
	if e := values.Get("end"); e != "" {
		parsed, err := time.Parse(time.RFC3339, e)
		if err != nil {
			return time.Time{}, time.Time{}, false
		}
		end = parsed
	}
	if !end.After(start) {
		ok = false
	}
	return start, end, ok
}

type aggregateGraphSummary struct {
	Samples    int     `json:"samples"`
	P95        float64 `json:"p95"`
	Peak       float64 `json:"peak"`
	Average    float64 `json:"average"`
	TotalBytes uint64  `json:"total_bytes"`
}

func aggregateGraphSummaryFromSamples(samples []Sample) aggregateGraphSummary {
	if len(samples) == 0 {
		return aggregateGraphSummary{}
	}
	values := sampleValues(samples)
	step := time.Minute * 5
	if len(samples) > 1 {
		step = samples[len(samples)-1].Time.Sub(samples[0].Time) / time.Duration(len(samples)-1)
	}
	return aggregateGraphSummary{
		Samples:    len(samples),
		P95:        Percentile(values, 95),
		Peak:       NthPeak(values, 1),
		Average:    Average(values),
		TotalBytes: estimateBillingBytes(samples, step),
	}
}

func (api aggregateGraphAPI) authorizeSeries(ctx context.Context, auth AuthContext, req MetricsAggregateRequest) error {
	if auth.IsAdmin {
		return nil
	}
	if api.network == nil {
		return nil
	}
	for _, portID := range req.PortIDs {
		port, err := api.network.GetPort(ctx, auth.TenantID, portID)
		if err != nil {
			return errors.New("Permission denied")
		}
		device, err := api.network.GetDevice(ctx, auth.TenantID, port.DeviceID)
		if err != nil {
			return errors.New("Permission denied")
		}
		resolved := MetricsQueryRequest{
			TenantID: auth.TenantID,
			PortID:   portID,
			DeviceID: device.ID,
			TargetID: device.TargetID,
		}
		if !canAccessMetrics(auth, resolved) {
			return errors.New("Permission denied")
		}
	}
	return nil
}

func decodeAggregateGraphRequest(r *http.Request) (AggregateGraph, error) {
	defer r.Body.Close()
	var graph AggregateGraph
	if err := json.NewDecoder(r.Body).Decode(&graph); err != nil {
		return AggregateGraph{}, err
	}
	if graph.Name == "" {
		return AggregateGraph{}, errors.New("aggregate graph name is required")
	}
	if graph.Aggregation != "" && !isSupportedAggregateGraphMethod(graph.Aggregation) {
		return AggregateGraph{}, errors.New("unsupported aggregation")
	}
	if graph.ValueMode != "" && !isSupportedMetricValueMode(graph.ValueMode) {
		return AggregateGraph{}, errors.New("unsupported value mode")
	}
	return graph, nil
}

func decodeAggregateGraphPortsRequest(r *http.Request) ([]AggregateGraphPort, error) {
	defer r.Body.Close()
	var body struct {
		Ports []AggregateGraphPort `json:"ports"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Ports, nil
}

func decodeAggregateGraphItemsRequest(r *http.Request) ([]AggregateGraphItem, error) {
	defer r.Body.Close()
	var body struct {
		Items []AggregateGraphItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	for i := range body.Items {
		if body.Items[i].ID == "" {
			body.Items[i].ID = ID(stableID("agitem", fmt.Sprintf("%d", i), body.Items[i].Metric))
		}
	}
	return body.Items, nil
}

func isSupportedItemDirection(direction string) bool {
	switch direction {
	case "in", "out", "other", "":
		return true
	default:
		return false
	}
}

func parseAggregateGraphSeriesQuery(r *http.Request) MetricsQueryRequest {
	values := r.URL.Query()
	timeMode := TimeMode(values.Get("time_mode"))
	if timeMode == "" {
		timeMode = TimeModeFixed
	}
	return MetricsQueryRequest{
		TimeMode:       timeMode,
		Step:           parseDurationSeconds(values.Get("step")),
		CollectionStep: parseDurationSeconds(values.Get("collection_step")),
		Window:         FixedTimeWindow(values.Get("window")),
		MaxDataPoints:  parsePositiveInt(values.Get("max_data_points")),
		ValueMode:      MetricValueMode(values.Get("value_mode")),
	}
}

func isSupportedAggregateGraphMethod(method AggregateGraphMethod) bool {
	switch method {
	case AggregateSum, AggregateAvg, AggregateMax, AggregateMin, AggregateCount:
		return true
	default:
		return false
	}
}

func isSupportedMetricValueMode(mode MetricValueMode) bool {
	switch mode {
	case MetricValueCorrected, MetricValueRaw, MetricValueBoth:
		return true
	default:
		return false
	}
}

func authorizeAggregateValueMode(auth AuthContext, mode MetricValueMode) error {
	if mode == "" || mode == MetricValueCorrected {
		return nil
	}
	req := AccessRequest{
		TenantID: auth.TenantID,
		UserID:   auth.UserID,
		RoleIDs:  auth.RoleIDs,
		Action:   ActionAdmin,
		Resource: ResourceRef{Type: ResourceTenant, ID: auth.TenantID},
	}
	if auth.IsAdmin || HasPermission(req, auth.Grants) {
		return nil
	}
	return errors.New("raw value mode requires admin permission")
}
