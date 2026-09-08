package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func TestFlowReportSpecValidationIsKindSpecific(t *testing.T) {
	base := flowAggregateQueryParameters{Report: &flowReportSpec{SchemaVersion: 1, Kind: flowReportEndpoints}}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, base)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("missing endpoint side error = %#v", err)
	}

	base.Report.Side = "source"
	decoded, err := decodeFlowAggregateQueryParameters(mustJSON(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Metric != flowquery.MetricEstimatedBPS || decoded.TopN != 20 || decoded.TargetPoints != flowquery.DefaultTargetPoints || decoded.Timezone != "UTC" {
		t.Fatalf("defaults = %+v", decoded)
	}

	base.Report = &flowReportSpec{SchemaVersion: 1, Kind: flowReportDimensions, GroupBy: flowquery.DimensionSourceIP}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, base)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("unsupported grouping error = %#v", err)
	}

	base.Report = &flowReportSpec{
		SchemaVersion: 1, Kind: flowReportOverview,
		PeakWindows: []flowReportPeakWindow{{Days: []uint8{1, 1}, StartLocal: "20:00", EndLocal: "23:00"}},
	}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, base)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("duplicate weekday error = %#v", err)
	}

	base.Report = &flowReportSpec{SchemaVersion: 1, Kind: flowReportOverview, PanelIDs: []string{"total", "total"}}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, base)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("duplicate panel error = %#v", err)
	}
	base.Report.PanelIDs = []string{"vpn_findings"}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, base)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("foreign panel error = %#v", err)
	}
	base.Report = &flowReportSpec{SchemaVersion: 1, Kind: flowReportEndpoints, Side: "source", PanelIDs: []string{"endpoint_category_in"}}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, base)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("endpoint prerequisite error = %#v", err)
	}
}

func TestFlowReportDimensionPointsReadsAggregateAndJointWireShapes(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{name: "aggregate", data: `{"points":[{"bucket":"2026-09-08T09:00:00Z","dimension_value":"192.0.2.1","geo_version":"geo-1","classification_version":7,"value":3,"generated_at":"2026-09-08T09:01:00Z"}]}`},
		{name: "joint", data: `{"points":[{"bucket":"2026-09-08T09:00:00Z","dimension_values":["2001:db8::1"],"geo_version":"geo-1","classification_version":7,"value":4,"observed_at":"2026-09-08T09:01:00Z"}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			points, err := flowReportDimensionPoints(json.RawMessage(test.data))
			if err != nil || len(points) != 1 || points[0].DimensionValue == "" || points[0].GeneratedAt.IsZero() {
				t.Fatalf("points=%+v err=%v", points, err)
			}
		})
	}
}

func TestFlowEndpointReportTableComposesBeforeServerFilterAndPagination(t *testing.T) {
	tablePanel := func(id string, rows []flowTableRow) flowReportPanel {
		data, err := json.Marshal(struct {
			Table flowTablePage `json:"table"`
		}{Table: flowTablePage{Items: rows, Total: len(rows), Limit: 100, FilterOptions: map[string][]flowTableFilterOption{}}})
		if err != nil {
			t.Fatal(err)
		}
		return flowReportPanel{ID: id, Status: "ready", Data: data}
	}
	categoryPanel := func(id string, address, category string, total float64) flowReportPanel {
		data, err := json.Marshal(struct {
			Summaries []struct {
				Address  string  `json:"address"`
				Category string  `json:"category"`
				Total    float64 `json:"total"`
			} `json:"summaries"`
		}{Summaries: []struct {
			Address  string  `json:"address"`
			Category string  `json:"category"`
			Total    float64 `json:"total"`
		}{{Address: address, Category: category, Total: total}}})
		if err != nil {
			t.Fatal(err)
		}
		return flowReportPanel{ID: id, Status: "ready", Data: data}
	}
	panels := []flowReportPanel{
		tablePanel("endpoint", []flowTableRow{
			{Label: "192.0.2.1", Path: []string{"192.0.2.1"}, Last: 30, Total: 300},
			{Label: "192.0.2.2", Path: []string{"192.0.2.2"}, Last: 10, Total: 100},
		}),
		tablePanel("endpoint_in", []flowTableRow{{Label: "192.0.2.1", Path: []string{"192.0.2.1"}, Last: 10, Total: 100}}),
		tablePanel("endpoint_out", []flowTableRow{{Label: "192.0.2.1", Path: []string{"192.0.2.1"}, Last: 20, Total: 200}}),
		categoryPanel("endpoint_category_in", "192.0.2.1", "overseas", 40),
		categoryPanel("endpoint_category_out", "192.0.2.1", "overseas", 80),
	}
	businessData := mustJSON(t, struct {
		Points []flowquery.JointPoint `json:"points"`
	}{Points: []flowquery.JointPoint{{DimensionValues: []string{"192.0.2.1", "customer-a"}}}})
	panels = append(panels, flowReportPanel{ID: "endpoint_business", Status: "ready", Data: businessData})
	request := flowTableRequest{
		SortBy: "overseas", SortDirection: "desc", Limit: 25,
		Filters: map[string][]string{"overseas": []string{"40/80"}},
	}
	composed, err := composeFlowEndpointReportTable(panels, request)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Table flowEndpointReportPage `json:"table"`
	}
	if err := json.Unmarshal(composed[0].Data, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Table.Total != 1 || len(envelope.Table.Items) != 1 || endpointAddress(envelope.Table.Items[0].flowTableRow) != "192.0.2.1" {
		t.Fatalf("table = %+v", envelope.Table)
	}
	overseas := envelope.Table.Items[0].Categories["overseas"]
	if overseas.Inbound != 40 || overseas.Outbound != 80 || overseas.InboundShare == nil || *overseas.InboundShare != .4 ||
		overseas.OutboundShare == nil || *overseas.OutboundShare != .4 {
		t.Fatalf("overseas = %+v", overseas)
	}
	if len(envelope.Table.FilterOptions["overseas"]) != 2 {
		t.Fatalf("filter options = %+v", envelope.Table.FilterOptions["overseas"])
	}
	if len(envelope.Table.Items[0].Businesses) != 1 || envelope.Table.Items[0].Businesses[0] != "customer-a" {
		t.Fatalf("businesses = %+v", envelope.Table.Items[0].Businesses)
	}
	if err := normalizeFlowEndpointTableRequest(&request); err != nil {
		t.Fatalf("derived endpoint field rejected: %v", err)
	}
}

func TestFlowBusinessMatrixTableComposesBeforeServerFilterAndPagination(t *testing.T) {
	bucket := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	panel := func(id string, points []flowquery.JointPoint) flowReportPanel {
		return flowReportPanel{ID: id, Status: "ready", Data: mustJSON(t, struct {
			Points []flowquery.JointPoint `json:"points"`
		}{Points: points})}
	}
	panels := []flowReportPanel{
		panel("business_category_in", []flowquery.JointPoint{
			{Bucket: bucket, DimensionValues: []string{"customer-a", "overseas"}, Value: 30},
			{Bucket: bucket, DimensionValues: []string{"customer-b", "unknown"}, Value: 10},
		}),
		panel("business_category_out", []flowquery.JointPoint{
			{Bucket: bucket, DimensionValues: []string{"customer-a", "overseas"}, Value: 20},
			{Bucket: bucket, DimensionValues: []string{"customer-b", "unknown"}, Value: 20},
		}),
	}
	request := flowTableRequest{Search: "customer-a", SortBy: "total", SortDirection: "desc", Limit: 1}
	composed, err := composeFlowBusinessMatrixTable(panels, request)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Table flowBusinessMatrixPage `json:"matrix_table"`
	}
	if err := json.Unmarshal(composed[0].Data, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Table.Total != 1 || len(envelope.Table.Items) != 1 || envelope.Table.Items[0].Business != "customer-a" {
		t.Fatalf("table = %+v", envelope.Table)
	}
	overseas := envelope.Table.Items[0].Categories["overseas"]
	if overseas.Inbound != 30 || overseas.Outbound != 20 || overseas.InboundShare == nil || *overseas.InboundShare != .75 || overseas.OutboundShare == nil || *overseas.OutboundShare != .5 {
		t.Fatalf("overseas = %+v", overseas)
	}
	if len(envelope.Table.FilterOptions["business"]) != 2 {
		t.Fatalf("filter options = %+v", envelope.Table.FilterOptions)
	}
}

func TestFlowVPNDistributionTableUsesServerPagingSearchSortAndFilters(t *testing.T) {
	source := []flowReportCount{
		{Value: "443/tcp", Count: 4, Bytes: 40},
		{Value: "8443/tcp", Count: 8, Bytes: 80},
		{Value: "53/udp", Count: 5, Bytes: 50},
	}
	page := buildFlowVPNDistributionTable(source, flowTableRequest{
		Search: "tcp", SortBy: "count", SortDirection: "desc", Limit: 1,
		Filters: map[string][]string{"bytes": {"80"}},
	})
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].Value != "8443/tcp" {
		t.Fatalf("page = %+v", page)
	}
	if len(page.FilterOptions["value"]) != 3 || len(page.FilterOptions["bytes"]) != 3 {
		t.Fatalf("filter options = %+v", page.FilterOptions)
	}
}

func TestFlowReportTableValidationIsKindSpecific(t *testing.T) {
	parameters := flowAggregateQueryParameters{Report: &flowReportSpec{
		SchemaVersion: 1, Kind: flowReportOverview,
		Tables: map[string]*flowTableRequest{"business_matrix": {Limit: 25}},
	}}
	decoded, err := decodeFlowAggregateQueryParameters(mustJSON(t, parameters))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Report.Tables["business_matrix"].SortBy != "total" {
		t.Fatalf("table = %+v", decoded.Report.Tables["business_matrix"])
	}
	parameters.Report.Tables = map[string]*flowTableRequest{"port_distribution": {Limit: 25}}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, parameters)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("foreign report table error = %#v", err)
	}
}

func TestFlowReportPanelSelectionOnlyExecutesSelectedPanels(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	aggregate := &flowReportAggregateRunnerStub{from: from}
	provider := ClickHouseFlowQueryProvider{
		Runner: aggregate, JointRunner: &flowReportJointRunnerStub{from: from},
		Readiness: flowReadinessStub{}, Now: func() time.Time { return from.Add(2 * time.Hour) },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","top_n":20,
			"report":{"schema_version":1,"kind":"overview","panel_ids":["category_in"]}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var report flowReportData
	if err := json.Unmarshal(result.Data, &report); err != nil {
		t.Fatal(err)
	}
	if aggregate.calls != 1 || len(report.Panels) != 1 || report.Panels[0].ID != "category_in" {
		t.Fatalf("calls=%d report=%+v", aggregate.calls, report)
	}
}

func TestFlowReportProviderComposesOverviewInOneProviderExecution(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	now := from.Add(2 * time.Hour)
	aggregate := &flowReportAggregateRunnerStub{from: from}
	joint := &flowReportJointRunnerStub{from: from}
	provider := ClickHouseFlowQueryProvider{
		Runner: aggregate, JointRunner: joint, Readiness: flowReadinessStub{}, Now: func() time.Time { return now },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","top_n":20,"target_points":100,"timezone":"Asia/Singapore",
			"report":{"schema_version":1,"kind":"overview","display_mode":"value"}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.calls != 4 || joint.calls != 2 {
		t.Fatalf("aggregate calls=%d joint calls=%d", aggregate.calls, joint.calls)
	}
	var report flowReportData
	if err := json.Unmarshal(result.Data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Kind != flowReportOverview || len(report.Panels) != 5 || report.Panels[0].ID != "total" || report.Panels[4].ID != "business_category_out" {
		t.Fatalf("report = %+v", report)
	}
	for _, panel := range report.Panels {
		if panel.Status != "ready" || len(panel.Data) == 0 {
			t.Fatalf("panel = %+v", panel)
		}
	}
	if result.Versions["dimension_snapshot_id"] != "snapshot-1" || result.Versions["classification_version"] != "7" || result.AsOf != from.Add(time.Minute) {
		t.Fatalf("result = %+v", result)
	}
	if report.Range.RequestedFrom != from || report.Range.EffectiveTo != from.Add(time.Hour) || report.Plan.Source == "" ||
		report.Watermark.LatestCompleteBucket != from || report.Completeness.CompleteRatio != 1 {
		t.Fatalf("report metadata = %+v", report)
	}
}

func TestFlowReportProviderComposesEndpointPageBeforePagination(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	aggregate := &flowReportAggregateRunnerStub{from: from}
	joint := &flowReportJointRunnerStub{from: from}
	provider := ClickHouseFlowQueryProvider{
		Runner: aggregate, JointRunner: joint, Readiness: flowReadinessStub{}, Now: func() time.Time { return from.Add(2 * time.Hour) },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		Limit: 250_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","top_n":20,"target_points":100,
			"table":{"sort_by":"business","sort_direction":"asc","limit":25,"filters":{"business":["business-a"]}},
			"report":{"schema_version":1,"kind":"endpoints","side":"source"}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var report flowReportData
	if err := json.Unmarshal(result.Data, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Panels) != 7 || joint.calls != 1 {
		t.Fatalf("panels=%d aggregate_calls=%d joint_calls=%d", len(report.Panels), aggregate.calls, joint.calls)
	}
	var endpoint struct {
		Table flowEndpointReportPage `json:"table"`
	}
	if err := json.Unmarshal(report.Panels[1].Data, &endpoint); err != nil {
		t.Fatal(err)
	}
	if endpoint.Table.Total != 1 || len(endpoint.Table.Items) != 1 || endpoint.Table.Items[0].Businesses[0] != "business-a" ||
		len(endpoint.Table.FilterOptions["overseas"]) == 0 {
		t.Fatalf("endpoint table = %+v", endpoint.Table)
	}
}

type flowReportFailingRunner struct{ calls int }

func (s *flowReportFailingRunner) Run(_ context.Context, compiled flowquery.Compiled) (flowquery.Result, error) {
	s.calls++
	if s.calls == 2 {
		return flowquery.Result{}, errors.New("clickhouse failed")
	}
	return (&flowReportAggregateRunnerStub{from: compiled.From}).Run(context.Background(), compiled)
}

func TestFlowReportRequiredPanelFailureReturnsNoPartialReport(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowReportFailingRunner{}, JointRunner: &flowReportJointRunnerStub{from: from},
		Readiness: flowReadinessStub{}, Now: func() time.Time { return from.Add(2 * time.Hour) },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","report":{"schema_version":1,"kind":"overview"}}`),
	})
	if err == nil || len(result.Data) != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestFlowReportProviderMarksOptionalJointPanelUnavailable(t *testing.T) {
	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowReportAggregateRunnerStub{from: from}, JointRunner: &flowReportJointRunnerStub{from: from},
		Readiness: flowReadinessStub{}, Now: func() time.Time { return from.Add(8 * 24 * time.Hour) },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(7 * 24 * time.Hour),
		Limit: 250_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","top_n":20,"report":{"schema_version":1,"kind":"overview"}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var report flowReportData
	if err := json.Unmarshal(result.Data, &report); err != nil {
		t.Fatal(err)
	}
	unavailable := 0
	for _, panel := range report.Panels {
		if strings.HasPrefix(panel.ID, "business_category_") && panel.Status == "unavailable" {
			unavailable++
		}
	}
	if unavailable != 2 || len(report.Warnings) != 2 {
		t.Fatalf("report = %+v", report)
	}
}

func TestVPNReportSummaryKeepsEvidenceAndTrafficSemanticsSeparate(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	items := []VPNFinding{
		{LocalIP: "10.0.0.1", PrimaryRemotePort: 443, LocalToRemoteBytes: 100, RemoteToLocalBytes: 200, RiskLevel: "high", CompleteRatio: .9, DecisionRuleID: "quic-long", RuleSetVersion: "rules-2", SourceGeneration: 2, WindowEnd: from.Add(5 * time.Minute)},
		{LocalIP: "10.0.0.1", PrimaryRemotePort: 443, LocalToRemoteBytes: 50, RemoteToLocalBytes: 25, CompleteRatio: 1, DecisionRuleID: "unknown", RuleSetVersion: "rules-2", SourceGeneration: 3, WindowEnd: from.Add(6 * time.Minute)},
	}
	summary := summarizeVPNFindings(items, from, from.Add(time.Hour), 100)
	if summary.FindingCount != 2 || summary.SuspectedHosts != 1 || summary.HighRiskHosts != 1 || summary.ActivePorts != 1 ||
		summary.OutboundBytes != 150 || summary.InboundBytes != 225 || summary.TotalBytes != 375 || summary.MinimumCompleteRatio != .9 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(summary.PortDistribution) != 1 || summary.PortDistribution[0].Count != 2 || len(summary.TypeDistribution) != 2 {
		t.Fatalf("distributions = ports=%+v types=%+v", summary.PortDistribution, summary.TypeDistribution)
	}
}

func TestOverseasVPNShareUsesOneAdditiveUnitAndExplicitUnknownGeo(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowReportAggregateRunnerStub{from: from}, Readiness: flowReadinessStub{},
		VPNFindings: flowReportVPNRepositoryStub{items: []VPNFinding{
			{WindowEnd: from.Add(time.Minute), RemoteCountry: "US", RemoteToLocalBytes: 20, LocalToRemoteBytes: 10, CompleteRatio: .9, DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-1", ClassificationVersion: 7},
			{WindowEnd: from.Add(2 * time.Minute), RemoteCountry: "", RemoteToLocalBytes: 4, LocalToRemoteBytes: 6, CompleteRatio: 1, DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-1", ClassificationVersion: 7},
		}},
		Now: func() time.Time { return from.Add(2 * time.Hour) },
	}
	parameters := flowAggregateQueryParameters{
		Metric: flowquery.MetricEstimatedBPS, Timezone: "UTC", TargetPoints: 100,
		Report: &flowReportSpec{SchemaVersion: 1, Kind: flowReportOverseas},
	}
	panel, err := provider.overseasVPNShareReportPanel(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour), Limit: 10_000, ValueLayer: QueryValueCustomer,
	}, parameters)
	if err != nil || panel.Status != "ready" || panel.Meta.Unit != "ratio" || panel.Meta.Completeness.CompleteRatio != .9 || !panel.Meta.Completeness.Partial {
		t.Fatalf("panel=%+v err=%v", panel, err)
	}
	var data struct {
		Points []flowOverseasVPNSharePoint `json:"points"`
	}
	if err := json.Unmarshal(panel.Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Points) != 3 || data.Points[2].VPNBytes != 30 || data.Points[2].TotalBytes != 200 || data.Points[2].Ratio == nil || *data.Points[2].Ratio != .15 || data.Points[2].UnknownGeo != 10 {
		t.Fatalf("data=%+v", data)
	}
}

func TestFlowReportHTTPContractAndCapabilities(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	provider := &queryProviderStub{query: func(_ context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
		if request.Dataset.Key != FlowTrafficDataset || request.TenantID != "tenant-a" || request.Limit != defaultQueryMaxRows {
			t.Fatalf("provider request = %#v", request)
		}
		parameters, err := decodeFlowAggregateQueryParameters(request.Parameters)
		if err != nil {
			t.Fatal(err)
		}
		if parameters.Report == nil || parameters.Report.Kind != flowReportDimensions || parameters.Report.GroupBy != flowquery.DimensionGeoProvince {
			t.Fatalf("parameters = %+v", parameters)
		}
		return QueryProviderResult{Data: json.RawMessage(`{"schema_version":1,"kind":"dimensions","display_mode":"share","panels":[]}`)}, nil
	}}
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	providers := NewQueryProviderRegistry()
	if err := providers.Register(QueryProviderRegistration{Kind: DatasetProviderClickHouse, Provider: provider, Enabled: true, MaxConcurrent: 2}); err != nil {
		t.Fatal(err)
	}
	gateway, err := NewQueryGateway(registries, nil, repo, providers)
	if err != nil {
		t.Fatal(err)
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		QueryGateway: gateway,
	})

	capabilities := httptest.NewRecorder()
	router.ServeHTTP(capabilities, httptest.NewRequest(http.MethodGet, "/api/v1/flow/reports/capabilities", nil))
	if capabilities.Code != http.StatusOK || !strings.Contains(capabilities.Body.String(), `"endpoints"`) || !strings.Contains(capabilities.Body.String(), `"previous_month"`) {
		t.Fatalf("capabilities status=%d body=%s", capabilities.Code, capabilities.Body.String())
	}

	body := `{
		"from":"2026-09-08T09:00:00Z","to":"2026-09-08T10:00:00Z",
		"metric":"estimated_bps","timezone":"Asia/Singapore","top_n":20,
		"report":{"schema_version":1,"kind":"dimensions","group_by":"geo.province","display_mode":"share"}
	}`
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/flow/reports/query", strings.NewReader(body))
	request.Header.Set(RequestIDHeader, "report-request-a")
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(response.Body.String(), `"schema_version":"query-result-v2"`) {
		t.Fatalf("status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}

	unknown := httptest.NewRecorder()
	router.ServeHTTP(unknown, httptest.NewRequest(http.MethodPost, "/api/v1/flow/reports/query", strings.NewReader(strings.Replace(body, `"metric":`, `"tenant_id":"tenant-b","metric":`, 1))))
	if unknown.Code != http.StatusBadRequest || !strings.Contains(unknown.Body.String(), "QUERY_INVALID") {
		t.Fatalf("unknown status=%d body=%s", unknown.Code, unknown.Body.String())
	}
}

type flowReportAggregateRunnerStub struct {
	from  time.Time
	calls int
}

func (s *flowReportAggregateRunnerStub) Run(_ context.Context, compiled flowquery.Compiled) (flowquery.Result, error) {
	s.calls++
	dimension := compiled.Dimension.Kind
	value := "total"
	if dimension == flowquery.DimensionCategory {
		value = "overseas"
	} else if dimension == flowquery.DimensionSourceIP || dimension == flowquery.DimensionDestinationIP {
		value = "192.0.2.1"
	}
	expected := uint64(compiled.To.Sub(compiled.From) / compiled.SourceBucketDuration)
	return flowquery.Result{
		Points: []flowquery.Point{{
			Bucket: s.from, DimensionValue: value, DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-1",
			ClassificationVersion: 7, Value: 100, ReceivedRecords: 1, GeneratedAt: s.from.Add(time.Minute),
		}},
		Metric: compiled.Metric, Dimension: compiled.Dimension,
		Plan: &flowquery.AggregatePlan{
			RequestedFrom: compiled.From, RequestedTo: compiled.To, EffectiveFrom: compiled.From, EffectiveTo: compiled.To,
			Source: compiled.Bucket, SourceStep: compiled.SourceBucketDuration, Interval: compiled.BucketDuration,
			SourceSeconds: uint32(compiled.SourceBucketDuration / time.Second), StepSeconds: uint32(compiled.BucketDuration / time.Second),
		},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: expected, CoveredBuckets: expected, Ratio: 1, Complete: true},
	}, nil
}

type flowReportJointRunnerStub struct {
	from  time.Time
	calls int
}

func (s *flowReportJointRunnerStub) Run(_ context.Context, compiled flowquery.CompiledJoint) (flowquery.JointResult, error) {
	s.calls++
	if compiled.To.Sub(compiled.From) > flowquery.MaxJointRange {
		return flowquery.JointResult{}, &flowquery.RequestError{Field: "from/to", Code: flowquery.ErrorLimitExceeded, Message: "joint range"}
	}
	values := make([]string, 0, len(compiled.Dimensions))
	for _, dimension := range compiled.Dimensions {
		switch dimension.Kind {
		case flowquery.DimensionSourceIP, flowquery.DimensionDestinationIP:
			values = append(values, "192.0.2.1")
		case flowquery.DimensionBusiness:
			values = append(values, "business-a")
		case flowquery.DimensionCategory:
			values = append(values, "overseas")
		default:
			values = append(values, "total")
		}
	}
	return flowquery.JointResult{
		Points: []flowquery.JointPoint{{
			Bucket: s.from, DimensionValues: values, DimensionSnapshotID: "snapshot-1",
			GeoVersion: "geo-1", ClassificationVersion: 7, Value: 100, ReceivedRecords: 1, ObservedAt: s.from.Add(time.Minute),
		}},
		Metric: compiled.Metric, Dimensions: compiled.Dimensions, Plan: compiled.Plan,
	}, nil
}

type flowReportVPNRepositoryStub struct {
	items []VPNFinding
	err   error
}

func (s flowReportVPNRepositoryStub) ListVPNFindingsForExport(context.Context, ID, VPNFindingListFilter, uint32) ([]VPNFinding, error) {
	return s.items, s.err
}

func TestFlowVPNReportDependencyStates(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	parameters := flowAggregateQueryParameters{TargetPoints: 100, Timezone: "UTC"}
	request := QueryProviderRequest{TenantID: "tenant-a", From: from, To: from.Add(time.Hour)}
	panel, err := (ClickHouseFlowQueryProvider{}).vpnReportPanel(context.Background(), request, parameters)
	if err != nil || panel.Status != "unavailable" {
		t.Fatalf("unavailable panel=%+v err=%v", panel, err)
	}
	want := errors.New("mysql unavailable")
	_, err = (ClickHouseFlowQueryProvider{VPNFindings: flowReportVPNRepositoryStub{err: want}}).vpnReportPanel(context.Background(), request, parameters)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
}

func TestFlowVPNReportRequiresAdditiveByteMetric(t *testing.T) {
	parameters := flowAggregateQueryParameters{Metric: flowquery.MetricEstimatedBPS, Report: &flowReportSpec{SchemaVersion: 1, Kind: flowReportVPN}}
	if _, err := decodeFlowAggregateQueryParameters(mustJSON(t, parameters)); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("metric error = %#v", err)
	}
}

type flowReportOverseasRunnerStub struct {
	result flowquery.OverseasResult
}

func (s flowReportOverseasRunnerStub) Run(context.Context, flowquery.CompiledOverseas) (flowquery.OverseasResult, error) {
	return s.result, nil
}

func TestFlowOverseasObservedPanelKeepsObservedCardinalitySeparate(t *testing.T) {
	from := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	point := flowquery.OverseasPoint{
		Bucket: from, Kind: flowquery.OverseasRowKPI, GeoScope: flowquery.OverseasScopeOverseas,
		Direction: flowquery.OverseasDirectionCombined, IPFamily: flowquery.OverseasIPFamilyAll,
		Value: 640, ObservedRemoteIPs: 7, ObservedLocalHosts: 3, ReceivedRecords: 2,
		DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-1", ClassificationVersion: 7,
		GeneratedAt: from.Add(time.Minute),
	}
	provider := ClickHouseFlowQueryProvider{Now: func() time.Time { return from.Add(2 * time.Hour) }, OverseasRunner: flowReportOverseasRunnerStub{result: flowquery.OverseasResult{
		Points: []flowquery.OverseasPoint{point}, Metric: flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 1, CoveredBuckets: 1, Ratio: 1, Complete: true},
	}}}
	panel, err := provider.overseasObservedReportPanel(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", From: from, To: from.Add(time.Minute), ValueLayer: QueryValueCustomer, Limit: 100,
	}, flowAggregateQueryParameters{Metric: flowquery.MetricEstimatedBPS, TopN: 20, TargetPoints: 100, Timezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	if panel.Status != "ready" || panel.Meta.Versions["geo_version"] != "geo-1" || panel.Meta.Completeness.CompleteRatio != 1 {
		t.Fatalf("panel = %+v", panel)
	}
	var result flowquery.OverseasResult
	if err := json.Unmarshal(panel.Data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 1 || result.Points[0].ObservedRemoteIPs != 7 || result.Points[0].Value != 640 {
		t.Fatalf("result = %+v", result)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
