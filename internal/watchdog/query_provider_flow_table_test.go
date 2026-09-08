package watchdog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func TestBuildFlowTableUsesGraphStatisticsBeforeServerPaging(t *testing.T) {
	from := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	points := []flowTablePoint{
		{bucket: from, path: []string{"alpha"}, dimensionSnapshotID: "d1", geoVersion: "g1", classificationVersion: 1, value: 80, received: 4, unknown: 1},
		{bucket: from.Add(2 * time.Minute), path: []string{"alpha"}, dimensionSnapshotID: "d1", geoVersion: "g1", classificationVersion: 1, value: 160, received: 6, quality: 2},
		{bucket: from, path: []string{"beta"}, dimensionSnapshotID: "d1", geoVersion: "g1", classificationVersion: 1, value: 40, received: 2},
	}
	request := flowTableRequest{SortBy: "maximum", SortDirection: "desc", Limit: 1}
	page := buildFlowTable(points, flowTablePlan{from: from, to: from.Add(3 * time.Minute), step: time.Minute}, "bits_per_second", request)
	if page.Total != 2 || len(page.Items) != 1 || page.Items[0].Label != "alpha" {
		t.Fatalf("page=%+v", page)
	}
	row := page.Items[0]
	if row.Last != 160 || row.Average != 80 || row.P95 != 160 || row.Maximum != 160 || row.Minimum != 0 || row.Total != 1800 {
		t.Fatalf("statistics=%+v", row)
	}
	if row.ReceivedRecords != 10 || row.UnknownSamplingRatio != .1 || row.QualityRecordRatio != .2 {
		t.Fatalf("quality=%+v", row)
	}
	if len(page.FilterOptions["dimension"]) != 2 || len(page.FilterOptions["maximum"]) != 2 {
		t.Fatalf("filter options=%+v", page.FilterOptions)
	}

	request = flowTableRequest{
		Search: "beta", SortBy: "dimension", SortDirection: "asc", Limit: 25,
		Filters: map[string][]string{"records": {"2"}},
	}
	page = buildFlowTable(points, flowTablePlan{from: from, to: from.Add(3 * time.Minute), step: time.Minute}, "bits_per_second", request)
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].Label != "beta" {
		t.Fatalf("filtered page=%+v", page)
	}
}

func TestBuildFlowTableStatisticsUseOnlySelectedLocalTimeWindows(t *testing.T) {
	from := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	points := []flowTablePoint{
		{bucket: from, path: []string{"a"}, value: 80},
		{bucket: from.Add(time.Hour), path: []string{"a"}, value: 160},
		{bucket: from.Add(2 * time.Hour), path: []string{"a"}, value: 8000},
	}
	page := buildFlowTable(points, flowTablePlan{
		from: from, to: from.Add(3 * time.Hour), step: time.Hour, timezone: "UTC",
		timeWindows: []flowquery.LocalTimeWindow{{Days: []uint8{1}, StartLocal: "00:00", EndLocal: "02:00"}},
	}, "bits_per_second", flowTableRequest{Limit: 10, SortBy: "maximum", SortDirection: "desc"})
	if len(page.Items) != 1 {
		t.Fatalf("items = %+v", page.Items)
	}
	row := page.Items[0]
	if row.Last != 160 || row.Maximum != 160 || row.Average != 120 || row.Total != 108000 {
		t.Fatalf("row = %+v", row)
	}
}

type flowDirectionRunnerStub struct {
	compiled []flowquery.Compiled
	from     time.Time
}

type flowJointDirectionRunnerStub struct {
	compiled []flowquery.CompiledJoint
	from     time.Time
}

func (s *flowJointDirectionRunnerStub) Run(_ context.Context, compiled flowquery.CompiledJoint) (flowquery.JointResult, error) {
	s.compiled = append(s.compiled, compiled)
	return flowquery.JointResult{
		Points: []flowquery.JointPoint{{
			Bucket: s.from, DimensionValues: []string{"total"}, DimensionSnapshotID: "d1", GeoVersion: "g1",
			ClassificationVersion: 1, Value: 100, ReceivedRecords: 2, ObservedAt: s.from.Add(time.Minute),
		}},
		Metric:     flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimensions: []flowquery.DimensionDefinition{{Kind: flowquery.DimensionTotal, Additive: true}},
		Plan:       compiled.Plan,
	}, nil
}

func (s *flowDirectionRunnerStub) Run(_ context.Context, compiled flowquery.Compiled) (flowquery.Result, error) {
	s.compiled = append(s.compiled, compiled)
	value := 800.0
	if len(s.compiled) == 2 {
		value = 400
	}
	return flowquery.Result{
		Points: []flowquery.Point{{
			Bucket: s.from, DimensionValue: "total", DimensionSnapshotID: "d1", GeoVersion: "g1",
			ClassificationVersion: 1, Value: value, ReceivedRecords: 10, GeneratedAt: s.from.Add(time.Minute),
		}},
		Metric:             flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimension:          flowquery.DimensionDefinition{Kind: flowquery.DimensionTotal, Additive: true},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 1, CoveredBuckets: 1, Ratio: 1, Complete: true},
	}, nil
}

func TestClickHouseFlowQueryProviderDirectionSplitReturnsServerTable(t *testing.T) {
	from := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	runner := &flowDirectionRunnerStub{from: from}
	provider := ClickHouseFlowQueryProvider{Runner: runner, Readiness: flowReadinessStub{}, Now: func() time.Time { return from.Add(time.Hour) }}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		StepSeconds: 3600, Limit: 10, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","dimension":"total","top_n":1,"target_points":300,"direction_split":true,
			"table":{"sort_by":"maximum","sort_direction":"desc","limit":25}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(runner.compiled) != 2 || !strings.Contains(runner.compiled[0].Query.Body, "business_direction IN") {
		t.Fatalf("compiled=%+v", runner.compiled)
	}
	var data struct {
		Points []flowquery.Point `json:"points"`
		Table  flowTablePage     `json:"table"`
	}
	if err := json.Unmarshal(result.Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Points) != 2 || data.Points[0].DimensionValue != "Inbound" || data.Points[1].DimensionValue != "Outbound" ||
		data.Table.Total != 2 || len(data.Table.Items) != 2 || data.Table.Items[0].Label != "Inbound" {
		t.Fatalf("data=%s", result.Data)
	}
	if result.Completeness.Partial || result.Completeness.CompleteRatio != 1 {
		t.Fatalf("completeness=%+v", result.Completeness)
	}
}

func TestClickHouseFlowQueryProviderDirectionSplitKeepsCrossDimensionBasePath(t *testing.T) {
	from := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	aggregate := &flowAggregateRunnerStub{}
	joint := &flowJointDirectionRunnerStub{from: from}
	provider := ClickHouseFlowQueryProvider{
		Runner: aggregate, JointRunner: joint, Readiness: flowReadinessStub{}, Now: func() time.Time { return from.Add(time.Hour) },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		Limit: 1_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","dimension":"total","top_n":1,"direction_split":true,
			"filter":{"op":"predicate","field":"asn","operator":"eq","values":["4134"]},
			"table":{"sort_by":"dimension","sort_direction":"asc","limit":25}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.compiled.Query.Body != "" || len(joint.compiled) != 2 ||
		!strings.Contains(joint.compiled[0].Query.Body, "remote_asn =") || result.StepSeconds != 60 || !result.Completeness.Partial {
		t.Fatalf("aggregate=%+v joint=%+v result=%+v", aggregate.compiled, joint.compiled, result)
	}
	var data struct {
		Points []flowquery.JointPoint `json:"points"`
		Table  flowTablePage          `json:"table"`
	}
	if err := json.Unmarshal(result.Data, &data); err != nil || len(data.Points) != 2 || data.Table.Total != 2 ||
		data.Table.Items[0].Label != "Inbound" || data.Table.Items[1].Label != "Outbound" {
		t.Fatalf("data=%s error=%v", result.Data, err)
	}
}

func TestDecodeFlowTableRejectsUnboundedOrUnknownControls(t *testing.T) {
	for _, raw := range []string{
		`{"metric":"estimated_bps","dimension":"category","top_n":5,"table":{"limit":101}}`,
		`{"metric":"estimated_bps","dimension":"category","top_n":5,"table":{"limit":25,"sort_by":"sql"}}`,
		`{"metric":"estimated_bps","dimension":"category","top_n":5,"table":{"limit":25,"filters":{"hidden":["x"]}}}`,
		`{"metric":"estimated_bps","dimension":"total","top_n":2,"direction_split":true,"table":{"limit":25}}`,
	} {
		if _, err := decodeFlowAggregateQueryParameters(json.RawMessage(raw)); queryErrorCode(err) != QueryErrorInvalidRequest {
			t.Fatalf("raw=%s error=%#v", raw, err)
		}
	}
}
