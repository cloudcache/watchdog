package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowAggregateRunnerStub struct {
	compiled flowquery.Compiled
	result   flowquery.Result
	err      error
}

type flowJointRunnerStub struct {
	compiled flowquery.CompiledJoint
	result   flowquery.JointResult
	err      error
}

type flowAddressSetRunnerStub struct {
	compiled flowquery.CompiledAddressSet
	result   flowquery.AddressSetResult
	err      error
}

func (s *flowAddressSetRunnerStub) Run(_ context.Context, compiled flowquery.CompiledAddressSet) (flowquery.AddressSetResult, error) {
	s.compiled = compiled
	return s.result, s.err
}

func (s *flowJointRunnerStub) Run(_ context.Context, compiled flowquery.CompiledJoint) (flowquery.JointResult, error) {
	s.compiled = compiled
	return s.result, s.err
}

func (s *flowAggregateRunnerStub) Run(_ context.Context, compiled flowquery.Compiled) (flowquery.Result, error) {
	s.compiled = compiled
	return s.result, s.err
}

type flowReadinessStub struct{ err error }

func (s flowReadinessStub) Ready(context.Context) error { return s.err }

type flowStorageBoundaryStub struct {
	boundary time.Time
	err      error
}

type flowOperatorBindingStub struct {
	binding FlowOperatorQueryBinding
	err     error
	request struct {
		tenantID, operatorID ID
		from, to             time.Time
	}
}

func (stub *flowOperatorBindingStub) ResolveFlowOperatorQueryBinding(_ context.Context, tenantID, operatorID ID, from, to time.Time) (FlowOperatorQueryBinding, error) {
	stub.request.tenantID, stub.request.operatorID = tenantID, operatorID
	stub.request.from, stub.request.to = from, to
	return stub.binding, stub.err
}

func (stub flowStorageBoundaryStub) FlowStorageArchiveThrough(context.Context, ID, time.Time, time.Time) (time.Time, error) {
	return stub.boundary, stub.err
}

func TestClickHouseFlowQueryProviderCompilesAuthenticatedEnvelopeAndMapsCompleteness(t *testing.T) {
	from := time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	runner := &flowAggregateRunnerStub{result: flowquery.Result{
		Points: []flowquery.Point{{
			Bucket: from, DimensionValue: "overseas", DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-2",
			ClassificationVersion: 3, Value: 800, ReceivedRecords: 10, UnknownSamplingRecords: 2,
			GeneratedAt: from.Add(2 * time.Minute),
		}},
		Metric:             flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimension:          flowquery.DimensionDefinition{Kind: flowquery.DimensionCategory, Additive: true},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 60, CoveredBuckets: 59, Ratio: 59.0 / 60.0},
	}}
	provider := ClickHouseFlowQueryProvider{Runner: runner, Readiness: flowReadinessStub{}, Now: func() time.Time { return now }}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		StepSeconds: 60, Limit: 120, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","dimension":"category","filters":{"target_ids":["target-a"]},"top_n":1,"include_other":true,"timezone":"Asia/Singapore"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if runner.compiled.From != from || runner.compiled.To != from.Add(time.Hour) || runner.compiled.Timezone != "Asia/Singapore" {
		t.Fatalf("compiled envelope = %#v", runner.compiled)
	}
	if result.Unit != "bits_per_second" || result.Timezone != "Asia/Singapore" || result.Completeness.CompleteRatio != 59.0/60.0 ||
		!result.Completeness.Partial || result.Completeness.UnknownRatio != 0.2 || result.AsOf != from.Add(2*time.Minute) {
		t.Fatalf("provider result = %#v", result)
	}
	if result.Versions["dimension_snapshot_id"] != "snapshot-1" || result.Versions["geo_version"] != "geo-2" || result.Versions["classification_version"] != "3" {
		t.Fatalf("versions = %#v", result.Versions)
	}
	var decoded flowquery.Result
	if err := json.Unmarshal(result.Data, &decoded); err != nil || len(decoded.Points) != 1 || decoded.Points[0].DimensionValue != "overseas" {
		t.Fatalf("data = %s, error = %v", result.Data, err)
	}
}

func TestClickHouseFlowQueryProviderPreparesInstalledOperatorIdentity(t *testing.T) {
	from := time.Date(2026, 9, 7, 9, 0, 0, 0, time.UTC)
	bindings := &flowOperatorBindingStub{binding: FlowOperatorQueryBinding{
		OperatorID: "operator-a", FlowISPID: 17, PublicationIDs: []ID{"publication-1", "publication-2"},
		DimensionSnapshotIDs: []string{"snapshot-1", "snapshot-2"}, ClassificationVersions: []uint32{4, 5}, ExpectedWorkers: 2,
	}}
	joint := &flowJointRunnerStub{result: flowquery.JointResult{
		Metric:     flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimensions: []flowquery.DimensionDefinition{{Kind: flowquery.DimensionCategory, Additive: true}},
		Plan: flowquery.JointPlan{RequestedFrom: from, RequestedTo: from.Add(time.Hour), EffectiveFrom: from,
			EffectiveTo: from.Add(time.Hour), Source: "flow_records", StepSeconds: 60, TargetPoints: 300, MaxRangeSeconds: 86_400},
	}}
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowAggregateRunnerStub{}, JointRunner: joint, Readiness: flowReadinessStub{}, OperatorBindings: bindings,
		Now: func() time.Time { return from.Add(time.Hour) },
	}
	request := QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Hour),
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","dimension":"category","top_n":20,"include_other":true,"target_points":300,
			"filter":{"op":"predicate","field":"geo.country","operator":"eq","values":["CN"]},
			"operator_selection":{"operator_id":"operator-a"}
		}`),
	}
	prepared, err := provider.PrepareQuery(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Parameters = prepared
	parameters, err := decodeFlowAggregateQueryParameters(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if parameters.OperatorSelection == nil || parameters.OperatorSelection.SchemaVersion != 1 ||
		parameters.OperatorSelection.FlowISPID != 17 || len(parameters.OperatorSelection.PublicationIDs) != 2 ||
		!flowFilterHasOnlyExpectedISP(parameters.Filter, 17) ||
		!reflect.DeepEqual(parameters.Filters.DimensionSnapshotIDs, []string{"snapshot-1", "snapshot-2"}) ||
		!reflect.DeepEqual(parameters.Filters.ClassificationVersions, []uint32{4, 5}) {
		t.Fatalf("prepared parameters = %+v", parameters)
	}
	result, err := provider.Query(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(joint.compiled.Query.Body, "remote_isp_id =") ||
		!strings.Contains(joint.compiled.Query.Body, "dimension_snapshot_id IN") ||
		!strings.Contains(joint.compiled.Query.Body, "classification_version IN") {
		t.Fatalf("compiled query = %s", joint.compiled.Query.Body)
	}
	if result.Versions["operator_id"] != "operator-a" || result.Versions["operator_flow_isp_id"] != "17" ||
		result.Versions["operator_publication_ids"] != "publication-1,publication-2" {
		t.Fatalf("operator provenance = %#v", result.Versions)
	}
	if bindings.request.tenantID != "tenant-a" || bindings.request.operatorID != "operator-a" ||
		bindings.request.from != from || bindings.request.to != from.Add(time.Hour) {
		t.Fatalf("binding request = %+v", bindings.request)
	}

	request.Parameters = json.RawMessage(`{"metric":"estimated_bps","dimension":"category","top_n":1,"filter":{"op":"predicate","field":"isp","operator":"eq","values":["17"]},"operator_selection":{"operator_id":"operator-a"}}`)
	if _, err := provider.PrepareQuery(context.Background(), request); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("client-supplied ISP constraint error = %#v", err)
	}
	bindings.err = ErrFlowOperatorQueryUnavailable
	request.Parameters = json.RawMessage(`{"metric":"estimated_bps","dimension":"category","top_n":1,"operator_selection":{"operator_id":"operator-a"}}`)
	if _, err := provider.PrepareQuery(context.Background(), request); queryErrorCode(err) != QueryErrorIncomplete {
		t.Fatalf("uninstalled operator binding error = %#v", err)
	}
}

func TestClickHouseFlowQueryProviderRejectsBypassStepAndEnvelopeRowOverflow(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	base := QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset},
		From: now.Add(-time.Hour), To: now, StepSeconds: 60, Limit: 10, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","dimension":"category","top_n":1,"include_other":false}`),
	}
	provider := ClickHouseFlowQueryProvider{Runner: &flowAggregateRunnerStub{}, Readiness: flowReadinessStub{}, Now: func() time.Time { return now }}

	bypass := base
	bypass.Parameters = json.RawMessage(`{"metric":"estimated_bps","dimension":"category","top_n":1,"tenant_id":"tenant-b"}`)
	if _, err := provider.Query(context.Background(), bypass); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("tenant bypass error = %#v", err)
	}
	badStep := base
	badStep.StepSeconds = 30
	if _, err := provider.Query(context.Background(), badStep); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("step error = %#v", err)
	}
	if _, err := provider.Query(context.Background(), base); queryErrorCode(err) != QueryErrorRowLimit {
		t.Fatalf("row budget error = %#v", err)
	}
}

func TestClickHouseFlowQueryProviderAutomaticallyPlansDisplayDensityAndSource(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 37, 0, 0, time.UTC)
	runner := &flowAggregateRunnerStub{result: flowquery.Result{
		Metric:             flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimension:          flowquery.DimensionDefinition{Kind: flowquery.DimensionCategory, Additive: true},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 168, CoveredBuckets: 168, Ratio: 1, Complete: true},
	}}
	provider := ClickHouseFlowQueryProvider{Runner: runner, Readiness: flowReadinessStub{}, Now: func() time.Time { return now }}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: now.Add(-7 * 24 * time.Hour), To: now,
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","dimension":"category","top_n":3,"include_other":true,"target_points":300}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if runner.compiled.Bucket != flowquery.BucketOneHour || runner.compiled.SourceBucketDuration != time.Hour ||
		runner.compiled.BucketDuration != time.Hour || result.StepSeconds != 3600 {
		t.Fatalf("automatic plan compiled=%+v result=%+v", runner.compiled, result)
	}
	var decoded flowquery.Result
	if err := json.Unmarshal(result.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Plan == nil || decoded.Plan.Source != flowquery.BucketOneHour || decoded.Plan.StepSeconds != 3600 || decoded.Plan.TargetPoints != 300 {
		t.Fatalf("response plan=%+v", decoded.Plan)
	}
}

func TestClickHouseFlowQueryProviderLabelsGeoByFactVersion(t *testing.T) {
	oldPath := writeFlowGeoV2Bundle(t, "geo-old", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	activePath := writeFlowGeoV2Bundle(t, "geo-current", time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))
	geo := NewFlowGeoService(activePath)
	if err := geo.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := geo.LoadHistorical(oldPath); err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)
	runner := &flowAggregateRunnerStub{result: flowquery.Result{
		Points: []flowquery.Point{{
			Bucket: from, DimensionValue: "330100", DimensionSnapshotID: "snapshot-old",
			GeoVersion: "geo-old", ClassificationVersion: 1, Value: 800, ReceivedRecords: 1,
		}},
		Metric:             flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimension:          flowquery.DimensionDefinition{Kind: flowquery.DimensionGeoCity, Additive: true},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 1, CoveredBuckets: 1, Ratio: 1, Complete: true},
	}}
	provider := ClickHouseFlowQueryProvider{
		Runner: runner, Readiness: flowReadinessStub{}, FlowGeo: geo,
		Now: func() time.Time { return from.Add(2 * time.Hour) },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: from.Add(time.Minute),
		StepSeconds: 60, Limit: 100, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","dimension":"geo.city","top_n":1,"include_other":false,
			"table":{"sort_by":"maximum","sort_direction":"desc","limit":25}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		flowquery.Result
		DimensionLabels map[string]FlowGeoLabel `json:"dimension_labels"`
		Table           flowTablePage           `json:"table"`
	}
	if err := json.Unmarshal(result.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	label := decoded.DimensionLabels["geo-old:330100"]
	if label.Code != "330100" || label.Version != "geo-old" || label.Name != "杭州市" ||
		len(label.Path) != 5 || label.Path[0].ID != "Asia" || len(decoded.Points) != 1 ||
		decoded.Points[0].DimensionValue != "330100" || decoded.Table.Total != 1 ||
		decoded.Table.Items[0].Label != "亚洲 → 东亚 → 中国 → 浙江省 → 杭州市" {
		t.Fatalf("decoded geo result=%+v label=%+v", decoded, label)
	}
}

func TestClickHouseFlowQueryProviderRunsDeduplicatedAddressSetCombination(t *testing.T) {
	to := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	from := to.Add(-time.Hour)
	runner := &flowAddressSetRunnerStub{result: flowquery.AddressSetResult{
		Points: []flowquery.AddressSetPoint{{
			Bucket: from, DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-1", ClassificationVersion: 2,
			Value: 800, ReceivedRecords: 10, UnknownSamplingRecords: 1, QualityRecords: 2,
		}},
		Metric:       flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Endpoint:     flowquery.AddressSetEndpointEither,
		Sets:         flowdimension.AddressSetFilter{IncludeAny: []string{"set-a", "set-b"}, ExcludeAny: []string{"set-c"}},
		VersionCount: 1,
	}}
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowAggregateRunnerStub{}, AddressSetRunner: runner, Readiness: flowReadinessStub{},
		Now: func() time.Time { return to },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: from, To: to,
		Limit: 100, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","dimension":"address_set","top_n":1,"address_set_endpoint":"either",
			"address_set_filter":{"include_any":["set-b","set-a"],"include_all":[],"exclude_any":["set-c"]},
			"table":{"sort_by":"maximum","sort_direction":"desc","limit":25}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(runner.compiled.Query.Body, "hasAny(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids))") ||
		!strings.Contains(runner.compiled.Query.Body, "NOT hasAny") || runner.compiled.Sets.IncludeAny[0] != "set-a" {
		t.Fatalf("compiled address-set query=%s sets=%+v", runner.compiled.Query.Body, runner.compiled.Sets)
	}
	if !result.Completeness.Partial || result.Completeness.UnknownRatio != 0.1 || result.StepSeconds != 60 {
		t.Fatalf("provider result=%+v", result)
	}
	var decoded struct {
		flowquery.Result
		AddressSetFilter flowdimension.AddressSetFilter `json:"address_set_filter"`
		Endpoint         flowquery.AddressSetEndpoint   `json:"address_set_endpoint"`
		Table            flowTablePage                  `json:"table"`
	}
	if err := json.Unmarshal(result.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Dimension.Kind != flowquery.DimensionAddressSet || decoded.Dimension.Additive ||
		decoded.Plan == nil || decoded.Plan.Source != flowquery.BucketFlowRecords || decoded.Endpoint != flowquery.AddressSetEndpointEither ||
		len(decoded.Points) != 1 || decoded.Points[0].DimensionValue != "address-set combination" || decoded.Table.Total != 1 {
		t.Fatalf("decoded result=%+v", decoded)
	}
}

func TestClickHouseFlowQueryProviderRejectsInvalidAddressSetEnvelope(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowAggregateRunnerStub{}, AddressSetRunner: &flowAddressSetRunnerStub{},
		Readiness: flowReadinessStub{}, Now: func() time.Time { return now },
	}
	base := QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: now.Add(-time.Hour), To: now,
		Limit: 100, ValueLayer: QueryValueCustomer,
	}
	for _, raw := range []string{
		`{"metric":"estimated_bps","dimension":"address_set","top_n":1,"address_set_filter":{"include_any":["set-a"]}}`,
		`{"metric":"estimated_bps","dimension":"category","top_n":1,"address_set_endpoint":"either","address_set_filter":{"include_any":["set-a"]}}`,
		`{"metric":"estimated_bps","dimension":"address_set","top_n":1,"address_set_endpoint":"either","filter":{"op":"predicate","field":"asn","operator":"eq","values":["4134"]},"address_set_filter":{"include_any":["set-a"]}}`,
	} {
		base.Parameters = json.RawMessage(raw)
		if _, err := provider.Query(context.Background(), base); queryErrorCode(err) != QueryErrorInvalidRequest {
			t.Fatalf("parameters=%s error=%#v", raw, err)
		}
	}
	base.Parameters = json.RawMessage(`{"metric":"estimated_bps","dimension":"address_set","top_n":1,"address_set_endpoint":"either","address_set_filter":{"include_any":["set-a"]}}`)
	base.StepSeconds = 300
	if _, err := provider.Query(context.Background(), base); queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("address-set step error=%#v", err)
	}
}

func TestClickHouseFlowQueryProviderAppliesVerifiedStorageBoundary(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	boundary := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	runner := &flowAggregateRunnerStub{result: flowquery.Result{
		Metric:             flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimension:          flowquery.DimensionDefinition{Kind: flowquery.DimensionCategory, Additive: true},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 168, CoveredBuckets: 168, Ratio: 1, Complete: true},
	}}
	provider := ClickHouseFlowQueryProvider{
		Runner: runner, Readiness: flowReadinessStub{}, StorageLifecycle: flowStorageBoundaryStub{boundary: boundary},
		Now: func() time.Time { return now },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: now.Add(-7 * 24 * time.Hour), To: now,
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","dimension":"category","top_n":3,"include_other":true,"target_points":300}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !runner.compiled.UsesRawFacts || !runner.compiled.ArchiveThrough.Equal(boundary) ||
		!strings.Contains(runner.compiled.Query.Body, "FROM flow_records FINAL") {
		t.Fatalf("compiled=%+v", runner.compiled)
	}
	if len(result.Completeness.Warnings) != 1 || !strings.Contains(result.Completeness.Warnings[0], "raw portion") {
		t.Fatalf("completeness=%+v", result.Completeness)
	}
}

func TestClickHouseFlowQueryProviderRunsBoundedTrueJointQuery(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	joint := &flowJointRunnerStub{result: flowquery.JointResult{
		Points: []flowquery.JointPoint{{
			Bucket: now.Add(-time.Hour), DimensionValues: []string{"CN", "4134"},
			DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-1", ClassificationVersion: 2,
			Value: 800, ReceivedRecords: 10, UnknownSamplingRecords: 1, ObservedAt: now.Add(-time.Minute),
		}},
		Metric: flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimensions: []flowquery.DimensionDefinition{
			{Kind: flowquery.DimensionGeoCountry, Additive: true}, {Kind: flowquery.DimensionASN, Additive: true},
		},
		Plan: flowquery.JointPlan{
			RequestedFrom: now.Add(-time.Hour), RequestedTo: now, EffectiveFrom: now.Add(-time.Hour), EffectiveTo: now,
			Source: "flow_records", StepSeconds: 60, TargetPoints: 300, MaxRangeSeconds: 86_400,
		},
	}}
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowAggregateRunnerStub{}, JointRunner: joint, Readiness: flowReadinessStub{}, Now: func() time.Time { return now },
	}
	result, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: now.Add(-time.Hour), To: now,
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","dimensions":["geo.country","asn"],"top_n":20,"include_other":true,"target_points":300}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(joint.compiled.Dimensions) != 2 || joint.compiled.Dimensions[0].Kind != flowquery.DimensionGeoCountry ||
		result.StepSeconds != 60 || !result.Completeness.Partial || result.Completeness.CompleteRatio != 1 ||
		result.Completeness.UnknownRatio != 0.1 || len(result.Completeness.Warnings) != 1 {
		t.Fatalf("compiled=%+v result=%+v", joint.compiled, result)
	}
	var decoded flowquery.JointResult
	if err := json.Unmarshal(result.Data, &decoded); err != nil || len(decoded.Points) != 1 || decoded.Points[0].DimensionValues[1] != "4134" {
		t.Fatalf("data=%s err=%v", result.Data, err)
	}
}

func TestClickHouseFlowQueryProviderRoutesCrossDimensionFilterToSingleDimensionBaseQuery(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	joint := &flowJointRunnerStub{result: flowquery.JointResult{
		Metric:     flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimensions: []flowquery.DimensionDefinition{{Kind: flowquery.DimensionASN, Additive: true}},
		Plan: flowquery.JointPlan{
			RequestedFrom: now.Add(-time.Hour), RequestedTo: now, EffectiveFrom: now.Add(-time.Hour), EffectiveTo: now,
			Source: "flow_records", StepSeconds: 60, TargetPoints: 300, MaxRangeSeconds: 86_400,
		},
	}}
	provider := ClickHouseFlowQueryProvider{
		Runner: &flowAggregateRunnerStub{}, JointRunner: joint, Readiness: flowReadinessStub{}, Now: func() time.Time { return now },
	}
	_, err := provider.Query(context.Background(), QueryProviderRequest{
		TenantID: "tenant-a", Dataset: DatasetDescriptor{Key: FlowTrafficDataset}, From: now.Add(-time.Hour), To: now,
		Limit: 10_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{
			"metric":"estimated_bps","dimension":"asn","top_n":20,"include_other":true,"target_points":300,
			"filter":{"op":"predicate","field":"src_ip","operator":"in","values":["203.0.113.0/24"]}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(joint.compiled.Dimensions) != 1 || joint.compiled.Dimensions[0].Kind != flowquery.DimensionASN ||
		!strings.Contains(joint.compiled.Query.Body, "isIPAddressInRange(toString(src_ip)") {
		t.Fatalf("compiled=%+v", joint.compiled)
	}
}

func TestClickHouseFlowQueryProviderReadinessAndResourceAuthorization(t *testing.T) {
	want := errors.New("clickhouse unavailable")
	provider := ClickHouseFlowQueryProvider{Runner: &flowAggregateRunnerStub{}, Readiness: flowReadinessStub{err: want}}
	if err := provider.Ready(context.Background()); !errors.Is(err, want) {
		t.Fatalf("ready error = %v", err)
	}
	auth := AuthContext{TenantID: "tenant-a", UserID: "user-a", Grants: []Permission{{
		TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTarget,
		ResourceID: "target-a", Actions: []Action{ActionView},
	}}}
	request := QueryProviderRequest{Parameters: json.RawMessage(`{"metric":"estimated_bps","dimension":"category","filters":{"target_ids":["target-a"]},"top_n":1}`)}
	if err := provider.AuthorizeQuery(context.Background(), auth, request); err != nil {
		t.Fatalf("authorized target rejected: %v", err)
	}
	request.Parameters = json.RawMessage(`{"metric":"estimated_bps","dimension":"category","filters":{"target_ids":["target-b"]},"top_n":1}`)
	if err := provider.AuthorizeQuery(context.Background(), auth, request); queryErrorCode(err) != QueryErrorPermissionDenied {
		t.Fatalf("unauthorized target error = %#v", err)
	}
	request.Parameters = json.RawMessage(`{
		"metric":"estimated_bps","dimension":"category","top_n":1,
		"filter":{"op":"predicate","field":"target","operator":"eq","values":["target-a"]}
	}`)
	if err := provider.AuthorizeQuery(context.Background(), auth, request); queryErrorCode(err) != QueryErrorPermissionDenied {
		t.Fatalf("typed resource filter error = %#v", err)
	}
}

func TestClickHouseFlowQueryProviderRequiresVPNViewForVPNReport(t *testing.T) {
	provider := ClickHouseFlowQueryProvider{}
	request := QueryProviderRequest{Parameters: json.RawMessage(`{
		"metric":"estimated_bytes","report":{"schema_version":1,"kind":"vpn"}
	}`)}
	auth := AuthContext{TenantID: "tenant-a", UserID: "user-a", Grants: []Permission{{
		TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTenant,
		ResourceID: "tenant-a", Actions: []Action{ActionViewCustomer},
	}}}
	if err := provider.AuthorizeQuery(context.Background(), auth, request); queryErrorCode(err) != QueryErrorPermissionDenied {
		t.Fatalf("missing vpn_view error = %#v", err)
	}
	auth.Grants = append(auth.Grants, Permission{
		TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTenant,
		ResourceID: "tenant-a", Actions: []Action{ActionVPNView},
	})
	if err := provider.AuthorizeQuery(context.Background(), auth, request); err != nil {
		t.Fatalf("vpn_view rejected: %v", err)
	}
}

func TestDecodeFlowQueryRequiresCanonicalFilter(t *testing.T) {
	_, err := decodeFlowAggregateQueryParameters(json.RawMessage(`{
		"metric":"estimated_bps","dimension":"category","top_n":1,
		"filter":{"op":"predicate","field":"protocol","operator":"in","values":["UDP","tcp"]}
	}`))
	if queryErrorCode(err) != QueryErrorInvalidRequest {
		t.Fatalf("non-canonical filter error = %#v", err)
	}
	parameters, err := decodeFlowAggregateQueryParameters(json.RawMessage(`{
		"metric":"estimated_bps","dimension":"category","top_n":1,
		"filter":{"op":"predicate","field":"protocol","operator":"in","values":["6","17"]}
	}`))
	if err != nil || parameters.Filter == nil {
		t.Fatalf("canonical filter parameters=%+v error=%v", parameters, err)
	}
}

func queryErrorCode(err error) QueryErrorCode {
	var target *QueryGatewayError
	if errors.As(err, &target) {
		return target.Code
	}
	return ""
}
