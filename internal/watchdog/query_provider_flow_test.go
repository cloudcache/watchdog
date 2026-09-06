package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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
