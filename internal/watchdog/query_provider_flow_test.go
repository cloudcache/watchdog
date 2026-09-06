package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowAggregateRunnerStub struct {
	compiled flowquery.Compiled
	result   flowquery.Result
	err      error
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
}

func queryErrorCode(err error) QueryErrorCode {
	var target *QueryGatewayError
	if errors.As(err, &target) {
		return target.Code
	}
	return ""
}
