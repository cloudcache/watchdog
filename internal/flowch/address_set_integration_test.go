// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestRealClickHouseAddressSetSemanticsAndLimits(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_address_set")

	bucket := time.Date(2026, 9, 5, 12, 30, 0, 0, time.UTC)
	snapshot := integrationAddressSetSnapshot(t, bucket)
	local := netip.MustParseAddr("10.1.2.3")
	both := netip.MustParseAddr("203.0.113.70")
	onlyA := netip.MustParseAddr("203.0.113.10")
	neither := netip.MustParseAddr("203.0.113.200")

	first := integrationAddressSetRecord(1, bucket.Add(10*time.Second), snapshot, local, both, 100)
	second := integrationAddressSetRecord(2, bucket.Add(20*time.Second), snapshot, local, onlyA, 200)
	second.QualityFlags = 1
	third := integrationAddressSetRecord(3, bucket.Add(30*time.Second), snapshot, local, both, 300)
	third.EstimatedValid = false
	third.EstimatedBytes = 0
	third.EstimatedPackets = 0
	fourth := integrationAddressSetRecord(4, bucket.Add(40*time.Second), snapshot, local, neither, 400)
	insertIntegrationBatch(t, ctx, native, integrationBatch(30, bucket.Add(2*time.Minute), first, second, third, fourth))

	// The later physical row for record 1 must replace its old counters under
	// FINAL before membership predicates and sums are evaluated.
	replacement := integrationAddressSetRecord(1, bucket.Add(10*time.Second), snapshot, local, both, 150)
	insertIntegrationBatch(t, ctx, native, integrationBatch(31, bucket.Add(3*time.Minute), replacement))

	runner, err := flowquery.NewAddressSetRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	union := runIntegrationAddressSet(t, ctx, runner, bucket, flowquery.MetricRawBytes, flowdimension.AddressSetFilter{
		IncludeAny: []string{"set-a", "set-b"},
	})
	assertAddressSetPoint(t, union, 650, 3, 1, 1)

	intersection := runIntegrationAddressSet(t, ctx, runner, bucket, flowquery.MetricRawBytes, flowdimension.AddressSetFilter{
		IncludeAll: []string{"set-a", "set-b"},
	})
	assertAddressSetPoint(t, intersection, 450, 2, 1, 0)

	difference := runIntegrationAddressSet(t, ctx, runner, bucket, flowquery.MetricRawBytes, flowdimension.AddressSetFilter{
		IncludeAny: []string{"set-a"}, ExcludeAny: []string{"set-b"},
	})
	assertAddressSetPoint(t, difference, 200, 1, 0, 1)

	estimated := runIntegrationAddressSet(t, ctx, runner, bucket, flowquery.MetricEstimatedBytes, flowdimension.AddressSetFilter{
		IncludeAny: []string{"set-a", "set-b"},
	})
	assertAddressSetPoint(t, estimated, 3_500, 3, 1, 1)

	compiled := compileIntegrationAddressSet(t, bucket, flowquery.MetricRawBytes, flowdimension.AddressSetFilter{IncludeAny: []string{"set-a", "set-b"}})
	expiredContext, expiredCancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer expiredCancel()
	timedResult, err := runner.Run(expiredContext, compiled)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || len(timedResult.Points) != 0 {
		t.Fatalf("expired address-set result=%+v error=%v", timedResult, err)
	}

	limitedRunner, err := flowquery.NewAddressSetRunner(&integrationSettingExecutor{
		executor: native.executor, overrides: map[string]string{"max_rows_to_read": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	limitedResult, err := limitedRunner.Run(ctx, compiled)
	if err == nil || len(limitedResult.Points) != 0 {
		t.Fatalf("scan-limited address-set result=%+v error=%v", limitedResult, err)
	}
}

func integrationAddressSetSnapshot(t *testing.T, effectiveFrom time.Time) *flowdimension.CompiledSnapshot {
	t.Helper()
	snapshot, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion,
		SnapshotID:    "snapshot-sets",
		TenantID:      "flow-it-tenant",
		Version:       1,
		EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "customer-a"}},
			{ID: "remote", CIDR: "203.0.113.0/24", Labels: map[string]string{"provider": "test"}},
		},
		AddressSets: []flowdimension.AddressSetDefinition{
			{ID: "set-a", Members: []string{"203.0.113.0/25"}, MatchDirection: "both", Enabled: true},
			{ID: "set-b", Members: []string{"203.0.113.64/26"}, MatchDirection: "both", Enabled: true},
		},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func integrationAddressSetRecord(index byte, eventTime time.Time, snapshot *flowdimension.CompiledSnapshot, source, destination netip.Addr, rawBytes uint64) flowworker.EnrichedRecord {
	record := testEnrichedRecord(index, rawBytes, rawBytes*10)
	record.EventTime = eventTime
	record.SourceIP = source
	record.DestinationIP = destination
	record.Dimensions = snapshot.ClassifyEndpoints(source, destination)
	return record
}

func compileIntegrationAddressSet(t *testing.T, bucket time.Time, metric flowquery.Metric, sets flowdimension.AddressSetFilter) flowquery.CompiledAddressSet {
	t.Helper()
	compiled, err := flowquery.CompileAddressSet(flowquery.Scope{TenantID: "flow-it-tenant"}, flowquery.AddressSetRequest{
		From: bucket, To: bucket.Add(time.Minute), Bucket: flowquery.BucketOneMinute,
		Metric: metric, View: flowquery.ViewCustomer, Endpoint: flowquery.AddressSetEndpointRemote, Sets: sets,
	}, bucket.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func runIntegrationAddressSet(t *testing.T, ctx context.Context, runner *flowquery.AddressSetRunner, bucket time.Time, metric flowquery.Metric, sets flowdimension.AddressSetFilter) flowquery.AddressSetResult {
	t.Helper()
	result, err := runner.Run(ctx, compileIntegrationAddressSet(t, bucket, metric, sets))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertAddressSetPoint(t *testing.T, result flowquery.AddressSetResult, value float64, records, unknownSampling, quality uint64) {
	t.Helper()
	if len(result.Points) != 1 || result.MixedVersions || result.VersionCount != 1 {
		t.Fatalf("address-set result=%+v", result)
	}
	point := result.Points[0]
	if point.Value != value || point.ReceivedRecords != records || point.UnknownSamplingRecords != unknownSampling || point.QualityRecords != quality {
		t.Fatalf("address-set point=%+v, want value=%v records=%d unknown=%d quality=%d", point, value, records, unknownSampling, quality)
	}
	if !point.SamplingCompletenessKnown || !point.QualityRecordRatioKnown ||
		point.SamplingCompleteness != float64(records-unknownSampling)/float64(records) ||
		point.QualityRecordRatio != float64(quality)/float64(records) {
		t.Fatalf("address-set quality ratios=%+v", point)
	}
}
