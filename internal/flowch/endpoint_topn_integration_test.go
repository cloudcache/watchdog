// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func TestRealClickHouseStorageV2RawEndpointCandidateQueryConservesTopAndOther(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_endpoint_topn")
	bucket := time.Date(2026, 9, 5, 18, 0, 0, 0, time.UTC)

	heavy := integrationDetailRecord(1, bucket.Add(10*time.Second), netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("203.0.113.10"), 100)
	medium := integrationDetailRecord(2, bucket.Add(20*time.Second), netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("203.0.113.20"), 50)
	light := integrationDetailRecord(3, bucket.Add(30*time.Second), netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("203.0.113.30"), 10)
	insertIntegrationBatch(t, ctx, native, integrationBatch(90, bucket.Add(time.Minute), heavy, medium, light))

	compiled, err := flowquery.Compile(flowquery.Scope{}, flowquery.Request{
		From: bucket, To: bucket.Add(time.Minute), Bucket: flowquery.BucketOneMinute,
		Metric: flowquery.MetricEstimatedBytes, Dimension: flowquery.DimensionDestinationIP,
		View: flowquery.ViewCustomer, TopN: 1, IncludeOther: true,
		StorageV2: true, ArchiveThrough: bucket,
		Filters: flowquery.Filters{TargetIDs: []string{"target-a"}},
	}, bucket.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowquery.NewRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, compiled)
	if err != nil {
		t.Fatalf("run bounded endpoint query: %v", err)
	}
	if len(result.Points) != 2 {
		t.Fatalf("points=%+v", result.Points)
	}
	if point := result.Points[0]; point.DimensionValue != "203.0.113.10" || point.Other || point.Value != 1000 || point.ReceivedRecords != 1 {
		t.Fatalf("top endpoint=%+v", point)
	}
	if point := result.Points[1]; point.DimensionValue != "_other" || !point.Other || point.Value != 600 || point.ReceivedRecords != 2 {
		t.Fatalf("other endpoints=%+v", point)
	}
	if !result.RollupCompleteness.Complete || result.RollupCompleteness.ExpectedBuckets != 1 || result.RollupCompleteness.CoveredBuckets != 1 {
		t.Fatalf("completeness=%+v", result.RollupCompleteness)
	}
}
