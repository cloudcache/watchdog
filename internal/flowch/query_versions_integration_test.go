// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func TestRealClickHouseAggregateVersionsTieBreakAndLimits(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_query_versions")
	bucket := time.Date(2026, 9, 5, 18, 0, 0, 0, time.UTC)

	first := integrationRecord(1, bucket.Add(10*time.Second), "city-alpha", 100)
	first.Dimensions.SnapshotID = "snapshot-a"
	first.RemoteGeo.Version = "geo-a"
	first.ClassificationVersion = 1
	second := integrationRecord(2, bucket.Add(20*time.Second), "city-beta", 100)
	second.Dimensions.SnapshotID = "snapshot-a"
	second.RemoteGeo.Version = "geo-a"
	second.ClassificationVersion = 1
	third := integrationRecord(3, bucket.Add(30*time.Second), "city-alpha", 100)
	third.Dimensions.SnapshotID = "snapshot-b"
	third.RemoteGeo.Version = "geo-b"
	third.ClassificationVersion = 2
	insertIntegrationBatch(t, ctx, native, integrationBatch(80, bucket.Add(2*time.Minute), first, second, third))

	rollup, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := rollup.Run(ctx, RollupRequest{
		TenantID: "flow-it-tenant", Resolution: RollupOneMinute, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.Add(3 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	compiled := compileIntegrationAggregate(t, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, flowquery.DimensionGeoCity, 2, true)
	runner, err := flowquery.NewRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if !result.MixedVersions || result.VersionCount != 2 || len(result.Points) != 3 ||
		!result.RollupCompleteness.Complete || result.RollupCompleteness.CoveredBuckets != 1 {
		t.Fatalf("versioned aggregate result=%+v", result)
	}
	wants := []struct {
		value, snapshot, geo string
		classification       uint32
		other                bool
	}{
		{"city-alpha", "snapshot-a", "geo-a", 1, false},
		{"city-alpha", "snapshot-b", "geo-b", 2, false},
		{"_other", "snapshot-a", "geo-a", 1, true},
	}
	var total float64
	var records uint64
	for index, want := range wants {
		point := result.Points[index]
		if point.DimensionValue != want.value || point.DimensionSnapshotID != want.snapshot ||
			point.GeoVersion != want.geo || point.ClassificationVersion != want.classification ||
			point.Other != want.other || point.Value != 100 || point.ReceivedRecords != 1 {
			t.Fatalf("versioned aggregate point[%d]=%+v, want=%+v", index, point, want)
		}
		total += point.Value
		records += point.ReceivedRecords
	}
	if total != 300 || records != 3 {
		t.Fatalf("versioned TopN is not conservative: total=%v records=%d", total, records)
	}

	for _, setting := range []string{"max_rows_to_read", "max_result_rows"} {
		limitedRunner, err := flowquery.NewRunner(&integrationSettingExecutor{
			executor: native.executor, overrides: map[string]string{setting: "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		limited, runErr := limitedRunner.Run(ctx, compiled)
		if runErr == nil || len(limited.Points) != 0 {
			t.Fatalf("%s-limited aggregate result=%+v error=%v", setting, limited, runErr)
		}
	}
}
