// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"strings"
	"testing"
	"time"
)

func TestCompileEndpointRollupJointUsesOneGenerationBoundAggregateScan(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	compiled, err := CompileEndpointRollupJoint(Scope{}, JointRequest{
		From: now.Add(-24 * time.Hour), To: now, Interval: 6 * time.Hour,
		TargetPoints: 5, Metric: MetricEstimatedBPS,
		Dimensions: []Dimension{DimensionSourceIP, DimensionCategory},
		Filters: Filters{
			DeviceIDs: []string{"device-a"}, Directions: []string{"in"},
			Categories: []string{"overseas"}, DimensionValues: []string{"192.0.2.10"},
		},
		View: ViewCustomer, TopN: 100, Timezone: "Asia/Singapore",
		TimeWindows:      []LocalTimeWindow{{Days: []uint8{1}, StartLocal: "08:00", EndLocal: "18:00"}},
		ExecutionTimeout: 2 * time.Minute,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"FROM flow_aggregate_1h AS source FINAL", "INNER JOIN latest USING (bucket, generation)",
		"source.dimension_kind = {dimension:String}", "source.dimension_value != '_other'",
		"CAST(source.category AS String)", "source.device_id IN ({device_0:String})",
		"source.dimension_value IN ({dimension_value_0:String})",
		"toTimeZone(toDateTime(source.bucket), {time_window_timezone:String})",
	} {
		if !strings.Contains(compiled.Query.Body, fragment) {
			t.Fatalf("rollup joint query missing %q:\n%s", fragment, compiled.Query.Body)
		}
	}
	if strings.Count(compiled.Query.Body, "FROM flow_aggregate_1h AS source FINAL") != 1 ||
		queryParameter(compiled.Query, "dimension_value_0") != "'::ffff:192.0.2.10'" ||
		compiled.Plan.Source != "flow_aggregate_1h" || compiled.Plan.StepSeconds != 21600 {
		t.Fatalf("compiled=%+v parameters=%+v", compiled, compiled.Query.Parameters)
	}
}

func TestCompileEndpointRollupJointRejectsUnsupportedCorrelation(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	_, err := CompileEndpointRollupJoint(Scope{}, JointRequest{
		From: now.Add(-time.Hour), To: now, Metric: MetricEstimatedBytes,
		Dimensions: []Dimension{DimensionSourceIP, DimensionASN}, View: ViewCustomer, TopN: 20,
	}, now)
	if !IsRequestError(err, "dimensions", ErrorUnsupported) {
		t.Fatalf("error=%v", err)
	}
}

func TestCompileBusinessCategoryRollupJointUsesTotalRows(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	compiled, err := CompileBusinessCategoryRollupJoint(Scope{}, JointRequest{
		From: now.Add(-24 * time.Hour), To: now, Interval: time.Hour,
		TargetPoints: 300, Metric: MetricEstimatedBytes,
		Dimensions: []Dimension{DimensionBusiness, DimensionCategory},
		Filters:    Filters{Directions: []string{"in"}},
		View:       ViewCustomer, TopN: 20, IncludeOther: true, Timezone: "Asia/Singapore",
		ExecutionTimeout: 2 * time.Minute,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"FROM flow_aggregate_1h AS source FINAL",
		"if(empty(source.business), '_unassigned', source.business), CAST(source.category AS String)",
		"source.business_direction IN ({direction_0:String})",
	} {
		if !strings.Contains(compiled.Query.Body, fragment) {
			t.Fatalf("business-category query missing %q:\n%s", fragment, compiled.Query.Body)
		}
	}
	if strings.Contains(compiled.Query.Body, "FROM flow_records") ||
		queryParameter(compiled.Query, "dimension") != "'total'" ||
		compiled.Plan.Source != "flow_aggregate_1h" {
		t.Fatalf("compiled=%+v parameters=%+v", compiled, compiled.Query.Parameters)
	}
}

func TestCompileBusinessCategoryRollupJointRejectsDimensionValues(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	_, err := CompileBusinessCategoryRollupJoint(Scope{}, JointRequest{
		From: now.Add(-time.Hour), To: now, Metric: MetricEstimatedBytes,
		Dimensions: []Dimension{DimensionBusiness, DimensionCategory},
		Filters:    Filters{DimensionValues: []string{"web"}},
		View:       ViewCustomer, TopN: 20,
	}, now)
	if !IsRequestError(err, "filters.dimension_values", ErrorUnsupported) {
		t.Fatalf("error=%v", err)
	}
}

func TestCompileBusinessCategoryHybridJointRanksAggregatePrefixAndRawTailTogether(t *testing.T) {
	now := time.Date(2026, 9, 23, 1, 3, 0, 0, time.UTC)
	from := time.Date(2026, 9, 22, 1, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 23, 1, 0, 0, 0, time.UTC)
	archiveThrough := to.Add(-time.Hour)
	compiled, err := CompileBusinessCategoryHybridJoint(Scope{}, JointRequest{
		From: from, To: to, Interval: time.Hour,
		TargetPoints: 300, Metric: MetricEstimatedBPS,
		Dimensions: []Dimension{DimensionBusiness, DimensionCategory},
		Filters:    Filters{DeviceIDs: []string{"device-a"}, Directions: []string{"in"}},
		View:       ViewCustomer, TopN: 20, IncludeOther: true, Timezone: "Asia/Singapore",
		TimeWindows: []LocalTimeWindow{{Days: []uint8{1, 2, 3, 4, 5}, StartLocal: "20:00", EndLocal: "23:00"}},
	}, archiveThrough, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"source.bucket < {archive_through:DateTime('UTC')}",
		"FROM flow_aggregate_1h AS source FINAL",
		"event_time >= {archive_through:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}",
		"FROM flow_records FINAL",
		"source.device_id IN ({device_0:String})",
		"device_id IN ({device_0:String})",
		"toTimeZone(toDateTime(source.bucket), {time_window_timezone:String})",
		"toTimeZone(toDateTime(event_time), {time_window_timezone:String})",
		"FROM source_rows",
		"sum(metric_value) OVER",
		"toFloat64(sum(metric_value)) * 8",
	} {
		if !strings.Contains(compiled.Query.Body, fragment) {
			t.Fatalf("hybrid business-category query missing %q:\n%s", fragment, compiled.Query.Body)
		}
	}
	if queryParameter(compiled.Query, "archive_through") != "'2026-09-23 00:00:00'" ||
		compiled.Plan.Source != "flow_aggregate_1h+flow_records" ||
		setting(compiled.Query, "max_rows_to_read") != "250000000" {
		t.Fatalf("compiled=%+v parameters=%+v", compiled, compiled.Query.Parameters)
	}
}

func TestCompileBusinessCategoryHybridJointRejectsNonHourlyBoundary(t *testing.T) {
	now := time.Date(2026, 9, 23, 1, 3, 0, 0, time.UTC)
	from := now.Add(-24 * time.Hour).Truncate(time.Hour)
	_, err := CompileBusinessCategoryHybridJoint(Scope{}, JointRequest{
		From: from, To: now.Truncate(time.Hour), Metric: MetricEstimatedBytes,
		Dimensions: []Dimension{DimensionBusiness, DimensionCategory},
		View:       ViewCustomer, TopN: 20,
	}, now.Add(-90*time.Minute), now)
	if !IsRequestError(err, "archive_through", ErrorInvalid) {
		t.Fatalf("error=%v", err)
	}
}
