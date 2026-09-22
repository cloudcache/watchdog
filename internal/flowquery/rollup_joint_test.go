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
