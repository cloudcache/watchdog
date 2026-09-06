// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"strings"
	"testing"
	"time"
)

func TestCompileJointUsesSameFactForOrderedDimensionTuple(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	request := validJointRequest(now)
	first, err := CompileJoint(Scope{TenantID: "tenant-a"}, request, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileJoint(Scope{TenantID: "tenant-a"}, request, now)
	if err != nil {
		t.Fatal(err)
	}
	if first.Query.Body != second.Query.Body || len(first.Query.Parameters) != len(second.Query.Parameters) {
		t.Fatal("joint compilation is not deterministic")
	}
	for _, fragment := range []string{
		"FROM flow_records FINAL", "AND disposition = 'count'",
		"if(empty(remote_geo_country_id), '_unassigned', remote_geo_country_id)",
		"if(remote_asn = 0, '_unassigned', toString(remote_asn))",
		"] AS source_dimensions", "GROUP BY output_bucket, source_dimensions",
		"tuple(source_dimensions, dimension_snapshot_id, geo_version, classification_version)",
		"if(is_top, source_dimensions, ['_other', '_other']) AS dimension_values",
		"max_rows_to_read", // checked below in settings, retained here as intent only
	} {
		if fragment == "max_rows_to_read" {
			continue
		}
		if !strings.Contains(first.Query.Body, fragment) {
			t.Fatalf("joint SQL missing %q:\n%s", fragment, first.Query.Body)
		}
	}
	if first.Plan.Source != "flow_records" || first.Plan.StepSeconds != 60 || len(first.Dimensions) != 2 ||
		first.Dimensions[0].Kind != DimensionGeoCountry || first.Dimensions[1].Kind != DimensionASN {
		t.Fatalf("compiled=%+v", first)
	}
	settings := make(map[string]string, len(first.Query.Settings))
	for _, setting := range first.Query.Settings {
		settings[setting.Key] = setting.Value
	}
	for _, key := range []string{"max_execution_time", "max_result_rows", "max_rows_to_read", "max_bytes_to_read", "max_memory_usage"} {
		if settings[key] == "" {
			t.Fatalf("missing ClickHouse guard %s", key)
		}
	}
}

func TestCompileJointRejectsAmbiguousOrUnboundedRequests(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		field string
		code  ErrorCode
		edit  func(*JointRequest)
	}{
		{"one dimension", "dimensions", ErrorLimitExceeded, func(r *JointRequest) { r.Dimensions = r.Dimensions[:1] }},
		{"duplicate", "dimensions", ErrorInvalid, func(r *JointRequest) { r.Dimensions[1] = r.Dimensions[0] }},
		{"total", "dimensions", ErrorUnsupported, func(r *JointRequest) { r.Dimensions[1] = DimensionTotal }},
		{"overlapping address set", "dimensions", ErrorUnsupported, func(r *JointRequest) { r.Dimensions[1] = DimensionAddressSet }},
		{"ambiguous dimension filter", "filters.dimension_values", ErrorUnsupported, func(r *JointRequest) { r.Filters.DimensionValues = []string{"CN"} }},
		{"long range", "from/to", ErrorLimitExceeded, func(r *JointRequest) { r.From = r.To.Add(-25 * time.Hour) }},
		{"future", "to", ErrorIncompleteRange, func(r *JointRequest) { r.To = now.Add(time.Minute) }},
		{"bad target points", "target_points", ErrorLimitExceeded, func(r *JointRequest) { r.TargetPoints = 4 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validJointRequest(now)
			test.edit(&request)
			_, err := CompileJoint(Scope{TenantID: "tenant-a"}, request, now)
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v, want %s/%s", err, test.field, test.code)
			}
		})
	}
}

func validJointRequest(now time.Time) JointRequest {
	return JointRequest{
		From: now.Add(-time.Hour), To: now, Metric: MetricEstimatedBPS,
		Dimensions: []Dimension{DimensionGeoCountry, DimensionASN},
		Filters:    Filters{Directions: []string{"out"}}, View: ViewCustomer,
		TopN: 20, IncludeOther: true, TargetPoints: 300, Timezone: "UTC",
	}
}
