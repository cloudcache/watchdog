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
	first, err := CompileJoint(Scope{}, request, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileJoint(Scope{}, request, now)
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
		"PARTITION BY source_dimensions, dimension_snapshot_id, geo_version, classification_version",
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
	for _, key := range []string{"max_execution_time", "max_result_rows", "max_rows_to_read", "max_bytes_to_read", "max_memory_usage", "max_bytes_before_external_group_by", "max_bytes_before_external_sort"} {
		if settings[key] == "" {
			t.Fatalf("missing ClickHouse guard %s", key)
		}
	}
}

func TestCompileJointRanksTopSeriesWithoutRescanningGroupedFacts(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	compiled, err := CompileJoint(Scope{}, validJointRequest(now), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"sum(estimated_bytes) OVER", "dense_rank() OVER", "series_rank <= {top_n:UInt16}"} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("joint query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	for _, forbidden := range []string{"top_series AS", "FROM top_series"} {
		if strings.Contains(compiled.Query.Body, forbidden) {
			t.Fatalf("joint query still performs a second ranking scan via %q:\n%s", forbidden, compiled.Query.Body)
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
		{"no dimensions", "dimensions", ErrorLimitExceeded, func(r *JointRequest) { r.Dimensions = nil }},
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
			_, err := CompileJoint(Scope{}, request, now)
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v, want %s/%s", err, test.field, test.code)
			}
		})
	}
}

func TestCompileJointSupportsTypedFilterOnSingleDimensionBasePath(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	request := validJointRequest(now)
	request.Dimensions = []Dimension{DimensionASN}
	request.Filter = &FilterExpression{Op: FilterAnd, Args: []FilterExpression{
		{Op: FilterPredicate, Field: "src_ip", Operator: FilterIn, Values: []string{"203.0.113.0/24"}},
		{Op: FilterPredicate, Field: "remote_port", Operator: FilterGreaterThanOrEqual, Values: []string{"443"}},
	}}
	compiled, err := CompileJoint(Scope{}, request, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(compiled.Dimensions) != 1 || compiled.Dimensions[0].Kind != DimensionASN {
		t.Fatalf("dimensions=%+v", compiled.Dimensions)
	}
	for _, fragment := range []string{
		"isIPAddressInRange(toString(src_ip), {typed_filter_", "remote_port >= {typed_filter_",
	} {
		if !strings.Contains(compiled.Query.Body, fragment) {
			t.Fatalf("query missing %q:\n%s", fragment, compiled.Query.Body)
		}
	}
}

func TestCompileJointSupportsFilteredTotalOnBasePath(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	request := validJointRequest(now)
	request.Dimensions = []Dimension{DimensionTotal}
	request.TopN = 1
	request.IncludeOther = false
	request.Filter = &FilterExpression{Op: FilterPredicate, Field: "src_ip", Operator: FilterIn, Values: []string{"203.0.113.0/24"}}
	compiled, err := CompileJoint(Scope{}, request, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(compiled.Dimensions) != 1 || compiled.Dimensions[0].Kind != DimensionTotal ||
		!strings.Contains(compiled.Query.Body, "'total'") || !strings.Contains(compiled.Query.Body, "] AS source_dimensions") {
		t.Fatalf("compiled=%+v query=%s", compiled, compiled.Query.Body)
	}
}

func TestCompileJointHistoricalGenerationPinsTableIdentityAndSupplierColumns(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	request := validJointRequest(now)
	request.View = ViewSupplier
	request.Reclassification = &ReclassificationSource{ID: "reclassification-01", Generation: 7}
	request.Filters.Categories = []string{"overseas"}
	request.Filters.GeoVersions = []string{"supplier-geo-v2"}
	request.Filter = &FilterExpression{Op: FilterAnd, Args: []FilterExpression{
		{Op: FilterPredicate, Field: "geo.country", Operator: FilterEqual, Values: []string{"US"}},
		{Op: FilterPredicate, Field: "asn", Operator: FilterGreaterThanOrEqual, Values: []string{"64512"}},
	}}
	compiled, err := CompileJoint(Scope{AllowedViews: []View{ViewSupplier}}, request, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"FROM flow_reclassified_records FINAL",
		"reclassification_id = {reclassification_id:String}",
		"reclassification_generation = {reclassification_generation:UInt64}",
		"supplier_remote_geo_country_id", "supplier_remote_asn", "supplier_category", "supplier_geo_version",
	} {
		if !strings.Contains(compiled.Query.Body, fragment) {
			t.Fatalf("historical supplier SQL missing %q:\n%s", fragment, compiled.Query.Body)
		}
	}
	if compiled.Plan.Source != "flow_reclassified_records" {
		t.Fatalf("source=%q", compiled.Plan.Source)
	}

	request.Reclassification.ID = "bad id"
	if _, err := CompileJoint(Scope{AllowedViews: []View{ViewSupplier}}, request, now); !IsRequestError(err, "reclassification", ErrorInvalid) {
		t.Fatalf("invalid source error=%v", err)
	}
}

func TestCompileJointMapsCanonicalIPv4DimensionFilterToClickHouseStorage(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	request := validJointRequest(now)
	request.Dimensions = []Dimension{DimensionDestinationIP}
	request.Filters.DimensionValues = []string{"192.0.2.20"}
	compiled, err := CompileJoint(Scope{}, request, now)
	if err != nil {
		t.Fatal(err)
	}
	if queryParameter(compiled.Query, "base_dimension_value_0") != "'::ffff:192.0.2.20'" {
		t.Fatalf("joint IP dimension filter parameters were not normalized: %+v", compiled.Query.Parameters)
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
