// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func TestCompileDetailFacetUsesSameScopeAndExcludesOwnColumnFilter(t *testing.T) {
	detail := validDetailRequest()
	request := DetailFacetRequest{
		IP: detail.IP, Endpoint: detail.Endpoint, From: detail.From, To: detail.To, View: detail.View,
		Field: string(DetailFieldRemoteCountry), Search: "c", Limit: 50,
		ColumnFilters: []DetailColumnFilter{
			{Field: string(DetailFieldRemoteCountry), Values: []string{"CN"}},
			{Field: string(DetailFieldSourcePort), Values: []string{"443"}},
		},
	}
	compiled, err := CompileDetailFacet(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CAST(source.remote_country AS String) AS value",
		"toUInt64(source.src_port) IN ({detail_column_1_0:UInt64})",
		"positionCaseInsensitiveUTF8(CAST(source.remote_country AS String), {facet_search:String}) > 0",
		"GROUP BY value", "ORDER BY count DESC, value ASC", "LIMIT {facet_limit:UInt16}",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("facet query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	if strings.Contains(compiled.Query.Body, "detail_column_0_0") || queryParameter(compiled.Query, "detail_column_0_0") != "" ||
		queryParameter(compiled.Query, "detail_column_1_0") != "'443'" || queryParameter(compiled.Query, "facet_search") != "'c'" {
		t.Fatalf("facet parameters=%+v query=\n%s", compiled.Query.Parameters, compiled.Query.Body)
	}
}

func TestCompileDetailFacetRejectsUnboundedOrUnsupportedRequests(t *testing.T) {
	detail := validDetailRequest()
	valid := DetailFacetRequest{IP: detail.IP, Endpoint: detail.Endpoint, From: detail.From, To: detail.To, View: detail.View, Field: "category", Limit: 50}
	for _, test := range []struct {
		name  string
		field string
		code  ErrorCode
		apply func(*DetailFacetRequest)
	}{
		{"missing field", "column_filters.field", ErrorUnsupported, func(value *DetailFacetRequest) { value.Field = "" }},
		{"unknown field", "column_filters.field", ErrorUnsupported, func(value *DetailFacetRequest) { value.Field = "sleep(1)" }},
		{"limit", "limit", ErrorLimitExceeded, func(value *DetailFacetRequest) { value.Limit = 101 }},
		{"search", "search", ErrorLimitExceeded, func(value *DetailFacetRequest) { value.Search = strings.Repeat("x", 129) }},
		{"supplier", "view", ErrorUnsupported, func(value *DetailFacetRequest) { value.View = ViewSupplier }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.apply(&request)
			_, err := CompileDetailFacet(fullScope("tenant-a"), request, detailNow())
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v want field=%s code=%s", err, test.field, test.code)
			}
		})
	}
}

type fakeDetailFacetExecutor struct {
	values []string
	counts []uint64
	err    error
}

func (e fakeDetailFacetExecutor) Do(ctx context.Context, query ch.Query) error {
	results, ok := query.Result.(proto.Results)
	if !ok || len(results) != 2 {
		return errors.New("facet runner did not install typed results")
	}
	values := results[0].Data.(*proto.ColStr)
	counts := results[1].Data.(*proto.ColUInt64)
	for _, value := range e.values {
		values.Append(value)
	}
	for _, count := range e.counts {
		counts.Append(count)
	}
	if query.OnResult != nil {
		if err := query.OnResult(ctx, proto.Block{Columns: 2, Rows: len(e.values)}); err != nil {
			return err
		}
	}
	return e.err
}

func TestDetailFacetRunnerNormalizesIPAndRejectsBadShape(t *testing.T) {
	detail := validDetailRequest()
	compiled, err := CompileDetailFacet(fullScope("tenant-a"), DetailFacetRequest{
		IP: detail.IP, Endpoint: detail.Endpoint, From: detail.From, To: detail.To, View: detail.View,
		Field: string(DetailFieldSourceIP), Limit: 2,
	}, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewDetailRunner(fakeDetailFacetExecutor{values: []string{"::ffff:192.0.2.1", "2001:db8::1"}, counts: []uint64{3, 2}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.RunFacet(context.Background(), compiled)
	if err != nil || len(result.Items) != 2 || result.Items[0].Value != "192.0.2.1" || result.Items[0].Count != 3 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	bad, err := NewDetailRunner(fakeDetailFacetExecutor{values: []string{"CN"}, counts: nil})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.RunFacet(context.Background(), compiled); err == nil {
		t.Fatal("mismatched facet columns were accepted")
	}
}
