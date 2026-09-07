// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func TestBuildVPNCandidateQueryIsAtomicVersionedAndReplayStable(t *testing.T) {
	request := validVPNCandidateRequest()
	first, err := buildVPNCandidateQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildVPNCandidateQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same candidate repair changed query or token")
	}
	for _, required := range []string{
		"INSERT INTO flow_vpn_candidates", "FROM flow_records FINAL", "FROM flow_aggregate_1m FINAL",
		"AND dimension_kind = '_generation'", "UNION ALL", "'_generation'",
		"dimension_snapshot_id, geo_version, classification_version",
		"key_row_kind, key_dimension_snapshot_id, key_geo_version, key_classification_version",
		"sumIf(estimated_bytes, estimated_valid AND business_direction = 'out')",
		"sumIf(estimated_bytes, estimated_valid AND business_direction = 'in')",
		"argMax(", "source_stream_id, kafka_partition, kafka_offset, record_index",
		"if(has(observed_protocols, toUInt8(6)), ['tcp'], [])",
		"toFloat64(covered_buckets) / toFloat64(expected_buckets)",
		"toFloat64(estimated_valid_records) / toFloat64(flow_record_count)",
		"{tenant:String}", "{rule_set_version:String}", "{generation:UInt64}",
	} {
		if !strings.Contains(first.Body, required) {
			t.Fatalf("candidate query missing %q", required)
		}
	}
	for _, forbidden := range []string{"watchdog_flow.flow_records", "watchdog_flow.flow_vpn_candidates", "'tls'", "'quic'", "443"} {
		if strings.Contains(first.Body, forbidden) {
			t.Fatalf("candidate query contains forbidden inference or database qualification %q", forbidden)
		}
	}
	if strings.Count(first.Body, "INSERT INTO") != 1 {
		t.Fatal("candidate rows and empty-window marker must use one atomic insert")
	}
	if setting(first, "async_insert") != "0" || setting(first, "wait_for_async_insert") != "1" || setting(first, "insert_deduplication_token") == "" {
		t.Fatal("candidate materialization is not synchronous and replay-stable")
	}
	if parameter(first, "tenant") != "'tenant-a'" || parameter(first, "rule_set_version") != "'vpn-rules-1'" {
		t.Fatalf("unexpected query parameters: %+v", first.Parameters)
	}
}

func TestBuildVPNCandidateQueryRejectsOpenUnalignedOrUnsafeRequests(t *testing.T) {
	valid := validVPNCandidateRequest()
	for _, mutate := range []func(*VPNCandidateRequest){
		func(value *VPNCandidateRequest) { value.TenantID = "tenant' OR 1=1" },
		func(value *VPNCandidateRequest) { value.RuleSetVersion = "bad version" },
		func(value *VPNCandidateRequest) { value.WindowStart = value.WindowStart.Add(time.Second) },
		func(value *VPNCandidateRequest) { value.WindowEnd = value.WindowStart },
		func(value *VPNCandidateRequest) { value.WindowEnd = value.WindowStart.Add(25 * time.Hour) },
		func(value *VPNCandidateRequest) { value.WindowEnd = value.WindowEnd.In(time.FixedZone("local", 3600)) },
		func(value *VPNCandidateRequest) { value.Generation = 0 },
		func(value *VPNCandidateRequest) {
			value.GeneratedAt = value.GeneratedAt.In(time.FixedZone("local", 3600))
		},
		func(value *VPNCandidateRequest) { value.GeneratedAt = value.WindowEnd.Add(-time.Second) },
	} {
		request := valid
		mutate(&request)
		if _, err := buildVPNCandidateQuery(request); err == nil {
			t.Fatalf("invalid request was accepted: %+v", request)
		}
	}
}

func TestVPNCandidateMaterializerClassifiesFailures(t *testing.T) {
	for _, test := range []struct {
		err       error
		permanent bool
	}{
		{err: &ch.Exception{Code: proto.ErrUnknownTable, Name: "UNKNOWN_TABLE", Message: "missing"}, permanent: true},
		{err: errors.New("connection reset"), permanent: false},
	} {
		recorder := &queryRecorder{errors: map[int]error{1: test.err}}
		err := (&VPNCandidateMaterializer{executor: recorder}).Run(context.Background(), validVPNCandidateRequest())
		var permanent *PermanentError
		if errors.As(err, &permanent) != test.permanent || len(recorder.queries) != 1 {
			t.Fatalf("error=%v permanent=%v queries=%d", err, errors.As(err, &permanent), len(recorder.queries))
		}
	}
}

func TestVPNCandidateMaterializerReadsAuthoritativeMarker(t *testing.T) {
	executor := &rollupGenerationExecutor{generation: 23, emit: true}
	request := validVPNCandidateRequest()
	generation, err := (&VPNCandidateMaterializer{executor: executor}).LatestGeneration(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if generation != 23 || !strings.Contains(executor.query.Body, "FROM flow_vpn_candidates FINAL") ||
		!strings.Contains(executor.query.Body, "row_kind = '_generation'") || parameter(executor.query, "rule_set_version") != "'vpn-rules-1'" {
		t.Fatalf("generation=%d query=%q", generation, executor.query.Body)
	}
	if _, err := (&VPNCandidateMaterializer{executor: &rollupGenerationExecutor{}}).LatestGeneration(context.Background(), request); err == nil {
		t.Fatal("missing ClickHouse generation result was accepted")
	}
}

func validVPNCandidateRequest() VPNCandidateRequest {
	return VPNCandidateRequest{
		TenantID: "tenant-a", RuleSetVersion: "vpn-rules-1",
		WindowStart: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 9, 5, 1, 15, 0, 0, time.UTC),
		Generation:  7, GeneratedAt: time.Date(2026, 9, 5, 1, 16, 0, 0, time.UTC),
	}
}
