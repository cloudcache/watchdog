// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type fakeCandidateRow struct {
	conversationKey                        string
	localIP, remoteIP                      netip.Addr
	primaryProtocol                        uint8
	primaryLocalPort, primaryRemotePort    uint16
	localToRemoteBytes, remoteToLocalBytes uint64
	flowRecordCount                        uint64
	activeBucketCount                      uint32
	maxDurationMS                          uint64
	packetBytesP50                         uint64
	remoteASN                              uint32
	remoteCountry, remotePrefixID          string
	localPrefixID                          string
	transportHints                         []string
	completeRatio                          float32
	evidenceJSON                           string
	dimensionSnapshotID, geoVersion        string
	classificationVersion                  uint32
	generation                             uint64
	generatedAt                            time.Time
	isMetadata                             uint8
	markerCount                            uint64
}

type fakeCandidateExecutor struct {
	blocks     [][]fakeCandidateRow
	err        error
	skipColumn string
	query      ch.Query
}

func (e *fakeCandidateExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	results, ok := query.Result.(proto.Results)
	if !ok {
		return errors.New("candidate runner did not install typed results")
	}
	for _, block := range e.blocks {
		for _, result := range results {
			result.Data.Reset()
		}
		for _, row := range block {
			appendFakeCandidateRow(results, row, e.skipColumn)
		}
		if err := query.OnResult(ctx, proto.Block{Columns: len(results), Rows: len(block)}); err != nil {
			return err
		}
	}
	return e.err
}

func TestCandidateQueryUsesLatestMarkerAndBoundedTypedParameters(t *testing.T) {
	request := validScoreWindowRequest()
	rules := compiledCandidateRules(t)
	query, err := buildCandidateQuery(request, rules)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"FROM flow_vpn_candidates FINAL", "row_kind = '_generation'", "row_kind = 'candidate'",
		"generation = latest_generation", "marker_count = 1", "LIMIT {read_limit:UInt64}",
		"lower(hex(conversation_key))", "dimension_snapshot_id, geo_version",
		"ORDER BY conversation_key, dimension_snapshot_id, geo_version, classification_version",
	} {
		if !strings.Contains(query.Body, required) {
			t.Fatalf("candidate reader query missing %q", required)
		}
	}
	for _, forbidden := range []string{"watchdog_flow.flow_vpn_candidates", "'tls'", "'quic'", "443"} {
		if strings.Contains(query.Body, forbidden) {
			t.Fatalf("candidate reader query contains forbidden literal %q", forbidden)
		}
	}
	if candidateParameter(query, "rule_set_version") != "'vpn-rules-1'" ||
		candidateParameter(query, "read_limit") != "'3'" || candidateSetting(query, "max_result_rows") != "4" {
		t.Fatalf("query limits or parameters are wrong: %+v %+v", query.Parameters, query.Settings)
	}
}

func TestCandidateRunnerScoresLatestGenerationAcrossUnorderedBlocks(t *testing.T) {
	request := validScoreWindowRequest()
	data := validCandidateDataRow(request)
	metadata := candidateMetadataRow(data.generation)
	executor := &fakeCandidateExecutor{blocks: [][]fakeCandidateRow{{metadata}, {data}}}
	runner, err := NewCandidateRunner(executor)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), request, compiledCandidateRules(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.RuleSetVersion != "vpn-rules-1" || result.Generation != data.generation || len(result.Candidates) != 1 {
		t.Fatalf("result=%+v", result)
	}
	got := result.Candidates[0]
	if got.Candidate.ConversationKey != data.conversationKey || got.Candidate.LocalIP != data.localIP || got.Candidate.RemoteIP != data.remoteIP ||
		got.Candidate.DimensionSnapshotID != "snapshot-1" || got.Candidate.GeoVersion != "geo-1" || got.Candidate.ClassificationVersion != 1 ||
		got.Score.RuleSetVersion != "vpn-rules-1" || got.Score.DimensionSnapshotID != "snapshot-1" || got.Score.GeoVersion != "geo-1" ||
		got.Score.ClassificationVersion != 1 || got.Score.Score != 20 || got.Generation != data.generation || got.GeneratedAt != data.generatedAt {
		t.Fatalf("scored candidate=%+v", got)
	}
	if got.MaterializationEvidence.ExpectedBuckets != 15 || got.MaterializationEvidence.EstimatedValidRecords != data.flowRecordCount {
		t.Fatalf("evidence=%+v", got.MaterializationEvidence)
	}
}

func TestCandidateRunnerAcceptsAuthoritativeEmptyGeneration(t *testing.T) {
	request := validScoreWindowRequest()
	runner, _ := NewCandidateRunner(&fakeCandidateExecutor{blocks: [][]fakeCandidateRow{{candidateMetadataRow(9)}}})
	result, err := runner.Run(context.Background(), request, compiledCandidateRules(t))
	if err != nil || result.Generation != 9 || len(result.Candidates) != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestCandidateRunnerRejectsMalformedPartialOrOversizeResults(t *testing.T) {
	request := validScoreWindowRequest()
	rules := compiledCandidateRules(t)
	valid := validCandidateDataRow(request)
	metadata := candidateMetadataRow(valid.generation)
	second := valid
	second.conversationKey = strings.Repeat("cd", 32)
	tests := []struct {
		name       string
		blocks     [][]fakeCandidateRow
		skipColumn string
		execErr    error
		mutate     func(*ScoreWindowRequest)
	}{
		{name: "missing marker", blocks: [][]fakeCandidateRow{{valid}}},
		{name: "marker count", blocks: [][]fakeCandidateRow{{valid, func() fakeCandidateRow { row := metadata; row.markerCount = 0; return row }()}}},
		{name: "mixed generation", blocks: [][]fakeCandidateRow{{func() fakeCandidateRow { row := valid; row.generation++; return row }(), metadata}}},
		{name: "duplicate", blocks: [][]fakeCandidateRow{{valid, valid, metadata}}},
		{name: "oversize", blocks: [][]fakeCandidateRow{{valid, second, metadata}}, mutate: func(value *ScoreWindowRequest) { value.MaxCandidates = 1 }},
		{name: "column mismatch", blocks: [][]fakeCandidateRow{{valid, metadata}}, skipColumn: "remote_asn"},
		{name: "unknown evidence", blocks: [][]fakeCandidateRow{{func() fakeCandidateRow {
			row := valid
			row.evidenceJSON = `{"schema_version":1,"covered_buckets":15,"expected_buckets":15,"estimated_valid_records":10,"quality_records":0,"extra":1}`
			return row
		}(), metadata}}},
		{name: "ratio mismatch", blocks: [][]fakeCandidateRow{{func() fakeCandidateRow { row := valid; row.completeRatio = .5; return row }(), metadata}}},
		{name: "invalid hint", blocks: [][]fakeCandidateRow{{func() fakeCandidateRow { row := valid; row.transportHints = []string{"tls-guessed"}; return row }(), metadata}}},
		{name: "execution failure", execErr: errors.New("connection reset")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := request
			if test.mutate != nil {
				test.mutate(&current)
			}
			runner, _ := NewCandidateRunner(&fakeCandidateExecutor{blocks: test.blocks, skipColumn: test.skipColumn, err: test.execErr})
			if result, err := runner.Run(context.Background(), current, rules); err == nil || len(result.Candidates) != 0 {
				t.Fatalf("partial or malformed result escaped: result=%+v error=%v", result, err)
			}
		})
	}
}

func TestCandidateQueryRejectsInvalidScopeWindowRulesAndLimits(t *testing.T) {
	valid := validScoreWindowRequest()
	rules := compiledCandidateRules(t)
	for _, mutate := range []func(*ScoreWindowRequest){
		func(value *ScoreWindowRequest) { value.WindowStart = value.WindowStart.Add(time.Second) },
		func(value *ScoreWindowRequest) { value.WindowEnd = value.WindowStart },
		func(value *ScoreWindowRequest) { value.WindowEnd = value.WindowStart.Add(25 * time.Hour) },
		func(value *ScoreWindowRequest) {
			value.WindowStart = value.WindowStart.In(time.FixedZone("local", 3600))
		},
		func(value *ScoreWindowRequest) { value.MaxCandidates = 0 },
		func(value *ScoreWindowRequest) { value.MaxCandidates = MaxScoredCandidates + 1 },
	} {
		request := valid
		mutate(&request)
		if _, err := buildCandidateQuery(request, rules); err == nil {
			t.Fatalf("invalid request was accepted: %+v", request)
		}
	}
	if _, err := buildCandidateQuery(valid, CompiledRuleSet{}); err == nil {
		t.Fatal("uncompiled rules were accepted")
	}
	runner, _ := NewCandidateRunner(&fakeCandidateExecutor{})
	if _, err := runner.Run(nil, valid, rules); err == nil {
		t.Fatal("nil context was accepted")
	}
}

func validScoreWindowRequest() ScoreWindowRequest {
	return ScoreWindowRequest{
		WindowStart: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 9, 5, 1, 15, 0, 0, time.UTC), MaxCandidates: 2,
	}
}

func compiledCandidateRules(t *testing.T) CompiledRuleSet {
	t.Helper()
	rules, err := CompileRuleSet(validRuleSet())
	if err != nil {
		t.Fatal(err)
	}
	return rules
}

func validCandidateDataRow(request ScoreWindowRequest) fakeCandidateRow {
	return fakeCandidateRow{
		conversationKey: strings.Repeat("ab", 32), localIP: netip.MustParseAddr("192.0.2.10"),
		remoteIP: netip.MustParseAddr("2001:db8::20"), primaryProtocol: 6,
		primaryLocalPort: 50_000, primaryRemotePort: 443,
		localToRemoteBytes: 1_000, remoteToLocalBytes: 900, flowRecordCount: 10,
		activeBucketCount: 5, maxDurationMS: 120_000, remoteASN: 64512,
		remoteCountry: "CN", remotePrefixID: "risk-prefix", transportHints: []string{"tcp"},
		completeRatio: 1, evidenceJSON: `{"schema_version":1,"covered_buckets":15,"expected_buckets":15,"estimated_valid_records":10,"quality_records":0}`,
		dimensionSnapshotID: "snapshot-1", geoVersion: "geo-1", classificationVersion: 1,
		generation: 7, generatedAt: request.WindowEnd.Add(time.Minute),
	}
}

func candidateMetadataRow(generation uint64) fakeCandidateRow {
	return fakeCandidateRow{
		localIP: netip.IPv6Unspecified(), remoteIP: netip.IPv6Unspecified(),
		generation: generation, generatedAt: time.UnixMilli(0).UTC(), isMetadata: 1, markerCount: 1,
	}
}

func appendFakeCandidateRow(results proto.Results, row fakeCandidateRow, skip string) {
	for _, result := range results {
		if result.Name == skip {
			continue
		}
		switch result.Name {
		case "conversation_key":
			result.Data.(*proto.ColStr).Append(row.conversationKey)
		case "local_ip":
			result.Data.(*proto.ColIPv6).Append(proto.ToIPv6(row.localIP))
		case "remote_ip":
			result.Data.(*proto.ColIPv6).Append(proto.ToIPv6(row.remoteIP))
		case "primary_protocol":
			result.Data.(*proto.ColUInt8).Append(row.primaryProtocol)
		case "primary_local_port":
			result.Data.(*proto.ColUInt16).Append(row.primaryLocalPort)
		case "primary_remote_port":
			result.Data.(*proto.ColUInt16).Append(row.primaryRemotePort)
		case "local_to_remote_bytes":
			result.Data.(*proto.ColUInt64).Append(row.localToRemoteBytes)
		case "remote_to_local_bytes":
			result.Data.(*proto.ColUInt64).Append(row.remoteToLocalBytes)
		case "flow_record_count":
			result.Data.(*proto.ColUInt64).Append(row.flowRecordCount)
		case "active_bucket_count":
			result.Data.(*proto.ColUInt32).Append(row.activeBucketCount)
		case "max_duration_ms":
			result.Data.(*proto.ColUInt64).Append(row.maxDurationMS)
		case "packet_bytes_p50":
			result.Data.(*proto.ColUInt64).Append(row.packetBytesP50)
		case "remote_asn":
			result.Data.(*proto.ColUInt32).Append(row.remoteASN)
		case "remote_country":
			result.Data.(*proto.ColStr).Append(row.remoteCountry)
		case "remote_prefix_id":
			result.Data.(*proto.ColStr).Append(row.remotePrefixID)
		case "local_prefix_id":
			result.Data.(*proto.ColStr).Append(row.localPrefixID)
		case "transport_hints":
			result.Data.(*proto.ColArr[string]).Append(row.transportHints)
		case "complete_ratio":
			result.Data.(*proto.ColFloat32).Append(row.completeRatio)
		case "evidence_json":
			result.Data.(*proto.ColStr).Append(row.evidenceJSON)
		case "dimension_snapshot_id":
			result.Data.(*proto.ColStr).Append(row.dimensionSnapshotID)
		case "geo_version":
			result.Data.(*proto.ColStr).Append(row.geoVersion)
		case "classification_version":
			result.Data.(*proto.ColUInt32).Append(row.classificationVersion)
		case "generation":
			result.Data.(*proto.ColUInt64).Append(row.generation)
		case "generated_at":
			result.Data.(*proto.ColDateTime64).Append(row.generatedAt)
		case "is_metadata":
			result.Data.(*proto.ColUInt8).Append(row.isMetadata)
		case "marker_count":
			result.Data.(*proto.ColUInt64).Append(row.markerCount)
		}
	}
}

func candidateParameter(query ch.Query, key string) string {
	for _, parameter := range query.Parameters {
		if parameter.Key == key {
			return parameter.Value
		}
	}
	return ""
}

func candidateSetting(query ch.Query, key string) string {
	for _, setting := range query.Settings {
		if setting.Key == key {
			return setting.Value
		}
	}
	return ""
}
