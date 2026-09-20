// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowvpn"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestRealClickHouseVPNCandidateRepairAndScoring(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_vpn_candidate")
	windowStart := time.Date(2026, 9, 5, 16, 0, 0, 0, time.UTC)
	windowEnd := windowStart.Add(2 * time.Minute)
	local := netip.MustParseAddr("2001:db8:10::1")
	remote := netip.MustParseAddr("2001:db8:20::1")

	out := integrationVPNRecord(1, windowStart.Add(10*time.Second), flowdimension.DirectionOut, local, remote, 1_000, true)
	in := integrationVPNRecord(2, windowStart.Add(time.Minute+20*time.Second), flowdimension.DirectionIn, local, remote, 900, true)
	unknownSampling := integrationVPNRecord(3, windowStart.Add(40*time.Second), flowdimension.DirectionOut, local, remote, 500, false)
	unknownSampling.QualityFlags = flowworker.QualitySamplingSelectorUnavailable
	initialRecords := []flowworker.EnrichedRecord{out, in, unknownSampling}
	insertIntegrationBatch(t, ctx, native, integrationBatch(60, windowEnd.Add(time.Minute), initialRecords...))

	rollup, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < 2; offset++ {
		bucket := windowStart.Add(time.Duration(offset) * time.Minute)
		if err := rollup.Run(ctx, RollupRequest{
			Resolution: RollupOneMinute, Bucket: bucket,
			Generation: 1, GeneratedAt: windowEnd.Add(2 * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}

	materializer, err := NewVPNCandidateMaterializer(native)
	if err != nil {
		t.Fatal(err)
	}
	request := VPNCandidateRequest{
		WindowStart: windowStart, WindowEnd: windowEnd,
		RuleSetVersion: "vpn-integration-v1", Generation: 1, GeneratedAt: windowEnd.Add(3 * time.Minute),
	}
	if err := materializer.Run(ctx, request); err != nil {
		t.Fatal(err)
	}
	assertVPNGeneration(t, ctx, materializer, request, 1)

	runner, err := flowvpn.NewCandidateRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	rules := integrationVPNRules(t)
	before := runIntegrationVPNCandidates(t, ctx, runner, request, rules)
	assertVPNCandidate(t, before, local, remote, 1_000, 900, 3, 2, 2.0/3.0, 1, false)

	late := integrationVPNRecord(4, windowStart.Add(time.Minute+45*time.Second), flowdimension.DirectionOut, local, remote, 700, true)
	insertIntegrationBatch(t, ctx, native, integrationBatch(61, windowEnd.Add(4*time.Minute), late))
	if err := rollup.Run(ctx, RollupRequest{
		Resolution: RollupOneMinute, Bucket: windowStart.Add(time.Minute),
		Generation: 2, GeneratedAt: windowEnd.Add(5 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	request.Generation = 2
	request.GeneratedAt = windowEnd.Add(6 * time.Minute)
	if err := materializer.Run(ctx, request); err != nil {
		t.Fatal(err)
	}
	assertVPNGeneration(t, ctx, materializer, request, 2)
	after := runIntegrationVPNCandidates(t, ctx, runner, request, rules)
	assertVPNCandidate(t, after, local, remote, 1_700, 900, 4, 2, 0.75, 2, true)

	droppedInitial := append([]flowworker.EnrichedRecord(nil), initialRecords...)
	for index := range droppedInitial {
		droppedInitial[index].Disposition = flowdimension.DispositionDrop
	}
	droppedLate := late
	droppedLate.Disposition = flowdimension.DispositionDrop
	// Reprocess the original Kafka coordinates (offsets 60/61) as dropped with a
	// later ingest generation so ReplacingMergeTree FINAL collapses to the DROP
	// version; a fresh offset would leave the original count rows and the
	// candidate would survive.
	insertIntegrationBatch(t, ctx, native, integrationBatch(60, windowEnd.Add(7*time.Minute), droppedInitial...))
	insertIntegrationBatch(t, ctx, native, integrationBatch(61, windowEnd.Add(7*time.Minute), droppedLate))
	request.Generation = 3
	request.GeneratedAt = windowEnd.Add(8 * time.Minute)
	if err := materializer.Run(ctx, request); err != nil {
		t.Fatal(err)
	}
	assertVPNGeneration(t, ctx, materializer, request, 3)
	empty := runIntegrationVPNCandidates(t, ctx, runner, request, rules)
	if empty.Generation != 3 || len(empty.Candidates) != 0 {
		t.Fatalf("authoritative empty VPN generation=%+v", empty)
	}
}

func TestRealClickHouseVPNCandidateVersionUpgradeAndRollback(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_vpn_versions")
	windowStart := time.Date(2026, 9, 6, 3, 0, 0, 0, time.UTC)
	windowEnd := windowStart.Add(2 * time.Minute)
	local := netip.MustParseAddr("10.10.0.1")
	remote := netip.MustParseAddr("198.51.100.20")

	v1Out := integrationVPNRecord(10, windowStart.Add(10*time.Second), flowdimension.DirectionOut, local, remote, 1_000, true)
	v1In := integrationVPNRecord(11, windowStart.Add(time.Minute+10*time.Second), flowdimension.DirectionIn, local, remote, 900, true)
	v2Out := integrationVPNRecord(12, windowStart.Add(20*time.Second), flowdimension.DirectionOut, local, remote, 700, true)
	v2In := integrationVPNRecord(13, windowStart.Add(time.Minute+20*time.Second), flowdimension.DirectionIn, local, remote, 600, true)
	for _, record := range []*flowworker.EnrichedRecord{&v2Out, &v2In} {
		record.Dimensions.SnapshotID = "snapshot-vpn-v2"
		record.Dimensions.Version = 2
		record.RemoteGeo.Version = "geo-vpn-v2"
		record.ClassificationVersion = 2
	}
	insertIntegrationBatch(t, ctx, native, integrationBatch(70, windowEnd.Add(time.Minute), v1Out, v1In, v2Out, v2In))

	rollup, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < 2; offset++ {
		bucket := windowStart.Add(time.Duration(offset) * time.Minute)
		if err := rollup.Run(ctx, RollupRequest{
			Resolution: RollupOneMinute, Bucket: bucket,
			Generation: 1, GeneratedAt: windowEnd.Add(2 * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}

	materializer, err := NewVPNCandidateMaterializer(native)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowvpn.NewCandidateRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	v1Rules := integrationVPNRulesVersion(t, "vpn-rules-v1", 20)
	v1Request := VPNCandidateRequest{
		WindowStart: windowStart, WindowEnd: windowEnd,
		RuleSetVersion: "vpn-rules-v1", Generation: 1, GeneratedAt: windowEnd.Add(3 * time.Minute),
	}
	if err := materializer.Run(ctx, v1Request); err != nil {
		t.Fatal(err)
	}
	v1Before := runIntegrationVPNCandidates(t, ctx, runner, v1Request, v1Rules)
	assertVPNVersionCandidates(t, v1Before, 1, 20)

	v2Rules := integrationVPNRulesVersion(t, "vpn-rules-v2", 30)
	v2Request := v1Request
	v2Request.RuleSetVersion = "vpn-rules-v2"
	v2Request.GeneratedAt = windowEnd.Add(4 * time.Minute)
	if err := materializer.Run(ctx, v2Request); err != nil {
		t.Fatal(err)
	}
	v2 := runIntegrationVPNCandidates(t, ctx, runner, v2Request, v2Rules)
	assertVPNVersionCandidates(t, v2, 1, 30)
	// Publishing v2 does not mutate the v1 machine result.
	assertVPNVersionCandidates(t, runIntegrationVPNCandidates(t, ctx, runner, v1Request, v1Rules), 1, 20)

	// A management-plane rollback selects the immutable v1 rule set and writes
	// a new v1 generation; it does not rewrite either historical generation.
	v1Request.Generation = 2
	v1Request.GeneratedAt = windowEnd.Add(5 * time.Minute)
	if err := materializer.Run(ctx, v1Request); err != nil {
		t.Fatal(err)
	}
	assertVPNVersionCandidates(t, runIntegrationVPNCandidates(t, ctx, runner, v1Request, v1Rules), 2, 20)
	assertVPNVersionCandidates(t, runIntegrationVPNCandidates(t, ctx, runner, v2Request, v2Rules), 1, 30)
}

func integrationVPNRecord(index byte, eventTime time.Time, direction flowdimension.BusinessDirection, local, remote netip.Addr, bytes uint64, estimatedValid bool) flowworker.EnrichedRecord {
	record := testEnrichedRecord(index, bytes, bytes)
	record.EventTime = eventTime
	record.IPProtocol = 6
	record.FlowDurationMS = 120_000
	record.LocalPort = 50_000
	record.RemotePort = 443
	record.RemoteASN = 64_512
	record.RemoteASNSource = flowworker.ASNSourceGeoV2
	record.RemoteGeo.Country = "US"
	record.RemoteGeo.CountryID = "US"
	record.RemoteGeo.Version = "geo-vpn"
	record.Dimensions.SnapshotID = "snapshot-vpn"
	record.Dimensions.Version = 1
	record.Dimensions.Direction = direction
	record.Dimensions.Local.IP = local
	record.Dimensions.Local.PrefixID = "local-vpn"
	record.Dimensions.Remote.IP = remote
	record.Dimensions.Remote.PrefixID = "risk-prefix"
	record.EstimatedValid = estimatedValid
	if !estimatedValid {
		record.EstimatedBytes = 0
		record.EstimatedPackets = 0
	}
	if direction == flowdimension.DirectionIn {
		record.SourceIP = remote
		record.DestinationIP = local
		record.SourcePort = 443
		record.DestinationPort = 50_000
	} else {
		record.SourceIP = local
		record.DestinationIP = remote
		record.SourcePort = 50_000
		record.DestinationPort = 443
	}
	return record
}

func integrationVPNRules(t *testing.T) flowvpn.CompiledRuleSet {
	return integrationVPNRulesVersion(t, "vpn-integration-v1", 20)
}

func integrationVPNRulesVersion(t *testing.T, version string, weight uint16) flowvpn.CompiledRuleSet {
	t.Helper()
	rules, err := flowvpn.CompileRuleSet(flowvpn.RuleSet{
		SchemaVersion: flowvpn.RuleSchemaV1, Version: version,
		MediumThreshold: 10, HighThreshold: 20, CriticalThreshold: 30,
		ProbeThreshold: 20, MinimumCompleteness: 0.7,
		Rules: []flowvpn.Rule{{
			ID: "remote-tls-port", Effect: flowvpn.EffectScore, Weight: weight,
			Match: flowvpn.Match{RemotePorts: []uint16{443}, Protocols: []uint8{6}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return rules
}

func assertVPNVersionCandidates(t testing.TB, result flowvpn.ScoreWindowResult, generation uint64, score uint16) {
	t.Helper()
	if result.Generation != generation || len(result.Candidates) != 2 {
		t.Fatalf("versioned VPN candidates generation=%d count=%d, want %d/2", result.Generation, len(result.Candidates), generation)
	}
	wantVersions := map[string]struct {
		geo            string
		classification uint32
	}{
		"snapshot-vpn":    {geo: "geo-vpn", classification: 1},
		"snapshot-vpn-v2": {geo: "geo-vpn-v2", classification: 2},
	}
	seen := make(map[string]struct{}, len(result.Candidates))
	for _, candidate := range result.Candidates {
		want, exists := wantVersions[candidate.Candidate.DimensionSnapshotID]
		if !exists || candidate.Candidate.GeoVersion != want.geo || candidate.Candidate.ClassificationVersion != want.classification ||
			candidate.Score.Score != score || candidate.Generation != generation || candidate.Score.RuleSetVersion != result.RuleSetVersion {
			t.Fatalf("versioned VPN candidate=%+v result=%+v", candidate, result)
		}
		seen[candidate.Candidate.DimensionSnapshotID] = struct{}{}
	}
	if len(seen) != len(wantVersions) {
		t.Fatalf("versioned VPN snapshots=%v", seen)
	}
}

func runIntegrationVPNCandidates(t *testing.T, ctx context.Context, runner *flowvpn.CandidateRunner, request VPNCandidateRequest, rules flowvpn.CompiledRuleSet) flowvpn.ScoreWindowResult {
	t.Helper()
	result, err := runner.Run(ctx, flowvpn.ScoreWindowRequest{
		WindowStart: request.WindowStart,
		WindowEnd:   request.WindowEnd, MaxCandidates: 10,
	}, rules)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertVPNCandidate(t *testing.T, result flowvpn.ScoreWindowResult, local, remote netip.Addr, outbound, inbound, records uint64, buckets uint32, completeness float64, generation uint64, probeRecommended bool) {
	t.Helper()
	if result.Generation != generation || result.RuleSetVersion != "vpn-integration-v1" || len(result.Candidates) != 1 {
		t.Fatalf("VPN candidate result=%+v", result)
	}
	wantKey := sha256.Sum256([]byte(local.String() + "\x00" + remote.String()))
	candidate := result.Candidates[0]
	if candidate.Candidate.ConversationKey != hex.EncodeToString(wantKey[:]) ||
		candidate.Candidate.LocalIP != local || candidate.Candidate.RemoteIP != remote ||
		candidate.Candidate.LocalToRemoteBytes != outbound || candidate.Candidate.RemoteToLocalBytes != inbound ||
		candidate.Candidate.FlowRecordCount != records || candidate.Candidate.ActiveBucketCount != buckets ||
		candidate.Candidate.RemoteASN != 64_512 || candidate.Candidate.RemoteCountry != "US" ||
		candidate.Candidate.RemotePrefixID != "risk-prefix" || candidate.Generation != generation ||
		candidate.MaterializationEvidence.CoveredBuckets != 2 || candidate.MaterializationEvidence.ExpectedBuckets != 2 ||
		candidate.MaterializationEvidence.EstimatedValidRecords != records-1 || candidate.MaterializationEvidence.QualityRecords != 1 ||
		candidate.Candidate.CompleteRatio < completeness-0.000_001 || candidate.Candidate.CompleteRatio > completeness+0.000_001 ||
		candidate.Score.Score != 20 || candidate.Score.ProbeRecommended != probeRecommended {
		t.Fatalf("VPN candidate=%+v", candidate)
	}
}

func assertVPNGeneration(t *testing.T, ctx context.Context, materializer *VPNCandidateMaterializer, request VPNCandidateRequest, want uint64) {
	t.Helper()
	generation, err := materializer.LatestGeneration(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if generation != want {
		t.Fatalf("VPN generation=%d, want %d", generation, want)
	}
}
