// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const MaxScoredCandidates = uint32(50_000)

type QueryExecutor interface {
	Do(context.Context, ch.Query) error
}

type ScoreWindowRequest struct {
	TenantID      string
	WindowStart   time.Time
	WindowEnd     time.Time
	MaxCandidates uint32
}

type MaterializationEvidence struct {
	SchemaVersion         uint32 `json:"schema_version"`
	CoveredBuckets        uint64 `json:"covered_buckets"`
	ExpectedBuckets       uint64 `json:"expected_buckets"`
	EstimatedValidRecords uint64 `json:"estimated_valid_records"`
	QualityRecords        uint64 `json:"quality_records"`
}

type ScoredCandidate struct {
	Candidate               Candidate               `json:"candidate"`
	Score                   Result                  `json:"score"`
	MaterializationEvidence MaterializationEvidence `json:"materialization_evidence"`
	Generation              uint64                  `json:"generation"`
	GeneratedAt             time.Time               `json:"generated_at"`
}

type ScoreWindowResult struct {
	RuleSetVersion string            `json:"rule_set_version"`
	Generation     uint64            `json:"generation"`
	Candidates     []ScoredCandidate `json:"candidates"`
}

type CandidateRunner struct {
	executor QueryExecutor
}

func NewCandidateRunner(executor QueryExecutor) (*CandidateRunner, error) {
	if executor == nil {
		return nil, errors.New("ClickHouse VPN candidate executor is required")
	}
	return &CandidateRunner{executor: executor}, nil
}

// Run reads one logical generation and scores it without side effects. The
// marker and data are selected in one ClickHouse query, so a concurrent repair
// cannot combine a generation lookup with rows from another generation.
func (r *CandidateRunner) Run(ctx context.Context, request ScoreWindowRequest, rules CompiledRuleSet) (ScoreWindowResult, error) {
	if ctx == nil {
		return ScoreWindowResult{}, errors.New("ClickHouse VPN candidate context is required")
	}
	query, err := buildCandidateQuery(request, rules)
	if err != nil {
		return ScoreWindowResult{}, err
	}
	if r == nil || r.executor == nil {
		return ScoreWindowResult{}, errors.New("ClickHouse VPN candidate runner is not initialized")
	}
	columns := newCandidateColumns()
	query.Result = columns.results()
	state := candidateResultState{
		request: request, rules: rules,
		seen: make(map[candidateResultKey]struct{}),
	}
	query.OnResult = func(_ context.Context, _ proto.Block) error {
		if state.err != nil {
			return state.err
		}
		state.err = state.consume(columns)
		return state.err
	}
	if err := r.executor.Do(ctx, query); err != nil {
		if state.err != nil {
			return ScoreWindowResult{}, state.err
		}
		return ScoreWindowResult{}, fmt.Errorf("execute ClickHouse VPN candidate query: %w", err)
	}
	if state.err != nil {
		return ScoreWindowResult{}, state.err
	}
	if state.metadataRows != 1 || state.markerCount != 1 || state.generation == 0 {
		return ScoreWindowResult{}, fmt.Errorf("invalid ClickHouse VPN candidate result: marker rows=%d marker_count=%d generation=%d", state.metadataRows, state.markerCount, state.generation)
	}
	for _, candidate := range state.candidates {
		if candidate.Generation != state.generation {
			return ScoreWindowResult{}, errors.New("invalid ClickHouse VPN candidate result: mixed generation")
		}
	}
	return ScoreWindowResult{
		RuleSetVersion: rules.version,
		Generation:     state.generation,
		Candidates:     state.candidates,
	}, nil
}

func buildCandidateQuery(request ScoreWindowRequest, rules CompiledRuleSet) (ch.Query, error) {
	if err := validateScoreWindowRequest(request, rules); err != nil {
		return ch.Query{}, err
	}
	return ch.Query{
		Body: vpnCandidateReadSQL,
		Parameters: ch.Parameters(map[string]any{
			"tenant":           request.TenantID,
			"window_start":     request.WindowStart.UTC().Format("2006-01-02 15:04:05"),
			"window_end":       request.WindowEnd.UTC().Format("2006-01-02 15:04:05"),
			"rule_set_version": rules.version,
			"read_limit":       uint64(request.MaxCandidates) + 1,
		}),
		Settings: []ch.Setting{
			{Key: "max_result_rows", Value: strconv.FormatUint(uint64(request.MaxCandidates)+2, 10), Important: true},
			{Key: "result_overflow_mode", Value: "throw", Important: true},
		},
	}, nil
}

func validateScoreWindowRequest(request ScoreWindowRequest, rules CompiledRuleSet) error {
	if !validTenantID(request.TenantID) || rules.version == "" || len(rules.rules) == 0 {
		return errors.New("VPN candidate tenant and compiled rule set are required")
	}
	start, end := request.WindowStart.UTC(), request.WindowEnd.UTC()
	_, startOffset := request.WindowStart.Zone()
	_, endOffset := request.WindowEnd.Zone()
	duration := end.Sub(start)
	if start.IsZero() || end.IsZero() || startOffset != 0 || endOffset != 0 ||
		start.Truncate(time.Minute) != start || end.Truncate(time.Minute) != end ||
		duration < time.Minute || duration > 24*time.Hour {
		return errors.New("VPN candidate window must be UTC, minute-aligned, and between one minute and 24 hours")
	}
	if request.MaxCandidates == 0 || request.MaxCandidates > MaxScoredCandidates {
		return fmt.Errorf("VPN candidate result limit must be 1..%d", MaxScoredCandidates)
	}
	return nil
}

func validTenantID(value string) bool {
	return len(value) <= 64 && validIdentifier(value)
}

type candidateColumns struct {
	conversationKey       proto.ColStr
	localIP               proto.ColIPv6
	remoteIP              proto.ColIPv6
	primaryProtocol       proto.ColUInt8
	primaryLocalPort      proto.ColUInt16
	primaryRemotePort     proto.ColUInt16
	localToRemoteBytes    proto.ColUInt64
	remoteToLocalBytes    proto.ColUInt64
	flowRecordCount       proto.ColUInt64
	activeBucketCount     proto.ColUInt32
	maxDurationMS         proto.ColUInt64
	remoteASN             proto.ColUInt32
	remoteCountry         proto.ColStr
	remotePrefixID        proto.ColStr
	transportHints        *proto.ColArr[string]
	completeRatio         proto.ColFloat32
	evidenceJSON          proto.ColStr
	dimensionSnapshotID   proto.ColStr
	geoVersion            proto.ColStr
	classificationVersion proto.ColUInt32
	generation            proto.ColUInt64
	generatedAt           *proto.ColDateTime64
	isMetadata            proto.ColUInt8
	markerCount           proto.ColUInt64
}

func newCandidateColumns() *candidateColumns {
	return &candidateColumns{
		transportHints: new(proto.ColStr).Array(),
		generatedAt:    new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC),
	}
}

func (c *candidateColumns) results() proto.Results {
	return proto.Results{
		{Name: "conversation_key", Data: &c.conversationKey},
		{Name: "local_ip", Data: &c.localIP}, {Name: "remote_ip", Data: &c.remoteIP},
		{Name: "primary_protocol", Data: &c.primaryProtocol}, {Name: "primary_local_port", Data: &c.primaryLocalPort},
		{Name: "primary_remote_port", Data: &c.primaryRemotePort}, {Name: "local_to_remote_bytes", Data: &c.localToRemoteBytes},
		{Name: "remote_to_local_bytes", Data: &c.remoteToLocalBytes}, {Name: "flow_record_count", Data: &c.flowRecordCount},
		{Name: "active_bucket_count", Data: &c.activeBucketCount}, {Name: "max_duration_ms", Data: &c.maxDurationMS},
		{Name: "remote_asn", Data: &c.remoteASN}, {Name: "remote_country", Data: &c.remoteCountry},
		{Name: "remote_prefix_id", Data: &c.remotePrefixID}, {Name: "transport_hints", Data: c.transportHints},
		{Name: "complete_ratio", Data: &c.completeRatio}, {Name: "evidence_json", Data: &c.evidenceJSON},
		{Name: "dimension_snapshot_id", Data: &c.dimensionSnapshotID}, {Name: "geo_version", Data: &c.geoVersion},
		{Name: "classification_version", Data: &c.classificationVersion}, {Name: "generation", Data: &c.generation},
		{Name: "generated_at", Data: c.generatedAt}, {Name: "is_metadata", Data: &c.isMetadata},
		{Name: "marker_count", Data: &c.markerCount},
	}
}

func (c *candidateColumns) rowCount() (int, error) {
	if c == nil {
		return 0, errors.New("VPN candidate result columns are not initialized")
	}
	want := c.conversationKey.Rows()
	for name, rows := range map[string]int{
		"local_ip": c.localIP.Rows(), "remote_ip": c.remoteIP.Rows(), "primary_protocol": c.primaryProtocol.Rows(),
		"primary_local_port": c.primaryLocalPort.Rows(), "primary_remote_port": c.primaryRemotePort.Rows(),
		"local_to_remote_bytes": c.localToRemoteBytes.Rows(), "remote_to_local_bytes": c.remoteToLocalBytes.Rows(),
		"flow_record_count": c.flowRecordCount.Rows(), "active_bucket_count": c.activeBucketCount.Rows(),
		"max_duration_ms": c.maxDurationMS.Rows(), "remote_asn": c.remoteASN.Rows(), "remote_country": c.remoteCountry.Rows(),
		"remote_prefix_id": c.remotePrefixID.Rows(), "transport_hints": c.transportHints.Rows(),
		"complete_ratio": c.completeRatio.Rows(), "evidence_json": c.evidenceJSON.Rows(),
		"dimension_snapshot_id": c.dimensionSnapshotID.Rows(), "geo_version": c.geoVersion.Rows(),
		"classification_version": c.classificationVersion.Rows(), "generation": c.generation.Rows(),
		"generated_at": c.generatedAt.Rows(), "is_metadata": c.isMetadata.Rows(), "marker_count": c.markerCount.Rows(),
	} {
		if rows != want {
			return 0, fmt.Errorf("invalid ClickHouse VPN candidate result: column %s has %d rows, want %d", name, rows, want)
		}
	}
	return want, nil
}

type candidateResultKey struct {
	conversationKey       string
	dimensionSnapshotID   string
	geoVersion            string
	classificationVersion uint32
}

type candidateResultState struct {
	request      ScoreWindowRequest
	rules        CompiledRuleSet
	candidates   []ScoredCandidate
	seen         map[candidateResultKey]struct{}
	rows         uint64
	metadataRows uint64
	markerCount  uint64
	generation   uint64
	err          error
}

func (s *candidateResultState) consume(columns *candidateColumns) error {
	rows, err := columns.rowCount()
	if err != nil {
		return err
	}
	for index := 0; index < rows; index++ {
		if columns.isMetadata[index] > 1 {
			return errors.New("invalid ClickHouse VPN candidate result: invalid metadata flag")
		}
		if columns.isMetadata[index] == 1 {
			if err := s.consumeMetadata(columns, index); err != nil {
				return err
			}
			continue
		}
		s.rows++
		if s.rows > uint64(s.request.MaxCandidates) {
			return fmt.Errorf("invalid ClickHouse VPN candidate result: candidates exceed hard limit %d", s.request.MaxCandidates)
		}
		candidate, err := s.candidate(columns, index)
		if err != nil {
			return err
		}
		key := candidateResultKey{
			conversationKey:       candidate.Candidate.ConversationKey,
			dimensionSnapshotID:   candidate.Candidate.DimensionSnapshotID,
			geoVersion:            candidate.Candidate.GeoVersion,
			classificationVersion: candidate.Candidate.ClassificationVersion,
		}
		if _, exists := s.seen[key]; exists {
			return errors.New("invalid ClickHouse VPN candidate result: duplicate candidate key")
		}
		s.seen[key] = struct{}{}
		s.candidates = append(s.candidates, candidate)
	}
	return nil
}

func (s *candidateResultState) consumeMetadata(columns *candidateColumns, index int) error {
	s.metadataRows++
	if s.metadataRows > 1 || columns.conversationKey.Row(index) != "" || columns.localIP[index] != (proto.IPv6{}) ||
		columns.remoteIP[index] != (proto.IPv6{}) || columns.primaryProtocol[index] != 0 || columns.primaryLocalPort[index] != 0 ||
		columns.primaryRemotePort[index] != 0 || columns.localToRemoteBytes[index] != 0 || columns.remoteToLocalBytes[index] != 0 ||
		columns.flowRecordCount[index] != 0 || columns.activeBucketCount[index] != 0 || columns.maxDurationMS[index] != 0 ||
		columns.remoteASN[index] != 0 || columns.remoteCountry.Row(index) != "" || columns.remotePrefixID.Row(index) != "" ||
		len(columns.transportHints.Row(index)) != 0 || columns.completeRatio[index] != 0 || columns.evidenceJSON.Row(index) != "" ||
		columns.dimensionSnapshotID.Row(index) != "" || columns.geoVersion.Row(index) != "" || columns.classificationVersion[index] != 0 ||
		columns.generatedAt.Row(index).UnixMilli() != 0 {
		return errors.New("invalid ClickHouse VPN candidate result: malformed generation marker")
	}
	s.generation = columns.generation[index]
	s.markerCount = columns.markerCount[index]
	return nil
}

func (s *candidateResultState) candidate(columns *candidateColumns, index int) (ScoredCandidate, error) {
	if columns.markerCount[index] != 0 || columns.generation[index] == 0 || columns.generatedAt.Row(index).Before(s.request.WindowEnd) {
		return ScoredCandidate{}, errors.New("invalid ClickHouse VPN candidate result: generation metadata is inconsistent")
	}
	evidence, err := decodeMaterializationEvidence(columns.evidenceJSON.Row(index), s.request, columns.flowRecordCount[index], float64(columns.completeRatio[index]))
	if err != nil {
		return ScoredCandidate{}, err
	}
	hints := columns.transportHints.Row(index)
	transportHints := make([]TransportHint, len(hints))
	for position, hint := range hints {
		transportHints[position] = TransportHint(hint)
	}
	candidate := Candidate{
		WindowStart: s.request.WindowStart, WindowEnd: s.request.WindowEnd,
		ConversationKey: columns.conversationKey.Row(index),
		LocalIP:         columns.localIP[index].ToIP(), RemoteIP: columns.remoteIP[index].ToIP(),
		PrimaryProtocol: columns.primaryProtocol[index], PrimaryLocalPort: columns.primaryLocalPort[index],
		PrimaryRemotePort: columns.primaryRemotePort[index], LocalToRemoteBytes: columns.localToRemoteBytes[index],
		RemoteToLocalBytes: columns.remoteToLocalBytes[index], FlowRecordCount: columns.flowRecordCount[index],
		ActiveBucketCount: columns.activeBucketCount[index], MaxDurationMS: columns.maxDurationMS[index],
		RemoteASN: columns.remoteASN[index], RemoteCountry: columns.remoteCountry.Row(index),
		RemotePrefixID: columns.remotePrefixID.Row(index), TransportHints: transportHints,
		CompleteRatio: float64(columns.completeRatio[index]), DimensionSnapshotID: columns.dimensionSnapshotID.Row(index),
		GeoVersion: columns.geoVersion.Row(index), ClassificationVersion: columns.classificationVersion[index],
	}
	candidate, err = normalizeCandidate(candidate)
	if err != nil {
		return ScoredCandidate{}, fmt.Errorf("invalid ClickHouse VPN candidate result: %w", err)
	}
	score, err := s.rules.Evaluate(candidate)
	if err != nil {
		return ScoredCandidate{}, fmt.Errorf("invalid ClickHouse VPN candidate result: %w", err)
	}
	return ScoredCandidate{
		Candidate: candidate, Score: score, MaterializationEvidence: evidence,
		Generation: columns.generation[index], GeneratedAt: columns.generatedAt.Row(index).UTC(),
	}, nil
}

func decodeMaterializationEvidence(value string, request ScoreWindowRequest, flowRecords uint64, completeRatio float64) (MaterializationEvidence, error) {
	var evidence MaterializationEvidence
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return MaterializationEvidence{}, fmt.Errorf("invalid ClickHouse VPN candidate result: decode evidence: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return MaterializationEvidence{}, errors.New("invalid ClickHouse VPN candidate result: evidence must contain one JSON object")
	}
	expectedBuckets := uint64(request.WindowEnd.Sub(request.WindowStart) / time.Minute)
	if flowRecords == 0 || evidence.SchemaVersion != 1 || evidence.ExpectedBuckets != expectedBuckets || evidence.ExpectedBuckets == 0 ||
		evidence.CoveredBuckets > evidence.ExpectedBuckets || evidence.EstimatedValidRecords > flowRecords ||
		evidence.QualityRecords > flowRecords {
		return MaterializationEvidence{}, errors.New("invalid ClickHouse VPN candidate result: evidence counters are inconsistent")
	}
	want := math.Min(
		float64(evidence.CoveredBuckets)/float64(evidence.ExpectedBuckets),
		float64(evidence.EstimatedValidRecords)/float64(flowRecords),
	)
	if math.IsNaN(completeRatio) || math.IsInf(completeRatio, 0) || math.Abs(completeRatio-want) > 1e-6 {
		return MaterializationEvidence{}, errors.New("invalid ClickHouse VPN candidate result: completeness does not match evidence")
	}
	return evidence, nil
}

const vpnCandidateReadSQL = `WITH
  latest AS (
    SELECT max(generation) AS latest_generation, count() AS marker_count
    FROM flow_vpn_candidates FINAL
    WHERE tenant_id = {tenant:String}
      AND window_start = {window_start:DateTime('UTC')}
      AND window_end = {window_end:DateTime('UTC')}
      AND rule_set_version = {rule_set_version:String}
      AND row_kind = '_generation'
  ),
  candidate_rows AS (
    SELECT
      lower(hex(conversation_key)) AS conversation_key,
      local_ip, remote_ip, primary_protocol, primary_local_port, primary_remote_port,
      local_to_remote_bytes, remote_to_local_bytes, flow_record_count,
      active_bucket_count, max_duration_ms, remote_asn, toString(remote_country) AS remote_country,
      remote_prefix_id, arrayMap(value -> toString(value), transport_hints) AS transport_hints,
      complete_ratio, evidence_json, dimension_snapshot_id, geo_version,
      classification_version, generation, generated_at
    FROM flow_vpn_candidates FINAL
    CROSS JOIN latest
    WHERE tenant_id = {tenant:String}
      AND window_start = {window_start:DateTime('UTC')}
      AND window_end = {window_end:DateTime('UTC')}
      AND rule_set_version = {rule_set_version:String}
      AND row_kind = 'candidate'
      AND generation = latest_generation
      AND marker_count = 1
    ORDER BY conversation_key, dimension_snapshot_id, geo_version, classification_version
    LIMIT {read_limit:UInt64}
  )
SELECT
  conversation_key, local_ip, remote_ip, primary_protocol, primary_local_port,
  primary_remote_port, local_to_remote_bytes, remote_to_local_bytes, flow_record_count,
  active_bucket_count, max_duration_ms, remote_asn, remote_country, remote_prefix_id,
  transport_hints, complete_ratio, evidence_json, dimension_snapshot_id, geo_version,
  classification_version, generation, generated_at, 0 AS is_metadata, 0 AS marker_count
FROM candidate_rows
UNION ALL
SELECT
  '', toIPv6('::'), toIPv6('::'), 0, 0, 0, 0, 0, 0, 0, 0, 0, '', '', [], 0, '', '', '', 0,
  latest_generation, toDateTime64(0, 3, 'UTC'), 1, marker_count
FROM latest`
