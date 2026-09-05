// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const maxVPNCandidateWindow = 24 * time.Hour

type VPNCandidateRequest struct {
	TenantID       string
	WindowStart    time.Time
	WindowEnd      time.Time
	RuleSetVersion string
	Generation     uint64
	GeneratedAt    time.Time
}

type VPNCandidateMaterializer struct {
	executor queryExecutor
}

func NewVPNCandidateMaterializer(native *NativeInserter) (*VPNCandidateMaterializer, error) {
	if native == nil || native.executor == nil {
		return nil, errors.New("ClickHouse native connection is required")
	}
	return &VPNCandidateMaterializer{executor: native.executor}, nil
}

// Run replaces one closed candidate window logically. Candidate rows and the
// generation marker share one synchronous INSERT SELECT, including empty
// windows, so a repair cannot expose stale rows from an older generation.
func (m *VPNCandidateMaterializer) Run(ctx context.Context, request VPNCandidateRequest) error {
	query, err := buildVPNCandidateQuery(request)
	if err != nil {
		return Permanent(err)
	}
	if m == nil || m.executor == nil {
		return Permanent(errors.New("ClickHouse VPN candidate materializer is not initialized"))
	}
	if err := m.executor.Do(ctx, query); err != nil {
		return classifyClickHouseError(fmt.Errorf("materialize ClickHouse VPN candidate window: %w", err))
	}
	return nil
}

// LatestGeneration reads only the marker row. Readers must use this value and
// never infer the current generation from surviving candidate rows.
func (m *VPNCandidateMaterializer) LatestGeneration(ctx context.Context, request VPNCandidateRequest) (uint64, error) {
	if err := ValidateVPNCandidateRequest(request); err != nil {
		return 0, Permanent(err)
	}
	if m == nil || m.executor == nil {
		return 0, Permanent(errors.New("ClickHouse VPN candidate materializer is not initialized"))
	}
	var generations proto.ColUInt64
	var generation uint64
	found := false
	query := ch.Query{
		Body: `SELECT max(generation) AS generation
FROM flow_vpn_candidates FINAL
WHERE tenant_id = {tenant:String}
  AND window_start = {window_start:DateTime('UTC')}
  AND window_end = {window_end:DateTime('UTC')}
  AND rule_set_version = {rule_set_version:String}
  AND row_kind = '_generation'`,
		Parameters: candidateParameters(request),
		Result:     proto.Results{{Name: "generation", Data: &generations}},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if block.Rows != 1 || generations.Rows() != 1 || found {
			return Permanent(errors.New("ClickHouse VPN candidate generation query returned an invalid row count"))
		}
		generation = generations[0]
		found = true
		return nil
	}
	if err := m.executor.Do(ctx, query); err != nil {
		return 0, classifyClickHouseError(fmt.Errorf("read ClickHouse VPN candidate generation: %w", err))
	}
	if !found {
		return 0, Permanent(errors.New("ClickHouse VPN candidate generation query returned no result"))
	}
	return generation, nil
}

func ValidateVPNCandidateRequest(request VPNCandidateRequest) error {
	if !validRollupTenant(request.TenantID) || !validCandidateIdentifier(request.RuleSetVersion) || request.Generation == 0 || request.GeneratedAt.IsZero() {
		return errors.New("VPN candidate tenant, rule-set version, generation, and generated_at are required")
	}
	start, end := request.WindowStart.UTC(), request.WindowEnd.UTC()
	_, startOffset := request.WindowStart.Zone()
	_, endOffset := request.WindowEnd.Zone()
	_, generatedOffset := request.GeneratedAt.Zone()
	duration := end.Sub(start)
	if start.IsZero() || end.IsZero() || startOffset != 0 || endOffset != 0 || generatedOffset != 0 ||
		start.Truncate(time.Minute) != start || end.Truncate(time.Minute) != end {
		return errors.New("VPN candidate window and generated_at must be UTC and minute-aligned")
	}
	if duration < time.Minute || duration > maxVPNCandidateWindow {
		return errors.New("VPN candidate window must be between one minute and 24 hours")
	}
	if request.GeneratedAt.UTC().Before(end) {
		return errors.New("VPN candidate generated_at precedes the closed window end")
	}
	return nil
}

func buildVPNCandidateQuery(request VPNCandidateRequest) (ch.Query, error) {
	if err := ValidateVPNCandidateRequest(request); err != nil {
		return ch.Query{}, err
	}
	tokenSource := fmt.Sprintf("watchdog-flow-vpn-candidate-v1\x00%s\x00%d\x00%d\x00%s\x00%d",
		request.TenantID, request.WindowStart.Unix(), request.WindowEnd.Unix(), request.RuleSetVersion, request.Generation)
	token := sha256.Sum256([]byte(tokenSource))
	return ch.Query{
		Body:       vpnCandidateSQL,
		Parameters: candidateParameters(request),
		Settings: []ch.Setting{
			{Key: "async_insert", Value: "0", Important: true},
			{Key: "wait_for_async_insert", Value: "1", Important: true},
			{Key: "insert_deduplication_token", Value: hex.EncodeToString(token[:]), Important: true},
		},
	}, nil
}

func candidateParameters(request VPNCandidateRequest) []proto.Parameter {
	return ch.Parameters(map[string]any{
		"tenant":           request.TenantID,
		"window_start":     request.WindowStart.UTC().Format("2006-01-02 15:04:05"),
		"window_end":       request.WindowEnd.UTC().Format("2006-01-02 15:04:05"),
		"rule_set_version": request.RuleSetVersion,
		"generation":       request.Generation,
		"generated_at":     request.GeneratedAt.UTC().Format("2006-01-02 15:04:05.000"),
	})
}

func validCandidateIdentifier(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}

const vpnCandidateSQL = `INSERT INTO flow_vpn_candidates (
  window_start, window_end, tenant_id, row_kind, conversation_key,
  local_ip, remote_ip, primary_protocol, primary_local_port, primary_remote_port,
  local_to_remote_bytes, remote_to_local_bytes, flow_record_count,
  active_bucket_count, max_duration_ms, remote_asn, remote_country,
  remote_prefix_id, transport_hints, complete_ratio, evidence_json,
  rule_set_version, dimension_snapshot_id, geo_version, classification_version,
  key_row_kind, key_dimension_snapshot_id, key_geo_version, key_classification_version,
  generation, generated_at)
WITH
  {window_start:DateTime('UTC')} AS candidate_start,
  {window_end:DateTime('UTC')} AS candidate_end,
  dateDiff('minute', candidate_start, candidate_end) AS expected_buckets,
  coverage AS (
    SELECT count() AS covered_buckets
    FROM flow_aggregate_1m FINAL
    WHERE tenant_id = {tenant:String}
      AND bucket >= candidate_start
      AND bucket < candidate_end
      AND dimension_kind = '_generation'
  ),
  candidates AS (
    SELECT
      tenant_id,
      local_ip,
      remote_ip,
      dimension_snapshot_id,
      geo_version,
      classification_version,
      argMax(
        tuple(ip_protocol, local_port, remote_port, remote_asn, remote_country, remote_prefix_id),
        tuple(estimated_valid, estimated_bytes, raw_bytes, event_time, hex(record_id))) AS primary,
      sumIf(estimated_bytes, estimated_valid AND business_direction = 'out') AS local_to_remote_bytes,
      sumIf(estimated_bytes, estimated_valid AND business_direction = 'in') AS remote_to_local_bytes,
      count() AS flow_record_count,
      uniqExact(toStartOfMinute(event_time)) AS active_bucket_count,
      max(flow_duration_ms) AS max_duration_ms,
      countIf(estimated_valid) AS estimated_valid_records,
      countIf(quality_flags != 0) AS quality_records,
      groupUniqArray(ip_protocol) AS observed_protocols
    FROM flow_records FINAL
    WHERE tenant_id = {tenant:String}
      AND event_time >= candidate_start
      AND event_time < candidate_end
      AND disposition = 'count'
      AND business_direction IN ('in', 'out')
      AND local_ip_valid
      AND remote_ip_valid
    GROUP BY tenant_id, local_ip, remote_ip,
      dimension_snapshot_id, geo_version, classification_version
  )
SELECT
  candidate_start,
  candidate_end,
  candidates.tenant_id,
  'candidate',
  SHA256(concat(candidates.tenant_id, '\0', toString(local_ip), '\0', toString(remote_ip))),
  local_ip,
  remote_ip,
  tupleElement(primary, 1),
  tupleElement(primary, 2),
  tupleElement(primary, 3),
  local_to_remote_bytes,
  remote_to_local_bytes,
  flow_record_count,
  active_bucket_count,
  max_duration_ms,
  tupleElement(primary, 4),
  tupleElement(primary, 5),
  tupleElement(primary, 6),
  if(has(observed_protocols, toUInt8(6)), ['tcp'], []),
  toFloat32(least(
    toFloat64(covered_buckets) / toFloat64(expected_buckets),
    toFloat64(estimated_valid_records) / toFloat64(flow_record_count))),
  concat(
    '{"schema_version":1,"covered_buckets":', toString(covered_buckets),
    ',"expected_buckets":', toString(expected_buckets),
    ',"estimated_valid_records":', toString(estimated_valid_records),
    ',"quality_records":', toString(quality_records), '}'),
  {rule_set_version:String},
  dimension_snapshot_id,
  geo_version,
  classification_version,
  toUInt8(1),
  dimension_snapshot_id,
  geo_version,
  classification_version,
  {generation:UInt64},
  {generated_at:DateTime64(3, 'UTC')}
FROM candidates
CROSS JOIN coverage
UNION ALL
SELECT
  candidate_start, candidate_end, {tenant:String}, '_generation',
  CAST('', 'FixedString(32)'), toIPv6('::'), toIPv6('::'), 0, 0, 0,
  0, 0, 0, 0, 0, 0, '', '', [], 0, '{"schema_version":1}',
  {rule_set_version:String}, '', '', 0, toUInt8(2), '', '', 0, {generation:UInt64},
  {generated_at:DateTime64(3, 'UTC')}`
