// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

// Findings materialization (KISS-06 VPN Tier-1): persist scored candidates from
// the flowvpn scoring pipeline into MySQL flow_vpn_findings. Re-materialization of
// the same (window, conversation, dimension/geo/classification version) UPSERTs in
// place — refreshing the score/verdict/evidence while preserving the human
// disposition and probe workflow — so a re-score never discards triage decisions.

// vpnFindingEvidence is the persisted evidence bundle: the matched-rule evidence
// plus the window's materialization provenance.
type vpnFindingEvidence struct {
	SchemaVersion   uint16                          `json:"schema_version"`
	Rules           []flowvpn.Evidence              `json:"rules"`
	Materialization flowvpn.MaterializationEvidence `json:"materialization"`
}

// findingKey is the deterministic natural key of a finding. Identical inputs
// re-materialize into the same row (the UNIQUE finding_key).
func findingKey(sc flowvpn.ScoredCandidate) string {
	c := sc.Candidate
	source := c.WindowStart.UTC().Format(time.RFC3339Nano) + "\x00" + c.WindowEnd.UTC().Format(time.RFC3339Nano) + "\x00" +
		c.ConversationKey + "\x00" + c.DimensionSnapshotID + "\x00" + c.GeoVersion + "\x00" +
		strconv.FormatUint(uint64(c.ClassificationVersion), 10)
	sum := sha256.Sum256([]byte(source))
	return hex.EncodeToString(sum[:])
}

// materializeVPNFindings UPSERTs each scored candidate into flow_vpn_findings and
// returns the number written. It is the pipeline sink a scheduler/worker calls
// after flowvpn scores one closed generation; the trigger/schedule is wired
// separately (worker phase).
func (s *Server) materializeVPNFindings(ctx context.Context, scored []flowvpn.ScoredCandidate) (int, error) {
	count := 0
	for _, sc := range scored {
		if err := s.upsertVPNFinding(ctx, sc); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (s *Server) upsertVPNFinding(ctx context.Context, sc flowvpn.ScoredCandidate) error {
	c := sc.Candidate
	score := sc.Score
	evidenceJSON, err := json.Marshal(vpnFindingEvidence{SchemaVersion: 1, Rules: score.Evidence, Materialization: sc.MaterializationEvidence})
	if err != nil {
		return err
	}
	var familyHints any
	if len(score.FamilyHints) > 0 {
		raw, err := json.Marshal(score.FamilyHints)
		if err != nil {
			return err
		}
		familyHints = raw
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO flow_vpn_findings
		(id, finding_key, window_start, window_end, conversation_key, local_ip, remote_ip,
		 primary_protocol, primary_local_port, primary_remote_port, local_to_remote_bytes, remote_to_local_bytes,
		 flow_record_count, active_bucket_count, max_duration_ms, packet_bytes_p50, remote_asn, remote_country,
		 remote_prefix_id, local_prefix_id, complete_ratio, score, risk_level, verdict, symmetry_ratio, dominance_ratio,
		 probe_recommended, probe_block_reason, decision_rule_id, rule_set_version, family_hints, evidence_json,
		 source_generation, generated_at)
		VALUES (?,?,?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?,?,?,?, ?,?,?,?,?,?, ?,?)
		ON DUPLICATE KEY UPDATE
		 local_ip=VALUES(local_ip), remote_ip=VALUES(remote_ip), primary_protocol=VALUES(primary_protocol),
		 primary_local_port=VALUES(primary_local_port), primary_remote_port=VALUES(primary_remote_port),
		 local_to_remote_bytes=VALUES(local_to_remote_bytes), remote_to_local_bytes=VALUES(remote_to_local_bytes),
		 flow_record_count=VALUES(flow_record_count), active_bucket_count=VALUES(active_bucket_count),
		 max_duration_ms=VALUES(max_duration_ms), packet_bytes_p50=VALUES(packet_bytes_p50),
		 remote_asn=VALUES(remote_asn), remote_country=VALUES(remote_country), remote_prefix_id=VALUES(remote_prefix_id),
		 local_prefix_id=VALUES(local_prefix_id), complete_ratio=VALUES(complete_ratio), score=VALUES(score),
		 risk_level=VALUES(risk_level), verdict=VALUES(verdict), symmetry_ratio=VALUES(symmetry_ratio),
		 dominance_ratio=VALUES(dominance_ratio), probe_recommended=VALUES(probe_recommended),
		 probe_block_reason=VALUES(probe_block_reason), decision_rule_id=VALUES(decision_rule_id),
		 rule_set_version=VALUES(rule_set_version), family_hints=VALUES(family_hints), evidence_json=VALUES(evidence_json),
		 source_generation=VALUES(source_generation), generated_at=VALUES(generated_at),
		 row_version=row_version+1, updated_at=CURRENT_TIMESTAMP(3)`,
		newID(), findingKey(sc), c.WindowStart.UTC(), c.WindowEnd.UTC(), c.ConversationKey, c.LocalIP.String(), c.RemoteIP.String(),
		c.PrimaryProtocol, c.PrimaryLocalPort, c.PrimaryRemotePort, c.LocalToRemoteBytes, c.RemoteToLocalBytes,
		c.FlowRecordCount, c.ActiveBucketCount, c.MaxDurationMS, c.PacketBytesP50, c.RemoteASN, c.RemoteCountry,
		c.RemotePrefixID, c.LocalPrefixID, c.CompleteRatio, score.Score, string(score.Level), string(score.Verdict), score.SymmetryRatio, score.DominanceRatio,
		score.ProbeRecommended, score.ProbeBlockReason, score.DecisionRuleID, score.RuleSetVersion, familyHints, evidenceJSON,
		sc.Generation, sc.GeneratedAt.UTC())
	return err
}
