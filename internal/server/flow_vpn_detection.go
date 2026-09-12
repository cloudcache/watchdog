// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowvpn"
)

// flow_vpn_detection.go is the production trigger for the VPN Tier-1 pipeline: a
// config-gated background loop that, per closed window, materializes candidates
// from flow_records (ClickHouse), scores them against the active rule set, and
// writes findings (MySQL). It is the caller that drives the previously-standalone
// candidate materializer, scorer, and findings writer as one pipeline.

var errNoActiveVPNRules = errors.New("no active VPN rules to compile")

type vpnDetectionSettings struct {
	window, interval, lag         time.Duration
	maxCandidates                 uint32
	medium, high, critical, probe uint16
	minCompleteness               float64
}

func vpnDetectionSettingsFrom(cfg FlowVPNConfig) vpnDetectionSettings {
	settings := vpnDetectionSettings{
		window: seconds(cfg.WindowSeconds, 300), interval: seconds(cfg.IntervalSeconds, 300), lag: seconds(cfg.LagSeconds, 120),
		maxCandidates: cfg.MaxCandidates, medium: cfg.MediumThreshold, high: cfg.HighThreshold,
		critical: cfg.CriticalThreshold, probe: cfg.ProbeThreshold, minCompleteness: cfg.MinCompleteness,
	}
	if settings.maxCandidates == 0 {
		settings.maxCandidates = flowvpn.MaxScoredCandidates
	}
	if settings.medium == 0 && settings.high == 0 && settings.critical == 0 {
		settings.medium, settings.high, settings.critical = 30, 60, 85
	}
	if settings.probe == 0 {
		settings.probe = 70
	}
	if settings.minCompleteness == 0 {
		settings.minCompleteness = 0.8
	}
	return settings
}

func seconds(value, fallback int) time.Duration {
	if value <= 0 {
		return time.Duration(fallback) * time.Second
	}
	return time.Duration(value) * time.Second
}

// startVPNDetection wires the candidate materializer + scorer on the shared
// ClickHouse pool and starts the background loop. It is a no-op unless the pipeline
// is enabled and ClickHouse is configured.
func (s *Server) startVPNDetection() error {
	if !s.cfg.Flow.VPN.Enabled {
		return nil
	}
	if s.clickHouse == nil {
		log.Printf("watchdog VPN detection is enabled but ClickHouse is not configured; pipeline disabled")
		return nil
	}
	materializer, err := flowch.NewVPNCandidateMaterializer(s.clickHouse)
	if err != nil {
		return err
	}
	runner, err := flowvpn.NewCandidateRunner(s.clickHouse)
	if err != nil {
		return err
	}
	s.vpnCandidateMaterializer = materializer
	s.vpnCandidateRunner = runner
	ctx, cancel := context.WithCancel(context.Background())
	s.vpnDetectCancel = cancel
	go s.vpnDetectionLoop(ctx, vpnDetectionSettingsFrom(s.cfg.Flow.VPN))
	return nil
}

func (s *Server) vpnDetectionLoop(ctx context.Context, settings vpnDetectionSettings) {
	ticker := time.NewTicker(settings.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runVPNDetectionTick(ctx, settings)
		}
	}
}

// runVPNDetectionTick processes the most-recent fully-closed window. Windows are
// idempotent (candidate generation replacement + finding UPSERT), so a re-run of
// a window is safe; a missed tick simply skips that window (a cursor-tracked
// backfill is a later refinement).
func (s *Server) runVPNDetectionTick(ctx context.Context, settings vpnDetectionSettings) {
	ruleSet, version, err := s.buildVPNRuleSet(ctx, settings)
	if err != nil {
		if !errors.Is(err, errNoActiveVPNRules) {
			log.Printf("watchdog VPN detection: build rule set: %v", err)
		}
		return
	}
	now := time.Now().UTC()
	to := now.Add(-settings.lag).Truncate(settings.window)
	from := to.Add(-settings.window)
	if !to.After(from) {
		return
	}
	count, err := s.runVPNDetectionWindow(ctx, from, to, ruleSet, version, settings.maxCandidates)
	if err != nil {
		log.Printf("watchdog VPN detection window [%s,%s): %v", from.Format(time.RFC3339), to.Format(time.RFC3339), err)
		return
	}
	if count > 0 {
		log.Printf("watchdog VPN detection materialized %d findings for [%s,%s)", count, from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
}

// runVPNDetectionWindow runs the full pipeline for one window and returns the
// number of findings written. The generation is the run's unix second — monotonic
// across runs — so re-materialization publishes a fresh candidate generation the
// scorer then reads.
func (s *Server) runVPNDetectionWindow(ctx context.Context, from, to time.Time, ruleSet flowvpn.CompiledRuleSet, version string, maxCandidates uint32) (int, error) {
	now := time.Now().UTC()
	if err := s.vpnCandidateMaterializer.Run(ctx, flowch.VPNCandidateRequest{
		WindowStart: from, WindowEnd: to, RuleSetVersion: version, Generation: uint64(now.Unix()), GeneratedAt: now,
	}); err != nil {
		return 0, err
	}
	scored, err := s.vpnCandidateRunner.Run(ctx, flowvpn.ScoreWindowRequest{WindowStart: from, WindowEnd: to, MaxCandidates: maxCandidates}, ruleSet)
	if err != nil {
		return 0, err
	}
	return s.materializeVPNFindings(ctx, scored.Candidates)
}

// buildVPNRuleSet compiles the active rules into an immutable rule set. The version
// is a deterministic hash of the active rules, so a rule change yields a new
// version (and thus a fresh candidate generation) while the risk thresholds come
// from configuration. Candidate materialization and scoring share this version.
func (s *Server) buildVPNRuleSet(ctx context.Context, settings vpnDetectionSettings) (flowvpn.CompiledRuleSet, string, error) {
	rows, err := s.db.QueryContext(ctx, vpnRuleSelect+" WHERE status='active' AND deleted_at IS NULL ORDER BY id")
	if err != nil {
		return flowvpn.CompiledRuleSet{}, "", err
	}
	defer rows.Close()
	var rules []flowvpn.Rule
	for rows.Next() {
		item, err := scanVPNRule(rows)
		if err != nil {
			return flowvpn.CompiledRuleSet{}, "", err
		}
		rules = append(rules, flowvpn.Rule{
			ID: item.ID, Effect: item.Effect, Weight: item.Weight, Priority: item.Priority,
			Match: item.Match, FamilyHint: item.FamilyHint,
		})
	}
	if err := rows.Err(); err != nil {
		return flowvpn.CompiledRuleSet{}, "", err
	}
	if len(rules) == 0 {
		return flowvpn.CompiledRuleSet{}, "", errNoActiveVPNRules
	}
	version := vpnRuleSetVersion(rules)
	compiled, err := flowvpn.CompileRuleSet(flowvpn.RuleSet{
		SchemaVersion: flowvpn.RuleSchemaV1, Version: version,
		MediumThreshold: settings.medium, HighThreshold: settings.high, CriticalThreshold: settings.critical,
		ProbeThreshold: settings.probe, MinimumCompleteness: settings.minCompleteness, Rules: rules,
	})
	if err != nil {
		return flowvpn.CompiledRuleSet{}, "", err
	}
	return compiled, version, nil
}

// vpnRuleSetVersion is a deterministic identifier of the active rules (already
// ordered by id), valid as a candidate rule_set_version.
func vpnRuleSetVersion(rules []flowvpn.Rule) string {
	raw, _ := json.Marshal(rules)
	sum := sha256.Sum256(raw)
	return "rs-" + hex.EncodeToString(sum[:])[:16]
}
