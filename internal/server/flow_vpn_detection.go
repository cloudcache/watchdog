// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
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
	window, interval, lag time.Duration
	maxCandidates         uint32
}

func vpnDetectionSettingsFrom(cfg FlowVPNConfig) vpnDetectionSettings {
	settings := vpnDetectionSettings{
		window: seconds(cfg.WindowSeconds, 300), interval: seconds(cfg.IntervalSeconds, 300), lag: seconds(cfg.LagSeconds, 120),
		maxCandidates: cfg.MaxCandidates,
	}
	if settings.maxCandidates == 0 {
		settings.maxCandidates = flowvpn.MaxScoredCandidates
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
	s.vpnRuleSetCatalog = flowvpn.NewRuleSetCatalog()
	s.vpnRuleSetBootID = newID()
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
	now := time.Now().UTC()
	to := now.Add(-settings.lag).Truncate(settings.window)
	from := to.Add(-settings.window)
	if !to.After(from) {
		return
	}
	// The scored window is half-open [from,to); resolve the rules at its final
	// included instant so an activation exactly at to starts with the next window.
	// Publication activation timestamps are stored at millisecond precision.
	installed, err := s.installVPNRuleSetForEventTime(ctx, to.Add(-time.Millisecond))
	if err != nil {
		if !errors.Is(err, errNoActiveVPNRules) {
			log.Printf("watchdog VPN detection: install published rule set for %s: %v", to.Format(time.RFC3339), err)
		}
		return
	}
	count, err := s.runVPNDetectionWindow(ctx, from, to, installed.Rules, installed.Metadata.SnapshotID, settings.maxCandidates)
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

const (
	vpnDetectionWorkerID      = "watchdog-server-vpn-detection"
	vpnDetectionWorkerVersion = "watchdog-vpn-detection-v1"
)

// installVPNRuleSetForEventTime resolves the publication active at the closed
// window boundary, verifies its immutable object, and atomically swaps the
// scorer. Any failure is ACKed and leaves the previous catalog entry untouched.
func (s *Server) installVPNRuleSetForEventTime(ctx context.Context, eventTime time.Time) (flowvpn.InstalledRuleSet, error) {
	if s.vpnRuleSetPublisher == nil || s.vpnRuleSetCatalog == nil || s.vpnRuleSetBootID == "" {
		return flowvpn.InstalledRuleSet{}, errors.New("VPN rule-set publication consumer is unavailable")
	}
	activation, err := s.vpnRuleSetPublisher.GetDimensionPublicationActivationAt(ctx, eventTime)
	if errors.Is(err, sql.ErrNoRows) {
		return flowvpn.InstalledRuleSet{}, errNoActiveVPNRules
	}
	if err != nil {
		return flowvpn.InstalledRuleSet{}, err
	}
	snapshot, err := s.vpnRuleSetPublisher.GetDimensionPublicationSnapshot(ctx, activation.SnapshotID)
	if err != nil {
		return flowvpn.InstalledRuleSet{}, err
	}
	if snapshot.ApprovalState != address.AddressDimensionApprovalApproved || snapshot.ObjectDeletedAt != nil {
		return flowvpn.InstalledRuleSet{}, s.failVPNRuleSetInstall(ctx, snapshot, "PUBLICATION_NOT_INSTALLABLE", errors.New("activated VPN rule set is not approved and available"))
	}
	if current, ok := s.vpnRuleSetCatalog.Current(); ok && current.Metadata.SnapshotID == snapshot.ID && current.Metadata.Checksum == snapshot.Checksum {
		return current, nil
	}

	path, err := s.addressObjects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		return flowvpn.InstalledRuleSet{}, s.failVPNRuleSetInstall(ctx, snapshot, "OBJECT_UNAVAILABLE", err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > flowvpn.MaxRuleSetBundleBytes {
		if err == nil {
			err = fmt.Errorf("VPN rule-set object must be a regular file of 1..%d bytes", flowvpn.MaxRuleSetBundleBytes)
		}
		return flowvpn.InstalledRuleSet{}, s.failVPNRuleSetInstall(ctx, snapshot, "OBJECT_UNAVAILABLE", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return flowvpn.InstalledRuleSet{}, s.failVPNRuleSetInstall(ctx, snapshot, "OBJECT_UNAVAILABLE", err)
	}
	if err := s.reportVPNRuleSetACK(ctx, snapshot, address.DimensionPublicationAckDownloaded, "", ""); err != nil {
		return flowvpn.InstalledRuleSet{}, err
	}
	rules, metadata, err := flowvpn.DecodeAndCompileRuleSetBundle(data, snapshot.Checksum)
	if err != nil {
		return flowvpn.InstalledRuleSet{}, s.failVPNRuleSetInstall(ctx, snapshot, "VERIFY_FAILED", err)
	}
	if metadata.SnapshotID != snapshot.ID || metadata.Version != snapshot.Version || !metadata.EffectiveFrom.Equal(snapshot.EffectiveFrom.UTC()) {
		return flowvpn.InstalledRuleSet{}, s.failVPNRuleSetInstall(ctx, snapshot, "IDENTITY_MISMATCH", errors.New("VPN rule-set object metadata does not match publication"))
	}
	previous, hadPrevious := s.vpnRuleSetCatalog.Current()
	if err := s.vpnRuleSetCatalog.Install(rules, metadata); err != nil {
		return flowvpn.InstalledRuleSet{}, s.failVPNRuleSetInstall(ctx, snapshot, "INSTALL_FAILED", err)
	}
	if err := s.reportVPNRuleSetACK(ctx, snapshot, address.DimensionPublicationAckInstalled, "", ""); err != nil {
		s.vpnRuleSetCatalog.Restore(previous, hadPrevious)
		return flowvpn.InstalledRuleSet{}, err
	}
	installed, ok := s.vpnRuleSetCatalog.Current()
	if !ok {
		return flowvpn.InstalledRuleSet{}, errors.New("VPN rule-set catalog lost installed publication")
	}
	return installed, nil
}

func (s *Server) failVPNRuleSetInstall(ctx context.Context, snapshot address.DimensionPublicationSnapshot, code string, cause error) error {
	message := cause.Error()
	runes := []rune(message)
	if len(runes) > 512 {
		message = string(runes[:512])
	}
	if err := s.reportVPNRuleSetACK(ctx, snapshot, address.DimensionPublicationAckFailed, code, message); err != nil {
		return errors.Join(cause, fmt.Errorf("report failed VPN rule-set install: %w", err))
	}
	return cause
}

func (s *Server) reportVPNRuleSetACK(ctx context.Context, snapshot address.DimensionPublicationSnapshot, state, errorCode, errorMessage string) error {
	_, err := s.vpnRuleSetPublisher.ReportDimensionPublicationAcknowledgement(ctx, address.DimensionPublicationAcknowledgement{
		SnapshotID: snapshot.ID, WorkerID: vpnDetectionWorkerID, BootID: s.vpnRuleSetBootID,
		SoftwareVersion: vpnDetectionWorkerVersion, Checksum: snapshot.Checksum, State: state,
		ErrorCode: errorCode, ErrorMessage: errorMessage,
	})
	return err
}
