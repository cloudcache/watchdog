// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	flowEnrichmentPublishJobType       = "flow.enrichment.publish"
	flowEnrichmentPublishPayloadSchema = 2
)

type flowEnrichmentPublishPayload struct {
	ProfileRowVersion   uint64   `json:"profile_row_version"`
	DimensionSnapshotID string   `json:"dimension_snapshot_id"`
	EffectiveFrom       string   `json:"effective_from"`
	WorkerIDs           []string `json:"worker_ids"`
}

// enqueueFlowEnrichmentPublishTx records the immutable inputs that must still
// be current when the background job compiles the classification bundle.
func enqueueFlowEnrichmentPublishTx(ctx context.Context, tx *sql.Tx, actor string, notBefore time.Time) (opjob.Job, error) {
	profile, err := scanFlowClassificationProfile(tx.QueryRowContext(ctx, `SELECT device_profiles,
		definition_digest,row_version,COALESCE(created_by,''),COALESCE(updated_by,''),created_at,updated_at
		FROM flow_classification_profiles WHERE id=1`))
	if err != nil {
		return opjob.Job{}, err
	}
	targets, err := flowEnrichmentTargets(ctx, tx, profile.Definition.DeviceProfiles, false)
	if err != nil {
		return opjob.Job{}, err
	}
	if notBefore.IsZero() {
		notBefore = time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	} else {
		notBefore = notBefore.UTC().Truncate(time.Minute)
		if !notBefore.After(time.Now().UTC()) {
			notBefore = time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
		}
	}
	var snapshotID string
	if err := tx.QueryRowContext(ctx, `SELECT snapshot_id FROM dimension_snapshot_activations
		WHERE module_key='flow' AND dimension_key='address' AND effective_from<=?
		ORDER BY effective_from DESC LIMIT 1`, notBefore).Scan(&snapshotID); err != nil {
		return opjob.Job{}, err
	}
	payload, err := opjob.EncodePayload(flowEnrichmentPublishPayloadSchema, flowEnrichmentPublishPayload{
		ProfileRowVersion: profile.RowVersion, DimensionSnapshotID: snapshotID,
		EffectiveFrom: notBefore.Format(time.RFC3339Nano), WorkerIDs: targets,
	})
	if err != nil {
		return opjob.Job{}, err
	}
	return opjob.EnqueueTx(ctx, tx, opjob.Job{
		JobType:        flowEnrichmentPublishJobType,
		IdempotencyKey: fmt.Sprintf("flow-enrichment:%d:%s", profile.RowVersion, snapshotID),
		RequestHash:    sha256hex(string(payload)), CheckpointJSON: payload, ProgressTotal: 1, CreatedBy: actor,
	})
}

func (s *Server) enqueueFlowEnrichmentForActivation(ctx context.Context, actor string, effectiveFrom time.Time) (opjob.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return opjob.Job{}, err
	}
	defer tx.Rollback()
	job, err := enqueueFlowEnrichmentPublishTx(ctx, tx, actor, effectiveFrom)
	if err != nil {
		return opjob.Job{}, err
	}
	if err := tx.Commit(); err != nil {
		return opjob.Job{}, err
	}
	return job, nil
}

func (s *Server) flowEnrichmentPublishHandler() opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var payload flowEnrichmentPublishPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, flowEnrichmentPublishPayloadSchema, &payload); err != nil {
			return "", err
		}
		effectiveFrom, err := time.Parse(time.RFC3339Nano, payload.EffectiveFrom)
		if err != nil || payload.ProfileRowVersion == 0 || payload.DimensionSnapshotID == "" ||
			len(payload.WorkerIDs) == 0 || !flowUTCMinute(effectiveFrom) {
			return "", opjob.TerminalError(errors.New("invalid Flow enrichment publish payload"))
		}
		publication, err := s.createFlowEnrichmentPublication(ctx, false, true, effectiveFrom,
			payload.ProfileRowVersion, payload.DimensionSnapshotID, payload.WorkerIDs, job.CreatedBy)
		if errors.Is(err, errFlowEnrichmentSuperseded) {
			return fmt.Sprintf("superseded:profile:%d", payload.ProfileRowVersion), nil
		}
		if errors.Is(err, errFlowEnrichmentNoTargets) || errors.Is(err, errFlowClassificationInvalid) || errors.Is(err, sql.ErrNoRows) {
			return "", opjob.TerminalError(err)
		}
		if err != nil {
			return "", err
		}
		if reporter := opjob.ReporterFromContext(ctx); reporter != nil {
			_ = reporter.Report(ctx, 1, nil)
		}
		return "flow-enrichment:" + publication.ID, nil
	}
}
