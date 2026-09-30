package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/gin-gonic/gin"
)

// flowLifecycleActorUsername is the users row recorded as the creator and
// publisher of config-managed policy revisions and as the approver of
// unattended raw deletions. It is disabled and has no password or role, so it
// can never sign in.
const flowLifecycleActorUsername = "system:flow-lifecycle"

// runFlowLifecycleConfig applies flow.lifecycle once the installation exists
// (the actor is a users row, and a fresh install requires an empty users table),
// retrying every archive scan interval, then runs unattended raw deletion when
// auto_delete is set.
func (s *Server) runFlowLifecycleConfig(ctx context.Context, store *flowlifecycle.Store, evidence flowlifecycle.RawDayEvidenceReader) {
	for {
		if s.installed.Load() {
			actor, err := s.syncFlowLifecycleConfig(ctx, store, time.Now())
			if err == nil {
				if s.cfg.Flow.Lifecycle.AutoDelete {
					deleter := &flowlifecycle.AutoDeleter{
						Store: store, Evidence: evidence, Actor: actor,
						Interval: flowArchiveScanInterval, MaxDays: flowArchiveScanBudget, Logf: log.Printf,
					}
					deleter.Run(ctx)
				}
				return
			}
			log.Printf("watchdog Flow lifecycle config not applied: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(flowArchiveScanInterval):
		}
	}
}

// syncFlowLifecycleConfig publishes flow.lifecycle as a new policy revision
// when the published one differs from it, and returns the actor's user id.
func (s *Server) syncFlowLifecycleConfig(ctx context.Context, store *flowlifecycle.Store, now time.Time) (string, error) {
	desired, err := s.cfg.Flow.Lifecycle.policy()
	if err != nil {
		return "", err
	}
	actor, err := s.ensureFlowLifecycleActor(ctx)
	if err != nil {
		return "", fmt.Errorf("ensure the lifecycle system actor: %w", err)
	}
	published, err := store.GetPublishedPolicy(ctx)
	if err == nil && sameFlowPolicy(published, desired) {
		return actor, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	desired.ID = ""
	draft, err := store.CreateDraft(ctx, desired, actor)
	if err != nil {
		return "", err
	}
	if _, err := store.Publish(ctx, draft.ID, actor, draft.RowVersion, now); err != nil {
		return "", err
	}
	log.Printf("watchdog Flow lifecycle published policy version %d from flow.lifecycle", draft.Version)
	return actor, nil
}

func (s *Server) ensureFlowLifecycleActor(ctx context.Context) (string, error) {
	if _, err := s.db.ExecContext(ctx, `
		INSERT IGNORE INTO users (id, username, display_name, password_hash, status)
		VALUES (?, ?, 'Flow lifecycle (system)', NULL, 'disabled')`,
		newID(), flowLifecycleActorUsername); err != nil {
		return "", err
	}
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM users WHERE username = ?`, flowLifecycleActorUsername).Scan(&id)
	return id, err
}

// sameFlowPolicy compares the retention semantics of two revisions, ignoring
// identity, status and audit fields.
func sameFlowPolicy(a, b flowlifecycle.Policy) bool {
	return a.BootstrapFrom.Equal(b.BootstrapFrom) && a.RawRetentionSeconds == b.RawRetentionSeconds &&
		a.ArchiveResolutionSeconds == b.ArchiveResolutionSeconds && a.ArchiveRetentionSeconds == b.ArchiveRetentionSeconds &&
		a.LateArrivalSeconds == b.LateArrivalSeconds && a.DeleteGraceSeconds == b.DeleteGraceSeconds &&
		a.MaxPartitionsPerRun == b.MaxPartitionsPerRun && a.RawDeleteEnabled == b.RawDeleteEnabled &&
		a.ArchiveDeleteEnabled == b.ArchiveDeleteEnabled && a.RequireBackupBeforeDelete == b.RequireBackupBeforeDelete &&
		a.AutoDelete == b.AutoDelete && a.WaiveKafkaCoverage == b.WaiveKafkaCoverage
}

// rejectConfigManagedFlowPolicy keeps the policy API read-only while
// flow.lifecycle owns the published revision.
func (s *Server) rejectConfigManagedFlowPolicy(c *gin.Context) {
	if s.cfg.Flow.Lifecycle.Enabled {
		fail(c, http.StatusConflict, "flow_lifecycle_config_managed",
			"The Flow lifecycle policy is managed by flow.lifecycle in the server configuration")
		return
	}
	c.Next()
}
