// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	_ "github.com/go-sql-driver/mysql"
)

// TestResolveFlowOperatorQueryBinding exercises the operator-binding resolver against
// the real KISS schema: the customer-ISP lookup, the ranged address-dimension
// activation timeline, and the "installed on every active flow worker" readiness
// gate. Gated on WATCHDOG_TEST_MYSQL_DSN (same pattern as the other integration tests).
func TestResolveFlowOperatorQueryBinding(t *testing.T) {
	db := openFlowOperatorTestDB(t)
	ctx := context.Background()
	from := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	// Shared address/flow dimension timeline (keyed by module_key/dimension_key, not by
	// operator), so each scenario clears and re-seeds it.
	reset := func() {
		db.Exec(`DELETE FROM dimension_snapshot_acks`)
		db.Exec(`DELETE FROM dimension_snapshot_activations WHERE module_key='flow' AND dimension_key='address'`)
		db.Exec(`DELETE FROM dimension_snapshots WHERE module_key='flow' AND dimension_key='address'`)
		db.Exec(`DELETE FROM agents WHERE kind='flow_worker'`)
		db.Exec(`DELETE FROM isp_operators WHERE code LIKE 'op-test-%'`)
	}
	reset()
	t.Cleanup(reset)

	// distinctByEffective avoids the (module_key,dimension_key,effective_from) unique
	// keys colliding across the snapshot + activation rows within one scenario.
	seedSnapshot := func(t *testing.T, id string, version int, snapEffective time.Time) {
		t.Helper()
		digest := "sha256:" + strings.Repeat("a", 64)
		if _, err := db.Exec(`INSERT INTO dimension_snapshots
			(id, module_key, dimension_key, version, effective_from, object_ref, checksum, draft_digest, bundle_schema_version, source_manifest)
			VALUES (?, 'flow', 'address', ?, ?, ?, 'sha256:snap', ?, 1, '[]')`,
			id, version, snapEffective.UTC(), "obj://"+id, digest); err != nil {
			t.Fatal(err)
		}
	}
	seedActivation := func(t *testing.T, snapshotID string, effective time.Time) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO dimension_snapshot_activations
			(id, module_key, dimension_key, snapshot_id, effective_from, reason)
			VALUES (?, 'flow', 'address', ?, ?, 'publish')`, newID(), snapshotID, effective.UTC()); err != nil {
			t.Fatal(err)
		}
	}
	seedWorker := func(t *testing.T) string {
		t.Helper()
		id := newID()
		if _, err := db.Exec(`INSERT INTO agents (id, kind, status, capabilities_json) VALUES (?, 'flow_worker', 'active', '[]')`, id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	seedInstalledAck := func(t *testing.T, snapshotID, workerID string) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO dimension_snapshot_acks
			(snapshot_id, worker_id, boot_id, software_version, checksum, state, installed_at)
			VALUES (?, ?, ?, 'v1', 'sha256:snap', 'installed', ?)`,
			snapshotID, workerID, newID(), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	seedOperator := func(t *testing.T, code string, flowISPID int, enabled bool) string {
		t.Helper()
		id := newID()
		if _, err := db.Exec(`INSERT INTO isp_operators (id, code, name, category, flow_isp_id, asns, enabled)
			VALUES (?, ?, ?, 'other', ?, '[]', ?)`, id, code, code, flowISPID, enabled); err != nil {
			t.Fatal(err)
		}
		return id
	}

	t.Run("happy path pins snapshot + isp, all workers installed", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-a", 42, true)
		snap := newID()
		seedSnapshot(t, snap, 1, from.Add(-48*time.Hour))
		seedActivation(t, snap, from.Add(-24*time.Hour)) // effective before `from` → covers range start
		worker := seedWorker(t)
		seedInstalledAck(t, snap, worker)

		binding, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if binding.FlowISPID != 42 || binding.ExpectedWorkers != 1 ||
			len(binding.DimensionSnapshotIDs) != 1 || binding.DimensionSnapshotIDs[0] != snap {
			t.Fatalf("binding = %+v", binding)
		}
	})

	t.Run("disabled operator is invalid", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-b", 7, false)
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); !errors.Is(err, errFlowOperatorInvalid) {
			t.Fatalf("disabled operator err = %v, want errFlowOperatorInvalid", err)
		}
	})

	t.Run("unknown operator is invalid", func(t *testing.T) {
		reset()
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, newID(), from, to); !errors.Is(err, errFlowOperatorInvalid) {
			t.Fatalf("unknown operator err = %v, want errFlowOperatorInvalid", err)
		}
	})

	t.Run("coverage gap at range start is unavailable", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-c", 42, true)
		snap := newID()
		seedSnapshot(t, snap, 1, from.Add(-48*time.Hour))
		seedActivation(t, snap, from.Add(30*time.Minute)) // starts AFTER `from` → start uncovered
		worker := seedWorker(t)
		seedInstalledAck(t, snap, worker)

		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); !errors.Is(err, errFlowOperatorUnavailable) {
			t.Fatalf("coverage gap err = %v, want errFlowOperatorUnavailable", err)
		}
	})

	t.Run("snapshot not installed on all active workers is unavailable", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-d", 42, true)
		snap := newID()
		seedSnapshot(t, snap, 1, from.Add(-48*time.Hour))
		seedActivation(t, snap, from.Add(-24*time.Hour))
		installed := seedWorker(t)
		_ = seedWorker(t) // a second active worker with no ack → rollout incomplete
		seedInstalledAck(t, snap, installed)

		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); !errors.Is(err, errFlowOperatorUnavailable) {
			t.Fatalf("incomplete rollout err = %v, want errFlowOperatorUnavailable", err)
		}
	})

	t.Run("no active flow workers is unavailable", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-e", 42, true)
		snap := newID()
		seedSnapshot(t, snap, 1, from.Add(-48*time.Hour))
		seedActivation(t, snap, from.Add(-24*time.Hour))
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); !errors.Is(err, errFlowOperatorUnavailable) {
			t.Fatalf("no workers err = %v, want errFlowOperatorUnavailable", err)
		}
	})
}

func openFlowOperatorTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	return db
}
