// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream"
)

// This test mutates only the dedicated watchdog_flow development database and
// restores every injected dirty/lock state before returning. It is opt-in so
// ordinary unit tests never depend on Docker or a shared ClickHouse server.
func TestMigratorRealClickHouseLifecycle(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_INTEGRATION=1 to run")
	}
	passwordFile := os.Getenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE")
	if passwordFile == "" {
		t.Fatal("WATCHDOG_CLICKHOUSE_PASSWORD_FILE is required")
	}
	password, err := flowstream.ReadSecretFile(passwordFile)
	if err != nil {
		t.Fatal(err)
	}
	address := os.Getenv("WATCHDOG_CLICKHOUSE_ADDRESS")
	if address == "" {
		address = "127.0.0.1:9000"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	native, err := NewNativeInserter(ctx, NativeConfig{
		Address: address, Database: "default", User: "default", Password: password,
		ClientName: "watchdog-flow-migrate-integration", DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, MaxConns: 1, MinConns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	migrations, err := LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	migrator, err := NewMigrator(native)
	if err != nil {
		t.Fatal(err)
	}

	if len(migrations) < 2 {
		t.Fatal("integration test requires an old and a new migration set")
	}
	olderMigrations := migrations[:len(migrations)-1]
	before, err := migrator.Inspect(ctx, olderMigrations, false)
	if err != nil {
		t.Fatal(err)
	}
	firstOwner := strings.Repeat("1", 32)
	first, err := migrator.Apply(ctx, olderMigrations, MigrationApplyOptions{LockOwner: firstOwner})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.AppliedVersions) != len(before.Plan.Pending) || len(first.AppliedVersions) != len(olderMigrations) {
		t.Fatalf("old plan=%+v applied=%+v", before.Plan, first.AppliedVersions)
	}
	oldInspection, err := migrator.Inspect(ctx, olderMigrations, false)
	if err != nil || len(oldInspection.Plan.Pending) != 0 {
		t.Fatalf("old binary inspection=%+v error=%v", oldInspection, err)
	}
	upgradeInspection, err := migrator.Inspect(ctx, migrations, false)
	if err != nil || len(upgradeInspection.Plan.Pending) != 1 {
		t.Fatalf("new binary inspection=%+v error=%v", upgradeInspection, err)
	}
	upgrade, err := migrator.Apply(ctx, migrations, MigrationApplyOptions{LockOwner: strings.Repeat("2", 32)})
	if err != nil {
		t.Fatal(err)
	}
	if len(upgrade.AppliedVersions) != 1 || upgrade.AppliedVersions[0] != migrations[len(migrations)-1].Version {
		t.Fatalf("upgrade applied=%+v", upgrade.AppliedVersions)
	}
	second, err := migrator.Apply(ctx, migrations, MigrationApplyOptions{LockOwner: strings.Repeat("9", 32)})
	if err != nil || len(second.AppliedVersions) != 0 {
		t.Fatalf("idempotent apply=%+v error=%v", second, err)
	}
	if _, err := migrator.Inspect(ctx, olderMigrations, false); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("old binary accepted a newer schema: %v", err)
	}
	drifted := append([]Migration(nil), migrations...)
	drifted[0].Checksum = strings.Repeat("f", 64)
	if _, err := migrator.Inspect(ctx, drifted, false); err == nil || !strings.Contains(err.Error(), "drift") {
		t.Fatalf("checksum drift was accepted: %v", err)
	}
	// Migration 003 extends ORDER BY with columns introduced by the same ALTER.
	// Replay it explicitly to cover a crash after DDL success but before the
	// statement checkpoint becomes visible.
	for replay := 0; replay < 2; replay++ {
		if err := native.executor.Do(ctx, synchronousMigrationQuery(migrations[2].Statements[4])); err != nil {
			t.Fatalf("replay migration 003 sorting-key statement: %v", err)
		}
	}

	heldOwner := strings.Repeat("3", 32)
	if err := migrator.acquireLock(ctx, heldOwner); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Apply(ctx, migrations, MigrationApplyOptions{LockOwner: strings.Repeat("4", 32)}); !errors.Is(err, ErrMigrationLocked) {
		t.Fatalf("concurrent apply error=%v", err)
	}
	if err := migrator.Unlock(ctx, strings.Repeat("5", 32)); err == nil {
		t.Fatal("wrong owner unlocked the real lock")
	}
	if err := migrator.Unlock(ctx, heldOwner); err != nil {
		t.Fatal(err)
	}

	inspection, err := migrator.Inspect(ctx, migrations, false)
	if err != nil {
		t.Fatal(err)
	}
	last := migrations[len(migrations)-1]
	dirtyGeneration := maxMigrationGeneration(inspection.Recorded) + 1
	completed := uint32(len(last.Statements) - 1)
	if err := migrator.writeState(ctx, last, MigrationFailed, completed, strings.Repeat("6", 32), "integration interruption", dirtyGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Apply(ctx, migrations, MigrationApplyOptions{LockOwner: strings.Repeat("7", 32)}); err == nil {
		t.Fatal("ordinary apply accepted an injected dirty state")
	}
	resumed, err := migrator.Apply(ctx, migrations, MigrationApplyOptions{Resume: true, LockOwner: strings.Repeat("8", 32)})
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.Plan.Resume || resumed.Plan.ResumeStatement != completed || len(resumed.AppliedVersions) != 1 || resumed.AppliedVersions[0] != last.Version {
		t.Fatalf("resume=%+v", resumed)
	}
	final, err := migrator.Inspect(ctx, migrations, false)
	if err != nil || final.Lock != nil || len(final.Plan.Pending) != 0 || len(final.Plan.Applied) != len(migrations) {
		t.Fatalf("final=%+v error=%v", final, err)
	}
}
