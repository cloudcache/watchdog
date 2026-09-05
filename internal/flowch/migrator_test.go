// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type migrationExecutor struct {
	queries             []ch.Query
	recorded            []AppliedMigration
	lock                *MigrationLock
	stateTable          bool
	migrationStatements []string
	stateWrites         []ch.Query
	failStatement       string
	cancelStatement     string
	cancel              context.CancelFunc
	dropContextErr      error
}

func (e *migrationExecutor) Do(ctx context.Context, query ch.Query) error {
	e.queries = append(e.queries, query)
	switch {
	case query.Body == "CREATE DATABASE IF NOT EXISTS watchdog_flow":
		return nil
	case query.Body == migrationStateDDL:
		e.stateTable = true
		return nil
	case strings.HasPrefix(query.Body, "CREATE TABLE watchdog_flow.flow_schema_migration_lock"):
		if e.lock != nil {
			return &ch.Exception{Code: proto.ErrTableAlreadyExists, Name: "TABLE_ALREADY_EXISTS"}
		}
		owner := strings.TrimSuffix(query.Body[strings.LastIndex(query.Body, "COMMENT '")+len("COMMENT '"):], "'")
		e.lock = &MigrationLock{Owner: owner, AcquiredAt: "2026-09-05T01:02:03.000Z"}
		return nil
	case strings.Contains(query.Body, "comment AS owner"):
		return e.emitLock(ctx, query)
	case strings.Contains(query.Body, "FROM system.tables"):
		return e.emitTableExists(ctx, query)
	case strings.Contains(query.Body, "FROM watchdog_flow.flow_schema_migrations FINAL"):
		return e.emitRecorded(ctx, query)
	case strings.HasPrefix(query.Body, "INSERT INTO watchdog_flow.flow_schema_migrations"):
		e.stateWrites = append(e.stateWrites, query)
		return nil
	case query.Body == "DROP TABLE watchdog_flow.flow_schema_migration_lock":
		e.dropContextErr = ctx.Err()
		e.lock = nil
		return nil
	default:
		e.migrationStatements = append(e.migrationStatements, query.Body)
		if query.Body == e.failStatement {
			return errors.New("injected statement failure")
		}
		if query.Body == e.cancelStatement {
			e.cancel()
			return ctx.Err()
		}
		return nil
	}
}

func (e *migrationExecutor) emitTableExists(ctx context.Context, query ch.Query) error {
	results := query.Result.(proto.Results)
	count := results[0].Data.(*proto.ColUInt64)
	if err := query.OnResult(ctx, proto.Block{Columns: 1}); err != nil {
		return err
	}
	table := migrationParameter(query, "table")
	exists := table == migrationStateTable && e.stateTable || table == migrationLockTable && e.lock != nil
	if exists {
		count.Append(1)
	} else {
		count.Append(0)
	}
	return query.OnResult(ctx, proto.Block{Columns: 1, Rows: 1})
}

func (e *migrationExecutor) emitLock(ctx context.Context, query ch.Query) error {
	if e.lock == nil {
		return &ch.Exception{Code: proto.ErrUnknownTable, Name: "UNKNOWN_TABLE"}
	}
	results := query.Result.(proto.Results)
	if err := query.OnResult(ctx, proto.Block{Columns: 2}); err != nil {
		return err
	}
	results[0].Data.(*proto.ColStr).Append(e.lock.Owner)
	results[1].Data.(*proto.ColStr).Append(e.lock.AcquiredAt)
	return query.OnResult(ctx, proto.Block{Columns: 2, Rows: 1})
}

func (e *migrationExecutor) emitRecorded(ctx context.Context, query ch.Query) error {
	if len(e.recorded) == 0 {
		return nil
	}
	results := query.Result.(proto.Results)
	for _, state := range e.recorded {
		results[0].Data.(*proto.ColUInt32).Append(state.Version)
		results[1].Data.(*proto.ColStr).Append(state.Name)
		results[2].Data.(*proto.ColStr).Append(state.Checksum)
		results[3].Data.(*proto.ColStr).Append(string(state.State))
		results[4].Data.(*proto.ColUInt32).Append(state.CompletedStatements)
		results[5].Data.(*proto.ColStr).Append(state.AttemptID)
		results[6].Data.(*proto.ColStr).Append(state.LastError)
		results[7].Data.(*proto.ColUInt64).Append(state.Generation)
	}
	return query.OnResult(ctx, proto.Block{Columns: 8, Rows: len(e.recorded)})
}

func TestMigratorApplyFreshTracksEveryStatementAndReleasesLock(t *testing.T) {
	migrations := migrationExecutorSet()
	executor := &migrationExecutor{}
	run, err := (&Migrator{executor: executor}).Apply(context.Background(), migrations, MigrationApplyOptions{LockOwner: strings.Repeat("a", 32)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(run.AppliedVersions, []uint32{1, 2}) || executor.lock != nil {
		t.Fatalf("run=%+v lock=%+v", run, executor.lock)
	}
	wantStatements := []string{"CREATE TABLE one", "ALTER TABLE one ADD COLUMN two UInt8", "CREATE TABLE second"}
	if !reflect.DeepEqual(executor.migrationStatements, wantStatements) {
		t.Fatalf("migration statements=%q", executor.migrationStatements)
	}
	if len(executor.stateWrites) != 7 {
		t.Fatalf("state writes=%d", len(executor.stateWrites))
	}
	if setting(executor.stateWrites[0], "async_insert") != "0" {
		t.Fatal("migration state write did not disable async insertion")
	}
	for _, query := range executor.queries {
		if query.Body == "CREATE TABLE one" && setting(query, "async_insert") != "0" {
			t.Fatal("migration statement did not disable async insertion")
		}
	}
	if migrationParameter(executor.stateWrites[0], "state") != string(MigrationApplying) ||
		migrationParameter(executor.stateWrites[3], "state") != string(MigrationApplied) ||
		migrationParameter(executor.stateWrites[6], "state") != string(MigrationApplied) {
		t.Fatalf("unexpected state sequence: %+v", executor.stateWrites)
	}
	if migrationParameter(executor.stateWrites[6], "completed") != "1" {
		t.Fatalf("final statement progress=%q", migrationParameter(executor.stateWrites[6], "completed"))
	}
}

func TestMigratorFailsFastWhenAnotherOwnerHoldsLock(t *testing.T) {
	existing := &MigrationLock{Owner: strings.Repeat("b", 32), AcquiredAt: "2026-09-05T01:02:03.000Z"}
	executor := &migrationExecutor{lock: existing}
	_, err := (&Migrator{executor: executor}).Apply(context.Background(), migrationExecutorSet(), MigrationApplyOptions{LockOwner: strings.Repeat("a", 32)})
	if !errors.Is(err, ErrMigrationLocked) || executor.lock != existing || len(executor.migrationStatements) != 0 {
		t.Fatalf("error=%v lock=%+v statements=%q", err, executor.lock, executor.migrationStatements)
	}
}

func TestMigratorExplicitResumeStartsAfterCheckpoint(t *testing.T) {
	migrations := migrationExecutorSet()
	executor := &migrationExecutor{recorded: []AppliedMigration{
		{Version: 1, Name: migrations[0].Name, Checksum: migrations[0].Checksum, State: MigrationFailed, CompletedStatements: 1, Generation: 9},
	}}
	run, err := (&Migrator{executor: executor}).Apply(context.Background(), migrations, MigrationApplyOptions{Resume: true, LockOwner: strings.Repeat("c", 32)})
	if err != nil {
		t.Fatal(err)
	}
	if !run.Plan.Resume || run.Plan.ResumeStatement != 1 || !reflect.DeepEqual(run.AppliedVersions, []uint32{1, 2}) ||
		!reflect.DeepEqual(executor.migrationStatements, []string{"ALTER TABLE one ADD COLUMN two UInt8", "CREATE TABLE second"}) {
		t.Fatalf("run=%+v statements=%q", run, executor.migrationStatements)
	}
	if migrationParameter(executor.stateWrites[0], "generation") != "10" {
		t.Fatalf("resume generation=%q", migrationParameter(executor.stateWrites[0], "generation"))
	}
}

func TestMigratorRecordsFailureAtLastCompletedStatementAndReleasesLock(t *testing.T) {
	migrations := migrationExecutorSet()
	executor := &migrationExecutor{failStatement: migrations[0].Statements[1]}
	run, err := (&Migrator{executor: executor}).Apply(context.Background(), migrations, MigrationApplyOptions{LockOwner: strings.Repeat("d", 32)})
	if err == nil || !strings.Contains(err.Error(), "statement 2") || executor.lock != nil || len(run.AppliedVersions) != 0 {
		t.Fatalf("run=%+v error=%v lock=%+v", run, err, executor.lock)
	}
	last := executor.stateWrites[len(executor.stateWrites)-1]
	if migrationParameter(last, "state") != string(MigrationFailed) || migrationParameter(last, "completed") != "1" ||
		!strings.Contains(migrationParameter(last, "error"), "injected statement failure") {
		t.Fatalf("failed state=%+v", last.Parameters)
	}
}

func TestMigratorReleasesLockAfterCallerCancellation(t *testing.T) {
	migrations := migrationExecutorSet()
	ctx, cancel := context.WithCancel(context.Background())
	executor := &migrationExecutor{cancelStatement: migrations[0].Statements[0], cancel: cancel}
	_, err := (&Migrator{executor: executor}).Apply(ctx, migrations, MigrationApplyOptions{LockOwner: strings.Repeat("g", 32)})
	if !errors.Is(err, context.Canceled) || executor.lock != nil || executor.dropContextErr != nil {
		t.Fatalf("error=%v lock=%+v drop context error=%v", err, executor.lock, executor.dropContextErr)
	}
}

func TestMigratorInspectIsReadOnlyAndUnlockRequiresExactOwner(t *testing.T) {
	migrations := migrationExecutorSet()
	owner := strings.Repeat("e", 32)
	executor := &migrationExecutor{
		stateTable: true,
		lock:       &MigrationLock{Owner: owner, AcquiredAt: "2026-09-05T01:02:03.000Z"},
		recorded: []AppliedMigration{
			{Version: 1, Name: migrations[0].Name, Checksum: migrations[0].Checksum, State: MigrationApplied, CompletedStatements: 2, Generation: 4},
		},
	}
	migrator := &Migrator{executor: executor}
	inspection, err := migrator.Inspect(context.Background(), migrations, false)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Lock == nil || inspection.Lock.Owner != owner || len(inspection.Plan.Applied) != 1 || len(inspection.Plan.Pending) != 1 {
		t.Fatalf("inspection=%+v", inspection)
	}
	for _, query := range executor.queries {
		if strings.HasPrefix(query.Body, "CREATE ") || strings.HasPrefix(query.Body, "INSERT ") || strings.HasPrefix(query.Body, "DROP ") {
			t.Fatalf("inspect issued a write: %q", query.Body)
		}
	}
	if err := migrator.Unlock(context.Background(), strings.Repeat("f", 32)); err == nil || executor.lock == nil {
		t.Fatalf("wrong owner unlocked table: error=%v", err)
	}
	if err := migrator.Unlock(context.Background(), owner); err != nil || executor.lock != nil {
		t.Fatalf("exact owner unlock failed: error=%v lock=%+v", err, executor.lock)
	}
}

func TestMigratorRejectsUnsafeOwnerAndTruncatesDiagnostics(t *testing.T) {
	if _, err := (&Migrator{executor: &migrationExecutor{}}).Apply(context.Background(), migrationExecutorSet(), MigrationApplyOptions{LockOwner: "short"}); err == nil {
		t.Fatal("unsafe owner was accepted")
	}
	value := strings.Repeat("界", maxMigrationError)
	truncated := truncateMigrationError(value)
	if len(truncated) > maxMigrationError || !strings.HasPrefix(value, truncated) {
		t.Fatalf("invalid diagnostic truncation: bytes=%d", len(truncated))
	}
}

func migrationExecutorSet() []Migration {
	return []Migration{
		{Version: 1, Name: "001_one.sql", Checksum: strings.Repeat("a", 64), Statements: []string{"CREATE TABLE one", "ALTER TABLE one ADD COLUMN two UInt8"}},
		{Version: 2, Name: "002_two.sql", Checksum: strings.Repeat("b", 64), Statements: []string{"CREATE TABLE second"}},
	}
}

func migrationParameter(query ch.Query, key string) string {
	value := parameter(query, key)
	return strings.TrimSuffix(strings.TrimPrefix(value, "'"), "'")
}
