// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const (
	migrationDatabase   = "watchdog_flow"
	migrationStateTable = "flow_schema_migrations"
	migrationLockTable  = "flow_schema_migration_lock"
	maxMigrationError   = 2048
)

var ErrMigrationLocked = errors.New("ClickHouse migration lock is held")

type MigrationLock struct {
	Owner      string `json:"owner"`
	AcquiredAt string `json:"acquired_at"`
}

type MigrationInspection struct {
	Recorded []AppliedMigration `json:"recorded"`
	Plan     MigrationPlan      `json:"plan"`
	Lock     *MigrationLock     `json:"lock,omitempty"`
}

type MigrationApplyOptions struct {
	Resume    bool
	LockOwner string
}

type MigrationRun struct {
	Plan            MigrationPlan `json:"plan"`
	AppliedVersions []uint32      `json:"applied_versions"`
	LockOwner       string        `json:"lock_owner"`
}

type Migrator struct {
	executor queryExecutor
}

func NewMigrator(native *NativeInserter) (*Migrator, error) {
	if native == nil || native.executor == nil {
		return nil, errors.New("ClickHouse native connection is required")
	}
	return &Migrator{executor: native.executor}, nil
}

// Inspect is read-only. A missing state table means no migrations have been
// applied; it is not created until Apply is explicitly requested.
func (m *Migrator) Inspect(ctx context.Context, available []Migration, resume bool) (MigrationInspection, error) {
	if m == nil || m.executor == nil {
		return MigrationInspection{}, errors.New("ClickHouse migrator is not initialized")
	}
	inspection := MigrationInspection{}
	if _, err := PlanMigrations(available, nil, false); err != nil {
		return inspection, err
	}
	stateExists, err := m.tableExists(ctx, migrationStateTable)
	if err != nil {
		return inspection, err
	}
	if stateExists {
		inspection.Recorded, err = m.readRecorded(ctx)
		if err != nil {
			return inspection, err
		}
	}
	lockExists, err := m.tableExists(ctx, migrationLockTable)
	if err != nil {
		return inspection, err
	}
	if lockExists {
		lock, lockErr := m.readLock(ctx)
		if lockErr != nil {
			return inspection, lockErr
		}
		inspection.Lock = &lock
	}
	inspection.Plan, err = PlanMigrations(available, inspection.Recorded, resume)
	return inspection, err
}

// Apply serializes schema changes with a fail-fast ClickHouse lock. Dirty
// migrations are resumed only when the caller explicitly opts in.
func (m *Migrator) Apply(ctx context.Context, available []Migration, options MigrationApplyOptions) (run MigrationRun, resultErr error) {
	if m == nil || m.executor == nil {
		return run, errors.New("ClickHouse migrator is not initialized")
	}
	if !validMigrationToken(options.LockOwner) {
		return run, errors.New("migration lock owner must be 16-128 safe ASCII characters")
	}
	run.LockOwner = options.LockOwner
	if _, err := PlanMigrations(available, nil, false); err != nil {
		return run, err
	}
	if err := m.bootstrap(ctx); err != nil {
		return run, err
	}
	if err := m.acquireLock(ctx, options.LockOwner); err != nil {
		return run, err
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := m.releaseLock(cleanupContext, options.LockOwner); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("release ClickHouse migration lock: %w", err))
		}
	}()

	recorded, err := m.readRecorded(ctx)
	if err != nil {
		return run, err
	}
	run.Plan, err = PlanMigrations(available, recorded, options.Resume)
	if err != nil {
		return run, err
	}
	generation := maxMigrationGeneration(recorded)
	for migrationIndex, migration := range run.Plan.Pending {
		completed := uint32(0)
		if migrationIndex == 0 && run.Plan.Resume {
			completed = run.Plan.ResumeStatement
		}
		generation++
		if err := m.writeState(ctx, migration, MigrationApplying, completed, options.LockOwner, "", generation); err != nil {
			return run, fmt.Errorf("record ClickHouse migration %03d applying state: %w", migration.Version, err)
		}
		for statementIndex := int(completed); statementIndex < len(migration.Statements); statementIndex++ {
			statement := migrationStatementForExecution(migration.Version, migration.Statements[statementIndex])
			if err := m.executor.Do(ctx, synchronousMigrationQuery(statement)); err != nil {
				statementErr := fmt.Errorf("execute ClickHouse migration %03d statement %d: %w", migration.Version, statementIndex+1, err)
				generation++
				if stateErr := m.writeState(ctx, migration, MigrationFailed, uint32(statementIndex), options.LockOwner, statementErr.Error(), generation); stateErr != nil {
					return run, errors.Join(statementErr, fmt.Errorf("record failed migration state: %w", stateErr))
				}
				return run, statementErr
			}
			completed = uint32(statementIndex + 1)
			generation++
			if err := m.writeState(ctx, migration, MigrationApplying, completed, options.LockOwner, "", generation); err != nil {
				checkpointErr := fmt.Errorf("record ClickHouse migration %03d statement %d progress: %w", migration.Version, statementIndex+1, err)
				generation++
				if stateErr := m.writeState(ctx, migration, MigrationFailed, completed, options.LockOwner, checkpointErr.Error(), generation); stateErr != nil {
					return run, errors.Join(checkpointErr, fmt.Errorf("record failed migration state: %w", stateErr))
				}
				return run, checkpointErr
			}
		}
		generation++
		if err := m.writeState(ctx, migration, MigrationApplied, uint32(len(migration.Statements)), options.LockOwner, "", generation); err != nil {
			return run, fmt.Errorf("record ClickHouse migration %03d applied state: %w", migration.Version, err)
		}
		run.AppliedVersions = append(run.AppliedVersions, migration.Version)
	}
	return run, nil
}

// migrationStatementForExecution preserves the immutable released migration
// bytes/checksum while adapting two legacy DateTime64 TTL expressions to the
// syntax accepted by ClickHouse 24.9. Both tables are rebuilt without those
// TTLs by Storage V2 migration 011; this compatibility applies only while a
// fresh installation crosses migration 001.
func migrationStatementForExecution(version uint32, statement string) string {
	if version != 1 {
		return statement
	}
	return strings.NewReplacer(
		"TTL event_time + INTERVAL", "TTL toDateTime(event_time) + INTERVAL",
		"TTL inserted_at + INTERVAL", "TTL toDateTime(inserted_at) + INTERVAL",
	).Replace(statement)
}

// Unlock removes an orphaned lock only when the exact owner token is given.
// Staleness is an operator decision because a slow migration and a crashed
// process are indistinguishable from ClickHouse alone.
func (m *Migrator) Unlock(ctx context.Context, owner string) error {
	if m == nil || m.executor == nil {
		return errors.New("ClickHouse migrator is not initialized")
	}
	if !validMigrationToken(owner) {
		return errors.New("migration lock owner must be 16-128 safe ASCII characters")
	}
	exists, err := m.tableExists(ctx, migrationLockTable)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("ClickHouse migration lock does not exist")
	}
	return m.releaseLock(ctx, owner)
}

func (m *Migrator) bootstrap(ctx context.Context) error {
	for _, statement := range []string{
		"CREATE DATABASE IF NOT EXISTS " + migrationDatabase,
		migrationStateDDL,
	} {
		if err := m.executor.Do(ctx, ch.Query{Body: statement}); err != nil {
			return fmt.Errorf("bootstrap ClickHouse migration metadata: %w", err)
		}
	}
	return nil
}

func (m *Migrator) acquireLock(ctx context.Context, owner string) error {
	err := m.executor.Do(ctx, ch.Query{
		Body: fmt.Sprintf(`CREATE TABLE watchdog_flow.flow_schema_migration_lock
	(marker UInt8)
ENGINE = TinyLog
COMMENT '%s'`, owner),
	})
	if err == nil {
		return nil
	}
	var exception *ch.Exception
	if errors.As(err, &exception) && exception.Code == proto.ErrTableAlreadyExists {
		lock, readErr := m.readLock(ctx)
		if readErr != nil {
			return errors.Join(ErrMigrationLocked, fmt.Errorf("inspect existing lock: %w", readErr))
		}
		return fmt.Errorf("%w by %q since %s", ErrMigrationLocked, lock.Owner, lock.AcquiredAt)
	}
	return fmt.Errorf("acquire ClickHouse migration lock: %w", err)
}

func (m *Migrator) releaseLock(ctx context.Context, owner string) error {
	lock, err := m.readLock(ctx)
	if err != nil {
		return err
	}
	if lock.Owner != owner {
		return fmt.Errorf("migration lock owner mismatch: current owner is %q", lock.Owner)
	}
	if err := m.executor.Do(ctx, ch.Query{Body: "DROP TABLE watchdog_flow.flow_schema_migration_lock"}); err != nil {
		return err
	}
	return nil
}

func (m *Migrator) tableExists(ctx context.Context, table string) (bool, error) {
	var count proto.ColUInt64
	var (
		found  bool
		result uint64
	)
	query := ch.Query{
		Body: `SELECT count() AS count
FROM system.tables
WHERE database = {database:String} AND name = {table:String}`,
		Parameters: ch.Parameters(map[string]any{"database": migrationDatabase, "table": table}),
		Result:     proto.Results{{Name: "count", Data: &count}},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if block.Rows != 1 || count.Rows() != 1 || found {
			return errors.New("ClickHouse table existence query returned an invalid row count")
		}
		result = count[0]
		found = true
		return nil
	}
	if err := m.executor.Do(ctx, query); err != nil {
		return false, fmt.Errorf("inspect ClickHouse migration table %q: %w", table, err)
	}
	if !found {
		return false, errors.New("ClickHouse table existence query returned no result")
	}
	return result == 1, nil
}

func (m *Migrator) readLock(ctx context.Context) (MigrationLock, error) {
	var owner, acquiredAt proto.ColStr
	var lock MigrationLock
	found := false
	query := ch.Query{
		Body: `SELECT comment AS owner,
       formatDateTime(metadata_modification_time, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS acquired_at
FROM system.tables
WHERE database = 'watchdog_flow' AND name = 'flow_schema_migration_lock'`,
		Result: proto.Results{{Name: "owner", Data: &owner}, {Name: "acquired_at", Data: &acquiredAt}},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if block.Rows != 1 || owner.Rows() != 1 || acquiredAt.Rows() != 1 || found {
			return errors.New("ClickHouse migration lock must contain exactly one row")
		}
		lock = MigrationLock{Owner: owner.Row(0), AcquiredAt: acquiredAt.Row(0)}
		found = true
		return nil
	}
	if err := m.executor.Do(ctx, query); err != nil {
		return MigrationLock{}, fmt.Errorf("read ClickHouse migration lock: %w", err)
	}
	if !found || lock.Owner == "" {
		return MigrationLock{}, errors.New("ClickHouse migration lock is empty")
	}
	return lock, nil
}

func (m *Migrator) readRecorded(ctx context.Context) ([]AppliedMigration, error) {
	var (
		versions            proto.ColUInt32
		names, checksums    proto.ColStr
		states, attemptIDs  proto.ColStr
		lastErrors          proto.ColStr
		completedStatements proto.ColUInt32
		generations         proto.ColUInt64
		recorded            []AppliedMigration
	)
	query := ch.Query{
		Body: `SELECT version, name, toString(checksum) AS checksum, toString(state) AS state,
       completed_statements, attempt_id, last_error, generation
FROM watchdog_flow.flow_schema_migrations FINAL
ORDER BY version`,
		Result: proto.Results{
			{Name: "version", Data: &versions}, {Name: "name", Data: &names},
			{Name: "checksum", Data: &checksums}, {Name: "state", Data: &states},
			{Name: "completed_statements", Data: &completedStatements}, {Name: "attempt_id", Data: &attemptIDs},
			{Name: "last_error", Data: &lastErrors}, {Name: "generation", Data: &generations},
		},
	}
	query.OnResult = func(_ context.Context, _ proto.Block) error {
		rows := versions.Rows()
		if names.Rows() != rows || checksums.Rows() != rows || states.Rows() != rows || completedStatements.Rows() != rows ||
			attemptIDs.Rows() != rows || lastErrors.Rows() != rows || generations.Rows() != rows {
			return errors.New("ClickHouse migration state columns have inconsistent row counts")
		}
		for index := 0; index < rows; index++ {
			recorded = append(recorded, AppliedMigration{
				Version: versions[index], Name: names.Row(index), Checksum: checksums.Row(index), State: MigrationState(states.Row(index)),
				CompletedStatements: completedStatements[index], AttemptID: attemptIDs.Row(index), LastError: lastErrors.Row(index), Generation: generations[index],
			})
		}
		return nil
	}
	if err := m.executor.Do(ctx, query); err != nil {
		return nil, fmt.Errorf("read ClickHouse migration state: %w", err)
	}
	return recorded, nil
}

func (m *Migrator) writeState(ctx context.Context, migration Migration, state MigrationState, completed uint32, attemptID, lastError string, generation uint64) error {
	query := ch.Query{
		Body: `INSERT INTO watchdog_flow.flow_schema_migrations
  (version, name, checksum, state, completed_statements, attempt_id, last_error, generation, updated_at)
VALUES
  ({version:UInt32}, {name:String}, {checksum:FixedString(64)}, {state:Enum8('applying' = 1, 'applied' = 2, 'failed' = 3)}, {completed:UInt32}, {attempt:String}, {error:String}, {generation:UInt64}, now64(3, 'UTC'))`,
		Parameters: ch.Parameters(map[string]any{
			"version": migration.Version, "name": migration.Name, "checksum": migration.Checksum,
			"state": string(state), "completed": completed, "attempt": attemptID,
			"error": truncateMigrationError(lastError), "generation": generation,
		}),
		Settings: synchronousMigrationSettings(),
	}
	return m.executor.Do(ctx, query)
}

func synchronousMigrationQuery(body string) ch.Query {
	return ch.Query{Body: body, Settings: synchronousMigrationSettings()}
}

func synchronousMigrationSettings() []ch.Setting {
	return []ch.Setting{
		{Key: "async_insert", Value: "0", Important: true},
		{Key: "wait_for_async_insert", Value: "1", Important: true},
	}
}

func validMigrationToken(value string) bool {
	if len(value) < 16 || len(value) > 128 {
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

func maxMigrationGeneration(recorded []AppliedMigration) uint64 {
	var maximum uint64
	for _, state := range recorded {
		if state.Generation > maximum {
			maximum = state.Generation
		}
	}
	return maximum
}

func truncateMigrationError(value string) string {
	if len(value) <= maxMigrationError {
		return value
	}
	value = value[:maxMigrationError]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

const migrationStateDDL = `CREATE TABLE IF NOT EXISTS watchdog_flow.flow_schema_migrations (
  version UInt32,
  name String,
  checksum FixedString(64),
  state Enum8('applying' = 1, 'applied' = 2, 'failed' = 3),
  completed_statements UInt32,
  attempt_id String,
  last_error String,
  generation UInt64,
  updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(generation)
ORDER BY version`
