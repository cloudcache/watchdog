package watchdog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestWatchdogMigrationContainsCoreTables(t *testing.T) {
	sqlText := readWatchdogInitSchema(t)
	re := regexp.MustCompile("(?i)CREATE\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS\\s+`?([a-z_]+)`?")
	matches := re.FindAllStringSubmatch(sqlText, -1)
	tables := make(map[string]bool, len(matches))
	for _, match := range matches {
		tables[strings.ToLower(match[1])] = true
	}
	for _, table := range []string{
		"tenants",
		"users",
		"targets",
		"network_devices",
		"network_ports",
		"network_interface_addresses",
		"port_policies",
		"traffic_policy_defaults",
		"snmp_profiles",
		"bgp_sessions",
		"export_tasks",
		"dashboards",
		"address_draft_revisions",
		"dimension_snapshots",
		"dimension_snapshot_activations",
		"dimension_snapshot_acks",
		"dimension_snapshot_references",
		"geo_dict",
		"isp_operators",
		"geo_lines",
		"billing_accounts",
		"permissions",
		"audit_logs",
		"operation_jobs",
		"operation_job_watermarks",
		"collector_agents",
		"collector_bindings",
		"collector_plan_revisions",
		"collector_service_principals",
		"collector_ownership_transfers",
	} {
		if !tables[table] {
			t.Fatalf("migration missing table %s", table)
		}
	}
}

func TestIdentityProjectionMigrationIsExpandOnly(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "011_identity_projection.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, fragment := range []string{"add column auth_provider", "add column external_subject_id", "password_hash varchar(255) null", "uq_users_external_identity"} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("identity migration missing %q", fragment)
		}
	}
	if strings.Contains(sqlText, "drop column password_hash") {
		t.Fatal("expand migration must retain password_hash for rollback")
	}
}

func TestWatchdogMigrationAppliesToMySQL(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run MySQL migration integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("ping mysql: %v", err)
	}
	first, err := ApplyMySQLMigrations(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Applied) != len(readWatchdogMigrations(t)) || first.CurrentVersion != "042" {
		t.Fatalf("first migration result = %#v", first)
	}
	second, err := ApplyMySQLMigrations(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 || second.CurrentVersion != "042" {
		t.Fatalf("second migration result = %#v", second)
	}
	if err := CheckMySQLSchemaCurrent(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"network_devices", "network_ports", "network_interface_addresses", "traffic_policy_defaults", "export_tasks", "operation_jobs", "operation_job_watermarks", "collector_agents", "collector_bindings", "collector_plan_revisions", "collector_service_principals", "collector_ownership_transfers"} {
		var name string
		if err := db.QueryRow("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", table).Scan(&name); err != nil {
			t.Fatalf("table %s not found after migration: %v", table, err)
		}
	}
	var removedStateTable int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'collector_state_restore_receipts'").Scan(&removedStateTable); err != nil {
		t.Fatal(err)
	}
	if removedStateTable != 0 {
		t.Fatal("legacy collector_state_restore_receipts table still exists")
	}
	var targetKindNullable string
	if err := db.QueryRow("SELECT is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'targets' AND column_name = 'kind'").Scan(&targetKindNullable); err != nil {
		t.Fatalf("targets.kind not found after migrations: %v", err)
	}
	if targetKindNullable != "NO" {
		t.Fatalf("targets.kind nullable = %s, want NO", targetKindNullable)
	}
	for _, legacy := range []struct{ table, column string }{{table: "monitor_targets"}, {table: "targets", column: "target_type"}} {
		var count int
		if err := db.QueryRow(`
			SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND (? = '' OR column_name = ?)
		`, legacy.table, legacy.column, legacy.column).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("legacy schema name still exists: %s.%s", legacy.table, legacy.column)
		}
	}
	var legacyIndexCount int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'targets' AND index_name = 'idx_targets_tenant_type_status'
	`).Scan(&legacyIndexCount); err != nil {
		t.Fatal(err)
	}
	if legacyIndexCount != 0 {
		t.Fatal("legacy target type index still exists")
	}
	var hostIdentityIndexCount int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'targets' AND index_name = 'uq_targets_tenant_kind_host' AND non_unique = 0
	`).Scan(&hostIdentityIndexCount); err != nil {
		t.Fatal(err)
	}
	if hostIdentityIndexCount != 3 {
		t.Fatalf("target host identity index column count = %d, want 3", hostIdentityIndexCount)
	}
	for _, column := range []string{"auth_provider", "external_subject_id"} {
		var nullable string
		if err := db.QueryRow("SELECT is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'users' AND column_name = ?", column).Scan(&nullable); err != nil {
			t.Fatalf("users.%s not found after migrations: %v", column, err)
		}
		if nullable != "YES" {
			t.Fatalf("users.%s nullable = %s, want YES", column, nullable)
		}
	}
	// Migration 029 drops password_hash: MySQL must hold no credential material,
	// PocketBase is the sole authentication authority.
	var passwordHashColumns int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'users' AND column_name = 'password_hash'").Scan(&passwordHashColumns); err != nil {
		t.Fatal(err)
	}
	if passwordHashColumns != 0 {
		t.Fatal("users.password_hash must be dropped after migrations; MySQL must store no credential material")
	}
	const tenantID = "tenant_identity_check"
	if _, err := db.Exec("DELETE FROM tenants WHERE id = ?", tenantID); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DELETE FROM tenants WHERE id = ?", tenantID)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Identity Check', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	assertOperationJobWatermarkForwardFill(t, db, tenantID)
	if _, err := db.Exec("INSERT INTO users (id, tenant_id, email, name, status) VALUES ('user_identity_check', ?, 'identity-check@watchdog.local', 'Identity Check', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO roles (id, tenant_id, name, scope) VALUES ('role_identity_admin', ?, 'admin', 'tenant')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO user_roles (user_id, role_id) VALUES ('user_identity_check', 'role_identity_admin')"); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	if err := store.LinkExternalIdentity(context.Background(), tenantID, "user_identity_check", "pocketbase", "pb_identity_check"); err != nil {
		t.Fatal(err)
	}
	projections, err := store.ListIdentityProjections(context.Background(), "pocketbase", "pb_identity_check")
	if err != nil {
		t.Fatal(err)
	}
	if len(projections) != 1 || projections[0].Tenant.ID != tenantID || projections[0].User.ID != "user_identity_check" {
		t.Fatalf("projections = %#v", projections)
	}
	admin, err := store.IsUserTenantAdmin(context.Background(), tenantID, "user_identity_check")
	if err != nil || !admin {
		t.Fatalf("admin = %v, err = %v", admin, err)
	}
}

func assertOperationJobWatermarkForwardFill(t *testing.T, db *sql.DB, tenantID string) {
	t.Helper()
	const (
		firstBucket    = uint64(1788611640)
		secondBucket   = uint64(1788611700)
		newerWatermark = uint64(1788611760)
	)
	for index, bucket := range []uint64{firstBucket, secondBucket} {
		checkpoint := fmt.Sprintf(`{"schema_version":1,"payload":{"resolution":"1m","bucket_unix":%d,"generation":1}}`, bucket)
		if _, err := db.Exec(`INSERT INTO operation_jobs (
			id, tenant_id, job_type, idempotency_key, request_hash, checkpoint_json
		) VALUES (?, ?, 'flow_rollup', ?, ?, ?)`,
			fmt.Sprintf("job_rollup_mig_%d", index), tenantID,
			fmt.Sprintf("flow_rollup:migration:%d", index), strings.Repeat(fmt.Sprint(index+1), 64), checkpoint,
		); err != nil {
			t.Fatal(err)
		}
	}
	runEmbeddedMigrationAgain(t, db, "028")
	var watermark uint64
	if err := db.QueryRow(`SELECT watermark_value FROM operation_job_watermarks
		WHERE tenant_id = ? AND job_type = 'flow_rollup' AND partition_key = 'v1:1m'`, tenantID).Scan(&watermark); err != nil {
		t.Fatal(err)
	}
	if watermark != secondBucket {
		t.Fatalf("forward-filled watermark = %d, want %d", watermark, secondBucket)
	}
	if _, err := db.Exec(`UPDATE operation_job_watermarks SET watermark_value = ?
		WHERE tenant_id = ? AND job_type = 'flow_rollup' AND partition_key = 'v1:1m'`, newerWatermark, tenantID); err != nil {
		t.Fatal(err)
	}
	runEmbeddedMigrationAgain(t, db, "028")
	if err := db.QueryRow(`SELECT watermark_value FROM operation_job_watermarks
		WHERE tenant_id = ? AND job_type = 'flow_rollup' AND partition_key = 'v1:1m'`, tenantID).Scan(&watermark); err != nil {
		t.Fatal(err)
	}
	if watermark != newerWatermark {
		t.Fatalf("forward-fill moved watermark backwards: got %d, want %d", watermark, newerWatermark)
	}
}

func runEmbeddedMigrationAgain(t *testing.T, db *sql.DB, version string) {
	t.Helper()
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Version != version {
			continue
		}
		for statementIndex, statement := range SplitSQLStatements(migration.SQL) {
			if _, err := db.Exec(statement); err != nil {
				t.Fatalf("repeat migration %s statement %d: %v", migration.Name, statementIndex+1, err)
			}
		}
		return
	}
	t.Fatalf("embedded migration %s not found", version)
}

type watchdogMigration struct {
	name string
	sql  string
}

func TestEmbeddedMySQLMigrationsAreOrderedAndChecksummed(t *testing.T) {
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 42 || migrations[0].Version != "001" || migrations[len(migrations)-1].Version != "042" {
		t.Fatalf("migrations = %#v", migrations)
	}
	for i, migration := range migrations {
		if len(migration.Checksum) != 64 || migration.SQL == "" {
			t.Fatalf("invalid migration %d: %#v", i, migration)
		}
	}
}

func TestDimensionPublicationLifecycleMigrationOwnsTheCompleteContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "042_dimension_publication_lifecycle.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"add column approval_state",
		"alter column approval_state set default 'pending'",
		"create table if not exists dimension_snapshot_activations",
		"insert ignore into dimension_snapshot_activations",
		"create table if not exists dimension_snapshot_references",
		"max_event_time <= retain_until",
		"modify column installed_at datetime(3) null",
		"dimension_snapshot_acks_chk_state",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("dimension lifecycle migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"drop table dimension_snapshots", "delete from dimension_snapshots", "update dimension_snapshots set status"} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("dimension lifecycle migration unexpectedly contains %q", forbidden)
		}
	}
}

func TestOperationJobWatermarkMigrationIsForwardOnlyAndMonotonic(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "028_operation_job_watermarks.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"create table if not exists operation_job_watermarks",
		"primary key (tenant_id, job_type, partition_key)",
		"greatest(watermark_value, values(watermark_value))",
		"from operation_jobs",
		"json_extract(checkpoint_json, '$.payload.bucket_unix')",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("watermark migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"drop table", "truncate table", "delete from operation_jobs"} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("watermark migration contains destructive statement %q", forbidden)
		}
	}
}

func TestLegacyFlowStateRestoreMigrationRemovesOnlyTheRetiredReceiptTable(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "027_remove_flow_state_restore.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(strings.TrimSpace(string(data)))
	if strings.Count(sqlText, ";") != 1 || !strings.Contains(sqlText, "drop table if exists collector_state_restore_receipts;") {
		t.Fatalf("legacy Flow state migration must contain exactly its idempotent table drop: %q", sqlText)
	}
	for _, forbidden := range []string{"operation_jobs", "collector_service_principals", "collector_ownership_transfers", "delete from", "truncate table"} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("legacy Flow state migration unexpectedly touches %q", forbidden)
		}
	}
	if strings.Contains(strings.ToLower(readWatchdogInitSchema(t)), "collector_state_restore_receipts") {
		t.Fatal("fresh-install schema still contains the retired collector state receipt table")
	}
}

func readWatchdogMigrations(t *testing.T) []watchdogMigration {
	t.Helper()
	dir := filepath.Join("..", "..", "deploy", "migration", "mysql")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	migrations := make([]watchdogMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), err)
		}
		migrations = append(migrations, watchdogMigration{name: entry.Name(), sql: string(data)})
	}
	return migrations
}

func readWatchdogInitSchema(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "install", "init.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	return string(data)
}

func splitSQLStatements(sqlText string) []string {
	parts := strings.Split(sqlText, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		statement := strings.TrimSpace(part)
		if statement == "" {
			continue
		}
		statements = append(statements, statement)
	}
	return statements
}
