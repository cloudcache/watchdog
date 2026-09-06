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
		"query_dataset_policies",
		"audit_logs",
		"operation_job_schedules",
		"operation_job_scheduler_state",
		"operation_job_system_watermarks",
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
	if len(first.Applied) != len(readWatchdogMigrations(t)) || first.CurrentVersion != "048" {
		t.Fatalf("first migration result = %#v", first)
	}
	second, err := ApplyMySQLMigrations(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 || second.CurrentVersion != "048" {
		t.Fatalf("second migration result = %#v", second)
	}
	if err := CheckMySQLSchemaCurrent(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"network_devices", "network_ports", "network_interface_addresses", "traffic_policy_defaults", "export_tasks", "query_dataset_policies", "operation_jobs", "operation_job_watermarks", "operation_job_schedules", "operation_job_scheduler_state", "operation_job_system_watermarks", "collector_agents", "collector_bindings", "collector_plan_revisions", "collector_service_principals", "collector_ownership_transfers"} {
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
	var scheduleColumnCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'operation_jobs' AND column_name = 'schedule_id'").Scan(&scheduleColumnCount); err != nil {
		t.Fatal(err)
	}
	if scheduleColumnCount != 1 {
		t.Fatalf("operation_jobs.schedule_id count = %d, want 1", scheduleColumnCount)
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
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Version != version {
			continue
		}
		for statementIndex, statement := range SplitSQLStatements(migration.SQL) {
			if _, err := conn.ExecContext(context.Background(), statement); err != nil {
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
	if len(migrations) != 48 || migrations[0].Version != "001" || migrations[len(migrations)-1].Version != "048" {
		t.Fatalf("migrations = %#v", migrations)
	}
	for i, migration := range migrations {
		if len(migration.Checksum) != 64 || migration.SQL == "" {
			t.Fatalf("invalid migration %d: %#v", i, migration)
		}
	}
}

func TestDimensionSourceManifestMigrationOwnsCompleteContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "047_dimension_source_manifest.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"source_manifest_version smallint unsigned", "source_manifest json",
		"source_prefix_count bigint unsigned", "json_type(source_manifest)",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("dimension source manifest migration missing %q", required)
		}
	}
}

func TestDimensionConsumerStatusIndexMigrationOwnsCompleteContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "048_dimension_consumer_status_index.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"idx_dimension_snapshot_acks_observed", "tenant_id, worker_id, attempted_at, snapshot_id",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("dimension consumer status index migration missing %q", required)
		}
	}
}

func TestExportExecutionMigrationOwnsCompleteContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "046_export_execution_contract.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"contract_version smallint unsigned",
		"dataset_key varchar(128)",
		"query_json json",
		"query_hash char(64)",
		"value_layer varchar(16)",
		"versions_json json",
		"authorization_json json",
		"operation_job_id char(26)",
		"references operation_jobs(id) on delete set null",
		"retention_seconds int unsigned",
		"artifact_schema_version smallint unsigned",
		"content_type varchar(128)",
		"row_count bigint unsigned",
		"format in (''csv'', ''parquet'')",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("export execution migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"attempt_count int", "lease_owner", "next_attempt_at"} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("export execution migration must reuse operation_jobs, found %q", forbidden)
		}
	}
}

func TestQueryGatewayPolicyMigrationOwnsCompleteContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "045_query_gateway_policy.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"create table if not exists query_dataset_policies",
		"primary key (tenant_id, dataset_key)",
		"allow_raw boolean not null default false",
		"allow_supplier boolean not null default false",
		"allow_customer boolean not null default true",
		"max_range_seconds int unsigned",
		"max_concurrent smallint unsigned",
		"max_result_rows int unsigned",
		"query_timeout_ms int unsigned",
		"row_version bigint unsigned",
		"on delete cascade",
		"on delete set null",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("query gateway policy migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"clickhouse_password", "provider_url", "drop table", "delete from"} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("query gateway policy migration unexpectedly contains %q", forbidden)
		}
	}
}

func TestOperationSchedulerMigrationOwnsCompleteContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "044_operation_scheduler.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"create table if not exists operation_job_schedules",
		"generated always as (coalesce(tenant_id, '__system__')) virtual",
		"unique key uq_operation_job_schedule_domain",
		"create table if not exists operation_job_scheduler_state",
		"cursor_due_at datetime(3)",
		"create table if not exists operation_job_system_watermarks",
		"primary key (job_type, partition_key)",
		"add column schedule_id",
		"idx_operation_jobs_schedule_status",
		"fk_operation_jobs_schedule",
		"on delete set null",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("operation scheduler migration missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"drop table operation_jobs",
		"delete from operation_jobs",
		"truncate table",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("operation scheduler migration unexpectedly contains %q", forbidden)
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

func TestDimensionPublicationTenantCascadeIsAForwardCorrection(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "043_dimension_publication_tenant_cascade.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, required := range []string{
		"drop foreign key fk_dimension_activation_snapshot",
		"drop foreign key fk_dimension_activation_rollback",
		"drop foreign key fk_dimension_reference_snapshot",
		"on delete cascade",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("dimension lifecycle correction missing %q", required)
		}
	}
	for _, forbidden := range []string{"drop table", "delete from", "truncate table"} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("dimension lifecycle correction unexpectedly contains %q", forbidden)
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
