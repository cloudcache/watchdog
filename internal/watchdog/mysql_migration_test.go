package watchdog

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestWatchdogMigrationContainsCoreTables(t *testing.T) {
	sqlText := readWatchdogInitSchema(t)
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z_]+)`)
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
		"billing_accounts",
		"permissions",
		"audit_logs",
		"operation_jobs",
		"collector_agents",
		"collector_bindings",
		"collector_plan_revisions",
		"collector_service_principals",
		"collector_ownership_transfers",
		"collector_state_restore_receipts",
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
	if len(first.Applied) != len(readWatchdogMigrations(t)) || first.CurrentVersion != "019" {
		t.Fatalf("first migration result = %#v", first)
	}
	second, err := ApplyMySQLMigrations(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 || second.CurrentVersion != "019" {
		t.Fatalf("second migration result = %#v", second)
	}
	if err := CheckMySQLSchemaCurrent(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"network_devices", "network_ports", "network_interface_addresses", "traffic_policy_defaults", "export_tasks", "operation_jobs", "collector_agents", "collector_bindings", "collector_plan_revisions", "collector_service_principals", "collector_ownership_transfers", "collector_state_restore_receipts"} {
		var name string
		if err := db.QueryRow("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", table).Scan(&name); err != nil {
			t.Fatalf("table %s not found after migration: %v", table, err)
		}
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
	for _, column := range []string{"auth_provider", "external_subject_id", "password_hash"} {
		var nullable string
		if err := db.QueryRow("SELECT is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'users' AND column_name = ?", column).Scan(&nullable); err != nil {
			t.Fatalf("users.%s not found after migrations: %v", column, err)
		}
		if nullable != "YES" {
			t.Fatalf("users.%s nullable = %s, want YES", column, nullable)
		}
	}
	const tenantID = "tenant_identity_check"
	if _, err := db.Exec("DELETE FROM tenants WHERE id = ?", tenantID); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DELETE FROM tenants WHERE id = ?", tenantID)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Identity Check', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
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

type watchdogMigration struct {
	name string
	sql  string
}

func TestEmbeddedMySQLMigrationsAreOrderedAndChecksummed(t *testing.T) {
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 19 || migrations[0].Version != "001" || migrations[len(migrations)-1].Version != "019" {
		t.Fatalf("migrations = %#v", migrations)
	}
	for i, migration := range migrations {
		if len(migration.Checksum) != 64 || migration.SQL == "" {
			t.Fatalf("invalid migration %d: %#v", i, migration)
		}
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
