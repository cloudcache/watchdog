package watchdog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TestInitSQLMatchesEmbeddedMigrations is the PLAT-00 parity gate: a fresh
// database created from install/init.sql must be structurally identical to one
// created by running every embedded migration. It needs a real MySQL server;
// set WATCHDOG_TEST_MYSQL_DSN (server-level DSN, e.g.
// "root:@tcp(127.0.0.1:3306)/?multiStatements=true") to enable it.
func TestInitSQLMatchesEmbeddedMigrations(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	if !strings.Contains(dsn, "multiStatements=true") {
		t.Fatalf("WATCHDOG_TEST_MYSQL_DSN must enable multiStatements=true to run init.sql")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	server, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer server.Close()
	if err := server.PingContext(ctx); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}

	suffix := randomSchemaSuffix(t)
	initSchema := "watchdog_parity_init_" + suffix
	migrationSchema := "watchdog_parity_mig_" + suffix
	createScratchSchema(ctx, t, server, initSchema)
	createScratchSchema(ctx, t, server, migrationSchema)

	initSQL, err := os.ReadFile("../../install/init.sql")
	if err != nil {
		t.Fatalf("read init.sql: %v", err)
	}
	initDB := openScratchSchema(t, dsn, initSchema)
	defer initDB.Close()
	if _, err := initDB.ExecContext(ctx, string(initSQL)); err != nil {
		t.Fatalf("apply init.sql: %v", err)
	}

	migrationDB := openScratchSchema(t, dsn, migrationSchema)
	defer migrationDB.Close()
	if _, err := ApplyMySQLMigrations(ctx, migrationDB); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	initTables := listBaseTables(ctx, t, initDB, initSchema)
	migrationTables := listBaseTables(ctx, t, migrationDB, migrationSchema)
	// The runner's bookkeeping table exists on both paths and is owned by the
	// migrator, not the schema contract.
	delete(initTables, "watchdog_schema_migrations")
	delete(migrationTables, "watchdog_schema_migrations")

	for name := range initTables {
		if _, ok := migrationTables[name]; !ok {
			t.Errorf("table %q exists in init.sql but not after migrations", name)
		}
	}
	for name := range migrationTables {
		if _, ok := initTables[name]; !ok {
			t.Errorf("table %q exists after migrations but not in init.sql", name)
		}
	}
	if t.Failed() {
		return
	}

	names := make([]string, 0, len(initTables))
	for name := range initTables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		initShape := normalizedTableShape(ctx, t, initDB, name)
		migrationShape := normalizedTableShape(ctx, t, migrationDB, name)
		if initShape != migrationShape {
			t.Errorf("table %q differs between init.sql and migrations:\n--- init.sql\n%s\n--- migrations\n%s", name, initShape, migrationShape)
		}
	}
}

func TestExportExecutionMigrationBackfillsLegacyRows(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.PingContext(ctx); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}

	schema := "watchdog_export_legacy_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Version == "046" {
			break
		}
		for index, statement := range SplitSQLStatements(migration.SQL) {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("apply %s statement %d: %v", migration.Name, index+1, err)
			}
		}
	}
	const tenantID, userID, exportID = "tenant_export_legacy", "user_export_legacy", "export_legacy_046"
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Legacy Export', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'legacy-export@watchdog.local', 'Legacy Export', 'active')", userID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO export_tasks (
			id, tenant_id, created_by, period_type, range_start, range_end,
			step_seconds, aggregation, value_mode, format, status
		) VALUES (?, ?, ?, 'custom', '2026-08-24 11:00:00', '2026-08-24 12:00:00',
			300, 'p95_5m', 'both', 'csv', 'failed')
	`, exportID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	for replay := 0; replay < 2; replay++ {
		for index, statement := range SplitSQLStatements(migrations[len(migrations)-1].SQL) {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("replay 046 pass %d statement %d: %v", replay+1, index+1, err)
			}
		}
	}
	var contractVersion, retention uint32
	var dataset, queryHash, valueLayer, queryJSON, versionsJSON, authorizationJSON string
	if err := db.QueryRowContext(ctx, `
		SELECT contract_version, dataset_key, query_hash, value_layer,
		       CAST(query_json AS CHAR), CAST(versions_json AS CHAR),
		       CAST(authorization_json AS CHAR), retention_seconds
		FROM export_tasks WHERE id = ?
	`, exportID).Scan(&contractVersion, &dataset, &queryHash, &valueLayer, &queryJSON, &versionsJSON, &authorizationJSON, &retention); err != nil {
		t.Fatal(err)
	}
	if contractVersion != 0 || dataset != "network.snmp_interface" || len(queryHash) != 64 || valueLayer != "customer" || retention != 604800 {
		t.Fatalf("legacy projection version=%d dataset=%q hash=%q layer=%q retention=%d", contractVersion, dataset, queryHash, valueLayer, retention)
	}
	for label, value := range map[string]string{
		"query": queryJSON, "versions": versionsJSON, "authorization": authorizationJSON,
	} {
		if !strings.Contains(value, `"legacy": true`) && label != "versions" {
			t.Fatalf("%s snapshot does not identify legacy evidence: %s", label, value)
		}
	}
	if !strings.Contains(versionsJSON, `"snapshot_complete": false`) || !strings.Contains(authorizationJSON, `"required_action": "export_raw"`) {
		t.Fatalf("versions=%s authorization=%s", versionsJSON, authorizationJSON)
	}
}

func randomSchemaSuffix(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return hex.EncodeToString(buf)
}

func createScratchSchema(ctx context.Context, t *testing.T, server *sql.DB, name string) {
	t.Helper()
	if _, err := server.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", name)); err != nil {
		t.Fatalf("create schema %s: %v", name, err)
	}
	// Cleanups run after deferred Close calls, so the drop needs its own
	// connection instead of the server handle.
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Logf("open cleanup connection: %v", err)
			return
		}
		defer db.Close()
		if _, err := db.ExecContext(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name)); err != nil {
			t.Logf("drop schema %s: %v", name, err)
		}
	})
}

func openScratchSchema(t *testing.T, serverDSN, schema string) *sql.DB {
	t.Helper()
	separator := strings.LastIndexByte(serverDSN, '/')
	if separator < 0 {
		t.Fatalf("cannot derive schema DSN from %q", serverDSN)
	}
	rest := serverDSN[separator+1:]
	params := ""
	if q := strings.IndexByte(rest, '?'); q >= 0 {
		params = rest[q:]
	}
	db, err := sql.Open("mysql", serverDSN[:separator+1]+schema+params)
	if err != nil {
		t.Fatalf("open schema %s: %v", schema, err)
	}
	return db
}

func listBaseTables(ctx context.Context, t *testing.T, db *sql.DB, schema string) map[string]struct{} {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
	`, schema)
	if err != nil {
		t.Fatalf("list tables for %s: %v", schema, err)
	}
	defer rows.Close()
	tables := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		tables[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}
	return tables
}

var (
	autoIncrementClause = regexp.MustCompile(` AUTO_INCREMENT=\d+`)
	// Columns restored from a dump carry explicit charset clauses while
	// natively created columns inherit the table default; both mean the same
	// thing when they equal the canonical charset.
	canonicalCharsetClause = regexp.MustCompile(` CHARACTER SET utf8mb4( COLLATE utf8mb4_unicode_ci)?`)
	canonicalCollateClause = regexp.MustCompile(` COLLATE utf8mb4_unicode_ci`)
	// MySQL prints charset introducers on CHECK literals depending on the
	// session that created the table; they carry no structural meaning here.
	charsetIntroducer = regexp.MustCompile(`_(?:utf8mb4|latin1)('[^']*')`)
)

// normalizedTableShape renders SHOW CREATE TABLE into an order-insensitive
// canonical form: init.sql declares keys and constraints inline while the
// migration path adds many of them through later ALTERs, so MySQL prints them
// in a different order even when the structures are identical.
func normalizedTableShape(ctx context.Context, t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	var name, createSQL string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&name, &createSQL); err != nil {
		t.Fatalf("show create table %s: %v", table, err)
	}
	createSQL = autoIncrementClause.ReplaceAllString(createSQL, "")
	createSQL = canonicalCharsetClause.ReplaceAllString(createSQL, "")
	createSQL = canonicalCollateClause.ReplaceAllString(createSQL, "")
	createSQL = charsetIntroducer.ReplaceAllString(createSQL, "$1")
	lines := strings.Split(createSQL, "\n")
	if len(lines) < 2 {
		return createSQL
	}
	body := lines[1 : len(lines)-1]
	normalized := make([]string, 0, len(body))
	for _, line := range body {
		trimmed := strings.TrimSuffix(strings.TrimSpace(line), ",")
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	sort.Strings(normalized)
	tail := strings.TrimSpace(lines[len(lines)-1])
	return strings.Join(normalized, "\n") + "\n" + tail
}
