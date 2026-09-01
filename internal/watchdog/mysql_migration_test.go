package watchdog

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestWatchdogMigrationContainsCoreTables(t *testing.T) {
	sqlText := readWatchdogMigration(t)
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z_]+)`)
	matches := re.FindAllStringSubmatch(sqlText, -1)
	tables := make(map[string]bool, len(matches))
	for _, match := range matches {
		tables[strings.ToLower(match[1])] = true
	}
	for _, table := range []string{
		"tenants",
		"users",
		"monitor_targets",
		"network_devices",
		"network_ports",
		"port_policies",
		"traffic_policy_defaults",
		"snmp_profiles",
		"bgp_sessions",
		"export_tasks",
		"billing_accounts",
		"permissions",
		"audit_logs",
	} {
		if !tables[table] {
			t.Fatalf("migration missing table %s", table)
		}
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
	for _, statement := range splitSQLStatements(readWatchdogMigration(t)) {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("exec statement %q: %v", statement, err)
		}
	}
	for _, table := range []string{"network_devices", "network_ports", "traffic_policy_defaults", "export_tasks"} {
		var name string
		if err := db.QueryRow("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", table).Scan(&name); err != nil {
			t.Fatalf("table %s not found after migration: %v", table, err)
		}
	}
}

func readWatchdogMigration(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "001_watchdog_backend.sql")
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
