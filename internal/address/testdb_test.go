package address

import (
	"database/sql"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/cloudcache/watchdog/deploy/schema"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// addressTestDB provisions an isolated, freshly-migrated MySQL database for a
// single integration test and returns a pool bound to it. It is the de-tenanted
// analogue of the legacy operationJobTestDB: legacy tests isolated concurrent
// cases by a per-test tenant inside one shared database; the single-domain port
// has no tenant column, so each test gets its own throwaway schema instead.
// Opt-in via WATCHDOG_TEST_MYSQL_DSN so ordinary unit tests need no MySQL.
func addressTestDB(t *testing.T) *sql.DB {
	t.Helper()
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	name := addressTestDBName(t.Name())
	dropAddressTestDB(t, baseDSN, name)
	createAddressTestDB(t, baseDSN, name)
	parsed.DBName = name
	db, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	applyAddressTestSchema(t, db)
	t.Cleanup(func() {
		_ = db.Close()
		dropAddressTestDB(t, baseDSN, name)
	})
	return db
}

var addressTestDBNamePattern = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func addressTestDBName(testName string) string {
	slug := strings.ToLower(addressTestDBNamePattern.ReplaceAllString(testName, "_"))
	slug = strings.Trim(slug, "_")
	if len(slug) > 40 {
		slug = slug[:40]
	}
	return "watchdog_addr_it_" + slug
}

func createAddressTestDB(t *testing.T, baseDSN, name string) {
	t.Helper()
	admin := openAddressTestAdmin(t, baseDSN)
	defer admin.Close()
	if _, err := admin.Exec("CREATE DATABASE `" + name + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
}

func dropAddressTestDB(t *testing.T, baseDSN, name string) {
	t.Helper()
	admin := openAddressTestAdmin(t, baseDSN)
	defer admin.Close()
	if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + name + "`"); err != nil {
		t.Fatalf("drop database %s: %v", name, err)
	}
}

func openAddressTestAdmin(t *testing.T, baseDSN string) *sql.DB {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = ""
	admin, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	return admin
}

// applyAddressTestSchema applies the embedded v2 MySQL baseline in filename order
// and seeds the singleton watchdog_installation row that the publication lock
// depends on. It mirrors internal/server.ApplyMySQLSchema rather than calling it,
// because internal/server imports internal/address (importing it back would cycle).
func applyAddressTestSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := fs.ReadDir(schema.MySQL, "mysql")
	if err != nil {
		t.Fatalf("read schema dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, "mysql/"+e.Name())
		}
	}
	sort.Strings(files)
	for _, file := range files {
		content, err := fs.ReadFile(schema.MySQL, file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, stmt := range splitAddressSchemaStatements(string(content)) {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatalf("apply %s: %v", file, err)
			}
		}
	}
	if _, err := db.Exec(`INSERT INTO watchdog_installation (id, schema_version) VALUES (1, 'test')
		ON DUPLICATE KEY UPDATE schema_version = VALUES(schema_version)`); err != nil {
		t.Fatalf("seed installation row: %v", err)
	}
}

// splitAddressSchemaStatements mirrors internal/server.splitStatements: strip
// line comments, split on ';'. Safe for this baseline (no ';'/'--' inside literals).
func splitAddressSchemaStatements(sqlText string) []string {
	var b strings.Builder
	for _, line := range strings.Split(sqlText, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	var out []string
	for _, part := range strings.Split(b.String(), ";") {
		if s := strings.TrimSpace(part); s != "" {
			out = append(out, s)
		}
	}
	return out
}
