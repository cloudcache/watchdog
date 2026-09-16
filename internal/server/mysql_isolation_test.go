package server

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// isolatedMySQLDSN creates a disposable database for tests that exercise the
// real MySQL repository. Tests must never apply schemas or fixtures directly
// to WATCHDOG_TEST_MYSQL_DSN: that DSN may point at a developer's live
// watchdog database.
func isolatedMySQLDSN(t testing.TB) string {
	t.Helper()
	baseDSN := strings.TrimSpace(os.Getenv("WATCHDOG_TEST_MYSQL_DSN"))
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse WATCHDOG_TEST_MYSQL_DSN: %v", err)
	}
	testDatabase := "watchdog_it_" + strings.ToLower(newID())
	adminConfig := parsed.Clone()
	adminConfig.DBName = ""
	admin, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		t.Fatalf("open MySQL admin connection: %v", err)
	}
	if _, err := admin.Exec("CREATE DATABASE `" + testDatabase + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		_ = admin.Close()
		t.Fatalf("create isolated MySQL database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + testDatabase + "`")
		_ = admin.Close()
	})
	parsed.DBName = testDatabase
	return parsed.FormatDSN()
}
