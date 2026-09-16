package billing_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

var isolatedMySQLSequence atomic.Uint64

// isolatedMySQLDSN prevents real-MySQL billing tests from applying schemas or
// fixtures to the database named by WATCHDOG_TEST_MYSQL_DSN.
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
	testDatabase := fmt.Sprintf("watchdog_billing_it_%d_%d", time.Now().UnixNano(), isolatedMySQLSequence.Add(1))
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
