package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

const bootstrapSchemaMigrations = `CREATE TABLE IF NOT EXISTS schema_migrations (
  version    VARCHAR(64) NOT NULL,
  store      VARCHAR(16) NOT NULL DEFAULT 'mysql',
  checksum   CHAR(64)    NOT NULL,
  applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (store, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

// InstallStatus is the traceable install state surfaced by the API.
type InstallStatus struct {
	SchemaVersion     string `json:"schema_version"`
	ProductVersion    string `json:"product_version"`
	AdminBootstrapped bool   `json:"admin_bootstrapped"`
	InstalledAt       string `json:"installed_at"`
}

// ApplyMySQLSchema applies every embedded MySQL baseline not yet recorded, in
// filename order, records each in schema_migrations, and updates the singleton
// watchdog_installation row to the latest version. Idempotent: all DDL uses
// IF NOT EXISTS, and applied versions are skipped.
func ApplyMySQLSchema(ctx context.Context, db *sql.DB, files fs.FS) error {
	if _, err := db.ExecContext(ctx, bootstrapSchemaMigrations); err != nil {
		return fmt.Errorf("bootstrap schema_migrations: %w", err)
	}
	names, err := sortedSQL(files, "mysql")
	if err != nil {
		return err
	}
	var latest string
	for _, name := range names {
		version := strings.TrimSuffix(pathBase(name), ".sql")
		content, err := fs.ReadFile(files, name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		latest = version
		applied, err := versionApplied(ctx, db, "mysql", version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		for _, stmt := range splitStatements(string(content)) {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("apply %s: %w", version, err)
			}
		}
		sum := sha256.Sum256(content)
		if _, err := db.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, store, checksum) VALUES (?, 'mysql', ?)`,
			version, hex.EncodeToString(sum[:]),
		); err != nil {
			return fmt.Errorf("record %s: %w", version, err)
		}
	}
	if latest != "" {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO watchdog_installation (id, schema_version)
			VALUES (1, ?)
			ON DUPLICATE KEY UPDATE schema_version = VALUES(schema_version)`, latest); err != nil {
			return fmt.Errorf("update installation: %w", err)
		}
	}
	return nil
}

// GetInstallStatus reads the singleton install state.
func GetInstallStatus(ctx context.Context, db *sql.DB) (InstallStatus, error) {
	var s InstallStatus
	var installedAt sql.NullTime
	err := db.QueryRowContext(ctx, `
		SELECT schema_version, product_version, admin_bootstrapped, installed_at
		FROM watchdog_installation WHERE id = 1`).
		Scan(&s.SchemaVersion, &s.ProductVersion, &s.AdminBootstrapped, &installedAt)
	if err == sql.ErrNoRows {
		return InstallStatus{}, nil
	}
	if err != nil {
		return InstallStatus{}, err
	}
	if installedAt.Valid {
		s.InstalledAt = installedAt.Time.UTC().Format("2006-01-02T15:04:05Z")
	}
	return s, nil
}

func sortedSQL(files fs.FS, dir string) ([]string, error) {
	entries, err := fs.ReadDir(files, dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, dir+"/"+e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func versionApplied(ctx context.Context, db *sql.DB, store, version string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_migrations WHERE store = ? AND version = ?`, store, version).Scan(&n)
	return n > 0, err
}

func pathBase(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// splitStatements strips line comments and splits DDL into individual statements.
// Safe for this baseline: no ';' or '--' appears inside string/identifier literals.
func splitStatements(sqlText string) []string {
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
