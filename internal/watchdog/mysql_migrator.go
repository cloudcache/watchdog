package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	mysqlmigrations "github.com/cloudcache/watchdog/deploy/migration/mysql"
)

const mysqlMigrationLock = "watchdog_schema_migrations"

type MySQLMigration struct {
	Version  string
	Name     string
	Checksum string
	SQL      string
}

type MySQLMigrationResult struct {
	CurrentVersion string
	Applied        []string
}

func EmbeddedMySQLMigrations() ([]MySQLMigration, error) {
	entries, err := fs.ReadDir(mysqlmigrations.Files, ".")
	if err != nil {
		return nil, err
	}
	migrations := make([]MySQLMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		separator := strings.IndexByte(entry.Name(), '_')
		if separator <= 0 {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		version := entry.Name()[:separator]
		for _, r := range version {
			if r < '0' || r > '9' {
				return nil, fmt.Errorf("invalid migration version %q", version)
			}
		}
		data, err := fs.ReadFile(mysqlmigrations.Files, entry.Name())
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(data)
		migrations = append(migrations, MySQLMigration{
			Version:  version,
			Name:     entry.Name(),
			Checksum: hex.EncodeToString(digest[:]),
			SQL:      string(data),
		})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	previous := 0
	for _, migration := range migrations {
		number, err := strconv.Atoi(migration.Version)
		if err != nil || number <= previous {
			return nil, fmt.Errorf("mysql migration sequence must be strictly increasing at %q", migration.Name)
		}
		previous = number
	}
	if len(migrations) == 0 {
		return nil, errors.New("no embedded MySQL migrations")
	}
	return migrations, nil
}

func ApplyMySQLMigrations(ctx context.Context, db *sql.DB) (MySQLMigrationResult, error) {
	if db == nil {
		return MySQLMigrationResult{}, errors.New("mysql database is required")
	}
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		return MySQLMigrationResult{}, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return MySQLMigrationResult{}, err
	}
	defer conn.Close()

	var acquired int
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", mysqlMigrationLock).Scan(&acquired); err != nil {
		return MySQLMigrationResult{}, err
	}
	if acquired != 1 {
		return MySQLMigrationResult{}, errors.New("timed out acquiring MySQL migration lock")
	}
	defer conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", mysqlMigrationLock)

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS watchdog_schema_migrations (
			version VARCHAR(32) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			checksum CHAR(64) NOT NULL,
			applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`); err != nil {
		return MySQLMigrationResult{}, err
	}
	applied, err := readAppliedMySQLMigrations(ctx, conn)
	if err != nil {
		return MySQLMigrationResult{}, err
	}
	result := MySQLMigrationResult{Applied: make([]string, 0)}
	known := make(map[string]struct{}, len(migrations))
	for _, migration := range migrations {
		known[migration.Version] = struct{}{}
	}
	for version := range applied {
		if _, ok := known[version]; !ok {
			return result, fmt.Errorf("mysql schema contains unknown migration version %s", version)
		}
	}
	for _, migration := range migrations {
		if checksum, ok := applied[migration.Version]; ok {
			if checksum != migration.Checksum {
				return result, fmt.Errorf("mysql migration %s checksum mismatch", migration.Name)
			}
			result.CurrentVersion = migration.Version
			continue
		}
		for i, statement := range SplitSQLStatements(migration.SQL) {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				return result, fmt.Errorf("apply mysql migration %s statement %d: %w", migration.Name, i+1, err)
			}
		}
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO watchdog_schema_migrations (version, name, checksum)
			VALUES (?, ?, ?)
		`, migration.Version, migration.Name, migration.Checksum); err != nil {
			return result, fmt.Errorf("record mysql migration %s: %w", migration.Name, err)
		}
		result.Applied = append(result.Applied, migration.Name)
		result.CurrentVersion = migration.Version
	}
	return result, nil
}

func CheckMySQLSchemaCurrent(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("mysql database is required")
	}
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, "SELECT version, checksum FROM watchdog_schema_migrations")
	if err != nil {
		return err
	}
	defer rows.Close()
	applied := make(map[string]string)
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return err
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, migration := range migrations {
		checksum, ok := applied[migration.Version]
		if !ok {
			return fmt.Errorf("mysql migration %s is not applied", migration.Name)
		}
		if checksum != migration.Checksum {
			return fmt.Errorf("mysql migration %s checksum mismatch", migration.Name)
		}
	}
	if len(applied) != len(migrations) {
		for version := range applied {
			known := false
			for _, migration := range migrations {
				if migration.Version == version {
					known = true
					break
				}
			}
			if !known {
				return fmt.Errorf("mysql schema contains unknown migration version %s", version)
			}
		}
	}
	return nil
}

func readAppliedMySQLMigrations(ctx context.Context, conn *sql.Conn) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version, checksum FROM watchdog_schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	applied := make(map[string]string)
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, err
		}
		applied[version] = checksum
	}
	return applied, rows.Err()
}
