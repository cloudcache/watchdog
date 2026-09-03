package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const installMarkerID = "default"

type InstallOptions struct {
	Config     BackendConfig
	ConfigPath string
	InitSQL    string
	LockPath   string
}

type InstallResult struct {
	LockPath           string
	LockExisted        bool
	StatementsExecuted int
	MigrationsApplied  []string
}

func RunInstall(ctx context.Context, opts InstallOptions) (InstallResult, error) {
	if opts.InitSQL == "" {
		return InstallResult{}, errors.New("init sql path is required")
	}
	if opts.LockPath == "" {
		return InstallResult{}, errors.New("install lock path is required")
	}
	store, err := OpenMySQLStore(ctx, opts.Config.MySQL)
	if err != nil {
		return InstallResult{}, err
	}
	defer store.Close()

	result := InstallResult{LockPath: opts.LockPath}
	if _, err := os.Stat(opts.LockPath); err == nil {
		result.LockExisted = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}

	// Always apply the schema. Every statement is CREATE TABLE IF NOT EXISTS,
	// so this is idempotent: re-running watchdog-install migrates an existing
	// deployment by creating any tables added in later revisions.
	count, err := executeSQLFile(ctx, store.db, opts.InitSQL)
	if err != nil {
		return result, err
	}
	result.StatementsExecuted = count
	migrations, err := ApplyMySQLMigrations(ctx, store.db)
	if err != nil {
		return result, err
	}
	result.MigrationsApplied = migrations.Applied
	if err := markInstalled(ctx, store.db, opts.LockPath, opts.ConfigPath); err != nil {
		return result, err
	}
	if err := writeInstallLock(opts.LockPath, opts.ConfigPath); err != nil {
		return result, err
	}
	return result, nil
}

func executeSQLFile(ctx context.Context, db *sql.DB, path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	statements := SplitSQLStatements(string(data))
	for i, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return i, fmt.Errorf("execute statement %d: %w", i+1, err)
		}
	}
	return len(statements), nil
}

func markInstalled(ctx context.Context, db *sql.DB, lockPath string, configPath string) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS watchdog_installation (
			id VARCHAR(64) PRIMARY KEY,
			installed BOOLEAN NOT NULL DEFAULT TRUE,
			lock_path VARCHAR(512) NOT NULL DEFAULT '',
			config_path VARCHAR(512) NOT NULL DEFAULT '',
			installed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO watchdog_installation (id, installed, lock_path, config_path, installed_at)
		VALUES (?, TRUE, ?, ?, CURRENT_TIMESTAMP(3))
		ON DUPLICATE KEY UPDATE
			installed = VALUES(installed),
			lock_path = VALUES(lock_path),
			config_path = VALUES(config_path),
			updated_at = CURRENT_TIMESTAMP(3)
	`, installMarkerID, lockPath, configPath)
	return err
}

func writeInstallLock(path string, configPath string) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	body := fmt.Sprintf("installed_at: %s\nconfig: %s\n", time.Now().UTC().Format(time.RFC3339), configPath)
	return os.WriteFile(path, []byte(body), 0o600)
}

func SplitSQLStatements(sqlText string) []string {
	var statements []string
	var current strings.Builder
	var quote rune
	lineComment := false
	blockComment := false
	escaped := false
	runes := []rune(sqlText)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		next := rune(0)
		if i+1 < len(runes) {
			next = runes[i+1]
		}
		if lineComment {
			if ch == '\n' {
				lineComment = false
				current.WriteRune(ch)
			}
			continue
		}
		if blockComment {
			if ch == '*' && next == '/' {
				blockComment = false
				i++
			}
			continue
		}
		if quote == 0 {
			if ch == '-' && next == '-' {
				lineComment = true
				i++
				continue
			}
			if ch == '#' {
				lineComment = true
				continue
			}
			if ch == '/' && next == '*' {
				blockComment = true
				i++
				continue
			}
			if ch == '\'' || ch == '"' || ch == '`' {
				quote = ch
				current.WriteRune(ch)
				continue
			}
			if ch == ';' {
				stmt := strings.TrimSpace(current.String())
				if stmt != "" {
					statements = append(statements, stmt)
				}
				current.Reset()
				continue
			}
			current.WriteRune(ch)
			continue
		}
		current.WriteRune(ch)
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && quote != '`' {
			escaped = true
			continue
		}
		if ch == quote {
			quote = 0
		}
	}
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}
	return statements
}
