// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxClickHouseMigrationBytes = 4 << 20

var clickHouseMigrationName = regexp.MustCompile(`^([0-9]{3})_([a-z0-9][a-z0-9_]*)\.sql$`)

type Migration struct {
	Version    uint32   `json:"version"`
	Name       string   `json:"name"`
	Checksum   string   `json:"checksum"`
	Statements []string `json:"-"`
}

type MigrationState string

const (
	MigrationApplying MigrationState = "applying"
	MigrationApplied  MigrationState = "applied"
	MigrationFailed   MigrationState = "failed"
)

type AppliedMigration struct {
	Version             uint32         `json:"version"`
	Name                string         `json:"name"`
	Checksum            string         `json:"checksum"`
	State               MigrationState `json:"state"`
	CompletedStatements uint32         `json:"completed_statements"`
	AttemptID           string         `json:"attempt_id"`
	LastError           string         `json:"last_error,omitempty"`
	Generation          uint64         `json:"generation"`
}

type MigrationPlan struct {
	Applied         []Migration `json:"applied"`
	Pending         []Migration `json:"pending"`
	Resume          bool        `json:"resume"`
	ResumeStatement uint32      `json:"resume_statement"`
}

// LoadMigrations reads and validates the immutable, consecutively numbered
// ClickHouse migration set. Checksum covers the exact file bytes so even a
// comment or whitespace edit to a published migration is detected.
func LoadMigrations(source fs.FS, directory string) ([]Migration, error) {
	if source == nil {
		return nil, errors.New("ClickHouse migration filesystem is required")
	}
	directory = path.Clean(directory)
	entries, err := fs.ReadDir(source, directory)
	if err != nil {
		return nil, fmt.Errorf("read ClickHouse migration directory: %w", err)
	}
	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		matches := clickHouseMigrationName.FindStringSubmatch(entry.Name())
		if matches == nil {
			return nil, fmt.Errorf("invalid ClickHouse migration filename %q", entry.Name())
		}
		version, err := strconv.ParseUint(matches[1], 10, 32)
		if err != nil || version == 0 {
			return nil, fmt.Errorf("invalid ClickHouse migration version in %q", entry.Name())
		}
		data, err := fs.ReadFile(source, path.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read ClickHouse migration %q: %w", entry.Name(), err)
		}
		if len(data) == 0 || len(data) > maxClickHouseMigrationBytes || !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 || bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}) {
			return nil, fmt.Errorf("ClickHouse migration %q must be non-empty UTF-8 SQL no larger than %d bytes", entry.Name(), maxClickHouseMigrationBytes)
		}
		statements, err := splitMigrationSQL(string(data))
		if err != nil {
			return nil, fmt.Errorf("parse ClickHouse migration %q: %w", entry.Name(), err)
		}
		if len(statements) == 0 {
			return nil, fmt.Errorf("ClickHouse migration %q contains no SQL statements", entry.Name())
		}
		digest := sha256.Sum256(data)
		migrations = append(migrations, Migration{
			Version: uint32(version), Name: entry.Name(), Checksum: hex.EncodeToString(digest[:]), Statements: statements,
		})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	if len(migrations) == 0 {
		return nil, errors.New("ClickHouse migration directory contains no migrations")
	}
	for index, migration := range migrations {
		expected := uint32(index + 1)
		if migration.Version != expected {
			return nil, fmt.Errorf("ClickHouse migrations must be consecutive from 001: got %03d, want %03d", migration.Version, expected)
		}
	}
	return migrations, nil
}

// PlanMigrations compares the local immutable set with the latest state read
// from ClickHouse. A dirty migration is never replayed implicitly; resume must
// be an explicit operator choice and only the first unapplied version may be
// resumed.
func PlanMigrations(available []Migration, recorded []AppliedMigration, resume bool) (MigrationPlan, error) {
	if len(available) == 0 {
		return MigrationPlan{}, errors.New("no ClickHouse migrations are available")
	}
	for index, migration := range available {
		checksum, checksumErr := hex.DecodeString(migration.Checksum)
		matches := clickHouseMigrationName.FindStringSubmatch(migration.Name)
		nameVersion, nameVersionErr := strconv.ParseUint(migrationNameVersion(matches), 10, 32)
		if migration.Version != uint32(index+1) || nameVersionErr != nil || uint32(nameVersion) != migration.Version || len(checksum) != sha256.Size || checksumErr != nil || len(migration.Statements) == 0 {
			return MigrationPlan{}, errors.New("available ClickHouse migration set is invalid")
		}
	}
	states := append([]AppliedMigration(nil), recorded...)
	sort.Slice(states, func(i, j int) bool { return states[i].Version < states[j].Version })
	plan := MigrationPlan{}
	for index, state := range states {
		expected := uint32(index + 1)
		if state.Version != expected {
			return MigrationPlan{}, fmt.Errorf("recorded ClickHouse migration history has a gap or duplicate at version %03d", state.Version)
		}
		if int(state.Version) > len(available) {
			return MigrationPlan{}, fmt.Errorf("ClickHouse schema version %03d is newer than the available migration set", state.Version)
		}
		migration := available[state.Version-1]
		if state.Name != migration.Name || state.Checksum != migration.Checksum {
			return MigrationPlan{}, fmt.Errorf("ClickHouse migration %03d checksum or name drift detected", state.Version)
		}
		switch state.State {
		case MigrationApplied:
			if state.CompletedStatements != uint32(len(migration.Statements)) {
				return MigrationPlan{}, fmt.Errorf("applied ClickHouse migration %03d has incomplete statement progress", state.Version)
			}
			plan.Applied = append(plan.Applied, migration)
		case MigrationApplying, MigrationFailed:
			if state.CompletedStatements > uint32(len(migration.Statements)) {
				return MigrationPlan{}, fmt.Errorf("dirty ClickHouse migration %03d has invalid statement progress", state.Version)
			}
			if index != len(states)-1 {
				return MigrationPlan{}, fmt.Errorf("dirty ClickHouse migration %03d is followed by newer recorded state", state.Version)
			}
			if !resume {
				return MigrationPlan{}, fmt.Errorf("ClickHouse migration %03d is %s; inspect and resume explicitly", state.Version, state.State)
			}
			plan.Pending = append(plan.Pending, available[state.Version-1:]...)
			plan.Resume = true
			plan.ResumeStatement = state.CompletedStatements
			return plan, nil
		default:
			return MigrationPlan{}, fmt.Errorf("ClickHouse migration %03d has unknown state %q", state.Version, state.State)
		}
	}
	plan.Pending = append(plan.Pending, available[len(states):]...)
	return plan, nil
}

func migrationNameVersion(matches []string) string {
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func splitMigrationSQL(input string) ([]string, error) {
	const (
		stateSQL = iota
		stateSingleQuote
		stateDoubleQuote
		stateBacktick
		stateLineComment
		stateBlockComment
	)
	state := stateSQL
	start := 0
	hasCode := false
	statements := make([]string, 0, 8)
	for index := 0; index < len(input); index++ {
		character := input[index]
		switch state {
		case stateSQL:
			switch {
			case character == '\'':
				hasCode = true
				state = stateSingleQuote
			case character == '"':
				hasCode = true
				state = stateDoubleQuote
			case character == '`':
				hasCode = true
				state = stateBacktick
			case character == '-' && index+1 < len(input) && input[index+1] == '-':
				state = stateLineComment
				index++
			case character == '/' && index+1 < len(input) && input[index+1] == '*':
				state = stateBlockComment
				index++
			case character == ';':
				if statement := strings.TrimSpace(input[start:index]); hasCode {
					statements = append(statements, statement)
				}
				start = index + 1
				hasCode = false
			default:
				if !strings.ContainsRune(" \t\r\n\f\v", rune(character)) {
					hasCode = true
				}
			}
		case stateSingleQuote, stateDoubleQuote, stateBacktick:
			quote := byte('\'')
			if state == stateDoubleQuote {
				quote = '"'
			} else if state == stateBacktick {
				quote = '`'
			}
			if character == '\\' && index+1 < len(input) {
				index++
				continue
			}
			if character == quote {
				if index+1 < len(input) && input[index+1] == quote {
					index++
					continue
				}
				state = stateSQL
			}
		case stateLineComment:
			if character == '\n' {
				state = stateSQL
			}
		case stateBlockComment:
			if character == '*' && index+1 < len(input) && input[index+1] == '/' {
				state = stateSQL
				index++
			}
		}
	}
	if state != stateSQL && state != stateLineComment {
		return nil, errors.New("unterminated quoted value or block comment")
	}
	if statement := strings.TrimSpace(input[start:]); hasCode {
		statements = append(statements, statement)
	}
	return statements, nil
}
