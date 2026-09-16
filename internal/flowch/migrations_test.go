// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsReadsCanonicalSetAndExactChecksums(t *testing.T) {
	migrations, err := LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 16 || migrations[0].Name != "001_flow_schema.sql" || migrations[11].Name != "012_snmp_telemetry.sql" || migrations[12].Name != "013_flow_vpn_candidate_features.sql" || migrations[13].Name != "014_snmp_events.sql" || migrations[14].Name != "015_flow_raw_delete_quarantine.sql" || migrations[15].Name != "016_flow_historical_reclassification.sql" {
		t.Fatalf("migrations=%+v", migrations)
	}
	for _, migration := range migrations {
		if len(migration.Checksum) != 64 || len(migration.Statements) == 0 {
			t.Fatalf("invalid loaded migration=%+v", migration)
		}
	}
	data, err := os.ReadFile("../../deploy/migration/clickhouse/001_flow_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	changed := fstest.MapFS{"001_flow_schema.sql": {Data: append(data, '\n')}}
	changedMigrations, err := LoadMigrations(changed, ".")
	if err != nil {
		t.Fatal(err)
	}
	if changedMigrations[0].Checksum == migrations[0].Checksum {
		t.Fatal("exact-byte migration checksum ignored a content change")
	}
}

func TestLoadMigrationsRejectsInvalidSets(t *testing.T) {
	large := make([]byte, maxClickHouseMigrationBytes+1)
	for index := range large {
		large[index] = 'x'
	}
	for _, test := range []struct {
		name string
		fs   fstest.MapFS
	}{
		{name: "empty", fs: fstest.MapFS{}},
		{name: "bad name", fs: fstest.MapFS{"1_bad.sql": {Data: []byte("SELECT 1")}}},
		{name: "starts at two", fs: fstest.MapFS{"002_two.sql": {Data: []byte("SELECT 2")}}},
		{name: "gap", fs: fstest.MapFS{"001_one.sql": {Data: []byte("SELECT 1")}, "003_three.sql": {Data: []byte("SELECT 3")}}},
		{name: "empty sql", fs: fstest.MapFS{"001_one.sql": {Data: []byte("  ")}}},
		{name: "nul", fs: fstest.MapFS{"001_one.sql": {Data: []byte("SELECT\x001")}}},
		{name: "invalid utf8", fs: fstest.MapFS{"001_one.sql": {Data: []byte{0xff}}}},
		{name: "bom", fs: fstest.MapFS{"001_one.sql": {Data: []byte("\xef\xbb\xbfSELECT 1")}}},
		{name: "oversize", fs: fstest.MapFS{"001_one.sql": {Data: large}}},
		{name: "unterminated", fs: fstest.MapFS{"001_one.sql": {Data: []byte("SELECT 'one")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := LoadMigrations(test.fs, "."); err == nil {
				t.Fatal("invalid migration set was accepted")
			}
		})
	}
}

func TestSplitMigrationSQLHandlesClickHouseQuotesAndComments(t *testing.T) {
	input := `-- first ; comment
CREATE TABLE x (value String DEFAULT ';');
/* second ; comment */ ALTER TABLE x ADD COLUMN IF NOT EXISTS quoted String DEFAULT 'it''s;ok';
SELECT "semi;colon", ` + "`identifier;name`" + `;
-- trailing comment`
	statements, err := splitMigrationSQL(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 3 || !strings.Contains(statements[0], "CREATE TABLE") || !strings.Contains(statements[1], "ALTER TABLE") || !strings.Contains(statements[2], "SELECT") {
		t.Fatalf("statements=%q", statements)
	}
	for _, malformed := range []string{"SELECT 'open", `SELECT "open`, "SELECT `open", "SELECT 1 /* open"} {
		if _, err := splitMigrationSQL(malformed); err == nil {
			t.Fatalf("malformed SQL %q was accepted", malformed)
		}
	}
}

func TestPlanMigrationsRejectsDriftGapsAheadAndDirtyByDefault(t *testing.T) {
	available := testMigrations(3)
	valid := []AppliedMigration{{Version: 1, Name: available[0].Name, Checksum: available[0].Checksum, State: MigrationApplied, CompletedStatements: 1}}
	plan, err := PlanMigrations(available, valid, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Applied) != 1 || len(plan.Pending) != 2 || plan.Resume {
		t.Fatalf("plan=%+v", plan)
	}
	for _, test := range []struct {
		name   string
		states []AppliedMigration
	}{
		{name: "duplicate", states: []AppliedMigration{valid[0], valid[0]}},
		{name: "gap", states: []AppliedMigration{{Version: 2, Name: available[1].Name, Checksum: available[1].Checksum, State: MigrationApplied, CompletedStatements: 1}}},
		{name: "ahead", states: append(valid, AppliedMigration{Version: 4, Name: "004_ahead.sql", Checksum: "x", State: MigrationApplied})},
		{name: "name drift", states: []AppliedMigration{{Version: 1, Name: "001_changed.sql", Checksum: available[0].Checksum, State: MigrationApplied, CompletedStatements: 1}}},
		{name: "checksum drift", states: []AppliedMigration{{Version: 1, Name: available[0].Name, Checksum: "changed", State: MigrationApplied, CompletedStatements: 1}}},
		{name: "dirty", states: []AppliedMigration{{Version: 1, Name: available[0].Name, Checksum: available[0].Checksum, State: MigrationFailed}}},
		{name: "unknown state", states: []AppliedMigration{{Version: 1, Name: available[0].Name, Checksum: available[0].Checksum, State: "mystery"}}},
		{name: "applied incomplete", states: []AppliedMigration{{Version: 1, Name: available[0].Name, Checksum: available[0].Checksum, State: MigrationApplied}}},
		{name: "dirty progress overflow", states: []AppliedMigration{{Version: 1, Name: available[0].Name, Checksum: available[0].Checksum, State: MigrationFailed, CompletedStatements: 2}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PlanMigrations(available, test.states, false); err == nil {
				t.Fatal("invalid recorded state was accepted")
			}
		})
	}
	wrongAvailable := append([]Migration(nil), available...)
	wrongAvailable[0].Name = "002_wrong_version.sql"
	if _, err := PlanMigrations(wrongAvailable, nil, false); err == nil {
		t.Fatal("filename/version mismatch in available migrations was accepted")
	}
}

func TestPlanMigrationsExplicitResumeStartsAtDirtyVersion(t *testing.T) {
	available := testMigrations(3)
	recorded := []AppliedMigration{
		{Version: 1, Name: available[0].Name, Checksum: available[0].Checksum, State: MigrationApplied, CompletedStatements: 1},
		{Version: 2, Name: available[1].Name, Checksum: available[1].Checksum, State: MigrationApplying, CompletedStatements: 1},
	}
	plan, err := PlanMigrations(available, recorded, true)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Resume || plan.ResumeStatement != 1 || !reflect.DeepEqual(plan.Applied, available[:1]) || !reflect.DeepEqual(plan.Pending, available[1:]) {
		t.Fatalf("resume plan=%+v", plan)
	}
	recorded = append(recorded, AppliedMigration{Version: 3, Name: available[2].Name, Checksum: available[2].Checksum, State: MigrationApplied, CompletedStatements: 1})
	if _, err := PlanMigrations(available, recorded, true); err == nil {
		t.Fatal("dirty state followed by a newer version was resumable")
	}
}

func testMigrations(count int) []Migration {
	result := make([]Migration, 0, count)
	for index := 1; index <= count; index++ {
		result = append(result, Migration{
			Version: uint32(index), Name: fmt.Sprintf("%03d_test.sql", index),
			Checksum: strings.Repeat(string(rune('a'+index)), 64), Statements: []string{"SELECT 1"},
		})
	}
	return result
}
