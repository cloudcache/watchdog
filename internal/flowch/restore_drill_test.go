// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func TestValidateRestoreDrillRequest(t *testing.T) {
	valid := RestoreDrillRequest{
		SourceDatabase: "watchdog_flow", RestoreDatabase: "watchdog_restore_20260916",
		BackupDisk: "flow_backups", BackupName: "raw/2026-09-16", BackupManifestFile: "/backup/.backup",
		SourceDate:   time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC),
		ArchiveMonth: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := validateRestoreDrillRequest(valid); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*RestoreDrillRequest){
		"source identifier": func(value *RestoreDrillRequest) { value.SourceDatabase = "bad-name" },
		"restore prefix":    func(value *RestoreDrillRequest) { value.RestoreDatabase = "production" },
		"same database": func(value *RestoreDrillRequest) {
			value.RestoreDatabase = "watchdog_restore_source"
			value.SourceDatabase = value.RestoreDatabase
		},
		"disk identifier": func(value *RestoreDrillRequest) { value.BackupDisk = "disk); DROP" },
		"absolute backup": func(value *RestoreDrillRequest) { value.BackupName = "/backup" },
		"parent backup":   func(value *RestoreDrillRequest) { value.BackupName = "raw/../backup" },
		"unsafe backup":   func(value *RestoreDrillRequest) { value.BackupName = "raw/'backup'" },
		"manifest":        func(value *RestoreDrillRequest) { value.BackupManifestFile = "" },
		"unaligned day":   func(value *RestoreDrillRequest) { value.SourceDate = value.SourceDate.Add(time.Hour) },
		"unaligned month": func(value *RestoreDrillRequest) { value.ArchiveMonth = value.ArchiveMonth.AddDate(0, 0, 1) },
	} {
		t.Run(name, func(t *testing.T) {
			input := valid
			mutate(&input)
			if err := validateRestoreDrillRequest(input); !errors.Is(err, ErrInvalidRestoreDrill) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestReadBackupManifestHashesActualManifest(t *testing.T) {
	data := []byte(`<config><version>1</version><uuid>backup-uuid</uuid><contents><file><name>part.bin</name><checksum>abc</checksum></file></contents></config>`)
	path := filepath.Join(t.TempDir(), ".backup")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	uuid, checksum, err := readBackupManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(data)
	if uuid != "backup-uuid" || checksum != hex.EncodeToString(want[:]) {
		t.Fatalf("uuid=%q checksum=%q", uuid, checksum)
	}
	bad := filepath.Join(t.TempDir(), ".backup")
	if err := os.WriteFile(bad, []byte(`<config><version>1</version></config>`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readBackupManifest(bad); !errors.Is(err, ErrInvalidRestoreDrill) {
		t.Fatalf("missing UUID error=%v", err)
	}
}

func TestEqualRestoreDrillSnapshotIncludesEveryProof(t *testing.T) {
	want := RestoreDrillSnapshot{
		Raw: StorageCounters{1, 2, 3, 4, 5, 6}, Archive: StorageCounters{1, 2, 3, 4, 5, 6}, PhysicalRecords: 2,
		KafkaCoverage: []DayOffsetCoverage{{SourceStreamID: "a", KafkaTopic: "raw", KafkaPartition: 1, FirstOffset: 2, LastOffsetExclusive: 3}},
		ArchiveMonth:  &RestoreDrillArchiveMonthSnapshot{Counters: StorageCounters{1, 2, 3, 4, 5, 6}, PhysicalRecords: 3},
	}
	if !equalRestoreDrillSnapshot(want, want) {
		t.Fatal("equal snapshot rejected")
	}
	changed := want
	changed.Raw.RawBytes++
	if equalRestoreDrillSnapshot(want, changed) {
		t.Fatal("counter mismatch accepted")
	}
	changed = want
	changed.KafkaCoverage = append([]DayOffsetCoverage(nil), want.KafkaCoverage...)
	changed.KafkaCoverage[0].LastOffsetExclusive++
	if equalRestoreDrillSnapshot(want, changed) {
		t.Fatal("Kafka coverage mismatch accepted")
	}
	changed = want
	changed.ArchiveMonth = &RestoreDrillArchiveMonthSnapshot{Counters: want.ArchiveMonth.Counters, PhysicalRecords: 4}
	if equalRestoreDrillSnapshot(want, changed) {
		t.Fatal("archive month physical record mismatch accepted")
	}
}

func TestRealClickHouseExternalBackupRestoreDrill(t *testing.T) {
	backupRoot := strings.TrimSpace(os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_BACKUP_ROOT"))
	if backupRoot == "" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_BACKUP_ROOT to the host path of the ClickHouse flow_backups disk")
	}
	const (
		sourceDatabase  = "watchdog_flow_it_restore_drill"
		restoreDatabase = "watchdog_restore_it_restore_drill"
		backupName      = "l5b3c-integration"
	)
	ctx, source := openDataIntegrationClickHouse(t, sourceDatabase)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	insertIntegrationBatch(t, ctx, source, integrationBatch(41, day.Add(time.Hour),
		integrationRecord(1, day.Add(time.Hour), "geo-city-a", 125),
		integrationRecord(2, day.Add(2*time.Hour), "geo-city-b", 375)))
	runner, err := NewRollupRunner(source)
	if err != nil {
		t.Fatal(err)
	}
	for hour := 0; hour < 24; hour++ {
		if err := runner.Run(ctx, RollupRequest{
			Resolution: RollupOneHour, Bucket: day.Add(time.Duration(hour) * time.Hour),
			Generation: 1, GeneratedAt: day.Add(48 * time.Hour),
		}); err != nil {
			t.Fatalf("roll up hour %d: %v", hour, err)
		}
	}

	backupPath := filepath.Join(backupRoot, backupName)
	if filepath.Clean(backupPath) == filepath.Clean(backupRoot) || filepath.Dir(backupPath) != filepath.Clean(backupRoot) {
		t.Fatalf("unsafe integration backup path %q", backupPath)
	}
	if err := os.RemoveAll(backupPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(backupPath); err != nil {
			t.Errorf("remove integration backup: %v", err)
		}
	})

	adminConfig := realMigrationConfig(t, "watchdog-flow-restore-drill-backup", 2*time.Minute)
	adminConfig.Database = "default"
	admin, err := NewNativeInserter(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := backupClickHouseDatabase(ctx, admin.executor, sourceDatabase, "flow_backups", backupName); err != nil {
		t.Fatal(err)
	}

	dataConfig := realMigrationConfig(t, "watchdog-flow-restore-drill", 2*time.Minute)
	result, err := RunRestoreDrill(ctx, dataConfig, RestoreDrillRequest{
		SourceDatabase: sourceDatabase, RestoreDatabase: restoreDatabase,
		BackupDisk: "flow_backups", BackupName: backupName,
		BackupManifestFile: filepath.Join(backupPath, ".backup"), SourceDate: day, ArchiveMonth: day.AddDate(0, 0, 1-day.Day()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Matched || !result.RestoreDatabaseCleaned || result.RestoreOperationID == "" ||
		result.BackupManifestUUID == "" || len(result.ManifestSHA256) != 64 || result.SourceBefore.PhysicalRecords != 2 ||
		result.SourceBefore.ArchiveMonth == nil || result.SourceBefore.ArchiveMonth.PhysicalRecords == 0 ||
		result.SourceBefore.Raw != (StorageCounters{RecordCount: 2, RawBytes: 500, RawPackets: 2, EstimatedBytes: 5000, EstimatedPackets: 20, EstimatedValidRecords: 2}) ||
		len(result.SourceBefore.KafkaCoverage) != 1 {
		t.Fatalf("restore drill result=%+v", result)
	}
	exists, err := clickHouseDatabaseExists(ctx, admin.executor, restoreDatabase)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("isolated restore database was not cleaned")
	}
}

func backupClickHouseDatabase(ctx context.Context, executor queryExecutor, database, disk, name string) error {
	var operationID proto.ColStr
	var status proto.ColEnum
	seen := false
	query := ch.Query{
		Body:       "BACKUP DATABASE " + database + " TO Disk({disk:String},{backup:String})",
		Parameters: ch.Parameters(map[string]any{"disk": disk, "backup": name}),
		Result: proto.Results{
			{Name: "id", Data: &operationID},
			{Name: "status", Data: &status},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if seen || block.Rows != 1 || operationID.Rows() != 1 || status.Rows() != 1 {
			return errors.New("invalid ClickHouse BACKUP result")
		}
		seen = true
		return nil
	}
	if err := executor.Do(ctx, query); err != nil {
		return err
	}
	if !seen || operationID.Row(0) == "" || status.Row(0) != "BACKUP_CREATED" {
		return errors.New("ClickHouse BACKUP did not complete")
	}
	return nil
}
