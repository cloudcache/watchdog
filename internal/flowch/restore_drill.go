// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const maxBackupManifestBytes = 64 << 20

var ErrInvalidRestoreDrill = errors.New("invalid Flow backup restore drill")

// RestoreDrillRequest identifies one immutable external ClickHouse backup and
// the isolated database into which it must be restored. BackupName is relative
// to the configured ClickHouse disk; accepting a raw BACKUP expression here
// would turn an operator input into SQL.
type RestoreDrillRequest struct {
	SourceDatabase      string
	RestoreDatabase     string
	BackupDisk          string
	BackupName          string
	BackupManifestFile  string
	SourceDate          time.Time
	ArchiveMonth        time.Time
	KeepRestoreDatabase bool
}

type RestoreDrillArchiveMonthSnapshot struct {
	Counters        StorageCounters `json:"counters"`
	PhysicalRecords uint64          `json:"physical_records"`
}

type RestoreDrillSnapshot struct {
	Raw             StorageCounters                   `json:"raw"`
	Archive         StorageCounters                   `json:"archive"`
	PhysicalRecords uint64                            `json:"physical_records"`
	KafkaCoverage   []DayOffsetCoverage               `json:"kafka_coverage"`
	ArchiveMonth    *RestoreDrillArchiveMonthSnapshot `json:"archive_month,omitempty"`
}

// RestoreDrillResult is suitable for attaching to the immutable MySQL backup
// evidence row. ManifestSHA256 hashes ClickHouse's real .backup manifest; that
// manifest in turn contains the checksums of every backed-up file.
type RestoreDrillResult struct {
	BackupRef              string               `json:"backup_ref"`
	BackupManifestUUID     string               `json:"backup_manifest_uuid"`
	ManifestSHA256         string               `json:"manifest_sha256"`
	RestoreTestRef         string               `json:"restore_test_ref"`
	SourceDatabase         string               `json:"source_database"`
	RestoreDatabase        string               `json:"restore_database"`
	SourceDate             time.Time            `json:"source_date"`
	ArchiveMonth           time.Time            `json:"archive_month,omitzero"`
	StartedAt              time.Time            `json:"started_at"`
	CompletedAt            time.Time            `json:"completed_at"`
	RestoreOperationID     string               `json:"restore_operation_id"`
	SourceBefore           RestoreDrillSnapshot `json:"source_before"`
	SourceAfter            RestoreDrillSnapshot `json:"source_after"`
	Restored               RestoreDrillSnapshot `json:"restored"`
	Matched                bool                 `json:"matched"`
	RestoreDatabaseCleaned bool                 `json:"restore_database_cleaned"`
}

type backupManifest struct {
	Version string `xml:"version"`
	UUID    string `xml:"uuid"`
}

// RunRestoreDrill restores a real external backup, checks the restored Flow
// schema, and independently recomputes the natural Kafka coordinates, logical
// and physical row counts, and byte/packet conservation tuple. It never writes
// production Flow data and refuses to reuse an existing restore database.
func RunRestoreDrill(ctx context.Context, config NativeConfig, request RestoreDrillRequest) (result RestoreDrillResult, runErr error) {
	request = normalizeRestoreDrillRequest(request)
	if err := validateRestoreDrillRequest(request); err != nil {
		return result, err
	}
	result = RestoreDrillResult{
		BackupRef:       "clickhouse-disk://" + request.BackupDisk + "/" + request.BackupName,
		SourceDatabase:  request.SourceDatabase,
		RestoreDatabase: request.RestoreDatabase,
		SourceDate:      request.SourceDate,
		ArchiveMonth:    request.ArchiveMonth,
		StartedAt:       time.Now().UTC().Truncate(time.Millisecond),
	}
	manifestUUID, checksum, err := readBackupManifest(request.BackupManifestFile)
	if err != nil {
		return result, fmt.Errorf("verify ClickHouse backup manifest: %w", err)
	}
	result.BackupManifestUUID, result.ManifestSHA256 = manifestUUID, checksum

	controlConfig := config
	controlConfig.Database = "default"
	controlConfig.ClientName = "watchdog-flow-restore-drill-control"
	control, err := NewNativeInserter(ctx, controlConfig)
	if err != nil {
		return result, err
	}
	defer control.Close()
	exists, err := clickHouseDatabaseExists(ctx, control.executor, request.RestoreDatabase)
	if err != nil {
		return result, err
	}
	if exists {
		return result, fmt.Errorf("%w: restore database already exists", ErrInvalidRestoreDrill)
	}

	sourceConfig := config
	sourceConfig.Database = request.SourceDatabase
	sourceConfig.ClientName = "watchdog-flow-restore-drill-source"
	source, err := NewNativeInserter(ctx, sourceConfig)
	if err != nil {
		return result, err
	}
	defer source.Close()
	if err := source.Ready(ctx); err != nil {
		return result, fmt.Errorf("verify source Flow schema: %w", err)
	}
	result.SourceBefore, err = readRestoreDrillSnapshot(ctx, source, request.SourceDate, request.ArchiveMonth)
	if err != nil {
		return result, fmt.Errorf("read source evidence before restore: %w", err)
	}

	restoreStarted := false
	if !request.KeepRestoreDatabase {
		defer func() {
			if !restoreStarted {
				return
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := dropClickHouseDatabase(cleanupCtx, control.executor, request.RestoreDatabase); err != nil {
				if runErr == nil {
					runErr = fmt.Errorf("drop isolated restore database: %w", err)
				}
				return
			}
			result.RestoreDatabaseCleaned = true
		}()
	}
	restoreStarted = true
	result.RestoreOperationID, err = restoreClickHouseDatabase(ctx, control.executor, request)
	if err != nil {
		return result, err
	}
	result.RestoreTestRef = "clickhouse-restore:" + result.RestoreOperationID + ":" + request.RestoreDatabase

	restoreConfig := config
	restoreConfig.Database = request.RestoreDatabase
	restoreConfig.ClientName = "watchdog-flow-restore-drill-isolated"
	restored, err := NewNativeInserter(ctx, restoreConfig)
	if err != nil {
		return result, err
	}
	if err := restored.Ready(ctx); err != nil {
		restored.Close()
		return result, fmt.Errorf("verify restored Flow schema: %w", err)
	}
	result.Restored, err = readRestoreDrillSnapshot(ctx, restored, request.SourceDate, request.ArchiveMonth)
	restored.Close()
	if err != nil {
		return result, fmt.Errorf("read restored evidence: %w", err)
	}
	result.SourceAfter, err = readRestoreDrillSnapshot(ctx, source, request.SourceDate, request.ArchiveMonth)
	if err != nil {
		return result, fmt.Errorf("read source evidence after restore: %w", err)
	}
	result.CompletedAt = time.Now().UTC().Truncate(time.Millisecond)
	result.Matched = equalRestoreDrillSnapshot(result.SourceBefore, result.SourceAfter) &&
		equalRestoreDrillSnapshot(result.SourceBefore, result.Restored)
	if !result.Matched {
		return result, errors.New("Flow backup restore drill evidence mismatch")
	}
	return result, nil
}

func normalizeRestoreDrillRequest(request RestoreDrillRequest) RestoreDrillRequest {
	request.SourceDatabase = strings.TrimSpace(request.SourceDatabase)
	request.RestoreDatabase = strings.TrimSpace(request.RestoreDatabase)
	request.BackupDisk = strings.TrimSpace(request.BackupDisk)
	request.BackupName = strings.TrimSpace(request.BackupName)
	request.BackupManifestFile = strings.TrimSpace(request.BackupManifestFile)
	request.SourceDate = request.SourceDate.UTC()
	if !request.ArchiveMonth.IsZero() {
		request.ArchiveMonth = request.ArchiveMonth.UTC()
	}
	return request
}

func validateRestoreDrillRequest(request RestoreDrillRequest) error {
	if !validClickHouseIdentifier(request.SourceDatabase) || !validClickHouseIdentifier(request.RestoreDatabase) ||
		!strings.HasPrefix(request.RestoreDatabase, "watchdog_restore_") || request.SourceDatabase == request.RestoreDatabase ||
		!validClickHouseIdentifier(request.BackupDisk) || !validBackupName(request.BackupName) ||
		len("clickhouse-disk://")+len(request.BackupDisk)+1+len(request.BackupName) > 512 ||
		strings.HasPrefix(request.BackupName, "/") || strings.Contains(request.BackupName, "\\") ||
		request.BackupManifestFile == "" || request.SourceDate.IsZero() || request.SourceDate != request.SourceDate.Truncate(24*time.Hour) ||
		(!request.ArchiveMonth.IsZero() && (request.ArchiveMonth.Day() != 1 || request.ArchiveMonth != request.ArchiveMonth.Truncate(24*time.Hour))) {
		return ErrInvalidRestoreDrill
	}
	for _, segment := range strings.Split(request.BackupName, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return ErrInvalidRestoreDrill
		}
	}
	return nil
}

func validBackupName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '/' || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func readBackupManifest(path string) (string, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	limited := &io.LimitedReader{R: file, N: maxBackupManifestBytes + 1}
	hash := sha256.New()
	tee := io.TeeReader(limited, hash)
	var manifest backupManifest
	if err := xml.NewDecoder(tee).Decode(&manifest); err != nil {
		return "", "", err
	}
	if _, err := io.Copy(hash, limited); err != nil {
		return "", "", err
	}
	if limited.N <= 0 || strings.TrimSpace(manifest.Version) == "" || strings.TrimSpace(manifest.UUID) == "" {
		return "", "", ErrInvalidRestoreDrill
	}
	return strings.TrimSpace(manifest.UUID), hex.EncodeToString(hash.Sum(nil)), nil
}

func readRestoreDrillSnapshot(ctx context.Context, native *NativeInserter, day, archiveMonth time.Time) (RestoreDrillSnapshot, error) {
	runner, err := NewRollupRunner(native)
	if err != nil {
		return RestoreDrillSnapshot{}, err
	}
	raw, archive, err := runner.DayStorageCounters(ctx, day)
	if err != nil {
		return RestoreDrillSnapshot{}, err
	}
	physical, err := runner.RawDayPhysicalRecords(ctx, day)
	if err != nil {
		return RestoreDrillSnapshot{}, err
	}
	coverage, err := runner.DayOffsetCoverage(ctx, day)
	if err != nil {
		return RestoreDrillSnapshot{}, err
	}
	slices.SortFunc(coverage, compareDayOffsetCoverage)
	result := RestoreDrillSnapshot{Raw: raw, Archive: archive, PhysicalRecords: physical, KafkaCoverage: coverage}
	if !archiveMonth.IsZero() {
		counters, err := runner.ArchiveMonthStorageCounters(ctx, archiveMonth)
		if err != nil {
			return RestoreDrillSnapshot{}, err
		}
		physicalRecords, err := runner.ArchiveMonthPhysicalRecords(ctx, archiveMonth)
		if err != nil {
			return RestoreDrillSnapshot{}, err
		}
		result.ArchiveMonth = &RestoreDrillArchiveMonthSnapshot{Counters: counters, PhysicalRecords: physicalRecords}
	}
	return result, nil
}

func compareDayOffsetCoverage(left, right DayOffsetCoverage) int {
	if value := strings.Compare(left.SourceStreamID, right.SourceStreamID); value != 0 {
		return value
	}
	if value := strings.Compare(left.KafkaTopic, right.KafkaTopic); value != 0 {
		return value
	}
	if left.KafkaPartition != right.KafkaPartition {
		if left.KafkaPartition < right.KafkaPartition {
			return -1
		}
		return 1
	}
	if left.FirstOffset != right.FirstOffset {
		if left.FirstOffset < right.FirstOffset {
			return -1
		}
		return 1
	}
	if left.LastOffsetExclusive < right.LastOffsetExclusive {
		return -1
	}
	if left.LastOffsetExclusive > right.LastOffsetExclusive {
		return 1
	}
	return 0
}

func equalRestoreDrillSnapshot(left, right RestoreDrillSnapshot) bool {
	return left.Raw == right.Raw && left.Archive == right.Archive && left.PhysicalRecords == right.PhysicalRecords &&
		slices.Equal(left.KafkaCoverage, right.KafkaCoverage) && equalArchiveMonthSnapshot(left.ArchiveMonth, right.ArchiveMonth)
}

func equalArchiveMonthSnapshot(left, right *RestoreDrillArchiveMonthSnapshot) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func clickHouseDatabaseExists(ctx context.Context, executor queryExecutor, database string) (bool, error) {
	if executor == nil || !validClickHouseIdentifier(database) {
		return false, ErrInvalidRestoreDrill
	}
	var count proto.ColUInt64
	query := ch.Query{
		Body:       "SELECT count() FROM system.databases WHERE name={database:String}",
		Parameters: ch.Parameters(map[string]any{"database": database}),
		Result:     proto.Results{{Name: "count()", Data: &count}},
	}
	if err := executor.Do(ctx, query); err != nil {
		return false, classifyClickHouseError(err)
	}
	if count.Rows() != 1 {
		return false, Permanent(errors.New("ClickHouse database existence query returned an invalid row count"))
	}
	return count[0] != 0, nil
}

func restoreClickHouseDatabase(ctx context.Context, executor queryExecutor, request RestoreDrillRequest) (string, error) {
	if executor == nil {
		return "", ErrInvalidRestoreDrill
	}
	var operationID proto.ColStr
	var status proto.ColEnum
	seen := false
	query := ch.Query{
		Body: fmt.Sprintf("RESTORE DATABASE %s AS %s FROM Disk({disk:String},{backup:String})",
			request.SourceDatabase, request.RestoreDatabase),
		Parameters: ch.Parameters(map[string]any{"disk": request.BackupDisk, "backup": request.BackupName}),
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
			return Permanent(errors.New("ClickHouse RESTORE returned an invalid result"))
		}
		seen = true
		return nil
	}
	if err := executor.Do(ctx, query); err != nil {
		return "", classifyClickHouseError(fmt.Errorf("restore ClickHouse backup: %w", err))
	}
	if !seen || status.Row(0) != "RESTORED" || strings.TrimSpace(operationID.Row(0)) == "" {
		return "", Permanent(fmt.Errorf("ClickHouse RESTORE status is %q", status.Row(0)))
	}
	return operationID.Row(0), nil
}

func dropClickHouseDatabase(ctx context.Context, executor queryExecutor, database string) error {
	if executor == nil || !validClickHouseIdentifier(database) || !strings.HasPrefix(database, "watchdog_restore_") {
		return ErrInvalidRestoreDrill
	}
	return executor.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database + " SYNC"})
}
