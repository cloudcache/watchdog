// Package flowlifecycle owns the single-domain Flow storage lifecycle
// contract. It contains no scheduler and performs no deletion; operation_jobs
// handlers must satisfy these guards before issuing ClickHouse partition DDL.
package flowlifecycle

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const ArchiveResolution = time.Hour

const (
	PolicyDraft     = "draft"
	PolicyPublished = "published"
	PolicyRetired   = "retired"
)

const (
	PartitionSealed         = "sealed"
	PartitionArchiveWritten = "archive_written"
	PartitionReconciled     = "reconciled"
	PartitionDeleteEligible = "delete_eligible"
	PartitionRawDeleted     = "raw_deleted"
	PartitionFailed         = "failed"
)

var (
	ErrInvalidPolicy    = errors.New("invalid Flow retention policy")
	ErrInvalidWatermark = errors.New("invalid Flow reconciliation watermark")
	ErrDeleteLocked     = errors.New("Flow partition deletion is locked")
)

// Policy is one immutable-on-publish, installation-wide policy revision.
// Seconds are used at API/SQL boundaries so the stored contract is language
// independent. ArchiveRetentionSeconds == 0 means retain indefinitely.
type Policy struct {
	ID                        string    `json:"id"`
	Version                   uint64    `json:"policy_version"`
	Status                    string    `json:"status"`
	BootstrapFrom             time.Time `json:"bootstrap_from"`
	RawRetentionSeconds       uint64    `json:"raw_retention_seconds"`
	ArchiveResolutionSeconds  uint32    `json:"archive_resolution_seconds"`
	ArchiveRetentionSeconds   uint64    `json:"archive_retention_seconds"`
	LateArrivalSeconds        uint32    `json:"late_arrival_seconds"`
	DeleteGraceSeconds        uint32    `json:"delete_grace_seconds"`
	MaxPartitionsPerRun       uint32    `json:"max_partitions_per_run"`
	RawDeleteEnabled          bool      `json:"raw_delete_enabled"`
	ArchiveDeleteEnabled      bool      `json:"archive_delete_enabled"`
	RequireBackupBeforeDelete bool      `json:"require_backup_before_delete"`
	RowVersion                uint64    `json:"row_version"`
	CreatedBy                 string    `json:"created_by,omitempty"`
	PublishedBy               string    `json:"published_by,omitempty"`
	RetiredBy                 string    `json:"retired_by,omitempty"`
	CreatedAt                 time.Time `json:"created_at"`
	PublishedAt               time.Time `json:"published_at,omitzero"`
	RetiredAt                 time.Time `json:"retired_at,omitzero"`
}

// NormalizePolicy applies only semantic defaults; it deliberately has no
// retention-day default. An operator must publish the destructive boundary.
func NormalizePolicy(policy Policy) (Policy, error) {
	policy.ID = strings.TrimSpace(policy.ID)
	policy.Status = strings.TrimSpace(policy.Status)
	policy.BootstrapFrom = UTCDate(policy.BootstrapFrom)
	if policy.ArchiveResolutionSeconds == 0 {
		policy.ArchiveResolutionSeconds = uint32(ArchiveResolution / time.Second)
	}
	if policy.Status == "" {
		policy.Status = PolicyDraft
	}
	if policy.ID == "" || len(policy.ID) > 26 || policy.Version == 0 || policy.Version > uint64(^uint32(0)) ||
		(policy.Status != PolicyDraft && policy.Status != PolicyPublished && policy.Status != PolicyRetired) ||
		policy.BootstrapFrom.IsZero() || policy.RawRetentionSeconds < 86400 || policy.RawRetentionSeconds > 315576000 ||
		policy.ArchiveResolutionSeconds != uint32(ArchiveResolution/time.Second) ||
		(policy.ArchiveRetentionSeconds != 0 && policy.ArchiveRetentionSeconds <= policy.RawRetentionSeconds) ||
		policy.LateArrivalSeconds > 604800 || policy.DeleteGraceSeconds < 3600 || policy.DeleteGraceSeconds > 2592000 ||
		policy.MaxPartitionsPerRun < 1 || policy.MaxPartitionsPerRun > 366 {
		return Policy{}, ErrInvalidPolicy
	}
	return policy, nil
}

// Generation gives each repair an ordered ClickHouse generation without a
// lookup: high 32 bits are the immutable policy version; low 32 bits are a
// non-zero repair attempt.
func Generation(policyVersion uint64, repairAttempt uint32) (uint64, error) {
	if policyVersion == 0 || policyVersion > uint64(^uint32(0)) || repairAttempt == 0 {
		return 0, ErrInvalidPolicy
	}
	return policyVersion<<32 | uint64(repairAttempt), nil
}

func SplitGeneration(generation uint64) (policyVersion uint32, repairAttempt uint32, err error) {
	policyVersion, repairAttempt = uint32(generation>>32), uint32(generation)
	if policyVersion == 0 || repairAttempt == 0 {
		return 0, 0, ErrInvalidPolicy
	}
	return policyVersion, repairAttempt, nil
}

// ArchiveEligibleAt is the first instant when a complete UTC source day has
// crossed both raw retention and late-arrival windows.
func ArchiveEligibleAt(sourceDate time.Time, policy Policy) (time.Time, error) {
	policy, err := NormalizePolicy(policy)
	if err != nil {
		return time.Time{}, err
	}
	day := UTCDate(sourceDate)
	if day.IsZero() || day.Before(policy.BootstrapFrom) {
		return time.Time{}, ErrInvalidPolicy
	}
	wait := policy.RawRetentionSeconds
	if uint64(policy.LateArrivalSeconds) > wait {
		wait = uint64(policy.LateArrivalSeconds)
	}
	return day.Add(24*time.Hour + time.Duration(wait)*time.Second), nil
}

func RawDeleteEligibleAt(sourceDate time.Time, policy Policy) (time.Time, error) {
	eligible, err := ArchiveEligibleAt(sourceDate, policy)
	if err != nil {
		return time.Time{}, err
	}
	return eligible.Add(time.Duration(policy.DeleteGraceSeconds) * time.Second), nil
}

// Watermark stores Kafka next offsets. ReconciledNext advances only after a
// contiguous receipt/fact comparison, and can never pass the stable committed
// snapshot obtained from the broker.
type Watermark struct {
	SourceStreamID       string
	KafkaTopic           string
	ConsumerGroup        string
	KafkaPartition       uint32
	BootstrapOffset      uint64
	CommittedNextOffset  uint64
	ReconciledNextOffset uint64
}

func (watermark Watermark) Validate() error {
	if strings.TrimSpace(watermark.SourceStreamID) == "" || strings.TrimSpace(watermark.KafkaTopic) == "" ||
		strings.TrimSpace(watermark.ConsumerGroup) == "" || watermark.BootstrapOffset > watermark.ReconciledNextOffset ||
		watermark.ReconciledNextOffset > watermark.CommittedNextOffset {
		return ErrInvalidWatermark
	}
	return nil
}

// OffsetCoverage is the immutable per-Kafka-partition evidence captured by a
// deletion receipt. LastOffsetExclusive is derived from all receipts whose
// min/max event time intersects the UTC partition being removed.
type OffsetCoverage struct {
	SourceStreamID       string    `json:"source_stream_id"`
	KafkaTopic           string    `json:"kafka_topic"`
	ConsumerGroup        string    `json:"consumer_group"`
	KafkaPartition       uint32    `json:"kafka_partition"`
	BootstrapOffset      uint64    `json:"bootstrap_offset"`
	FirstOffset          uint64    `json:"first_offset"`
	LastOffsetExclusive  uint64    `json:"last_offset_exclusive"`
	CommittedNextOffset  uint64    `json:"committed_next_offset"`
	ReconciledNextOffset uint64    `json:"reconciled_next_offset"`
	CommittedSnapshotAt  time.Time `json:"committed_snapshot_at"`
	VerifiedAt           time.Time `json:"verified_at"`
}

func (coverage OffsetCoverage) validate() error {
	if strings.TrimSpace(coverage.SourceStreamID) == "" || strings.TrimSpace(coverage.KafkaTopic) == "" ||
		strings.TrimSpace(coverage.ConsumerGroup) == "" || coverage.BootstrapOffset > coverage.FirstOffset ||
		coverage.FirstOffset >= coverage.LastOffsetExclusive || coverage.CommittedSnapshotAt.IsZero() || coverage.VerifiedAt.IsZero() ||
		coverage.LastOffsetExclusive > coverage.CommittedNextOffset || coverage.LastOffsetExclusive > coverage.ReconciledNextOffset {
		return ErrDeleteLocked
	}
	return nil
}

type BackupEvidence struct {
	ID              string    `json:"id"`
	StorageKind     string    `json:"storage_kind"`
	CoveredFrom     time.Time `json:"covered_from"`
	CoveredThrough  time.Time `json:"covered_through"`
	BackupRef       string    `json:"backup_ref"`
	ChecksumSHA256  string    `json:"checksum_sha256"`
	Status          string    `json:"status"`
	VerifiedBy      string    `json:"verified_by,omitempty"`
	VerifiedAt      time.Time `json:"verified_at"`
	RestoreTestedAt time.Time `json:"restore_tested_at"`
	RestoreTestRef  string    `json:"restore_test_ref"`
	RevokedBy       string    `json:"revoked_by,omitempty"`
	RevokedAt       time.Time `json:"revoked_at,omitzero"`
	RowVersion      uint64    `json:"row_version"`
	CreatedAt       time.Time `json:"created_at"`
}

func (evidence BackupEvidence) covers(kind string, start, end time.Time) bool {
	return evidence.Status == "verified" && (evidence.StorageKind == kind || evidence.StorageKind == "all") &&
		!evidence.VerifiedAt.IsZero() && !evidence.RestoreTestedAt.IsZero() && strings.TrimSpace(evidence.BackupRef) != "" &&
		validSHA256(evidence.ChecksumSHA256) && strings.TrimSpace(evidence.RestoreTestRef) != "" &&
		!UTCDate(start).Before(UTCDate(evidence.CoveredFrom)) && !UTCDate(evidence.CoveredThrough).Before(UTCDate(end))
}

type Counters struct {
	RecordCount           uint64
	RawBytes              uint64
	RawPackets            uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	EstimatedValidRecords uint64
}

type RawDayDeleteGuard struct {
	Now              time.Time
	SourceDate       time.Time
	Policy           Policy
	State            string
	Generation       uint64
	ReconciledAt     time.Time
	DeleteEligibleAt time.Time
	Source           Counters
	Archive          Counters
	Coverage         []OffsetCoverage
	Backup           *BackupEvidence
}

// ValidateRawDayDelete is fail-closed. It proves counter conservation, stable
// Kafka coverage, the late/grace windows and optional restore evidence before
// a handler may DROP the flow_records YYYYMMDD partition.
func ValidateRawDayDelete(guard RawDayDeleteGuard) error {
	policy, err := NormalizePolicy(guard.Policy)
	if err != nil || policy.Status != PolicyPublished || !policy.RawDeleteEnabled || guard.Now.IsZero() {
		return ErrDeleteLocked
	}
	day := UTCDate(guard.SourceDate)
	version, _, err := SplitGeneration(guard.Generation)
	if err != nil || uint64(version) != policy.Version {
		return ErrDeleteLocked
	}
	earliest, err := RawDeleteEligibleAt(day, policy)
	if err != nil || guard.State != PartitionDeleteEligible || guard.ReconciledAt.IsZero() ||
		guard.DeleteEligibleAt.Before(earliest) || guard.Now.UTC().Before(guard.DeleteEligibleAt.UTC()) || guard.Source != guard.Archive {
		return ErrDeleteLocked
	}
	if guard.Source.RecordCount > 0 && len(guard.Coverage) == 0 {
		return ErrDeleteLocked
	}
	seen := make(map[string]struct{}, len(guard.Coverage))
	for _, coverage := range guard.Coverage {
		if err := coverage.validate(); err != nil {
			return err
		}
		key := fmt.Sprintf("%s\x00%d", coverage.SourceStreamID, coverage.KafkaPartition)
		if _, duplicate := seen[key]; duplicate {
			return ErrDeleteLocked
		}
		seen[key] = struct{}{}
	}
	end := day.Add(24 * time.Hour)
	if policy.RequireBackupBeforeDelete && (guard.Backup == nil || !guard.Backup.covers("raw", day, end)) {
		return ErrDeleteLocked
	}
	return nil
}

func UTCDate(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
