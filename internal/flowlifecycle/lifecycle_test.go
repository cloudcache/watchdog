package flowlifecycle

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validPolicy() Policy {
	return Policy{
		ID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Version: 7, Status: PolicyPublished,
		BootstrapFrom:       time.Date(2026, 1, 1, 8, 0, 0, 0, time.FixedZone("CST", 8*3600)),
		RawRetentionSeconds: 2 * 86400, LateArrivalSeconds: 3 * 86400,
		DeleteGraceSeconds: 3600, MaxPartitionsPerRun: 7,
		RawDeleteEnabled: true, RequireBackupBeforeDelete: true,
	}
}

func TestPolicyHasNoImplicitRetentionAndUsesMaxWindow(t *testing.T) {
	policy := validPolicy()
	normalized, err := NormalizePolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.ArchiveResolutionSeconds != 3600 || normalized.BootstrapFrom.Format(time.RFC3339) != "2026-01-01T00:00:00Z" {
		t.Fatalf("normalized=%+v", normalized)
	}
	day := time.Date(2026, 2, 4, 19, 0, 0, 0, time.FixedZone("CST", 8*3600))
	eligible, err := ArchiveEligibleAt(day, normalized)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 2, 8, 0, 0, 0, 0, time.UTC); !eligible.Equal(want) {
		t.Fatalf("eligible=%s want=%s", eligible, want)
	}
	missing := normalized
	missing.RawRetentionSeconds = 0
	if _, err := NormalizePolicy(missing); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("zero retention error=%v", err)
	}
}

func TestGenerationRoundTripAndBounds(t *testing.T) {
	generation, err := Generation(19, 4)
	if err != nil {
		t.Fatal(err)
	}
	version, attempt, err := SplitGeneration(generation)
	if err != nil || version != 19 || attempt != 4 {
		t.Fatalf("split=(%d,%d,%v)", version, attempt, err)
	}
	for _, input := range []struct {
		version uint64
		attempt uint32
	}{{0, 1}, {1, 0}, {uint64(^uint32(0)) + 1, 1}} {
		if _, err := Generation(input.version, input.attempt); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("Generation(%d,%d) error=%v", input.version, input.attempt, err)
		}
	}
}

func TestArchiveDeleteRequiresExplicitRetentionAndFullUTCMonth(t *testing.T) {
	policy := validPolicy()
	policy.ArchiveRetentionSeconds = 30 * 86400
	policy.ArchiveDeleteEnabled = true
	policy, err := NormalizePolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	month := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	eligible, err := ArchiveDeleteEligibleAt(month, policy)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 3, 31, 1, 0, 0, 0, time.UTC)
	if !eligible.Equal(want) {
		t.Fatalf("eligible=%s want=%s", eligible, want)
	}
	withoutRetention := policy
	withoutRetention.ArchiveRetentionSeconds = 0
	if _, err := NormalizePolicy(withoutRetention); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("archive deletion without retention error=%v", err)
	}
	if _, err := ArchiveDeleteEligibleAt(month.Add(24*time.Hour), policy); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("partial month error=%v", err)
	}
}

func TestWatermarkRequiresContiguousCommittedCoverage(t *testing.T) {
	valid := Watermark{SourceStreamID: "stream-a", KafkaTopic: "raw", ConsumerGroup: "worker", BootstrapOffset: 10, ReconciledNextOffset: 20, CommittedNextOffset: 21}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.ReconciledNextOffset = 22
	if !errors.Is(invalid.Validate(), ErrInvalidWatermark) {
		t.Fatal("watermark beyond committed snapshot was accepted")
	}
}

func TestRawDeleteGuardRequiresEveryProof(t *testing.T) {
	policy := validPolicy()
	day := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	generation, _ := Generation(policy.Version, 1)
	earliest, _ := RawDeleteEligibleAt(day, policy)
	counters := Counters{RecordCount: 10, RawBytes: 200, RawPackets: 20, EstimatedBytes: 2000, EstimatedPackets: 200}
	guard := RawDayDeleteGuard{
		Now: earliest.Add(time.Second), SourceDate: day, Policy: policy,
		State: PartitionDeleteEligible, Generation: generation, ReconciledAt: earliest.Add(-time.Hour), DeleteEligibleAt: earliest,
		Source: counters, Archive: counters,
		Coverage: []OffsetCoverage{{SourceStreamID: "stream-a", KafkaTopic: "raw", ConsumerGroup: "worker", BootstrapOffset: 10, FirstOffset: 50, LastOffsetExclusive: 61, CommittedNextOffset: 62, ReconciledNextOffset: 61, CommittedSnapshotAt: earliest.Add(-2 * time.Hour), VerifiedAt: earliest.Add(-time.Hour)}},
		Backup: &BackupEvidence{
			StorageKind: "raw", CoveredFrom: day, CoveredThrough: day.Add(24 * time.Hour), Status: "verified",
			BackupRef: "s3://watchdog/flow/day", ChecksumSHA256: strings.Repeat("a", 64),
			VerifiedAt: day.Add(48 * time.Hour), RestoreTestedAt: day.Add(48 * time.Hour), RestoreTestRef: "restore-run/a",
		},
	}
	if err := ValidateRawDayDelete(guard); err != nil {
		t.Fatalf("valid guard: %v", err)
	}

	tests := map[string]func(*RawDayDeleteGuard){
		"feature disabled": func(value *RawDayDeleteGuard) { value.Policy.RawDeleteEnabled = false },
		"too early":        func(value *RawDayDeleteGuard) { value.Now = earliest.Add(-time.Second) },
		"counter drift":    func(value *RawDayDeleteGuard) { value.Archive.RawBytes++ },
		"coverage gap":     func(value *RawDayDeleteGuard) { value.Coverage[0].ReconciledNextOffset = 60 },
		"missing backup":   func(value *RawDayDeleteGuard) { value.Backup = nil },
		"wrong state":      func(value *RawDayDeleteGuard) { value.State = PartitionReconciled },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := guard
			candidate.Coverage = append([]OffsetCoverage(nil), guard.Coverage...)
			mutate(&candidate)
			if !errors.Is(ValidateRawDayDelete(candidate), ErrDeleteLocked) {
				t.Fatalf("unsafe deletion accepted: %+v", candidate)
			}
		})
	}
}
