package flowlifecycle

import (
	"errors"
	"testing"
	"time"
)

func TestValidateRawDayApprovalRequest(t *testing.T) {
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	now := day.Add(7 * 24 * time.Hour)
	counters := Counters{RecordCount: 2, RawBytes: 20, RawPackets: 2, EstimatedBytes: 200, EstimatedPackets: 20, EstimatedValidRecords: 2}
	readiness := RawDeleteReadiness{
		SourceDate: day, CheckedAt: now.Add(-time.Minute), PolicyID: "policy-a", PolicyVersion: 4,
		PartitionState: PartitionReconciled, PartitionVersion: 7, Generation: uint64(4)<<32 | 1,
		DeleteEligibleAt: now.Add(-time.Hour), EvidenceReady: true, Source: counters, Archive: counters,
		Coverage: []OffsetCoverage{{
			SourceStreamID: "stream-a", KafkaTopic: "flow.raw", ConsumerGroup: "worker", KafkaPartition: 2,
			BootstrapOffset: 1, FirstOffset: 5, LastOffsetExclusive: 9, CommittedNextOffset: 10, ReconciledNextOffset: 10,
			CommittedSnapshotAt: now.Add(-2 * time.Hour), VerifiedAt: now.Add(-time.Hour),
		}},
	}
	if got, err := validateRawDayApprovalRequest(readiness, 7, "admin", now); err != nil || !got.Equal(day) {
		t.Fatalf("valid approval rejected: day=%v err=%v", got, err)
	}
	tests := map[string]func(*RawDeleteReadiness){
		"evidence": func(value *RawDeleteReadiness) { value.EvidenceReady = false },
		"state":    func(value *RawDeleteReadiness) { value.PartitionState = PartitionDeleteEligible },
		"version":  func(value *RawDeleteReadiness) { value.PartitionVersion++ },
		"future":   func(value *RawDeleteReadiness) { value.CheckedAt = now.Add(time.Second) },
		"counters": func(value *RawDeleteReadiness) { value.Archive.RawBytes++ },
		"coverage": func(value *RawDeleteReadiness) { value.Coverage = nil },
		"duplicate": func(value *RawDeleteReadiness) {
			value.Coverage = append(value.Coverage, value.Coverage[0])
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := readiness
			candidate.Coverage = append([]OffsetCoverage(nil), readiness.Coverage...)
			mutate(&candidate)
			if _, err := validateRawDayApprovalRequest(candidate, 7, "admin", now); !errors.Is(err, ErrInvalidDeletionApproval) {
				t.Fatalf("invalid approval accepted: %+v err=%v", candidate, err)
			}
		})
	}
}
