package flowlifecycle

import (
	"errors"
	"testing"
	"time"
)

func TestNormalizeBackupEvidence(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	valid := BackupEvidence{
		StorageKind: "raw", CoveredFrom: now.Add(-48 * time.Hour), CoveredThrough: now.Add(-24 * time.Hour),
		BackupRef: "s3://watchdog/flow/2026-09-14", ChecksumSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		RestoreTestedAt: now.Add(-time.Hour), RestoreTestRef: "restore-run/2026-09-16T1100Z",
	}
	normalized, err := NormalizeBackupEvidence(valid, now)
	if err != nil || normalized.ID == "" || normalized.CoveredFrom.Hour() != 0 {
		t.Fatalf("normalize valid evidence: value=%+v err=%v", normalized, err)
	}
	for name, mutate := range map[string]func(*BackupEvidence){
		"kind":       func(value *BackupEvidence) { value.StorageKind = "other" },
		"range":      func(value *BackupEvidence) { value.CoveredThrough = value.CoveredFrom },
		"backup ref": func(value *BackupEvidence) { value.BackupRef = "" },
		"checksum":   func(value *BackupEvidence) { value.ChecksumSHA256 = "not-sha256" },
		"future":     func(value *BackupEvidence) { value.RestoreTestedAt = now.Add(time.Second) },
		"restore ref": func(value *BackupEvidence) {
			value.RestoreTestRef = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := NormalizeBackupEvidence(candidate, now); !errors.Is(err, ErrInvalidBackupEvidence) {
				t.Fatalf("invalid evidence accepted: %+v err=%v", candidate, err)
			}
		})
	}
}
