package flowcollect

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
)

func TestQualityCommitAccumulatorCoalescesInWALOrderAndMergesSources(t *testing.T) {
	now := time.Unix(70_000, 0)
	first := qualityCommitJournalFixture(t, "collector-a", 3, 10, 1, walHeaderSize, 9, 10, now)
	second := qualityCommitJournalFixture(t, "collector-a", 3, 10, 2, walHeaderSize+100, 10, 11, now.Add(time.Second))
	accumulator, err := newQualityCommitAccumulator(nil, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := accumulator.Coalesce([]*flowpb.QualityJournalRecord{second, first}, "collector-a", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 || checkpoints[0].StateGeneration != 1 || len(checkpoints[0].SourceStates) != 2 || checkpoints[0].ExporterState.ExpectedSequence != 12 {
		t.Fatalf("quality journals were not coalesced in WAL order: %+v", checkpoints)
	}
	if checkpoints[0].SourceStates[0].SourceIdValue != 9 || checkpoints[0].SourceStates[1].SourceIdValue != 10 {
		t.Fatalf("quality source state order is unstable: %+v", checkpoints[0].SourceStates)
	}
	if checkpoints[0].LastWalSegment != second.WalSegment || checkpoints[0].LastWalOffset != second.WalOffset {
		t.Fatalf("quality checkpoint WAL watermark is wrong: %+v", checkpoints[0])
	}
	if replayed, err := accumulator.Coalesce([]*flowpb.QualityJournalRecord{first, second}, "collector-a", now.Add(3*time.Second)); err != nil || len(replayed) != 0 {
		t.Fatalf("already folded quality journals were not idempotent: checkpoints=%+v err=%v", replayed, err)
	}

	third := qualityCommitJournalFixture(t, "collector-a", 3, 11, 3, walHeaderSize+200, 9, 12, now.Add(4*time.Second))
	checkpoints, err = accumulator.Coalesce([]*flowpb.QualityJournalRecord{third}, "collector-a", now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 || checkpoints[0].StateGeneration != 2 || checkpoints[0].RegistryVersion != 11 || len(checkpoints[0].SourceStates) != 2 || checkpoints[0].ExporterState.ExpectedSequence != 13 {
		t.Fatalf("quality checkpoint generation/source merge did not advance: %+v", checkpoints)
	}
}

func TestQualityCommitAccumulatorResetsGenerationAcrossOwnershipEpoch(t *testing.T) {
	now := time.Unix(80_000, 0)
	oldJournal := qualityCommitJournalFixture(t, "collector-a", 3, 10, 1, walHeaderSize, 9, 10, now)
	accumulator, err := newQualityCommitAccumulator(nil, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	old, err := accumulator.Coalesce([]*flowpb.QualityJournalRecord{oldJournal}, "collector-a", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	old[0].StateGeneration = 99
	if err := setQualityCheckpointChecksum(old[0]); err != nil {
		t.Fatal(err)
	}
	accumulator, err = newQualityCommitAccumulator(old, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	newJournal := qualityCommitJournalFixture(t, "collector-b", 4, 11, 2, walHeaderSize+100, 10, 11, now.Add(2*time.Second))
	current, err := accumulator.Coalesce([]*flowpb.QualityJournalRecord{newJournal}, "collector-b", now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 || current[0].OwnershipEpoch != 4 || current[0].StateGeneration != 1 || current[0].CollectorId != "collector-b" || len(current[0].SourceStates) != 2 {
		t.Fatalf("new quality owner did not reset generation and preserve its baseline: %+v", current)
	}
}

func TestQualityCommitAccumulatorRejectsRegressionsAndCapacityOverflow(t *testing.T) {
	now := time.Unix(90_000, 0)
	current := qualityCheckpointFixture(t, "collector-a", 4, 5, 11, now)
	accumulator, err := newQualityCommitAccumulator([]*flowpb.QualityCheckpoint{current}, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	staleEpoch := qualityCommitJournalFixture(t, "collector-a", 3, 12, 2, walHeaderSize+100, 9, 11, now.Add(time.Second))
	if _, err := accumulator.Coalesce([]*flowpb.QualityJournalRecord{staleEpoch}, "collector-a", now.Add(2*time.Second)); err == nil {
		t.Fatal("quality ownership epoch regression was accepted")
	}

	accumulator, err = newQualityCommitAccumulator(nil, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	first := qualityCommitJournalFixture(t, "collector-a", 3, 10, 1, walHeaderSize, 9, 10, now)
	second := qualityCommitJournalFixture(t, "collector-a", 3, 10, 2, walHeaderSize+100, 10, 11, now.Add(time.Second))
	if _, err := accumulator.Coalesce([]*flowpb.QualityJournalRecord{first, second}, "collector-a", now.Add(2*time.Second)); err == nil {
		t.Fatal("quality source capacity overflow was accepted")
	}
	if checkpoints := accumulator.Snapshot(); len(checkpoints) != 0 {
		t.Fatalf("failed quality coalesce mutated committed state: %+v", checkpoints)
	}
}

func TestQualityJournalV1IsLocallyValidButCannotBecomeRemoteState(t *testing.T) {
	now := time.Unix(100_000, 0)
	journal := qualityCommitJournalFixture(t, "collector-a", 3, 10, 1, walHeaderSize, 9, 10, now)
	journal.StateSchemaVersion = qualityJournalSchemaV1
	journal.TenantId = ""
	journal.ExporterId = ""
	journal.RegistryVersion = 0
	journal.OwnershipEpoch = 0
	journal.ObservationDomainId = 0
	digest, err := qualityJournalDigest(journal)
	if err != nil {
		t.Fatal(err)
	}
	journal.PayloadSha256 = digest[:]
	if err := validateQualityJournalRecord(journal); err != nil {
		t.Fatalf("legacy local quality journal was rejected: %v", err)
	}
	if _, err := qualityCheckpointFromJournal(journal, 1, now); err == nil {
		t.Fatal("legacy quality journal was promoted without owner metadata")
	}
}

func TestQualitySnapshotV1RemainsLocallyReadable(t *testing.T) {
	config := testQualityStateConfig()
	tracker := NewQualityTracker(config, nil)
	snapshot, err := tracker.BuildSnapshot("collector-a", time.Unix(110_000, 0), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.StateSchemaVersion = qualitySnapshotSchemaV1
	digest, err := qualitySnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.PayloadSha256 = digest[:]
	if err := validateQualitySnapshot(snapshot, "collector-a", config); err != nil {
		t.Fatalf("legacy local quality snapshot was rejected: %v", err)
	}
}

func qualityCommitJournalFixture(t *testing.T, collectorID string, ownershipEpoch, registryVersion uint64, marker byte, offset int64, sourceID, sequence uint32, receivedAt time.Time) *flowpb.QualityJournalRecord {
	t.Helper()
	record := qualityWALRecord(marker, receivedAt)
	record.RegistryVersion = registryVersion
	record.TenantID = "tenant-a"
	record.ExporterID = "exporter-a"
	record.TargetID = "target-a"
	record.Segment = 1
	record.Offset = offset
	binding := SourceBinding{Protocol: ProtocolSFlow5, SourcePrefix: "192.0.2.1/32", TenantID: record.TenantID, ExporterID: record.ExporterID, TargetID: record.TargetID, OwnershipEpoch: ownershipEpoch, SamplingMode: SamplingModeSampled, Enabled: true}
	decoded := sflowQualityDatagram(sequence, sequence*100, sequence, sequence*100, 100, 0)
	decoded.Records[0].SourceIDValue = sourceID
	tracker := NewQualityTracker(testQualityStateConfig(), nil)
	decoded, _ = tracker.Observe(record, decoded)
	journal, err := tracker.BuildJournalRecord(record, decoded, binding, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	return journal
}
