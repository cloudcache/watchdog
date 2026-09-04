package flowcollect

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

func TestQualityCheckpointIdentitySurvivesOwnerChange(t *testing.T) {
	now := time.Unix(10_000, 0)
	first := qualityCheckpointFixture(t, "collector-a", 3, 7, 10, now)
	second := qualityCheckpointFixture(t, "collector-b", 3, 7, 11, now.Add(time.Second))
	if !bytes.Equal(first.StateIdentityKey, second.StateIdentityKey) || !bytes.Equal(first.StateKey, second.StateKey) {
		t.Fatal("quality checkpoint key changed with collector ownership")
	}
	third := qualityCheckpointFixture(t, "collector-b", 4, 1, 11, now.Add(2*time.Second))
	if !bytes.Equal(first.StateIdentityKey, third.StateIdentityKey) || bytes.Equal(first.StateKey, third.StateKey) {
		t.Fatal("quality checkpoint epoch did not fence the compacted key")
	}
	kafkaKey, err := qualityCheckpointKafkaKey(third)
	if err != nil || !isQualityCheckpointKafkaKey(kafkaKey) || len(kafkaKey) != 33 || bytes.Equal(kafkaKey, third.StateKey) {
		t.Fatalf("quality checkpoint Kafka key is not safely typed: key=%x err=%v", kafkaKey, err)
	}
}

func TestQualityCheckpointBindsNetFlowObservationDomain(t *testing.T) {
	now := time.Unix(15_000, 0)
	record := qualityWALRecord(2, now)
	record.Protocol = ProtocolNetFlow9
	record.Source = netip.MustParseAddrPort("192.0.2.1:2055")
	record.ObservationDomainID = 42
	record.RegistryVersion = 10
	record.TenantID = "tenant-a"
	record.ExporterID = "exporter-a"
	record.Segment = 1
	record.Offset = walHeaderSize
	decoded := netflowQualityDatagram(10, 1, 1000, 100)
	decoded.Protocol = ProtocolNetFlow9
	decoded.SequenceScope = 42
	tracker := NewQualityTracker(testQualityStateConfig(), nil)
	decoded, _ = tracker.Observe(record, decoded)
	journal, err := tracker.BuildJournalRecord(record, decoded, "collector-a")
	if err != nil {
		t.Fatal(err)
	}
	binding := SourceBinding{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", TenantID: record.TenantID, ExporterID: record.ExporterID, TargetID: "target-a", OwnershipEpoch: 3, SamplingMode: SamplingModeSampled, Enabled: true}
	checkpoint, err := BuildQualityCheckpoint(record, binding, "collector-a", 1, now, journal)
	if err != nil {
		t.Fatal(err)
	}
	tampered := proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	tampered.ObservationDomainId++
	setQualityCheckpointDigest(t, tampered)
	if err := validateQualityCheckpoint(tampered, 100); err == nil {
		t.Fatal("quality checkpoint accepted a mismatched NetFlow observation domain")
	}
}

func TestQualityCheckpointRecoverySelectsNewOwnershipEpoch(t *testing.T) {
	now := time.Unix(20_000, 0)
	oldOwner := qualityCheckpointFixture(t, "collector-a", 3, 99, 10, now.Add(-time.Minute))
	newOwner := qualityCheckpointFixture(t, "collector-b", 4, 1, 11, now.Add(-time.Second))
	registry := qualityCheckpointRegistry(t, "collector-b", 4, 11, now)
	config := testQualityStateConfig()

	eligible, err := eligibleRemoteQualityCheckpoints(registry, []*flowpb.QualityCheckpoint{oldOwner, newOwner}, config, now)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := selectQualityCheckpoints(eligible)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0] != newOwner {
		t.Fatalf("new ownership epoch did not win: %+v", selected)
	}
}

func TestQualityCheckpointRecoveryRejectsEpochReuse(t *testing.T) {
	now := time.Unix(30_000, 0)
	oldOwner := qualityCheckpointFixture(t, "collector-a", 4, 7, 10, now.Add(-time.Second))
	registry := qualityCheckpointRegistry(t, "collector-b", 4, 11, now)
	if _, err := eligibleRemoteQualityCheckpoints(registry, []*flowpb.QualityCheckpoint{oldOwner}, testQualityStateConfig(), now); err == nil {
		t.Fatal("quality ownership epoch reuse across collectors was accepted")
	}
}

func TestQualityCheckpointRecoveryRejectsFuturePlanAndFiltersStaleState(t *testing.T) {
	now := time.Unix(40_000, 0)
	registry := qualityCheckpointRegistry(t, "collector-b", 4, 11, now)
	futurePlan := qualityCheckpointFixture(t, "collector-a", 3, 1, 12, now.Add(-time.Second))
	if _, err := eligibleRemoteQualityCheckpoints(registry, []*flowpb.QualityCheckpoint{futurePlan}, testQualityStateConfig(), now); err == nil {
		t.Fatal("quality checkpoint from a future plan revision was accepted")
	}
	stale := qualityCheckpointFixture(t, "collector-a", 3, 1, 10, now.Add(-2*time.Hour))
	eligible, err := eligibleRemoteQualityCheckpoints(registry, []*flowpb.QualityCheckpoint{stale}, testQualityStateConfig(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != 0 {
		t.Fatal("stale quality checkpoint was restored")
	}
}

func TestQualityCheckpointSelectionRejectsEqualOrderConflict(t *testing.T) {
	now := time.Unix(50_000, 0)
	checkpoint := qualityCheckpointFixture(t, "collector-a", 3, 7, 10, now)
	conflict := proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	conflict.ExporterState.ExpectedSequence++
	setQualityCheckpointDigest(t, conflict)
	if _, err := selectQualityCheckpoints([]*flowpb.QualityCheckpoint{checkpoint, conflict}); err == nil {
		t.Fatal("equal-order quality checkpoint conflict was accepted")
	}
}

func TestQualityCheckpointValidationRejectsTamperingAndDuplicateSources(t *testing.T) {
	now := time.Unix(60_000, 0)
	checkpoint := qualityCheckpointFixture(t, "collector-a", 3, 7, 10, now)
	tampered := proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	tampered.ExporterState.ExpectedSequence++
	if err := validateQualityCheckpoint(tampered, 100); err == nil {
		t.Fatal("quality checkpoint checksum tampering was accepted")
	}
	duplicate := proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	duplicate.SourceStates = append(duplicate.SourceStates, proto.Clone(duplicate.SourceStates[0]).(*flowpb.QualitySourceState))
	setQualityCheckpointDigest(t, duplicate)
	if err := validateQualityCheckpoint(duplicate, 100); err == nil {
		t.Fatal("duplicate quality source state was accepted")
	}
	missing := proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	missing.SourceStates[0] = nil
	setQualityCheckpointDigest(t, missing)
	if err := validateQualityCheckpoint(missing, 100); err == nil {
		t.Fatal("missing quality source state was accepted")
	}
}

func qualityCheckpointFixture(t *testing.T, collectorID string, ownershipEpoch, generation, registryVersion uint64, committedAt time.Time) *flowpb.QualityCheckpoint {
	t.Helper()
	record := qualityWALRecord(1, committedAt)
	record.Segment = 1
	record.Offset = walHeaderSize
	record.RegistryVersion = registryVersion
	record.TenantID = "tenant-a"
	record.ExporterID = "exporter-a"
	record.TargetID = "target-a"
	binding := SourceBinding{Protocol: ProtocolSFlow5, SourcePrefix: "192.0.2.1/32", TenantID: record.TenantID, ExporterID: record.ExporterID, TargetID: record.TargetID, OwnershipEpoch: ownershipEpoch, SamplingMode: SamplingModeSampled, Enabled: true}
	tracker := NewQualityTracker(testQualityStateConfig(), nil)
	decoded, _ := tracker.Observe(record, sflowQualityDatagram(10, 1000, 20, 1000, 100, 0))
	journal, err := tracker.BuildJournalRecord(record, decoded, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := BuildQualityCheckpoint(record, binding, collectorID, generation, committedAt, journal)
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func qualityCheckpointRegistry(t *testing.T, collectorID string, ownershipEpoch, revision uint64, now time.Time) *Registry {
	t.Helper()
	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.CollectorID = collectorID
	plan.Revision = revision
	plan.Sources = []SourceBinding{{Protocol: ProtocolSFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: ownershipEpoch, SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func setQualityCheckpointDigest(t *testing.T, checkpoint *flowpb.QualityCheckpoint) {
	t.Helper()
	digest, err := qualityCheckpointDigest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.PayloadSha256 = digest[:]
}
