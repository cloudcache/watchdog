package flowcollect

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

func TestStateCleanupDecoderLifecycleIsFencedPersistableAndIdempotent(t *testing.T) {
	base := time.Unix(200_000, 0).UTC()
	old := stateCleanupCollectFixture(t, "collector-a", 3, 10, 7, base)
	job, err := NewCollectStateCleanupJob("cleanup-1", "approval-1", "user-1", old, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Tombstone(); err == nil {
		t.Fatal("tombstone was available before the ownership fence")
	}
	fence := stateCleanupFence(old, "collector-b", 4, 11, base)
	if err := job.ConfirmFence(fence, base.Add(4*time.Second)); !errors.Is(err, ErrStateCleanupFenceNotMature) {
		t.Fatalf("immature ownership fence error=%v", err)
	}
	if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err != nil {
		t.Fatalf("identical fence retry was not idempotent: %v", err)
	}
	replacement := cloneCollectStateForCleanup(t, old, "collector-b", 4, 11, 8, base.Add(6*time.Second))
	observation := FrozenReplacementObservation{
		CapturedAt: base.Add(7 * time.Second), Position: KafkaRecordPosition{Partition: 1, Offset: 20}, HighWatermark: 21,
		RestoredOldOwnershipEpoch: 3, RestoredOldGeneration: 7, NewEpochBaselineGeneration: 7,
	}
	if err := job.ObserveCollectStateReplacement(replacement, observation); err != nil {
		t.Fatal(err)
	}
	if err := job.ObserveCollectStateReplacement(replacement, observation); err != nil {
		t.Fatalf("identical replacement retry was not idempotent: %v", err)
	}
	tombstone, err := job.Tombstone()
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.Kind() != StateCheckpointDecoder || !bytes.Equal(tombstone.Key(), old.StateKey) {
		t.Fatalf("wrong decoder tombstone: kind=%q key=%x", tombstone.Kind(), tombstone.Key())
	}
	returnedKey := tombstone.Key()
	returnedKey[0] ^= 0xff
	if bytes.Equal(returnedKey, tombstone.Key()) {
		t.Fatal("tombstone key getter exposed mutable job state")
	}
	message, err := stateTombstoneProducerMessage("state", tombstone, base.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	messageKey, err := message.Key.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if message.Topic != "state" || message.Partition != -1 || message.Value != nil || !bytes.Equal(messageKey, old.StateKey) {
		t.Fatalf("decoder tombstone message is invalid: %+v key=%x", message, messageKey)
	}
	receipt := StateTombstoneReceipt{Position: KafkaRecordPosition{Partition: 1, Offset: 30}, PublishedAt: base.Add(8 * time.Second)}
	if err := job.MarkTombstonePublished(receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := job.Tombstone(); err == nil {
		t.Fatal("tombstone remained available after its Kafka receipt was persisted")
	}
	if err := job.MarkTombstonePublished(receipt); err != nil {
		t.Fatalf("identical tombstone receipt retry was not idempotent: %v", err)
	}
	tooEarly := FrozenTombstoneVerification{CapturedAt: base.Add(9 * time.Second), Partition: 1, HighWatermark: 30, KeyAbsent: true}
	if err := job.VerifyTombstone(tooEarly); err == nil {
		t.Fatal("verification boundary that excludes the tombstone was accepted")
	}
	verification := FrozenTombstoneVerification{CapturedAt: base.Add(10 * time.Second), Partition: 1, HighWatermark: 31, KeyAbsent: true}
	if err := job.VerifyTombstone(verification); err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(job.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var persisted StateCleanupSnapshot
	if err := json.Unmarshal(payload, &persisted); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreStateCleanupJob(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Snapshot().Phase != StateCleanupComplete {
		t.Fatalf("completed cleanup phase was not restored: %+v", restored.Snapshot())
	}
	if err := restored.VerifyTombstone(verification); err != nil {
		t.Fatalf("identical verification retry was not idempotent: %v", err)
	}
}

func TestStateCleanupQualityUsesTypedOldEpochKey(t *testing.T) {
	base := time.Unix(300_000, 0).UTC()
	old := qualityCheckpointFixture(t, "collector-a", 5, 9, 20, base)
	job, err := NewQualityStateCleanupJob("cleanup-2", "approval-2", "user-2", old, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	job = roundTripStateCleanupJob(t, job, StateCleanupAwaitingFence)
	fence := stateCleanupFenceForQuality(old, "collector-b", 6, 21, base)
	if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	job = roundTripStateCleanupJob(t, job, StateCleanupAwaitingReplacement)
	replacement := qualityCheckpointFixture(t, "collector-b", 6, 1, 21, base.Add(6*time.Second))
	observation := FrozenReplacementObservation{
		CapturedAt: base.Add(7 * time.Second), Position: KafkaRecordPosition{Partition: 2, Offset: 40}, HighWatermark: 41,
		RestoredOldOwnershipEpoch: 5, RestoredOldGeneration: 9, NewEpochBaselineGeneration: 0,
	}
	if err := job.ObserveQualityStateReplacement(replacement, observation); err != nil {
		t.Fatal(err)
	}
	job = roundTripStateCleanupJob(t, job, StateCleanupReadyToTombstone)
	tombstone, err := job.Tombstone()
	if err != nil {
		t.Fatal(err)
	}
	wantKey, err := qualityCheckpointKafkaKey(old)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.Kind() != StateCheckpointQuality || !bytes.Equal(tombstone.Key(), wantKey) || !isQualityCheckpointKafkaKey(tombstone.Key()) {
		t.Fatalf("wrong quality tombstone: kind=%q key=%x", tombstone.Kind(), tombstone.Key())
	}
	message, err := stateTombstoneProducerMessage("state", tombstone, base.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if message.Value != nil {
		t.Fatal("quality tombstone encoded an empty value instead of Kafka null")
	}
	receipt := StateTombstoneReceipt{Position: KafkaRecordPosition{Partition: 2, Offset: 50}, PublishedAt: base.Add(8 * time.Second)}
	if err := job.MarkTombstonePublished(receipt); err != nil {
		t.Fatal(err)
	}
	job = roundTripStateCleanupJob(t, job, StateCleanupAwaitingVerification)
	if err := job.VerifyTombstone(FrozenTombstoneVerification{CapturedAt: base.Add(9 * time.Second), Partition: 2, HighWatermark: 51, KeyAbsent: false}); err == nil {
		t.Fatal("live old quality key was accepted as verified tombstone")
	}
	if err := job.VerifyTombstone(FrozenTombstoneVerification{CapturedAt: base.Add(10 * time.Second), Partition: 2, HighWatermark: 51, KeyAbsent: true}); err != nil {
		t.Fatal(err)
	}
	roundTripStateCleanupJob(t, job, StateCleanupComplete)
}

func TestStateCleanupRejectsUnsafeOwnershipAndReplacementEvidence(t *testing.T) {
	base := time.Unix(400_000, 0).UTC()
	old := qualityCheckpointFixture(t, "collector-a", 5, 9, 20, base)

	t.Run("shared old principal", func(t *testing.T) {
		job, err := NewQualityStateCleanupJob("cleanup-3", "approval-3", "user-3", old, base.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		fence := stateCleanupFenceForQuality(old, "collector-b", 6, 21, base)
		fence.UniqueOldPrincipal = false
		if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err == nil {
			t.Fatal("shared old principal was accepted as fenced")
		}
	})

	t.Run("epoch reuse", func(t *testing.T) {
		job, err := NewQualityStateCleanupJob("cleanup-4", "approval-4", "user-4", old, base.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		fence := stateCleanupFenceForQuality(old, "collector-b", 5, 21, base)
		if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err == nil {
			t.Fatal("ownership epoch reuse was accepted")
		}
	})

	t.Run("replacement did not advance", func(t *testing.T) {
		job, err := NewQualityStateCleanupJob("cleanup-5", "approval-5", "user-5", old, base.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		fence := stateCleanupFenceForQuality(old, "collector-b", 6, 21, base)
		if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err != nil {
			t.Fatal(err)
		}
		replacement := qualityCheckpointFixture(t, "collector-b", 6, 1, 21, base.Add(6*time.Second))
		observation := FrozenReplacementObservation{
			CapturedAt: base.Add(7 * time.Second), Position: KafkaRecordPosition{Partition: 2, Offset: 40}, HighWatermark: 41,
			RestoredOldOwnershipEpoch: 5, RestoredOldGeneration: 9, NewEpochBaselineGeneration: 1,
		}
		if err := job.ObserveQualityStateReplacement(replacement, observation); err == nil {
			t.Fatal("replacement without a new-epoch generation advance was accepted")
		}
	})

	t.Run("wrong restore proof", func(t *testing.T) {
		job, err := NewQualityStateCleanupJob("cleanup-6", "approval-6", "user-6", old, base.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		fence := stateCleanupFenceForQuality(old, "collector-b", 6, 21, base)
		if err := job.ConfirmFence(fence, base.Add(6*time.Second)); err != nil {
			t.Fatal(err)
		}
		replacement := qualityCheckpointFixture(t, "collector-b", 6, 1, 21, base.Add(6*time.Second))
		observation := FrozenReplacementObservation{
			CapturedAt: base.Add(7 * time.Second), Position: KafkaRecordPosition{Partition: 2, Offset: 40}, HighWatermark: 41,
			RestoredOldOwnershipEpoch: 4, RestoredOldGeneration: 9,
		}
		if err := job.ObserveQualityStateReplacement(replacement, observation); err == nil {
			t.Fatal("replacement without proof of restoring the exact old epoch was accepted")
		}
	})
}

func TestStateCleanupSnapshotIsDefensiveAndRejectsPhaseForgery(t *testing.T) {
	base := time.Unix(500_000, 0).UTC()
	old := qualityCheckpointFixture(t, "collector-a", 5, 9, 20, base)
	job, err := NewQualityStateCleanupJob("cleanup-7", "approval-7", "user-7", old, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := job.Snapshot()
	snapshot.Old.KafkaKey[0] ^= 0xff
	again := job.Snapshot()
	if bytes.Equal(snapshot.Old.KafkaKey, again.Old.KafkaKey) {
		t.Fatal("snapshot exposed mutable cleanup job state")
	}
	corruptKey := cloneStateCleanupSnapshot(again)
	corruptKey.Old.KafkaKey[0] ^= 0xff
	if _, err := RestoreStateCleanupJob(corruptKey); err == nil {
		t.Fatal("cleanup snapshot with a forged Kafka key was accepted")
	}
	forged := again
	forged.Phase = StateCleanupComplete
	if _, err := RestoreStateCleanupJob(forged); err == nil {
		t.Fatal("completed cleanup snapshot without evidence was accepted")
	}
}

func stateCleanupCollectFixture(t *testing.T, collectorID string, epoch, revision, generation uint64, at time.Time) *flowpb.CollectState {
	t.Helper()
	decoder, record, decoded := collectStateFixture(t)
	defer decoder.Close()
	record.RegistryVersion = revision
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: epoch}, collectorID, decoder)
	if err != nil {
		t.Fatal(err)
	}
	if state.StateGeneration != generation {
		state = cloneCollectStateForCleanup(t, state, collectorID, epoch, revision, generation, at)
	} else {
		state.ReceivedAtUnixMs = at.UnixMilli()
		digest, err := collectStateDigest(state)
		if err != nil {
			t.Fatal(err)
		}
		state.PayloadSha256 = digest[:]
	}
	return state
}

func cloneCollectStateForCleanup(t *testing.T, state *flowpb.CollectState, collectorID string, epoch, revision, generation uint64, at time.Time) *flowpb.CollectState {
	t.Helper()
	clone := proto.Clone(state).(*flowpb.CollectState)
	clone.CollectorId = collectorID
	clone.RegistryVersion = revision
	clone.OwnershipEpoch = epoch
	clone.StateGeneration = generation
	clone.ReceivedAtUnixMs = at.UnixMilli()
	var identity [32]byte
	copy(identity[:], clone.StateIdentityKey)
	key := makeCollectStateKey(identity, epoch)
	clone.StateKey = key[:]
	var datagramID DatagramID
	copy(datagramID[:], clone.DatagramId)
	id := makeCollectStateID(datagramID, key)
	clone.StateId = id[:]
	digest, err := collectStateDigest(clone)
	if err != nil {
		t.Fatal(err)
	}
	clone.PayloadSha256 = digest[:]
	if err := validateCollectState(clone, ""); err != nil {
		t.Fatal(err)
	}
	return clone
}

func stateCleanupFence(old *flowpb.CollectState, newCollector string, newEpoch, newRevision uint64, base time.Time) OwnershipFenceEvidence {
	return OwnershipFenceEvidence{
		OldCollectorID: old.CollectorId, NewCollectorID: newCollector,
		OldPlanRevision: old.RegistryVersion, NewPlanRevision: newRevision,
		OldOwnershipEpoch: old.OwnershipEpoch, NewOwnershipEpoch: newEpoch,
		OldPlanRevokedAt: base.Add(time.Second), OldPlanExpiresAt: base.Add(4 * time.Second),
		OldOwnerDrainedAt: base.Add(2 * time.Second), OldPrincipalWriteRevokedAt: base.Add(2 * time.Second),
		NewPlanActivatedAt: base.Add(2 * time.Second), MaxClockSkew: time.Second, ACLPropagationDelay: 2 * time.Second,
		UniqueOldPrincipal: true,
	}
}

func stateCleanupFenceForQuality(old *flowpb.QualityCheckpoint, newCollector string, newEpoch, newRevision uint64, base time.Time) OwnershipFenceEvidence {
	return OwnershipFenceEvidence{
		OldCollectorID: old.CollectorId, NewCollectorID: newCollector,
		OldPlanRevision: old.RegistryVersion, NewPlanRevision: newRevision,
		OldOwnershipEpoch: old.OwnershipEpoch, NewOwnershipEpoch: newEpoch,
		OldPlanRevokedAt: base.Add(time.Second), OldPlanExpiresAt: base.Add(4 * time.Second),
		OldOwnerDrainedAt: base.Add(2 * time.Second), OldPrincipalWriteRevokedAt: base.Add(2 * time.Second),
		NewPlanActivatedAt: base.Add(2 * time.Second), MaxClockSkew: time.Second, ACLPropagationDelay: 2 * time.Second,
		UniqueOldPrincipal: true,
	}
}

func roundTripStateCleanupJob(t *testing.T, job *StateCleanupJob, want StateCleanupPhase) *StateCleanupJob {
	t.Helper()
	payload, err := json.Marshal(job.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var snapshot StateCleanupSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreStateCleanupJob(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot().Phase; got != want {
		t.Fatalf("restored cleanup phase=%q, want %q", got, want)
	}
	return restored
}
