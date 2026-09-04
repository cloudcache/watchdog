package flowcollect

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sort"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

// qualityCommitAccumulator coalesces only terminally committed journal
// post-states. It deliberately has no access to QualityTracker so an in-flight
// observation can never leak into a cross-owner checkpoint.
type qualityCommitAccumulator struct {
	maxCheckpoints int
	maxSources     int
	checkpoints    map[[32]byte]*flowpb.QualityCheckpoint
}

func newQualityCommitAccumulator(initial []*flowpb.QualityCheckpoint, maxCheckpoints, maxSources int) (*qualityCommitAccumulator, error) {
	if maxCheckpoints <= 0 || maxSources <= 0 {
		return nil, errors.New("quality commit checkpoint and source capacities must be positive")
	}
	for index, checkpoint := range initial {
		if err := validateQualityCheckpoint(checkpoint, maxSources); err != nil {
			return nil, fmt.Errorf("initial quality checkpoint %d: %w", index, err)
		}
	}
	selected, err := selectQualityCheckpoints(initial)
	if err != nil {
		return nil, err
	}
	if err := validateQualityCheckpointCapacity(selected, maxCheckpoints, maxSources); err != nil {
		return nil, err
	}
	accumulator := &qualityCommitAccumulator{maxCheckpoints: maxCheckpoints, maxSources: maxSources, checkpoints: make(map[[32]byte]*flowpb.QualityCheckpoint, len(selected))}
	for _, checkpoint := range selected {
		var identity [32]byte
		copy(identity[:], checkpoint.StateIdentityKey)
		accumulator.checkpoints[identity] = proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	}
	return accumulator, nil
}

func (a *qualityCommitAccumulator) Coalesce(journals []*flowpb.QualityJournalRecord, collectorID string, committedAt time.Time) ([]*flowpb.QualityCheckpoint, error) {
	if a == nil || a.maxCheckpoints <= 0 || a.maxSources <= 0 || collectorID == "" || committedAt.IsZero() {
		return nil, errors.New("quality commit accumulator, collector, and checkpoint time are required")
	}
	ordered := cloneQualityJournals(journals)
	for index, journal := range ordered {
		if err := validateQualityJournalRecord(journal); err != nil {
			return nil, fmt.Errorf("committed quality journal %d: %w", index, err)
		}
		if journal.StateSchemaVersion != qualityJournalSchemaVersion || journal.CollectorId != collectorID {
			return nil, fmt.Errorf("committed quality journal %d is not owned schema v%d state", index, qualityJournalSchemaVersion)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].WalSegment != ordered[j].WalSegment {
			return ordered[i].WalSegment < ordered[j].WalSegment
		}
		return ordered[i].WalOffset < ordered[j].WalOffset
	})
	working := cloneQualityCheckpointMap(a.checkpoints)
	dirty := make(map[[32]byte]uint64)
	var previousSegment uint64
	var previousOffset int64
	for index, journal := range ordered {
		if index > 0 && journal.WalSegment == previousSegment && journal.WalOffset == previousOffset {
			return nil, errors.New("committed quality journals contain a duplicate WAL position")
		}
		previousSegment, previousOffset = journal.WalSegment, journal.WalOffset

		key, _, err := exporterStateFromProto(journal.ExporterState)
		if err != nil {
			return nil, err
		}
		identity := makeQualityCheckpointIdentityKey(journal.TenantId, journal.ExporterId, key, journal.ObservationDomainId)
		current := working[identity]
		if current != nil {
			if current.OwnershipEpoch > journal.OwnershipEpoch {
				return nil, errors.New("committed quality journal ownership epoch regressed")
			}
			if current.OwnershipEpoch == journal.OwnershipEpoch {
				if current.CollectorId != journal.CollectorId {
					return nil, errors.New("committed quality journal reused an ownership epoch")
				}
				if journal.RegistryVersion < current.RegistryVersion {
					return nil, errors.New("committed quality journal plan revision regressed")
				}
				if compareWALPosition(journal.WalSegment, journal.WalOffset, current.LastWalSegment, current.LastWalOffset) <= 0 {
					continue
				}
			}
		}
		generation, exists := dirty[identity]
		if !exists {
			generation = 1
			if current != nil && current.OwnershipEpoch == journal.OwnershipEpoch {
				if current.StateGeneration == math.MaxUint64 {
					return nil, errors.New("quality checkpoint generation exhausted")
				}
				generation = current.StateGeneration + 1
			}
			dirty[identity] = generation
		}
		checkpoint, err := qualityCheckpointFromJournal(journal, generation, committedAt)
		if err != nil {
			return nil, err
		}
		if current != nil {
			checkpoint.SourceStates, err = mergeQualitySourceStates(current.SourceStates, checkpoint.SourceStates, key, a.maxSources)
			if err != nil {
				return nil, err
			}
			if err := setQualityCheckpointChecksum(checkpoint); err != nil {
				return nil, err
			}
		}
		working[identity] = checkpoint
	}
	if err := validateQualityCheckpointMapCapacity(working, a.maxCheckpoints, a.maxSources); err != nil {
		return nil, err
	}
	identities := make([][32]byte, 0, len(dirty))
	for identity := range dirty {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return bytes.Compare(identities[i][:], identities[j][:]) < 0 })
	checkpoints := make([]*flowpb.QualityCheckpoint, 0, len(identities))
	for _, identity := range identities {
		checkpoint := working[identity]
		if err := validateQualityCheckpoint(checkpoint, a.maxSources); err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, proto.Clone(checkpoint).(*flowpb.QualityCheckpoint))
	}
	a.checkpoints = working
	return checkpoints, nil
}

func (a *qualityCommitAccumulator) PruneBefore(cutoff time.Time) [][32]byte {
	if a == nil || cutoff.IsZero() {
		return nil
	}
	removed := make([][32]byte, 0)
	for identity, checkpoint := range a.checkpoints {
		if time.UnixMilli(checkpoint.CommittedAtUnixMs).Before(cutoff) {
			delete(a.checkpoints, identity)
			removed = append(removed, identity)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return bytes.Compare(removed[i][:], removed[j][:]) < 0 })
	return removed
}

func compareWALPosition(leftSegment uint64, leftOffset int64, rightSegment uint64, rightOffset int64) int {
	if leftSegment < rightSegment {
		return -1
	}
	if leftSegment > rightSegment {
		return 1
	}
	if leftOffset < rightOffset {
		return -1
	}
	if leftOffset > rightOffset {
		return 1
	}
	return 0
}

func (a *qualityCommitAccumulator) Snapshot() []*flowpb.QualityCheckpoint {
	if a == nil {
		return nil
	}
	identities := make([][32]byte, 0, len(a.checkpoints))
	for identity := range a.checkpoints {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return bytes.Compare(identities[i][:], identities[j][:]) < 0 })
	result := make([]*flowpb.QualityCheckpoint, 0, len(identities))
	for _, identity := range identities {
		result = append(result, proto.Clone(a.checkpoints[identity]).(*flowpb.QualityCheckpoint))
	}
	return result
}

func qualityCheckpointFromJournal(journal *flowpb.QualityJournalRecord, generation uint64, committedAt time.Time) (*flowpb.QualityCheckpoint, error) {
	key, _, err := exporterStateFromProto(journal.ExporterState)
	if err != nil {
		return nil, err
	}
	var datagramID DatagramID
	copy(datagramID[:], journal.DatagramId)
	record := WALRecord{
		DatagramID: datagramID,
		Segment:    journal.WalSegment,
		Offset:     journal.WalOffset,
		WALInput: WALInput{
			Protocol:            key.protocol,
			Source:              netip.AddrPortFrom(key.source, 0),
			ObservationDomainID: journal.ObservationDomainId,
			RegistryVersion:     journal.RegistryVersion,
			TenantID:            journal.TenantId,
			ExporterID:          journal.ExporterId,
		},
	}
	binding := SourceBinding{Protocol: key.protocol, TenantID: journal.TenantId, ExporterID: journal.ExporterId, OwnershipEpoch: journal.OwnershipEpoch}
	return BuildQualityCheckpoint(record, binding, journal.CollectorId, generation, committedAt, journal)
}

func mergeQualitySourceStates(current, updates []*flowpb.QualitySourceState, exporter exporterQualityKey, maxSources int) ([]*flowpb.QualitySourceState, error) {
	states := make(map[[2]uint32]*flowpb.QualitySourceState, len(current)+len(updates))
	for _, group := range [][]*flowpb.QualitySourceState{current, updates} {
		for _, encoded := range group {
			key, _, err := sourceStateFromProto(encoded)
			if err != nil {
				return nil, err
			}
			if key.exporter != exporter {
				return nil, errors.New("quality source state belongs to another exporter")
			}
			states[[2]uint32{key.sourceType, key.sourceValue}] = proto.Clone(encoded).(*flowpb.QualitySourceState)
		}
	}
	if len(states) > maxSources {
		return nil, errors.New("quality checkpoint exceeds source state capacity")
	}
	merged := make([]*flowpb.QualitySourceState, 0, len(states))
	for _, state := range states {
		merged = append(merged, state)
	}
	sortQualitySourceStates(merged)
	return merged, nil
}

func setQualityCheckpointChecksum(checkpoint *flowpb.QualityCheckpoint) error {
	digest, err := qualityCheckpointDigest(checkpoint)
	if err != nil {
		return err
	}
	checkpoint.PayloadSha256 = digest[:]
	return nil
}

func cloneQualityJournals(journals []*flowpb.QualityJournalRecord) []*flowpb.QualityJournalRecord {
	cloned := make([]*flowpb.QualityJournalRecord, 0, len(journals))
	for _, journal := range journals {
		if journal == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, proto.Clone(journal).(*flowpb.QualityJournalRecord))
	}
	return cloned
}

func cloneQualityCheckpointMap(checkpoints map[[32]byte]*flowpb.QualityCheckpoint) map[[32]byte]*flowpb.QualityCheckpoint {
	cloned := make(map[[32]byte]*flowpb.QualityCheckpoint, len(checkpoints))
	for identity, checkpoint := range checkpoints {
		cloned[identity] = proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	}
	return cloned
}

func cloneQualityCheckpoints(checkpoints []*flowpb.QualityCheckpoint) []*flowpb.QualityCheckpoint {
	cloned := make([]*flowpb.QualityCheckpoint, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if checkpoint == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, proto.Clone(checkpoint).(*flowpb.QualityCheckpoint))
	}
	return cloned
}

func validateQualityCheckpointCapacity(checkpoints []*flowpb.QualityCheckpoint, maxCheckpoints, maxSources int) error {
	if len(checkpoints) > maxCheckpoints {
		return errors.New("quality checkpoints exceed exporter state capacity")
	}
	totalSources := 0
	for _, checkpoint := range checkpoints {
		totalSources += len(checkpoint.SourceStates)
		if totalSources > maxSources {
			return errors.New("quality checkpoints exceed source state capacity")
		}
	}
	return nil
}

func validateQualityCheckpointMapCapacity(checkpoints map[[32]byte]*flowpb.QualityCheckpoint, maxCheckpoints, maxSources int) error {
	values := make([]*flowpb.QualityCheckpoint, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		values = append(values, checkpoint)
	}
	return validateQualityCheckpointCapacity(values, maxCheckpoints, maxSources)
}
