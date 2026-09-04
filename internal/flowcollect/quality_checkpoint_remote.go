package flowcollect

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

const (
	qualityCheckpointSchemaVersion = 1
	qualityCheckpointKeyPrefix     = byte('Q')
)

// BuildQualityCheckpoint creates an owner-fenced checkpoint from one absolute
// quality journal post-state. The caller must only publish it after the
// datagram has a durable terminal WAL acknowledgement.
func BuildQualityCheckpoint(record WALRecord, binding SourceBinding, collectorID string, generation uint64, committedAt time.Time, journal *flowpb.QualityJournalRecord) (*flowpb.QualityCheckpoint, error) {
	if err := validateQualityJournalRecord(journal); err != nil {
		return nil, err
	}
	if journal.StateSchemaVersion != qualityJournalSchemaVersion || journal.ExporterState == nil || collectorID == "" || journal.CollectorId != collectorID || generation == 0 || committedAt.IsZero() || record.RegistryVersion == 0 || binding.Protocol != record.Protocol || binding.TenantID == "" || binding.ExporterID == "" || binding.TenantID != record.TenantID || binding.ExporterID != record.ExporterID {
		return nil, errors.New("quality checkpoint identity and committed state are required")
	}
	if journal.TenantId != binding.TenantID || journal.ExporterId != binding.ExporterID || journal.RegistryVersion != record.RegistryVersion || journal.OwnershipEpoch != binding.EffectiveOwnershipEpoch() || journal.ObservationDomainId != record.ObservationDomainID {
		return nil, errors.New("quality checkpoint journal owner metadata does not match its binding")
	}
	if !bytes.Equal(journal.DatagramId, record.DatagramID[:]) {
		return nil, errors.New("quality checkpoint journal does not match the WAL datagram")
	}
	exporterKey, _, err := exporterStateFromProto(journal.ExporterState)
	if err != nil {
		return nil, err
	}
	if err := validateQualityCheckpointRecordIdentity(record, exporterKey); err != nil {
		return nil, err
	}
	identityKey := makeQualityCheckpointIdentityKey(binding.TenantID, binding.ExporterID, exporterKey, record.ObservationDomainID)
	stateKey := makeQualityCheckpointKey(identityKey, binding.EffectiveOwnershipEpoch())
	checkpoint := &flowpb.QualityCheckpoint{
		CheckpointSchemaVersion: qualityCheckpointSchemaVersion,
		StateKey:                bytes.Clone(stateKey[:]),
		StateIdentityKey:        bytes.Clone(identityKey[:]),
		TenantId:                binding.TenantID,
		CollectorId:             collectorID,
		ExporterId:              binding.ExporterID,
		RegistryVersion:         record.RegistryVersion,
		OwnershipEpoch:          binding.EffectiveOwnershipEpoch(),
		StateGeneration:         generation,
		CommittedAtUnixMs:       committedAt.UnixMilli(),
		ObservationDomainId:     record.ObservationDomainID,
		ExporterState:           proto.Clone(journal.ExporterState).(*flowpb.QualityExporterState),
		SourceStates:            cloneQualitySourceStates(journal.SourceStates),
		LastWalSegment:          journal.WalSegment,
		LastWalOffset:           journal.WalOffset,
		LastDatagramId:          bytes.Clone(journal.DatagramId),
	}
	sortQualitySourceStates(checkpoint.SourceStates)
	digest, err := qualityCheckpointDigest(checkpoint)
	if err != nil {
		return nil, err
	}
	checkpoint.PayloadSha256 = digest[:]
	if err := validateQualityCheckpoint(checkpoint, 0); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func validateQualityCheckpointRecordIdentity(record WALRecord, key exporterQualityKey) error {
	if key.protocol != record.Protocol || !key.source.IsValid() || key.source.Unmap() != record.Source.Addr().Unmap() {
		return errors.New("quality checkpoint exporter does not match the WAL source")
	}
	switch record.Protocol {
	case ProtocolSFlow5:
		if record.ObservationDomainID != 0 {
			return errors.New("sFlow quality checkpoint has an observation domain")
		}
		if key.scope != 0 || !key.agent.IsValid() {
			return errors.New("sFlow quality checkpoint identity is incomplete")
		}
	case ProtocolNetFlow5:
		if record.ObservationDomainID != 0 {
			return errors.New("NetFlow v5 quality checkpoint has an observation domain")
		}
		if key.agent.IsValid() || key.subAgent != 0 {
			return errors.New("NetFlow v5 quality checkpoint contains sFlow identity fields")
		}
	case ProtocolNetFlow9, ProtocolIPFIX:
		if key.scope != record.ObservationDomainID {
			return errors.New("NetFlow v9/IPFIX quality checkpoint scope does not match the observation domain")
		}
		if key.agent.IsValid() || key.subAgent != 0 {
			return errors.New("NetFlow v9/IPFIX quality checkpoint contains sFlow identity fields")
		}
	default:
		return errors.New("quality checkpoint protocol is invalid")
	}
	return nil
}

func validateQualityCheckpoint(checkpoint *flowpb.QualityCheckpoint, maxSources int) error {
	if checkpoint == nil || checkpoint.CheckpointSchemaVersion != qualityCheckpointSchemaVersion || len(checkpoint.StateKey) != sha256.Size || len(checkpoint.StateIdentityKey) != sha256.Size || checkpoint.TenantId == "" || checkpoint.CollectorId == "" || checkpoint.ExporterId == "" || checkpoint.RegistryVersion == 0 || checkpoint.OwnershipEpoch == 0 || checkpoint.StateGeneration == 0 || checkpoint.CommittedAtUnixMs == 0 || checkpoint.ExporterState == nil || checkpoint.LastWalSegment == 0 || checkpoint.LastWalOffset < walHeaderSize || len(checkpoint.LastDatagramId) != sha256.Size || len(checkpoint.PayloadSha256) != sha256.Size {
		return errors.New("quality checkpoint is invalid")
	}
	if maxSources > 0 && len(checkpoint.SourceStates) > maxSources {
		return errors.New("quality checkpoint exceeds source state capacity")
	}
	key, _, err := exporterStateFromProto(checkpoint.ExporterState)
	if err != nil {
		return err
	}
	record := WALRecord{WALInput: WALInput{Protocol: key.protocol, Source: netip.AddrPortFrom(key.source, 0), ObservationDomainID: checkpoint.ObservationDomainId}}
	if err := validateQualityCheckpointRecordIdentity(record, key); err != nil {
		return err
	}
	expectedIdentity := makeQualityCheckpointIdentityKey(checkpoint.TenantId, checkpoint.ExporterId, key, checkpoint.ObservationDomainId)
	if !bytes.Equal(checkpoint.StateIdentityKey, expectedIdentity[:]) {
		return errors.New("quality checkpoint identity key mismatch")
	}
	expectedKey := makeQualityCheckpointKey(expectedIdentity, checkpoint.OwnershipEpoch)
	if !bytes.Equal(checkpoint.StateKey, expectedKey[:]) {
		return errors.New("quality checkpoint state key mismatch")
	}
	seenSources := make(map[[2]uint32]struct{}, len(checkpoint.SourceStates))
	for _, source := range checkpoint.SourceStates {
		sourceKey, _, err := sourceStateFromProto(source)
		if err != nil {
			return err
		}
		if sourceKey.exporter != key {
			return errors.New("quality checkpoint source belongs to another exporter")
		}
		identity := [2]uint32{sourceKey.sourceType, sourceKey.sourceValue}
		if _, duplicate := seenSources[identity]; duplicate {
			return errors.New("quality checkpoint contains duplicate source state")
		}
		seenSources[identity] = struct{}{}
	}
	digest, err := qualityCheckpointDigest(checkpoint)
	if err != nil {
		return err
	}
	if !bytes.Equal(checkpoint.PayloadSha256, digest[:]) {
		return errors.New("quality checkpoint payload checksum mismatch")
	}
	return nil
}

func eligibleRemoteQualityCheckpoints(registry *Registry, checkpoints []*flowpb.QualityCheckpoint, config QualityConfig, now time.Time) ([]*flowpb.QualityCheckpoint, error) {
	if registry == nil || config.StateTTL <= 0 || config.MaxExporters <= 0 || config.MaxDataSources <= 0 || now.IsZero() {
		return nil, errors.New("quality checkpoint recovery requires registry, limits, and current time")
	}
	eligible := make([]*flowpb.QualityCheckpoint, 0, len(checkpoints))
	for index, checkpoint := range checkpoints {
		if err := validateQualityCheckpoint(checkpoint, config.MaxDataSources); err != nil {
			return nil, fmt.Errorf("quality checkpoint %d: %w", index, err)
		}
		committedAt := time.UnixMilli(checkpoint.CommittedAtUnixMs)
		if committedAt.After(now.Add(5 * time.Minute)) {
			return nil, fmt.Errorf("quality checkpoint %d time is in the future", index)
		}
		if now.Sub(committedAt) > config.StateTTL {
			continue
		}
		accepted, err := authorizeQualityCheckpoint(checkpoint, registry, true)
		if err != nil {
			return nil, fmt.Errorf("quality checkpoint %d: %w", index, err)
		}
		if accepted {
			eligible = append(eligible, checkpoint)
		}
	}
	return eligible, nil
}

func authorizeQualityCheckpoint(checkpoint *flowpb.QualityCheckpoint, registry *Registry, allowPreviousOwner bool) (bool, error) {
	if registry == nil || checkpoint == nil || checkpoint.ExporterState == nil {
		return false, errors.New("quality checkpoint and registry are required")
	}
	key, _, err := exporterStateFromProto(checkpoint.ExporterState)
	if err != nil {
		return false, err
	}
	plan := registry.Plan()
	if checkpoint.RegistryVersion > plan.Revision {
		return false, fmt.Errorf("quality checkpoint plan revision %d is newer than active revision %d", checkpoint.RegistryVersion, plan.Revision)
	}
	binding, admitted := registry.Admit(key.protocol, key.source, checkpoint.ObservationDomainId)
	if !admitted || binding.TenantID != checkpoint.TenantId || binding.ExporterID != checkpoint.ExporterId {
		return false, nil
	}
	currentEpoch := binding.EffectiveOwnershipEpoch()
	if checkpoint.OwnershipEpoch > currentEpoch {
		return false, fmt.Errorf("quality checkpoint ownership epoch %d is newer than active epoch %d", checkpoint.OwnershipEpoch, currentEpoch)
	}
	if checkpoint.OwnershipEpoch == currentEpoch && checkpoint.CollectorId != plan.CollectorID {
		return false, fmt.Errorf("quality checkpoint ownership epoch %d is reused across collectors", currentEpoch)
	}
	if !allowPreviousOwner && checkpoint.CollectorId != plan.CollectorID {
		return false, nil
	}
	return true, nil
}

func selectQualityCheckpoints(checkpoints []*flowpb.QualityCheckpoint) ([]*flowpb.QualityCheckpoint, error) {
	selected := make(map[[32]byte]*flowpb.QualityCheckpoint, len(checkpoints))
	for _, checkpoint := range checkpoints {
		var identity [32]byte
		copy(identity[:], checkpoint.StateIdentityKey)
		current, exists := selected[identity]
		if !exists {
			selected[identity] = checkpoint
			continue
		}
		comparison := compareQualityCheckpointOrder(checkpoint, current)
		if comparison > 0 {
			selected[identity] = checkpoint
			continue
		}
		if comparison == 0 && !bytes.Equal(checkpoint.PayloadSha256, current.PayloadSha256) {
			return nil, fmt.Errorf("quality checkpoint conflict for identity %x epoch=%d generation=%d", identity, checkpoint.OwnershipEpoch, checkpoint.StateGeneration)
		}
	}
	identities := make([][32]byte, 0, len(selected))
	for identity := range selected {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return bytes.Compare(identities[i][:], identities[j][:]) < 0 })
	result := make([]*flowpb.QualityCheckpoint, 0, len(identities))
	for _, identity := range identities {
		result = append(result, selected[identity])
	}
	return result, nil
}

func compareQualityCheckpointOrder(left, right *flowpb.QualityCheckpoint) int {
	if left.OwnershipEpoch != right.OwnershipEpoch {
		if left.OwnershipEpoch > right.OwnershipEpoch {
			return 1
		}
		return -1
	}
	if left.StateGeneration > right.StateGeneration {
		return 1
	}
	if left.StateGeneration < right.StateGeneration {
		return -1
	}
	return 0
}

func makeQualityCheckpointIdentityKey(tenantID, exporterID string, key exporterQualityKey, observationDomainID uint64) [32]byte {
	hash := sha256.New()
	writeHashField(hash, []byte("watchdog.flow.quality-state.identity.v1"))
	writeHashField(hash, []byte(tenantID))
	writeHashField(hash, []byte(exporterID))
	writeHashField(hash, []byte{byte(key.protocol)})
	writeHashField(hash, address16(key.source))
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], observationDomainID)
	writeHashField(hash, value[:])
	binary.BigEndian.PutUint64(value[:], key.scope)
	writeHashField(hash, value[:])
	writeHashField(hash, address16(key.agent))
	var subAgent [4]byte
	binary.BigEndian.PutUint32(subAgent[:], key.subAgent)
	writeHashField(hash, subAgent[:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func makeQualityCheckpointKey(identity [32]byte, ownershipEpoch uint64) [32]byte {
	hash := sha256.New()
	writeHashField(hash, []byte("watchdog.flow.quality-state.partition.v1"))
	writeHashField(hash, identity[:])
	var epoch [8]byte
	binary.BigEndian.PutUint64(epoch[:], ownershipEpoch)
	writeHashField(hash, epoch[:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func qualityCheckpointKafkaKey(checkpoint *flowpb.QualityCheckpoint) ([]byte, error) {
	if checkpoint == nil || len(checkpoint.StateKey) != sha256.Size {
		return nil, errors.New("quality checkpoint state key is invalid")
	}
	key := make([]byte, 1+sha256.Size)
	key[0] = qualityCheckpointKeyPrefix
	copy(key[1:], checkpoint.StateKey)
	return key, nil
}

func isQualityCheckpointKafkaKey(key []byte) bool {
	return len(key) == 1+sha256.Size && key[0] == qualityCheckpointKeyPrefix
}

func qualityCheckpointDigest(checkpoint *flowpb.QualityCheckpoint) ([32]byte, error) {
	clone := proto.Clone(checkpoint).(*flowpb.QualityCheckpoint)
	clone.PayloadSha256 = nil
	return qualityProtoDigest(clone)
}

func cloneQualitySourceStates(states []*flowpb.QualitySourceState) []*flowpb.QualitySourceState {
	cloned := make([]*flowpb.QualitySourceState, 0, len(states))
	for _, state := range states {
		if state == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, proto.Clone(state).(*flowpb.QualitySourceState))
	}
	return cloned
}

func sortQualitySourceStates(states []*flowpb.QualitySourceState) {
	sort.Slice(states, func(i, j int) bool {
		if states[i].SourceIdType != states[j].SourceIdType {
			return states[i].SourceIdType < states[j].SourceIdType
		}
		return states[i].SourceIdValue < states[j].SourceIdValue
	})
}
