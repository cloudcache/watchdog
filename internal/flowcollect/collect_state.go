package flowcollect

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

const (
	collectStateSchemaVersion = 2
	collectStateSchemaV1      = 1
	collectStateHeaderSize    = 16
	collectStateMaxBytes      = 64 << 20
)

var collectStateMagic = [8]byte{'W', 'D', 'F', 'S', 'T', 'A', 'T', 'E'}

type CollectStateStore struct {
	dir         string
	collectorID string
	mu          sync.Mutex
}

func OpenCollectStateStore(dir, collectorID string, registry *Registry, decoder *Decoder) (*CollectStateStore, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	collectorID = strings.TrimSpace(collectorID)
	if dir == "." || collectorID == "" || registry == nil || decoder == nil {
		return nil, errors.New("collect-state dir, collector ID, registry, and decoder are required")
	}
	if registry.Plan().CollectorID != collectorID {
		return nil, errors.New("collect-state collector ID does not match active registry")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create collect-state dir: %w", err)
	}
	store := &CollectStateStore{dir: dir, collectorID: collectorID}
	if err := store.restore(registry, decoder); err != nil {
		return nil, err
	}
	return store, nil
}

func BuildCollectState(record WALRecord, decoded DecodedDatagram, binding SourceBinding, collectorID string, decoder *Decoder) (*flowpb.CollectState, error) {
	if !decoded.CollectStateChanged || (decoded.Protocol != ProtocolNetFlow9 && decoded.Protocol != ProtocolIPFIX) {
		return nil, errors.New("datagram does not contain changed collect state")
	}
	if decoder == nil {
		return nil, errors.New("decoder is required")
	}
	templateJSON, samplingJSON, generation, err := decoder.SnapshotState(decoded.Protocol, record.Source.Addr(), decoded.ObservationDomainID)
	if err != nil {
		return nil, err
	}
	identityKey := makeCollectStateIdentityKey(binding.TenantID, binding.ExporterID, decoded.Protocol, record.Source.Addr(), decoded.ObservationDomainID)
	epoch := binding.EffectiveOwnershipEpoch()
	key := makeCollectStateKey(identityKey, epoch)
	id := makeCollectStateID(record.DatagramID, key)
	state := &flowpb.CollectState{
		StateSchemaVersion:  collectStateSchemaVersion,
		StateId:             id[:],
		StateKey:            key[:],
		DatagramId:          record.DatagramID[:],
		TenantId:            binding.TenantID,
		CollectorId:         collectorID,
		ExporterId:          binding.ExporterID,
		RegistryVersion:     record.RegistryVersion,
		ReceivedAtUnixMs:    record.ReceivedAt.UnixMilli(),
		Protocol:            uint32(decoded.Protocol),
		SourceIp:            address16(record.Source.Addr()),
		ObservationDomainId: decoded.ObservationDomainID,
		TemplatesJson:       templateJSON,
		SamplingRatesJson:   samplingJSON,
		StateIdentityKey:    identityKey[:],
		OwnershipEpoch:      epoch,
		StateGeneration:     generation,
	}
	digest, err := collectStateDigest(state)
	if err != nil {
		return nil, err
	}
	state.PayloadSha256 = digest[:]
	return state, nil
}

func (s *CollectStateStore) Persist(state *flowpb.CollectState) error {
	if s == nil {
		return errors.New("collect-state store is required")
	}
	if err := validateCollectState(state, s.collectorID); err != nil {
		return err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal collect state: %w", err)
	}
	if len(payload) > collectStateMaxBytes-collectStateHeaderSize {
		return errors.New("collect-state checkpoint exceeds size limit")
	}
	frame := make([]byte, collectStateHeaderSize+len(payload))
	copy(frame[:8], collectStateMagic[:])
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[12:16], crc32.ChecksumIEEE(payload))
	copy(frame[collectStateHeaderSize:], payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	temp, err := os.CreateTemp(s.dir, ".collect-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create collect-state temp file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o640); err != nil {
		_ = temp.Close()
		return fmt.Errorf("chmod collect-state temp file: %w", err)
	}
	if err := writeFull(temp, frame); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write collect-state checkpoint: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync collect-state checkpoint: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close collect-state checkpoint: %w", err)
	}
	destination := filepath.Join(s.dir, hex.EncodeToString(state.StateKey)+".state")
	if err := os.Rename(tempPath, destination); err != nil {
		return fmt.Errorf("install collect-state checkpoint: %w", err)
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open collect-state directory: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync collect-state directory: %w", err)
	}
	return nil
}

func (s *CollectStateStore) restore(registry *Registry, decoder *Decoder) error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("read collect-state dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".state") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	now := time.Now()
	candidates := make([]*flowpb.CollectState, 0, len(names))
	for _, name := range names {
		state, err := readCollectStateFile(filepath.Join(s.dir, name))
		if err != nil {
			return fmt.Errorf("restore collect state %s: %w", name, err)
		}
		if err := validateCollectState(state, s.collectorID); err != nil {
			return fmt.Errorf("restore collect state %s: %w", name, err)
		}
		if name != hex.EncodeToString(state.StateKey)+".state" {
			return fmt.Errorf("restore collect state %s: state key does not match file name", name)
		}
		eligible, err := authorizeCollectState(state, registry, false)
		if err != nil {
			return fmt.Errorf("restore collect state %s: %w", name, err)
		}
		if !eligible {
			continue
		}
		checkpointTime := time.UnixMilli(state.ReceivedAtUnixMs)
		if checkpointTime.After(now.Add(5 * time.Minute)) {
			return fmt.Errorf("restore collect state %s: checkpoint time is in the future", name)
		}
		if now.Sub(checkpointTime) > decoder.stateTTL {
			continue
		}
		candidates = append(candidates, state)
	}
	selected, err := selectCollectStates(candidates)
	if err != nil {
		return err
	}
	for _, state := range selected {
		if err := restoreDecoderCollectState(decoder, state); err != nil {
			return fmt.Errorf("restore collect state %x: %w", state.StateKey, err)
		}
	}
	return nil
}

// CollectStateRecord preserves the Kafka position used to audit a remote
// restore. Selection does not use offset as a correctness tie-breaker: two
// different payloads for the same ownership epoch and generation are a
// split-brain conflict and fail closed.
type CollectStateRecord struct {
	State     *flowpb.CollectState
	Partition int32
	Offset    int64
}

// RestoreRemoteCollectStates validates topic snapshots against the active
// signed registry before mutating decoder state. Schema-v1 snapshots are only
// eligible for same-collector rolling upgrades; schema-v2 can cross owners
// when the current plan carries a strictly newer ownership epoch.
func RestoreRemoteCollectStates(decoder *Decoder, registry *Registry, records []CollectStateRecord, now time.Time) (int, error) {
	if decoder == nil || registry == nil || now.IsZero() {
		return 0, errors.New("decoder, registry, and restore time are required")
	}
	candidates := make([]*flowpb.CollectState, 0, len(records))
	for index, record := range records {
		state := record.State
		if err := validateCollectState(state, ""); err != nil {
			return 0, fmt.Errorf("collect-state record %d partition=%d offset=%d: %w", index, record.Partition, record.Offset, err)
		}
		eligible, err := authorizeCollectState(state, registry, true)
		if err != nil {
			return 0, fmt.Errorf("collect-state record %d: %w", index, err)
		}
		if !eligible {
			continue
		}
		checkpointTime := time.UnixMilli(state.ReceivedAtUnixMs)
		if checkpointTime.After(now.Add(5 * time.Minute)) {
			return 0, fmt.Errorf("collect-state record %d checkpoint time is in the future", index)
		}
		if now.Sub(checkpointTime) > decoder.stateTTL {
			continue
		}
		candidates = append(candidates, state)
	}
	selected, err := selectCollectStates(candidates)
	if err != nil {
		return 0, err
	}
	for _, state := range selected {
		if err := restoreDecoderCollectState(decoder, state); err != nil {
			return 0, fmt.Errorf("restore remote collect state %x: %w", state.StateKey, err)
		}
	}
	return len(selected), nil
}

func authorizeCollectState(state *flowpb.CollectState, registry *Registry, allowPreviousOwner bool) (bool, error) {
	plan := registry.Plan()
	binding, admitted := registry.Admit(Protocol(state.Protocol), addressFrom16(state.SourceIp), state.ObservationDomainId)
	if !admitted || binding.TenantID != state.TenantId || binding.ExporterID != state.ExporterId {
		return false, nil
	}
	if state.StateSchemaVersion == collectStateSchemaV1 && state.CollectorId != plan.CollectorID {
		return false, nil
	}
	stateEpoch := collectStateOwnershipEpoch(state)
	currentEpoch := binding.EffectiveOwnershipEpoch()
	if stateEpoch > currentEpoch {
		return false, fmt.Errorf("ownership epoch %d is newer than active epoch %d", stateEpoch, currentEpoch)
	}
	if stateEpoch == currentEpoch && state.CollectorId != plan.CollectorID {
		return false, fmt.Errorf("ownership epoch %d is reused across collectors", stateEpoch)
	}
	if !allowPreviousOwner && state.CollectorId != plan.CollectorID {
		return false, nil
	}
	return true, nil
}

func selectCollectStates(states []*flowpb.CollectState) ([]*flowpb.CollectState, error) {
	selected := make(map[[32]byte]*flowpb.CollectState, len(states))
	for _, state := range states {
		identity := collectStateIdentity(state)
		current, exists := selected[identity]
		if !exists {
			selected[identity] = state
			continue
		}
		comparison := compareCollectStateOrder(state, current)
		if comparison > 0 {
			selected[identity] = state
			continue
		}
		if comparison == 0 && !bytes.Equal(state.PayloadSha256, current.PayloadSha256) {
			return nil, fmt.Errorf("collect-state conflict for identity %x epoch=%d generation=%d", identity, collectStateOwnershipEpoch(state), collectStateGeneration(state))
		}
	}
	identities := make([][32]byte, 0, len(selected))
	for identity := range selected {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return bytes.Compare(identities[i][:], identities[j][:]) < 0 })
	result := make([]*flowpb.CollectState, 0, len(identities))
	for _, identity := range identities {
		result = append(result, selected[identity])
	}
	return result, nil
}

func collectStateIdentity(state *flowpb.CollectState) [32]byte {
	if state.StateSchemaVersion == collectStateSchemaVersion && len(state.StateIdentityKey) == sha256.Size {
		var identity [32]byte
		copy(identity[:], state.StateIdentityKey)
		return identity
	}
	return makeCollectStateIdentityKey(state.TenantId, state.ExporterId, Protocol(state.Protocol), addressFrom16(state.SourceIp), state.ObservationDomainId)
}

func compareCollectStateOrder(left, right *flowpb.CollectState) int {
	leftEpoch, rightEpoch := collectStateOwnershipEpoch(left), collectStateOwnershipEpoch(right)
	if leftEpoch != rightEpoch {
		if leftEpoch > rightEpoch {
			return 1
		}
		return -1
	}
	leftGeneration, rightGeneration := collectStateGeneration(left), collectStateGeneration(right)
	if leftGeneration > rightGeneration {
		return 1
	}
	if leftGeneration < rightGeneration {
		return -1
	}
	return 0
}

func collectStateOwnershipEpoch(state *flowpb.CollectState) uint64 {
	if state.StateSchemaVersion == collectStateSchemaV1 {
		return 1
	}
	return state.OwnershipEpoch
}

func collectStateGeneration(state *flowpb.CollectState) uint64 {
	if state.StateSchemaVersion == collectStateSchemaV1 {
		return 1
	}
	return state.StateGeneration
}

func readCollectStateFile(path string) (*flowpb.CollectState, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < collectStateHeaderSize || info.Size() > collectStateMaxBytes {
		return nil, errors.New("invalid collect-state checkpoint size")
	}
	header := make([]byte, collectStateHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, err
	}
	if !bytes.Equal(header[:8], collectStateMagic[:]) {
		return nil, errors.New("invalid collect-state checkpoint magic")
	}
	payloadSize := binary.BigEndian.Uint32(header[8:12])
	if int64(payloadSize)+collectStateHeaderSize != info.Size() {
		return nil, errors.New("invalid collect-state checkpoint payload length")
	}
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(file, payload); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[12:16]) {
		return nil, errors.New("collect-state checkpoint CRC mismatch")
	}
	state := &flowpb.CollectState{}
	if err := proto.Unmarshal(payload, state); err != nil {
		return nil, fmt.Errorf("unmarshal collect state: %w", err)
	}
	return state, nil
}

func validateCollectState(state *flowpb.CollectState, collectorID string) error {
	if state == nil || (state.StateSchemaVersion != collectStateSchemaV1 && state.StateSchemaVersion != collectStateSchemaVersion) {
		return errors.New("unsupported collect-state schema")
	}
	if (collectorID != "" && state.CollectorId != collectorID) || state.CollectorId == "" || state.TenantId == "" || state.ExporterId == "" || state.RegistryVersion == 0 {
		return errors.New("collect-state identity is invalid")
	}
	if len(state.DatagramId) != sha256.Size || len(state.StateKey) != sha256.Size || len(state.StateId) != sha256.Size || len(state.PayloadSha256) != sha256.Size {
		return errors.New("collect-state identifiers are invalid")
	}
	if len(state.SourceIp) != 16 || proto.Size(state) > collectStateMaxBytes-collectStateHeaderSize {
		return errors.New("collect-state payload shape is invalid")
	}
	protocol := Protocol(state.Protocol)
	if protocol != ProtocolNetFlow9 && protocol != ProtocolIPFIX {
		return errors.New("collect-state protocol is invalid")
	}
	if state.ObservationDomainId > uint64(^uint32(0)) {
		return errors.New("collect-state observation domain is invalid")
	}
	source, ok := netip.AddrFromSlice(state.SourceIp)
	if !ok {
		return errors.New("collect-state source IP is invalid")
	}
	var expectedKey [32]byte
	if state.StateSchemaVersion == collectStateSchemaV1 {
		expectedKey = makeCollectStateKeyV1(state.CollectorId, protocol, source.Unmap(), state.ObservationDomainId)
	} else {
		if len(state.StateIdentityKey) != sha256.Size || state.OwnershipEpoch == 0 || state.StateGeneration == 0 {
			return errors.New("collect-state ownership metadata is invalid")
		}
		expectedIdentityKey := makeCollectStateIdentityKey(state.TenantId, state.ExporterId, protocol, source.Unmap(), state.ObservationDomainId)
		if !bytes.Equal(state.StateIdentityKey, expectedIdentityKey[:]) {
			return errors.New("collect-state identity key mismatch")
		}
		expectedKey = makeCollectStateKey(expectedIdentityKey, state.OwnershipEpoch)
	}
	if !bytes.Equal(state.StateKey, expectedKey[:]) {
		return errors.New("collect-state key mismatch")
	}
	var datagramID DatagramID
	copy(datagramID[:], state.DatagramId)
	expectedID := makeCollectStateID(datagramID, expectedKey)
	if !bytes.Equal(state.StateId, expectedID[:]) {
		return errors.New("collect-state ID mismatch")
	}
	digest, err := collectStateDigest(state)
	if err != nil {
		return err
	}
	if !bytes.Equal(state.PayloadSha256, digest[:]) {
		return errors.New("collect-state payload checksum mismatch")
	}
	return nil
}

func collectStateDigest(state *flowpb.CollectState) ([32]byte, error) {
	clone := proto.Clone(state).(*flowpb.CollectState)
	clone.PayloadSha256 = nil
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshal collect-state checksum payload: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func makeCollectStateKeyV1(collectorID string, protocol Protocol, source netip.Addr, domain uint64) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(collectorID))
	_, _ = hash.Write([]byte{0, byte(protocol)})
	if source.IsValid() {
		value := source.As16()
		_, _ = hash.Write(value[:])
	}
	var domainBytes [8]byte
	binary.BigEndian.PutUint64(domainBytes[:], domain)
	_, _ = hash.Write(domainBytes[:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func makeCollectStateIdentityKey(tenantID, exporterID string, protocol Protocol, source netip.Addr, domain uint64) [32]byte {
	hash := sha256.New()
	writeHashField(hash, []byte("watchdog.flow.collect-state.identity.v2"))
	writeHashField(hash, []byte(tenantID))
	writeHashField(hash, []byte(exporterID))
	writeHashField(hash, []byte{byte(protocol)})
	if source.IsValid() {
		value := source.Unmap().As16()
		writeHashField(hash, value[:])
	} else {
		writeHashField(hash, nil)
	}
	var domainBytes [8]byte
	binary.BigEndian.PutUint64(domainBytes[:], domain)
	writeHashField(hash, domainBytes[:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func makeCollectStateKey(identityKey [32]byte, ownershipEpoch uint64) [32]byte {
	hash := sha256.New()
	writeHashField(hash, []byte("watchdog.flow.collect-state.partition.v2"))
	writeHashField(hash, identityKey[:])
	var epochBytes [8]byte
	binary.BigEndian.PutUint64(epochBytes[:], ownershipEpoch)
	writeHashField(hash, epochBytes[:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func writeHashField(hash io.Writer, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write(value)
}

func restoreDecoderCollectState(decoder *Decoder, state *flowpb.CollectState) error {
	if state.StateSchemaVersion == collectStateSchemaV1 {
		return decoder.RestoreStateRevision(Protocol(state.Protocol), addressFrom16(state.SourceIp), state.ObservationDomainId, 1, state.TemplatesJson, state.SamplingRatesJson)
	}
	return decoder.RestoreStateRevision(Protocol(state.Protocol), addressFrom16(state.SourceIp), state.ObservationDomainId, state.StateGeneration, state.TemplatesJson, state.SamplingRatesJson)
}

func addressFrom16(value []byte) netip.Addr {
	address, _ := netip.AddrFromSlice(value)
	return address.Unmap()
}

func makeCollectStateID(datagramID DatagramID, key [32]byte) [32]byte {
	input := make([]byte, 0, len(datagramID)+len(key))
	input = append(input, datagramID[:]...)
	input = append(input, key[:]...)
	return sha256.Sum256(input)
}
