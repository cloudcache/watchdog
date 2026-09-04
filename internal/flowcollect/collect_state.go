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
	collectStateSchemaVersion = 1
	collectStateHeaderSize    = 16
	collectStateMaxBytes      = 64 << 20
)

var collectStateMagic = [8]byte{'W', 'D', 'F', 'S', 'T', 'A', 'T', 'E'}

type CollectStateStore struct {
	dir         string
	collectorID string
	mu          sync.Mutex
}

func OpenCollectStateStore(dir, collectorID string, decoder *Decoder) (*CollectStateStore, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	collectorID = strings.TrimSpace(collectorID)
	if dir == "." || collectorID == "" || decoder == nil {
		return nil, errors.New("collect-state dir, collector ID, and decoder are required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create collect-state dir: %w", err)
	}
	store := &CollectStateStore{dir: dir, collectorID: collectorID}
	if err := store.restore(decoder); err != nil {
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
	templateJSON, samplingJSON, err := decoder.SnapshotState(decoded.Protocol, record.Source.Addr(), decoded.ObservationDomainID)
	if err != nil {
		return nil, err
	}
	key := makeCollectStateKey(collectorID, decoded.Protocol, record.Source.Addr(), decoded.ObservationDomainID)
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

func (s *CollectStateStore) restore(decoder *Decoder) error {
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
		checkpointTime := time.UnixMilli(state.ReceivedAtUnixMs)
		if checkpointTime.After(time.Now().Add(5 * time.Minute)) {
			return fmt.Errorf("restore collect state %s: checkpoint time is in the future", name)
		}
		if time.Since(checkpointTime) > decoder.stateTTL {
			continue
		}
		if err := decoder.RestoreState(state.TemplatesJson, state.SamplingRatesJson); err != nil {
			return fmt.Errorf("restore collect state %s: %w", name, err)
		}
	}
	return nil
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
	if state == nil || state.StateSchemaVersion != collectStateSchemaVersion {
		return errors.New("unsupported collect-state schema")
	}
	if state.CollectorId != collectorID || state.TenantId == "" || state.ExporterId == "" || state.RegistryVersion == 0 {
		return errors.New("collect-state identity is invalid")
	}
	if len(state.DatagramId) != sha256.Size || len(state.StateKey) != sha256.Size || len(state.StateId) != sha256.Size || len(state.PayloadSha256) != sha256.Size {
		return errors.New("collect-state identifiers are invalid")
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
	expectedKey := makeCollectStateKey(state.CollectorId, protocol, source.Unmap(), state.ObservationDomainId)
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

func makeCollectStateKey(collectorID string, protocol Protocol, source netip.Addr, domain uint64) [32]byte {
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

func makeCollectStateID(datagramID DatagramID, key [32]byte) [32]byte {
	input := make([]byte, 0, len(datagramID)+len(key))
	input = append(input, datagramID[:]...)
	input = append(input, key[:]...)
	return sha256.Sum256(input)
}
