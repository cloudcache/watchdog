package flowcollect

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
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
	qualityFrameHeaderSize = 16
	qualityRecordMaxBytes  = 16 << 20
)

var (
	qualityJournalMagic  = [8]byte{'W', 'D', 'Q', 'J', 'R', 'N', '0', '1'}
	qualitySnapshotMagic = [8]byte{'W', 'D', 'Q', 'S', 'N', 'P', '0', '1'}
)

type QualityStateStore struct {
	mu          sync.Mutex
	compactMu   sync.Mutex
	dir         string
	collectorID string
	config      QualityConfig
	tracker     *QualityTracker
	wal         *WAL
	metrics     *Metrics
	journal     *os.File
	size        int64
	durableEnd  int64
	syncNotify  chan struct{}
	poisoned    error
	closed      bool
	stopSync    chan struct{}
	doneSync    chan struct{}
	pending     map[DatagramID]*flowpb.QualityDecision
	targets     map[DatagramID]int64
	journals    map[DatagramID]*flowpb.QualityJournalRecord
	completed   map[DatagramID]struct{}
	committed   *qualityCommitAccumulator
	dirty       map[[32]byte]struct{}
}

type QualityStoreState struct {
	JournalBytes    int64
	MaxJournalBytes int64
	UsageRatio      float64
	Pending         int
	Committed       int
	Dirty           int
	Writable        bool
}

func OpenQualityStateStore(dir, collectorID string, config QualityConfig, tracker *QualityTracker, wal *WAL, metrics *Metrics) (*QualityStateStore, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	collectorID = strings.TrimSpace(collectorID)
	if dir == "." || collectorID == "" || tracker == nil || wal == nil || config.StateTTL <= 0 || config.AnomalyWindow <= 0 || config.AnomalyWindow > config.StateTTL || config.JournalFsync <= 0 || config.CheckpointEvery <= 0 || config.JournalMaxBytes <= qualityFrameHeaderSize || config.MaxExporters <= 0 || config.MaxDataSources <= 0 {
		return nil, errors.New("quality-state dir, collector, tracker, WAL, and positive limits are required")
	}
	if wal.collectorID != collectorID {
		return nil, errors.New("quality-state collector ID does not match the WAL")
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create quality-state dir: %w", err)
	}
	committed, err := newQualityCommitAccumulator(nil, config.MaxExporters, config.MaxDataSources)
	if err != nil {
		return nil, err
	}
	store := &QualityStateStore{dir: dir, collectorID: collectorID, config: config, tracker: tracker, wal: wal, metrics: metrics, syncNotify: make(chan struct{}), stopSync: make(chan struct{}), doneSync: make(chan struct{}), pending: make(map[DatagramID]*flowpb.QualityDecision), targets: make(map[DatagramID]int64), journals: make(map[DatagramID]*flowpb.QualityJournalRecord), completed: make(map[DatagramID]struct{}), committed: committed, dirty: make(map[[32]byte]struct{})}
	pendingWAL, err := wal.pendingDatagramIDs()
	if err != nil {
		return nil, fmt.Errorf("read pending WAL IDs for quality restore: %w", err)
	}
	if err := store.restoreSnapshot(pendingWAL); err != nil {
		return nil, err
	}
	journalPath := filepath.Join(dir, "quality.journal")
	journal, err := os.OpenFile(journalPath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open quality journal: %w", err)
	}
	store.journal = journal
	if err := store.restoreJournal(pendingWAL); err != nil {
		_ = journal.Close()
		return nil, err
	}
	go store.syncLoop()
	return store, nil
}

func (s *QualityStateStore) Complete(id DatagramID) {
	s.mu.Lock()
	delete(s.pending, id)
	delete(s.targets, id)
	if s.journals[id] != nil {
		s.completed[id] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *QualityStateStore) State() QualityStoreState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return QualityStoreState{
		JournalBytes:    s.size,
		MaxJournalBytes: s.config.JournalMaxBytes,
		UsageRatio:      float64(s.size) / float64(s.config.JournalMaxBytes),
		Pending:         len(s.pending),
		Committed:       len(s.committed.checkpoints),
		Dirty:           len(s.dirty),
		Writable:        !s.closed && s.poisoned == nil && s.size < s.config.JournalMaxBytes,
	}
}

// PendingQualityCheckpoints returns immutable publication candidates. A
// candidate remains dirty until its exact epoch/generation/payload is acked.
func (s *QualityStateStore) PendingQualityCheckpoints() []*flowpb.QualityCheckpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	identities := cloneQualityIdentities(s.dirty)
	result := make([]*flowpb.QualityCheckpoint, 0, len(identities))
	for _, identity := range identities {
		if checkpoint := s.committed.checkpoints[identity]; checkpoint != nil {
			result = append(result, proto.Clone(checkpoint).(*flowpb.QualityCheckpoint))
		}
	}
	return result
}

// MarkQualityCheckpointPublished clears dirty state only for the exact
// checkpoint acknowledged by Kafka. A delayed acknowledgement for an older
// generation cannot clear a newer candidate.
func (s *QualityStateStore) MarkQualityCheckpointPublished(checkpoint *flowpb.QualityCheckpoint) error {
	if err := validateQualityCheckpoint(checkpoint, s.config.MaxDataSources); err != nil {
		return err
	}
	if checkpoint.CollectorId != s.collectorID {
		return errors.New("published quality checkpoint collector mismatch")
	}
	var identity [32]byte
	copy(identity[:], checkpoint.StateIdentityKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.committed.checkpoints[identity]
	if current == nil {
		return errors.New("published quality checkpoint identity is unknown")
	}
	comparison := compareQualityCheckpointOrder(checkpoint, current)
	if comparison < 0 {
		return nil
	}
	if comparison > 0 || !bytes.Equal(checkpoint.PayloadSha256, current.PayloadSha256) {
		return errors.New("published quality checkpoint does not match committed state")
	}
	delete(s.dirty, identity)
	return nil
}

func (s *QualityStateStore) AppendAndWait(ctx context.Context, journal *flowpb.QualityJournalRecord) error {
	if err := s.Append(journal); err != nil {
		return err
	}
	var id DatagramID
	copy(id[:], journal.DatagramId)
	return s.WaitDatagram(ctx, id)
}

func (s *QualityStateStore) Append(journal *flowpb.QualityJournalRecord) error {
	if err := validateQualityJournalRecord(journal); err != nil {
		return err
	}
	if journal.CollectorId != s.collectorID {
		return errors.New("quality journal collector ID does not match the store")
	}
	var id DatagramID
	copy(id[:], journal.DatagramId)

	s.mu.Lock()
	if existing := s.journals[id]; existing != nil {
		if !proto.Equal(existing, journal) {
			s.mu.Unlock()
			return errors.New("quality journal changed for an existing datagram ID")
		}
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(journal)
	if err != nil {
		return fmt.Errorf("marshal quality journal record: %w", err)
	}
	frame, err := qualityFrame(qualityJournalMagic, payload, qualityRecordMaxBytes)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("quality-state store is closed")
	}
	if s.poisoned != nil {
		err := s.poisoned
		s.mu.Unlock()
		return fmt.Errorf("quality-state store is not writable: %w", err)
	}
	if existing := s.journals[id]; existing != nil {
		if !proto.Equal(existing, journal) {
			s.mu.Unlock()
			return errors.New("quality journal changed for an existing datagram ID")
		}
		s.mu.Unlock()
		return nil
	}
	if s.size+int64(len(frame)) > s.config.JournalMaxBytes {
		s.mu.Unlock()
		return errors.New("quality journal hard limit reached")
	}
	if err := writeFull(s.journal, frame); err != nil {
		s.poisonLocked(err)
		s.mu.Unlock()
		return fmt.Errorf("append quality journal: %w", err)
	}
	s.size += int64(len(frame))
	target := s.size
	s.pending[id] = proto.Clone(journal.Decision).(*flowpb.QualityDecision)
	s.targets[id] = target
	s.journals[id] = proto.Clone(journal).(*flowpb.QualityJournalRecord)
	s.metrics.QualityJournalAppends.Add(1)
	s.mu.Unlock()
	return nil
}

func (s *QualityStateStore) WaitDatagram(ctx context.Context, id DatagramID) error {
	s.mu.Lock()
	target, exists := s.targets[id]
	s.mu.Unlock()
	if !exists {
		return nil
	}
	return s.waitDurable(ctx, target)
}

func (s *QualityStateStore) Compact(createdAt time.Time) error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("quality-state store is closed")
	}
	captured := make(map[DatagramID]struct{}, len(s.completed))
	committedJournals := make([]*flowpb.QualityJournalRecord, 0, len(s.completed))
	for id := range s.completed {
		captured[id] = struct{}{}
		if journal := s.journals[id]; journal != nil && journal.StateSchemaVersion == qualityJournalSchemaVersion {
			lastSeen := time.UnixMilli(journal.ExporterState.LastSeenUnixMs)
			if !lastSeen.Before(createdAt.Add(-s.config.StateTTL)) {
				committedJournals = append(committedJournals, proto.Clone(journal).(*flowpb.QualityJournalRecord))
			}
		}
	}
	s.mu.Unlock()

	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("sync WAL before quality checkpoint: %w", err)
	}
	if err := s.Sync(); err != nil {
		return err
	}
	s.mu.Lock()
	changed, err := s.committed.Coalesce(committedJournals, s.collectorID, createdAt)
	if err == nil {
		for _, checkpoint := range changed {
			var identity [32]byte
			copy(identity[:], checkpoint.StateIdentityKey)
			s.dirty[identity] = struct{}{}
		}
		for _, identity := range s.committed.PruneBefore(createdAt.Add(-s.config.StateTTL)) {
			delete(s.dirty, identity)
		}
	}
	checkpoints := s.committed.Snapshot()
	dirty := cloneQualityIdentities(s.dirty)
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("coalesce committed quality state: %w", err)
	}
	pending := s.unacknowledgedDecisions()
	snapshot, err := s.tracker.BuildSnapshot(s.collectorID, createdAt, pending, checkpoints, dirty)
	if err != nil {
		return err
	}
	if err := s.persistSnapshot(snapshot); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("quality-state store is closed")
	}
	retained := make([]*flowpb.QualityJournalRecord, 0, len(s.journals)-len(captured))
	for id, journal := range s.journals {
		if _, folded := captured[id]; !folded {
			retained = append(retained, proto.Clone(journal).(*flowpb.QualityJournalRecord))
		}
	}
	sortQualityJournals(retained)
	if err := s.rewriteJournalLocked(retained); err != nil {
		s.poisonLocked(err)
		s.mu.Unlock()
		return err
	}
	for id := range captured {
		delete(s.journals, id)
		delete(s.completed, id)
	}
	s.metrics.QualityStateCheckpoints.Add(1)
	s.mu.Unlock()
	return nil
}

func (s *QualityStateStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("quality-state store is closed")
	}
	if s.poisoned != nil {
		return fmt.Errorf("quality-state store is not writable: %w", s.poisoned)
	}
	if err := s.journal.Sync(); err != nil {
		s.poisonLocked(err)
		return fmt.Errorf("sync quality journal: %w", err)
	}
	s.markDurableLocked()
	return nil
}

func (s *QualityStateStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stopSync)
	errSync := s.journal.Sync()
	if errSync == nil {
		s.markDurableLocked()
	}
	errClose := s.journal.Close()
	s.mu.Unlock()
	<-s.doneSync
	return errors.Join(errSync, errClose)
}

func (s *QualityStateStore) waitDurable(ctx context.Context, target int64) error {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return errors.New("quality-state store is closed")
		}
		if s.poisoned != nil {
			err := s.poisoned
			s.mu.Unlock()
			return fmt.Errorf("quality journal sync failed: %w", err)
		}
		if target <= s.durableEnd {
			s.mu.Unlock()
			return nil
		}
		notify := s.syncNotify
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (s *QualityStateStore) syncLoop() {
	ticker := time.NewTicker(s.config.JournalFsync)
	defer func() { ticker.Stop(); close(s.doneSync) }()
	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if !s.closed {
				if err := s.journal.Sync(); err != nil {
					s.poisonLocked(err)
				} else {
					s.markDurableLocked()
				}
			}
			s.mu.Unlock()
		case <-s.stopSync:
			return
		}
	}
}

func (s *QualityStateStore) markDurableLocked() {
	s.durableEnd = s.size
	close(s.syncNotify)
	s.syncNotify = make(chan struct{})
}

func (s *QualityStateStore) poisonLocked(err error) {
	if s.poisoned == nil {
		s.poisoned = err
		s.metrics.QualityJournalFailures.Add(1)
	}
	close(s.syncNotify)
	s.syncNotify = make(chan struct{})
}

func (s *QualityStateStore) unacknowledgedDecisions() []*flowpb.QualityDecision {
	s.mu.Lock()
	decisions := make(map[DatagramID]*flowpb.QualityDecision, len(s.pending))
	for id, decision := range s.pending {
		decisions[id] = proto.Clone(decision).(*flowpb.QualityDecision)
	}
	s.mu.Unlock()
	pending := make([]*flowpb.QualityDecision, 0, len(decisions))
	for id, decision := range decisions {
		if !s.wal.datagramDurablyAcknowledged(id) {
			pending = append(pending, decision)
		}
	}
	return pending
}

func (s *QualityStateStore) restoreSnapshot(pendingWAL map[DatagramID]struct{}) error {
	path := filepath.Join(s.dir, "quality.snapshot")
	payload, exists, err := readQualitySnapshotFrame(path, s.config.JournalMaxBytes)
	if err != nil || !exists {
		return err
	}
	snapshot := &flowpb.QualityStateSnapshot{}
	if err := proto.Unmarshal(payload, snapshot); err != nil {
		return fmt.Errorf("unmarshal quality snapshot: %w", err)
	}
	if time.UnixMilli(snapshot.CreatedAtUnixMs).After(time.Now().Add(5 * time.Minute)) {
		return errors.New("quality snapshot time is in the future")
	}
	if err := s.tracker.RestoreSnapshot(snapshot, s.collectorID); err != nil {
		return fmt.Errorf("restore quality snapshot: %w", err)
	}
	committed, err := newQualityCommitAccumulator(snapshot.CommittedCheckpoints, s.config.MaxExporters, s.config.MaxDataSources)
	if err != nil {
		return fmt.Errorf("restore committed quality checkpoints: %w", err)
	}
	dirty := make(map[[32]byte]struct{}, len(snapshot.DirtyStateIdentityKeys))
	for _, encoded := range snapshot.DirtyStateIdentityKeys {
		var identity [32]byte
		copy(identity[:], encoded)
		dirty[identity] = struct{}{}
	}
	s.committed = committed
	s.dirty = dirty
	for _, decision := range snapshot.PendingDecisions {
		var id DatagramID
		copy(id[:], decision.DatagramId)
		if _, exists := pendingWAL[id]; !exists {
			continue
		}
		if err := s.tracker.RememberDecision(decision); err != nil {
			return err
		}
		s.pending[id] = proto.Clone(decision).(*flowpb.QualityDecision)
		s.targets[id] = 0
	}
	s.metrics.QualityStateRestores.Add(1)
	return nil
}

func (s *QualityStateStore) restoreJournal(pendingWAL map[DatagramID]struct{}) error {
	info, err := s.journal.Stat()
	if err != nil {
		return err
	}
	if info.Size() > s.config.JournalMaxBytes {
		return errors.New("quality journal exceeds configured hard limit")
	}
	valid := int64(0)
	for valid < info.Size() {
		header := make([]byte, qualityFrameHeaderSize)
		n, err := s.journal.ReadAt(header, valid)
		if errors.Is(err, io.EOF) && n < qualityFrameHeaderSize {
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n != qualityFrameHeaderSize || !bytes.Equal(header[:8], qualityJournalMagic[:]) {
			return errors.New("quality journal contains an invalid frame header")
		}
		payloadSize := int64(binary.BigEndian.Uint32(header[8:12]))
		if payloadSize <= 0 || payloadSize > qualityRecordMaxBytes-qualityFrameHeaderSize {
			return errors.New("quality journal frame size is invalid")
		}
		end := valid + qualityFrameHeaderSize + payloadSize
		if end > info.Size() {
			break
		}
		payload := make([]byte, payloadSize)
		if _, err := s.journal.ReadAt(payload, valid+qualityFrameHeaderSize); err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[12:16]) {
			return errors.New("quality journal frame CRC mismatch")
		}
		journal := &flowpb.QualityJournalRecord{}
		if err := proto.Unmarshal(payload, journal); err != nil {
			return fmt.Errorf("unmarshal quality journal record: %w", err)
		}
		if journal.CollectorId != s.collectorID {
			return errors.New("quality journal collector ID does not match the store")
		}
		if err := s.tracker.ApplyJournalRecord(journal); err != nil {
			return fmt.Errorf("apply quality journal record: %w", err)
		}
		var id DatagramID
		copy(id[:], journal.DatagramId)
		if existing := s.journals[id]; existing != nil {
			if !proto.Equal(existing, journal) {
				return errors.New("quality journal contains conflicting duplicate datagram state")
			}
			valid = end
			continue
		}
		s.journals[id] = proto.Clone(journal).(*flowpb.QualityJournalRecord)
		if _, exists := pendingWAL[id]; exists {
			if err := s.tracker.RememberDecision(journal.Decision); err != nil {
				return err
			}
			s.pending[id] = proto.Clone(journal.Decision).(*flowpb.QualityDecision)
			s.targets[id] = end
		} else {
			s.completed[id] = struct{}{}
		}
		valid = end
	}
	if valid != info.Size() {
		if err := s.journal.Truncate(valid); err != nil {
			return fmt.Errorf("truncate incomplete quality journal tail: %w", err)
		}
	}
	if _, err := s.journal.Seek(valid, io.SeekStart); err != nil {
		return err
	}
	if err := s.journal.Sync(); err != nil {
		return err
	}
	s.size, s.durableEnd = valid, valid
	if valid > 0 {
		s.metrics.QualityStateRestores.Add(1)
	}
	return nil
}

func (s *QualityStateStore) rewriteJournalLocked(journals []*flowpb.QualityJournalRecord) error {
	temp, err := os.CreateTemp(s.dir, ".quality-journal-*.tmp")
	if err != nil {
		return fmt.Errorf("create compacted quality journal: %w", err)
	}
	tempPath := temp.Name()
	keepTemp := false
	defer func() {
		if !keepTemp {
			_ = temp.Close()
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o640); err != nil {
		return err
	}
	newTargets := make(map[DatagramID]int64, len(s.pending))
	for id := range s.pending {
		newTargets[id] = 0
	}
	var size int64
	for _, journal := range journals {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(journal)
		if err != nil {
			return fmt.Errorf("marshal retained quality journal: %w", err)
		}
		frame, err := qualityFrame(qualityJournalMagic, payload, qualityRecordMaxBytes)
		if err != nil {
			return err
		}
		if size+int64(len(frame)) > s.config.JournalMaxBytes {
			return errors.New("retained quality journal exceeds hard limit")
		}
		if err := writeFull(temp, frame); err != nil {
			return fmt.Errorf("write compacted quality journal: %w", err)
		}
		size += int64(len(frame))
		var id DatagramID
		copy(id[:], journal.DatagramId)
		if _, pending := s.pending[id]; pending {
			newTargets[id] = size
		}
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync compacted quality journal: %w", err)
	}
	if _, err := temp.Seek(size, io.SeekStart); err != nil {
		return fmt.Errorf("seek compacted quality journal: %w", err)
	}
	journalPath := filepath.Join(s.dir, "quality.journal")
	if err := os.Rename(tempPath, journalPath); err != nil {
		return fmt.Errorf("replace compacted quality journal: %w", err)
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return fmt.Errorf("sync quality-state directory: %w", err)
	}
	old := s.journal
	s.journal = temp
	keepTemp = true
	s.size, s.durableEnd = size, size
	s.targets = newTargets
	close(s.syncNotify)
	s.syncNotify = make(chan struct{})
	if err := old.Close(); err != nil {
		return fmt.Errorf("close replaced quality journal: %w", err)
	}
	return nil
}

func sortQualityJournals(journals []*flowpb.QualityJournalRecord) {
	sort.Slice(journals, func(i, j int) bool {
		if journals[i].WalSegment != journals[j].WalSegment {
			return journals[i].WalSegment < journals[j].WalSegment
		}
		return journals[i].WalOffset < journals[j].WalOffset
	})
}

func cloneQualityIdentities(identities map[[32]byte]struct{}) [][32]byte {
	result := make([][32]byte, 0, len(identities))
	for identity := range identities {
		result = append(result, identity)
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i][:], result[j][:]) < 0 })
	return result
}

func (s *QualityStateStore) persistSnapshot(snapshot *flowpb.QualityStateSnapshot) error {
	if err := validateQualitySnapshot(snapshot, s.collectorID, s.config); err != nil {
		return err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal quality snapshot: %w", err)
	}
	frame, err := qualityFrame(qualitySnapshotMagic, payload, s.config.JournalMaxBytes)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(s.dir, ".quality-snapshot-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o640); err != nil {
		_ = temp.Close()
		return err
	}
	if err := writeFull(temp, frame); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, filepath.Join(s.dir, "quality.snapshot")); err != nil {
		return err
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	err = errors.Join(directory.Sync(), directory.Close())
	if err != nil {
		return fmt.Errorf("sync quality-state directory: %w", err)
	}
	return nil
}

func qualityFrame(magic [8]byte, payload []byte, maxBytes int64) ([]byte, error) {
	if len(payload) == 0 || uint64(len(payload)) > uint64(^uint32(0)) || int64(len(payload)) > maxBytes-qualityFrameHeaderSize {
		return nil, errors.New("quality-state frame exceeds size limit")
	}
	frame := make([]byte, qualityFrameHeaderSize+len(payload))
	copy(frame[:8], magic[:])
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[12:16], crc32.ChecksumIEEE(payload))
	copy(frame[qualityFrameHeaderSize:], payload)
	return frame, nil
}

func readQualitySnapshotFrame(path string, maxBytes int64) ([]byte, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, false, err
	}
	if info.Size() < qualityFrameHeaderSize || info.Size() > maxBytes {
		return nil, false, errors.New("quality snapshot size is invalid")
	}
	header := make([]byte, qualityFrameHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, false, err
	}
	if !bytes.Equal(header[:8], qualitySnapshotMagic[:]) {
		return nil, false, errors.New("quality snapshot magic is invalid")
	}
	payloadSize := int64(binary.BigEndian.Uint32(header[8:12]))
	if payloadSize+qualityFrameHeaderSize != info.Size() {
		return nil, false, errors.New("quality snapshot payload length is invalid")
	}
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(file, payload); err != nil {
		return nil, false, err
	}
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[12:16]) {
		return nil, false, errors.New("quality snapshot frame CRC mismatch")
	}
	return payload, true, nil
}
