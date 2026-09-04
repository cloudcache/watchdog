package flowcollect

import (
	"bytes"
	"context"
	"crypto/sha256"
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
)

const (
	attemptJournalVersion = uint16(1)
	attemptHeaderSize     = 64
	attemptRecordSize     = 48
)

var (
	attemptJournalMagic = [8]byte{'W', 'D', 'A', 'T', 'M', 'P', '0', '1'}
	attemptRecordMagic  = [4]byte{'A', 'T', 'M', '1'}
)

type AttemptStore struct {
	mu          sync.Mutex
	dir         string
	collectorID string
	config      DiagnosticsConfig
	wal         *WAL
	metrics     *Metrics
	journal     *os.File
	size        int64
	durableEnd  int64
	generations map[DatagramID]uint32
	targets     map[DatagramID]int64
	syncNotify  chan struct{}
	poisoned    error
	closed      bool
	stopSync    chan struct{}
	doneSync    chan struct{}
}

type AttemptStoreState struct {
	JournalBytes    int64
	MaxJournalBytes int64
	UsageRatio      float64
	Tracked         int
	Writable        bool
}

func OpenAttemptStore(dir, collectorID string, config DiagnosticsConfig, wal *WAL, metrics *Metrics) (*AttemptStore, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	collectorID = strings.TrimSpace(collectorID)
	if dir == "." || collectorID == "" || wal == nil || config.AttemptJournalFsync <= 0 || config.AttemptCheckpointEvery <= 0 || config.AttemptJournalMaxBytes < attemptHeaderSize+attemptRecordSize {
		return nil, errors.New("attempt-store dir, collector, WAL, and positive limits are required")
	}
	if wal.collectorID != collectorID {
		return nil, errors.New("attempt-store collector ID does not match the WAL")
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create attempt-store dir: %w", err)
	}
	pending, err := wal.pendingDatagramIDs()
	if err != nil {
		return nil, fmt.Errorf("read pending WAL IDs for attempt restore: %w", err)
	}
	path := filepath.Join(dir, "attempt.journal")
	journal, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open attempt journal: %w", err)
	}
	store := &AttemptStore{
		dir: dir, collectorID: collectorID, config: config, wal: wal, metrics: metrics, journal: journal,
		generations: make(map[DatagramID]uint32), targets: make(map[DatagramID]int64),
		syncNotify: make(chan struct{}), stopSync: make(chan struct{}), doneSync: make(chan struct{}),
	}
	if err := store.restore(pending); err != nil {
		_ = journal.Close()
		return nil, err
	}
	if store.size != int64(attemptHeaderSize+len(store.generations)*attemptRecordSize) {
		if err := store.rewriteLocked(); err != nil {
			_ = journal.Close()
			return nil, fmt.Errorf("compact restored attempt journal: %w", err)
		}
	}
	go store.syncLoop()
	return store, nil
}

func (s *AttemptStore) Generation(id DatagramID) (uint32, error) {
	if s == nil {
		return 0, errors.New("attempt store is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("attempt store is closed")
	}
	if s.poisoned != nil {
		return 0, fmt.Errorf("attempt store is not writable: %w", s.poisoned)
	}
	return s.generations[id], nil
}

// Advance durably records the generation for the next processing attempt.
// The caller must not retry or publish a terminal DLQ result until it returns.
func (s *AttemptStore) Advance(ctx context.Context, id DatagramID, next uint32) error {
	if s == nil || ctx == nil || next == 0 {
		return errors.New("attempt store, context, and positive generation are required")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("attempt store is closed")
	}
	if s.poisoned != nil {
		err := s.poisoned
		s.mu.Unlock()
		return fmt.Errorf("attempt store is not writable: %w", err)
	}
	current := s.generations[id]
	if next < current {
		s.mu.Unlock()
		return fmt.Errorf("attempt generation regressed from %d to %d", current, next)
	}
	if next == current {
		target := s.targets[id]
		s.mu.Unlock()
		return s.waitDurable(ctx, target)
	}
	if current == ^uint32(0) || next != current+1 {
		s.mu.Unlock()
		return fmt.Errorf("attempt generation skipped from %d to %d", current, next)
	}
	if s.size+attemptRecordSize > s.config.AttemptJournalMaxBytes {
		if err := s.compactLocked(); err != nil {
			s.poisonLocked(err)
			s.mu.Unlock()
			return err
		}
	}
	if s.size+attemptRecordSize > s.config.AttemptJournalMaxBytes {
		s.metrics.AttemptJournalFailures.Add(1)
		s.mu.Unlock()
		return errors.New("attempt journal hard limit reached")
	}
	record := marshalAttemptRecord(id, next)
	if err := writeFull(s.journal, record); err != nil {
		s.poisonLocked(err)
		s.mu.Unlock()
		return fmt.Errorf("append attempt journal: %w", err)
	}
	s.size += attemptRecordSize
	s.generations[id] = next
	s.targets[id] = s.size
	target := s.size
	s.metrics.AttemptJournalAppends.Add(1)
	s.mu.Unlock()
	return s.waitDurable(ctx, target)
}

func (s *AttemptStore) Compact() error {
	if s == nil {
		return errors.New("attempt store is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("attempt store is closed")
	}
	if s.poisoned != nil {
		return fmt.Errorf("attempt store is not writable: %w", s.poisoned)
	}
	if err := s.compactLocked(); err != nil {
		s.poisonLocked(err)
		return err
	}
	return nil
}

func (s *AttemptStore) compactLocked() error {
	// WAL acknowledgement durability is the deletion authority. Keeping the
	// attempt generation until after this sync prevents ACK loss plus process
	// crash from resetting a poison datagram's bounded retry count.
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("sync WAL before attempt checkpoint: %w", err)
	}
	for id := range s.generations {
		if s.wal.datagramDurablyAcknowledged(id) {
			delete(s.generations, id)
			delete(s.targets, id)
		}
	}
	return s.rewriteLocked()
}

func (s *AttemptStore) rewriteLocked() error {
	required := int64(attemptHeaderSize + len(s.generations)*attemptRecordSize)
	if required > s.config.AttemptJournalMaxBytes {
		return fmt.Errorf("pending attempts require %d bytes, journal limit is %d", required, s.config.AttemptJournalMaxBytes)
	}
	temporary, err := os.CreateTemp(s.dir, ".attempt-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := writeFull(temporary, marshalAttemptHeader(s.collectorID, time.Now())); err != nil {
		_ = temporary.Close()
		return err
	}
	ids := make([]DatagramID, 0, len(s.generations))
	for id := range s.generations {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	for _, id := range ids {
		if err := writeFull(temporary, marshalAttemptRecord(id, s.generations[id])); err != nil {
			_ = temporary.Close()
			return err
		}
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	path := filepath.Join(s.dir, "attempt.journal")
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return err
	}
	if err := s.journal.Close(); err != nil {
		return err
	}
	s.journal, err = os.OpenFile(path, os.O_RDWR, 0o640)
	if err != nil {
		return err
	}
	if _, err := s.journal.Seek(required, io.SeekStart); err != nil {
		return err
	}
	s.size, s.durableEnd = required, required
	for id := range s.targets {
		s.targets[id] = 0
	}
	close(s.syncNotify)
	s.syncNotify = make(chan struct{})
	s.metrics.AttemptStateCheckpoints.Add(1)
	return nil
}

func (s *AttemptStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("attempt store is closed")
	}
	if s.poisoned != nil {
		return fmt.Errorf("attempt store is not writable: %w", s.poisoned)
	}
	if err := s.journal.Sync(); err != nil {
		s.poisonLocked(err)
		return fmt.Errorf("sync attempt journal: %w", err)
	}
	s.markDurableLocked()
	return nil
}

func (s *AttemptStore) State() AttemptStoreState {
	if s == nil {
		return AttemptStoreState{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return AttemptStoreState{
		JournalBytes: s.size, MaxJournalBytes: s.config.AttemptJournalMaxBytes,
		UsageRatio: float64(s.size) / float64(s.config.AttemptJournalMaxBytes), Tracked: len(s.generations),
		Writable: !s.closed && s.poisoned == nil && s.size+attemptRecordSize <= s.config.AttemptJournalMaxBytes,
	}
}

func (s *AttemptStore) RestoredCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.generations)
}

func (s *AttemptStore) Close() error {
	if s == nil {
		return nil
	}
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

func (s *AttemptStore) restore(pending map[DatagramID]struct{}) error {
	info, err := s.journal.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		header := marshalAttemptHeader(s.collectorID, time.Now())
		if err := writeFull(s.journal, header); err != nil {
			return err
		}
		if err := s.journal.Sync(); err != nil {
			return err
		}
		if err := syncDirectory(s.dir); err != nil {
			return err
		}
		info, err = s.journal.Stat()
		if err != nil {
			return err
		}
	}
	if info.Size() < attemptHeaderSize || info.Size() > s.config.AttemptJournalMaxBytes {
		return errors.New("attempt journal size is invalid")
	}
	header := make([]byte, attemptHeaderSize)
	if _, err := s.journal.ReadAt(header, 0); err != nil {
		return err
	}
	if err := validateAttemptHeader(header, s.collectorID); err != nil {
		return err
	}
	valid := int64(attemptHeaderSize)
	seen := make(map[DatagramID]uint32)
	for valid < info.Size() {
		if info.Size()-valid < attemptRecordSize {
			break
		}
		record := make([]byte, attemptRecordSize)
		if _, err := s.journal.ReadAt(record, valid); err != nil {
			return err
		}
		id, generation, err := unmarshalAttemptRecord(record)
		if err != nil {
			return fmt.Errorf("attempt journal offset %d: %w", valid, err)
		}
		if previous := seen[id]; previous != 0 && generation != previous+1 {
			return fmt.Errorf("attempt journal generation for %s advanced from %d to %d", id, previous, generation)
		}
		seen[id] = generation
		if _, exists := pending[id]; exists {
			s.generations[id] = generation
			s.targets[id] = 0
		}
		valid += attemptRecordSize
	}
	if valid != info.Size() {
		if err := s.journal.Truncate(valid); err != nil {
			return fmt.Errorf("truncate incomplete attempt journal tail: %w", err)
		}
	}
	if _, err := s.journal.Seek(valid, io.SeekStart); err != nil {
		return err
	}
	if err := s.journal.Sync(); err != nil {
		return err
	}
	s.size, s.durableEnd = valid, valid
	if len(s.generations) > 0 {
		s.metrics.AttemptStateRestores.Add(uint64(len(s.generations)))
	}
	return nil
}

func (s *AttemptStore) waitDurable(ctx context.Context, target int64) error {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return errors.New("attempt store is closed")
		}
		if s.poisoned != nil {
			err := s.poisoned
			s.mu.Unlock()
			return fmt.Errorf("attempt journal sync failed: %w", err)
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

func (s *AttemptStore) syncLoop() {
	ticker := time.NewTicker(s.config.AttemptJournalFsync)
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

func (s *AttemptStore) markDurableLocked() {
	s.durableEnd = s.size
	close(s.syncNotify)
	s.syncNotify = make(chan struct{})
}

func (s *AttemptStore) poisonLocked(err error) {
	if s.poisoned == nil {
		s.poisoned = err
		s.metrics.AttemptJournalFailures.Add(1)
	}
	close(s.syncNotify)
	s.syncNotify = make(chan struct{})
}

func marshalAttemptHeader(collectorID string, createdAt time.Time) []byte {
	header := make([]byte, attemptHeaderSize)
	copy(header[:8], attemptJournalMagic[:])
	binary.BigEndian.PutUint16(header[8:10], attemptJournalVersion)
	binary.BigEndian.PutUint16(header[10:12], attemptHeaderSize)
	identity := sha256.Sum256([]byte(collectorID))
	copy(header[12:44], identity[:])
	binary.BigEndian.PutUint64(header[44:52], uint64(createdAt.UnixMilli()))
	binary.BigEndian.PutUint32(header[60:64], crc32.ChecksumIEEE(header[:60]))
	return header
}

func validateAttemptHeader(header []byte, collectorID string) error {
	if len(header) != attemptHeaderSize || !bytes.Equal(header[:8], attemptJournalMagic[:]) || binary.BigEndian.Uint16(header[8:10]) != attemptJournalVersion || binary.BigEndian.Uint16(header[10:12]) != attemptHeaderSize || binary.BigEndian.Uint32(header[60:64]) != crc32.ChecksumIEEE(header[:60]) {
		return errors.New("attempt journal header is invalid")
	}
	identity := sha256.Sum256([]byte(collectorID))
	if !bytes.Equal(header[12:44], identity[:]) {
		return errors.New("attempt journal collector identity mismatch")
	}
	return nil
}

func marshalAttemptRecord(id DatagramID, generation uint32) []byte {
	record := make([]byte, attemptRecordSize)
	copy(record[:4], attemptRecordMagic[:])
	copy(record[4:36], id[:])
	binary.BigEndian.PutUint32(record[36:40], generation)
	binary.BigEndian.PutUint32(record[44:48], crc32.ChecksumIEEE(record[:44]))
	return record
}

func unmarshalAttemptRecord(record []byte) (DatagramID, uint32, error) {
	if len(record) != attemptRecordSize || !bytes.Equal(record[:4], attemptRecordMagic[:]) || binary.BigEndian.Uint32(record[44:48]) != crc32.ChecksumIEEE(record[:44]) {
		return DatagramID{}, 0, errors.New("attempt journal record is invalid")
	}
	var id DatagramID
	copy(id[:], record[4:36])
	generation := binary.BigEndian.Uint32(record[36:40])
	if generation == 0 {
		return DatagramID{}, 0, errors.New("attempt journal generation is zero")
	}
	return id, generation, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
