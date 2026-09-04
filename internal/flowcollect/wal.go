package flowcollect

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	walFormatVersion = uint16(1)
	walHeaderSize    = 64
	walRecordPrefix  = 8
	ackRecordSize    = 48
)

var (
	walMagic        = [8]byte{'W', 'D', 'W', 'A', 'L', '0', '0', '1'}
	ackMagic        = [4]byte{'A', 'C', 'K', '2'}
	ErrWALHardLimit = errors.New("flow WAL hard watermark reached")
	ErrWALClosed    = errors.New("flow WAL is closed")
)

type DatagramID [32]byte

func (id DatagramID) String() string { return fmt.Sprintf("%x", id[:]) }

type WALInput struct {
	Protocol            Protocol
	ReceivedAt          time.Time
	Source              netip.AddrPort
	ObservationDomainID uint64
	RegistryVersion     uint64
	TenantID            string
	ExporterID          string
	TargetID            string
	DeviceID            string
	Payload             []byte
}

type WALRecord struct {
	DatagramID DatagramID
	Segment    uint64
	Offset     int64
	EndOffset  int64
	WALInput
}

type WALState struct {
	Bytes         int64
	MaxBytes      int64
	UsageRatio    float64
	SoftWatermark bool
	HardWatermark bool
	Writable      bool
	OldestAge     time.Duration
}

type ReplayCursor struct {
	Segment uint64
	Offset  int64
}

type WAL struct {
	mu           sync.Mutex
	scanMu       sync.Mutex
	dir          string
	collectorID  string
	config       WALConfig
	bootID       [16]byte
	segmentSeq   uint64
	segment      *os.File
	segmentSize  int64
	usage        int64
	ackFile      *os.File
	lockFile     *os.File
	acks         map[DatagramID]ackState
	durableAcks  map[DatagramID]ackState
	closed       bool
	stopSync     chan struct{}
	doneSync     chan struct{}
	durableSeq   uint64
	durableEnd   int64
	syncNotify   chan struct{}
	poisoned     error
	metrics      *Metrics
	hardStopped  bool
	segmentFirst map[uint64]time.Time
}

type ackState struct {
	childCount uint32
	children   map[uint32]struct{}
}

func OpenWAL(dir, collectorID string, config WALConfig) (*WAL, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	collectorID = strings.TrimSpace(collectorID)
	if dir == "." || collectorID == "" {
		return nil, errors.New("WAL dir and collector ID are required")
	}
	if config.MaxBytes <= 0 || config.MaxAge <= 0 || config.SegmentBytes <= 0 || config.SegmentBytes > config.MaxBytes || config.FsyncInterval <= 0 ||
		config.SoftWatermark <= 0 || config.SoftWatermark >= config.HardWatermark || config.HardWatermark >= 1 {
		return nil, errors.New("invalid WAL configuration")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	w := &WAL{dir: dir, collectorID: collectorID, config: config, acks: make(map[DatagramID]ackState), durableAcks: make(map[DatagramID]ackState), segmentFirst: make(map[uint64]time.Time), stopSync: make(chan struct{}), doneSync: make(chan struct{}), syncNotify: make(chan struct{})}
	lockFile, err := os.OpenFile(filepath.Join(dir, "wal.lock"), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open WAL lock: %w", err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("lock WAL directory: %w", err)
	}
	w.lockFile = lockFile
	if _, err := rand.Read(w.bootID[:]); err != nil {
		w.closeFiles()
		return nil, fmt.Errorf("create WAL boot ID: %w", err)
	}
	if err := w.loadAcknowledgements(); err != nil {
		w.closeFiles()
		return nil, err
	}
	segments, err := w.segmentFiles()
	if err != nil {
		w.closeFiles()
		return nil, err
	}
	for _, segment := range segments {
		seq, validSize, firstRecordAt, err := recoverSegment(segment)
		if err != nil {
			w.closeFiles()
			return nil, err
		}
		if seq > w.segmentSeq {
			w.segmentSeq = seq
		}
		w.usage += validSize
		if !firstRecordAt.IsZero() {
			w.segmentFirst[seq] = firstRecordAt
		}
	}
	w.segmentSeq++
	if err := w.createSegment(); err != nil {
		w.closeFiles()
		return nil, err
	}
	go w.syncLoop()
	return w, nil
}

func (w *WAL) Append(input WALInput) (WALRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return WALRecord{}, ErrWALClosed
	}
	if w.poisoned != nil {
		return WALRecord{}, fmt.Errorf("WAL is not writable: %w", w.poisoned)
	}
	if len(input.Payload) == 0 {
		return WALRecord{}, errors.New("WAL payload is empty")
	}
	payloadCRC := crc32.ChecksumIEEE(input.Payload)
	projectedBody, err := marshalWALBody(input, DatagramID{}, payloadCRC)
	if err != nil {
		return WALRecord{}, err
	}
	projected := int64(walRecordPrefix + len(projectedBody))
	if w.segmentSize > walHeaderSize && w.segmentSize+projected > w.config.SegmentBytes {
		if err := w.rotate(); err != nil {
			return WALRecord{}, err
		}
	}
	if w.usage+projected >= int64(float64(w.config.MaxBytes)*w.config.HardWatermark) {
		w.hardStopped = true
		return WALRecord{}, ErrWALHardLimit
	}
	offset := w.segmentSize
	id := makeDatagramID(w.collectorID, w.bootID, w.segmentSeq, offset, payloadCRC)
	body, err := marshalWALBody(input, id, payloadCRC)
	if err != nil {
		return WALRecord{}, err
	}
	prefix := make([]byte, walRecordPrefix)
	binary.BigEndian.PutUint32(prefix[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(prefix[4:8], crc32.ChecksumIEEE(body))
	if err := writeFull(w.segment, prefix); err != nil {
		w.markSyncErrorLocked(err)
		return WALRecord{}, fmt.Errorf("append WAL prefix: %w", err)
	}
	if err := writeFull(w.segment, body); err != nil {
		w.markSyncErrorLocked(err)
		return WALRecord{}, fmt.Errorf("append WAL body: %w", err)
	}
	written := int64(len(prefix) + len(body))
	w.segmentSize += written
	w.usage += written
	if _, exists := w.segmentFirst[w.segmentSeq]; !exists {
		w.segmentFirst[w.segmentSeq] = input.ReceivedAt
	}
	input.Payload = bytes.Clone(input.Payload)
	return WALRecord{DatagramID: id, Segment: w.segmentSeq, Offset: offset, EndOffset: w.segmentSize, WALInput: input}, nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWALClosed
	}
	if w.poisoned != nil {
		return fmt.Errorf("WAL is not writable: %w", w.poisoned)
	}
	if err := w.segment.Sync(); err != nil {
		w.markSyncErrorLocked(err)
		return err
	}
	if err := w.ackFile.Sync(); err != nil {
		w.markSyncErrorLocked(err)
		return err
	}
	w.markDurableLocked()
	return nil
}

func (w *WAL) WaitDurable(ctx context.Context, record WALRecord) error {
	for {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return ErrWALClosed
		}
		if w.poisoned != nil {
			err := w.poisoned
			w.mu.Unlock()
			return fmt.Errorf("WAL sync failed: %w", err)
		}
		if record.Segment < w.durableSeq || (record.Segment == w.durableSeq && record.EndOffset <= w.durableEnd) {
			w.mu.Unlock()
			return nil
		}
		notify := w.syncNotify
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (w *WAL) Acknowledge(id DatagramID) error {
	return w.AcknowledgeChild(id, 0, 1)
}

func (w *WAL) AcknowledgeChild(id DatagramID, childIndex, childCount uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWALClosed
	}
	if childCount == 0 || childIndex >= childCount {
		return errors.New("invalid WAL child acknowledgement")
	}
	state, exists := w.acks[id]
	if exists && state.childCount != childCount {
		return errors.New("WAL acknowledgement child count changed")
	}
	if _, exists := state.children[childIndex]; exists {
		return nil
	}
	record := marshalAcknowledgement(id, childIndex, childCount)
	if err := writeFull(w.ackFile, record); err != nil {
		return fmt.Errorf("append WAL acknowledgement: %w", err)
	}
	if state.children == nil {
		state = ackState{childCount: childCount, children: make(map[uint32]struct{})}
	}
	state.children[childIndex] = struct{}{}
	w.acks[id] = state
	return nil
}

func (w *WAL) ChildAcknowledged(id DatagramID, childIndex, childCount uint32) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	state, exists := w.acks[id]
	if !exists || state.childCount != childCount {
		return false
	}
	_, exists = state.children[childIndex]
	return exists
}

func (w *WAL) datagramDurablyAcknowledged(id DatagramID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return acknowledgementComplete(w.durableAcks[id])
}

// pendingDatagramIDs returns the durable recovery view: records still present
// in raw WAL without a durable terminal acknowledgement. A missing ID is not
// assumed pending because acknowledged segments may already be reclaimed.
func (w *WAL) pendingDatagramIDs() (map[DatagramID]struct{}, error) {
	w.scanMu.Lock()
	defer w.scanMu.Unlock()
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrWALClosed
	}
	if err := w.segment.Sync(); err != nil {
		w.markSyncErrorLocked(err)
		w.mu.Unlock()
		return nil, err
	}
	if err := w.ackFile.Sync(); err != nil {
		w.markSyncErrorLocked(err)
		w.mu.Unlock()
		return nil, err
	}
	w.markDurableLocked()
	segments, err := w.segmentFiles()
	if err != nil {
		w.mu.Unlock()
		return nil, err
	}
	completed := make(map[DatagramID]struct{}, len(w.durableAcks))
	for id, state := range w.durableAcks {
		if acknowledgementComplete(state) {
			completed[id] = struct{}{}
		}
	}
	w.mu.Unlock()
	pending := make(map[DatagramID]struct{})
	for _, path := range segments {
		if err := scanSegment(path, func(record WALRecord) error {
			if _, acknowledged := completed[record.DatagramID]; !acknowledged {
				pending[record.DatagramID] = struct{}{}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return pending, nil
}

func (w *WAL) Replay(fn func(WALRecord) error) error {
	var callbackErr error
	_, _, err := w.ReplayFrom(ReplayCursor{}, func(record WALRecord) bool {
		if err := fn(record); err != nil {
			callbackErr = err
			return false
		}
		return true
	})
	return errors.Join(callbackErr, err)
}

func (w *WAL) ReplayFrom(cursor ReplayCursor, fn func(WALRecord) bool) (ReplayCursor, bool, error) {
	w.scanMu.Lock()
	defer w.scanMu.Unlock()
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return cursor, false, ErrWALClosed
	}
	if err := w.segment.Sync(); err != nil {
		w.markSyncErrorLocked(err)
		w.mu.Unlock()
		return cursor, false, err
	}
	if err := w.ackFile.Sync(); err != nil {
		w.markSyncErrorLocked(err)
		w.mu.Unlock()
		return cursor, false, err
	}
	w.markDurableLocked()
	segments, err := w.segmentFiles()
	if err != nil {
		w.mu.Unlock()
		return cursor, false, err
	}
	completed := make(map[DatagramID]struct{}, len(w.acks))
	for id, state := range w.acks {
		if acknowledgementComplete(state) {
			completed[id] = struct{}{}
		}
	}
	w.mu.Unlock()
	visited := false
	for _, path := range segments {
		seq, err := segmentSequence(path)
		if err != nil {
			return cursor, false, err
		}
		if cursor.Segment != 0 && seq < cursor.Segment {
			continue
		}
		start := int64(walHeaderSize)
		if seq == cursor.Segment && cursor.Offset >= walHeaderSize {
			start = cursor.Offset
		}
		visited = true
		next, stopped, err := scanSegmentFrom(path, start, func(record WALRecord) bool {
			if _, exists := completed[record.DatagramID]; exists {
				return true
			}
			return fn(record)
		})
		if err != nil {
			return cursor, false, err
		}
		cursor = ReplayCursor{Segment: seq, Offset: next}
		if stopped {
			return cursor, false, nil
		}
	}
	return cursor, visited, nil
}

func (w *WAL) Reclaim() (int, error) {
	w.scanMu.Lock()
	defer w.scanMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWALClosed
	}
	if err := w.ackFile.Sync(); err != nil {
		w.markSyncErrorLocked(err)
		return 0, err
	}
	w.cloneDurableAcksLocked()
	segments, err := w.segmentFiles()
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, path := range segments {
		seq, err := segmentSequence(path)
		if err != nil {
			return removed, err
		}
		if seq == w.segmentSeq {
			continue
		}
		allAcked := true
		segmentIDs := make([]DatagramID, 0)
		if err := scanSegment(path, func(record WALRecord) error {
			segmentIDs = append(segmentIDs, record.DatagramID)
			if state, exists := w.durableAcks[record.DatagramID]; !exists || !acknowledgementComplete(state) {
				allAcked = false
			}
			return nil
		}); err != nil {
			return removed, err
		}
		if !allAcked {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			return removed, err
		}
		if err := os.Remove(path); err != nil {
			return removed, err
		}
		w.usage -= info.Size()
		delete(w.segmentFirst, seq)
		for _, id := range segmentIDs {
			delete(w.acks, id)
			delete(w.durableAcks, id)
		}
		removed++
	}
	if removed > 0 {
		if err := w.rewriteAcknowledgementsLocked(); err != nil {
			return removed, err
		}
		if float64(w.usage)/float64(w.config.MaxBytes) < w.config.SoftWatermark {
			w.hardStopped = false
		}
	}
	return removed, nil
}

func (w *WAL) State() WALState {
	w.mu.Lock()
	defer w.mu.Unlock()
	ratio := float64(w.usage) / float64(w.config.MaxBytes)
	oldest := time.Time{}
	for _, firstRecordAt := range w.segmentFirst {
		if oldest.IsZero() || firstRecordAt.Before(oldest) {
			oldest = firstRecordAt
		}
	}
	oldestAge := time.Duration(0)
	if !oldest.IsZero() {
		oldestAge = max(time.Since(oldest), 0)
	}
	return WALState{Bytes: w.usage, MaxBytes: w.config.MaxBytes, UsageRatio: ratio, SoftWatermark: ratio >= w.config.SoftWatermark, HardWatermark: ratio >= w.config.HardWatermark || w.hardStopped, Writable: !w.closed && w.poisoned == nil, OldestAge: oldestAge}
}

func (w *WAL) SetMetrics(metrics *Metrics) {
	w.mu.Lock()
	w.metrics = metrics
	w.mu.Unlock()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.stopSync)
	err1 := w.segment.Sync()
	errAckSync := w.ackFile.Sync()
	if err1 == nil && errAckSync == nil {
		w.markDurableLocked()
	}
	err2 := w.segment.Close()
	err3 := w.ackFile.Close()
	err4 := unix.Flock(int(w.lockFile.Fd()), unix.LOCK_UN)
	err5 := w.lockFile.Close()
	w.mu.Unlock()
	<-w.doneSync
	return errors.Join(err1, errAckSync, err2, err3, err4, err5)
}

func (w *WAL) syncLoop() {
	ticker := time.NewTicker(w.config.FsyncInterval)
	defer func() { ticker.Stop(); close(w.doneSync) }()
	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if !w.closed {
				startedAt := time.Now()
				segmentErr := w.segment.Sync()
				ackErr := w.ackFile.Sync()
				if w.metrics != nil {
					w.metrics.observeWALFsync(time.Since(startedAt))
				}
				if err := errors.Join(segmentErr, ackErr); err == nil {
					w.markDurableLocked()
				} else {
					w.markSyncErrorLocked(err)
				}
			}
			w.mu.Unlock()
		case <-w.stopSync:
			return
		}
	}
}

func (w *WAL) createSegment() error {
	path := filepath.Join(w.dir, fmt.Sprintf("segment-%020d.wal", w.segmentSeq))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if err != nil {
		return fmt.Errorf("create WAL segment: %w", err)
	}
	header := make([]byte, walHeaderSize)
	copy(header[:8], walMagic[:])
	binary.BigEndian.PutUint16(header[8:10], walFormatVersion)
	binary.BigEndian.PutUint16(header[10:12], walHeaderSize)
	binary.BigEndian.PutUint64(header[12:20], w.segmentSeq)
	copy(header[20:36], w.bootID[:])
	binary.BigEndian.PutUint64(header[36:44], uint64(time.Now().UnixMilli()))
	binary.BigEndian.PutUint32(header[60:64], crc32.ChecksumIEEE(header[:60]))
	if err := writeFull(file, header); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	w.segment = file
	w.segmentSize = walHeaderSize
	w.usage += walHeaderSize
	w.markDurableLocked()
	return nil
}

func (w *WAL) rotate() error {
	if err := w.segment.Sync(); err != nil {
		return err
	}
	w.markDurableLocked()
	if err := w.segment.Close(); err != nil {
		return err
	}
	w.segmentSeq++
	return w.createSegment()
}

func (w *WAL) markDurableLocked() {
	w.durableSeq, w.durableEnd = w.segmentSeq, w.segmentSize
	w.cloneDurableAcksLocked()
	close(w.syncNotify)
	w.syncNotify = make(chan struct{})
}

func (w *WAL) cloneDurableAcksLocked() {
	w.durableAcks = make(map[DatagramID]ackState, len(w.acks))
	for id, state := range w.acks {
		children := make(map[uint32]struct{}, len(state.children))
		for child := range state.children {
			children[child] = struct{}{}
		}
		w.durableAcks[id] = ackState{childCount: state.childCount, children: children}
	}
}

func (w *WAL) markSyncErrorLocked(err error) {
	if w.poisoned == nil {
		w.poisoned = err
	}
	close(w.syncNotify)
	w.syncNotify = make(chan struct{})
}

func (w *WAL) loadAcknowledgements() error {
	path := filepath.Join(w.dir, "ack.log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return err
	}
	valid := 0
	for valid+ackRecordSize <= len(data) {
		record := data[valid : valid+ackRecordSize]
		if !bytes.Equal(record[:4], ackMagic[:]) || binary.BigEndian.Uint32(record[44:48]) != crc32.ChecksumIEEE(record[:44]) {
			break
		}
		var id DatagramID
		copy(id[:], record[4:36])
		childIndex := binary.BigEndian.Uint32(record[36:40])
		childCount := binary.BigEndian.Uint32(record[40:44])
		if childCount == 0 || childIndex >= childCount {
			break
		}
		state, exists := w.acks[id]
		if exists && state.childCount != childCount {
			break
		}
		if state.children == nil {
			state = ackState{childCount: childCount, children: make(map[uint32]struct{})}
		}
		state.children[childIndex] = struct{}{}
		w.acks[id] = state
		valid += ackRecordSize
	}
	if valid != len(data) {
		if err := file.Truncate(int64(valid)); err != nil {
			file.Close()
			return err
		}
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return err
	}
	w.ackFile = file
	return nil
}

func (w *WAL) rewriteAcknowledgementsLocked() error {
	tempPath := filepath.Join(w.dir, "ack.log.tmp")
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o640)
	if err != nil {
		return fmt.Errorf("create compacted WAL acknowledgement journal: %w", err)
	}
	ids := make([]DatagramID, 0, len(w.acks))
	for id := range w.acks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	for _, id := range ids {
		state := w.acks[id]
		children := make([]int, 0, len(state.children))
		for child := range state.children {
			children = append(children, int(child))
		}
		sort.Ints(children)
		for _, child := range children {
			record := marshalAcknowledgement(id, uint32(child), state.childCount)
			if err := writeFull(file, record); err != nil {
				_ = file.Close()
				_ = os.Remove(tempPath)
				return err
			}
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	ackPath := filepath.Join(w.dir, "ack.log")
	if err := os.Rename(tempPath, ackPath); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if directory, err := os.Open(w.dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	if err := w.ackFile.Close(); err != nil {
		return err
	}
	w.ackFile, err = os.OpenFile(ackPath, os.O_RDWR, 0o640)
	if err != nil {
		return err
	}
	_, err = w.ackFile.Seek(0, io.SeekEnd)
	return err
}

func marshalAcknowledgement(id DatagramID, childIndex, childCount uint32) []byte {
	record := make([]byte, ackRecordSize)
	copy(record[:4], ackMagic[:])
	copy(record[4:36], id[:])
	binary.BigEndian.PutUint32(record[36:40], childIndex)
	binary.BigEndian.PutUint32(record[40:44], childCount)
	binary.BigEndian.PutUint32(record[44:48], crc32.ChecksumIEEE(record[:44]))
	return record
}

func acknowledgementComplete(state ackState) bool {
	return state.childCount > 0 && uint32(len(state.children)) == state.childCount
}

func (w *WAL) segmentFiles() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "segment-") || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		paths = append(paths, filepath.Join(w.dir, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

func (w *WAL) closeFiles() {
	if w.segment != nil {
		_ = w.segment.Close()
	}
	if w.ackFile != nil {
		_ = w.ackFile.Close()
	}
	if w.lockFile != nil {
		_ = unix.Flock(int(w.lockFile.Fd()), unix.LOCK_UN)
		_ = w.lockFile.Close()
	}
}

func recoverSegment(path string) (uint64, int64, time.Time, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	defer file.Close()
	seq, err := readSegmentHeader(file)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("recover %s: %w", path, err)
	}
	valid := int64(walHeaderSize)
	firstRecordAt := time.Time{}
	reader := bufio.NewReader(file)
	for {
		prefix := make([]byte, walRecordPrefix)
		_, err := io.ReadFull(reader, prefix)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			break
		}
		length := binary.BigEndian.Uint32(prefix[:4])
		if length == 0 || length > 16<<20 {
			break
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			break
		}
		if binary.BigEndian.Uint32(prefix[4:8]) != crc32.ChecksumIEEE(body) {
			break
		}
		record, err := unmarshalWALBody(body)
		if err != nil {
			break
		}
		if firstRecordAt.IsZero() {
			firstRecordAt = record.ReceivedAt
		}
		valid += int64(walRecordPrefix) + int64(length)
	}
	info, err := file.Stat()
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	if info.Size() != valid {
		if err := file.Truncate(valid); err != nil {
			return 0, 0, time.Time{}, err
		}
	}
	return seq, valid, firstRecordAt, nil
}

func scanSegment(path string, fn func(WALRecord) error) error {
	var callbackErr error
	_, _, err := scanSegmentFrom(path, walHeaderSize, func(record WALRecord) bool {
		callbackErr = fn(record)
		return callbackErr == nil
	})
	return errors.Join(callbackErr, err)
}

func scanSegmentFrom(path string, start int64, fn func(WALRecord) bool) (int64, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return start, false, err
	}
	defer file.Close()
	seq, err := readSegmentHeader(file)
	if err != nil {
		return start, false, err
	}
	if start < walHeaderSize {
		start = walHeaderSize
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return start, false, err
	}
	reader := bufio.NewReader(file)
	offset := start
	for {
		prefix := make([]byte, walRecordPrefix)
		_, err := io.ReadFull(reader, prefix)
		if errors.Is(err, io.EOF) {
			return offset, false, nil
		}
		if err != nil {
			return offset, false, err
		}
		length := binary.BigEndian.Uint32(prefix[:4])
		if length == 0 || length > 16<<20 {
			return offset, false, errors.New("invalid WAL record length")
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			return offset, false, err
		}
		if binary.BigEndian.Uint32(prefix[4:8]) != crc32.ChecksumIEEE(body) {
			return offset, false, errors.New("WAL record checksum mismatch")
		}
		record, err := unmarshalWALBody(body)
		if err != nil {
			return offset, false, err
		}
		record.Segment, record.Offset, record.EndOffset = seq, offset, offset+int64(walRecordPrefix)+int64(length)
		if !fn(record) {
			return offset, true, nil
		}
		offset = record.EndOffset
	}
}

func readSegmentHeader(reader io.Reader) (uint64, error) {
	header := make([]byte, walHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, err
	}
	if !bytes.Equal(header[:8], walMagic[:]) {
		return 0, errors.New("invalid WAL magic")
	}
	if binary.BigEndian.Uint16(header[8:10]) != walFormatVersion || binary.BigEndian.Uint16(header[10:12]) != walHeaderSize {
		return 0, errors.New("unsupported WAL format")
	}
	if binary.BigEndian.Uint32(header[60:64]) != crc32.ChecksumIEEE(header[:60]) {
		return 0, errors.New("WAL header checksum mismatch")
	}
	return binary.BigEndian.Uint64(header[12:20]), nil
}

func segmentSequence(path string) (uint64, error) {
	name := filepath.Base(path)
	value := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".wal")
	return strconv.ParseUint(value, 10, 64)
}

func makeDatagramID(collector string, boot [16]byte, segment uint64, offset int64, payloadCRC uint32) DatagramID {
	h := sha256.New()
	writeHashPart(h, []byte(collector))
	writeHashPart(h, boot[:])
	var fields [20]byte
	binary.BigEndian.PutUint64(fields[0:8], segment)
	binary.BigEndian.PutUint64(fields[8:16], uint64(offset))
	binary.BigEndian.PutUint32(fields[16:20], payloadCRC)
	writeHashPart(h, fields[:])
	var id DatagramID
	copy(id[:], h.Sum(nil))
	return id
}

func writeHashPart(writer io.Writer, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func marshalWALBody(input WALInput, id DatagramID, payloadCRC uint32) ([]byte, error) {
	if !input.Source.IsValid() {
		return nil, errors.New("WAL source address is invalid")
	}
	stringsToWrite := []string{input.TenantID, input.ExporterID, input.TargetID, input.DeviceID}
	for _, value := range stringsToWrite {
		if len(value) > 65535 {
			return nil, errors.New("WAL metadata field exceeds 65535 bytes")
		}
	}
	buf := bytes.NewBuffer(make([]byte, 0, 128+len(input.Payload)))
	_ = binary.Write(buf, binary.BigEndian, walFormatVersion)
	buf.WriteByte(byte(input.Protocol))
	buf.WriteByte(0)
	_ = binary.Write(buf, binary.BigEndian, input.ReceivedAt.UnixMilli())
	ip := input.Source.Addr().As16()
	buf.Write(ip[:])
	_ = binary.Write(buf, binary.BigEndian, input.Source.Port())
	_ = binary.Write(buf, binary.BigEndian, input.ObservationDomainID)
	_ = binary.Write(buf, binary.BigEndian, input.RegistryVersion)
	buf.Write(id[:])
	_ = binary.Write(buf, binary.BigEndian, payloadCRC)
	for _, value := range stringsToWrite {
		_ = binary.Write(buf, binary.BigEndian, uint16(len(value)))
		buf.WriteString(value)
	}
	_ = binary.Write(buf, binary.BigEndian, uint32(len(input.Payload)))
	buf.Write(input.Payload)
	return buf.Bytes(), nil
}

func unmarshalWALBody(body []byte) (WALRecord, error) {
	reader := bytes.NewReader(body)
	var version uint16
	if err := binary.Read(reader, binary.BigEndian, &version); err != nil || version != walFormatVersion {
		return WALRecord{}, errors.New("unsupported WAL record format")
	}
	protocol, err := reader.ReadByte()
	if err != nil {
		return WALRecord{}, err
	}
	if _, err := reader.ReadByte(); err != nil {
		return WALRecord{}, err
	}
	var receivedMS int64
	if err := binary.Read(reader, binary.BigEndian, &receivedMS); err != nil {
		return WALRecord{}, err
	}
	var ip16 [16]byte
	if _, err := io.ReadFull(reader, ip16[:]); err != nil {
		return WALRecord{}, err
	}
	var port uint16
	var domain, registry uint64
	if err := binary.Read(reader, binary.BigEndian, &port); err != nil {
		return WALRecord{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &domain); err != nil {
		return WALRecord{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &registry); err != nil {
		return WALRecord{}, err
	}
	var id DatagramID
	if _, err := io.ReadFull(reader, id[:]); err != nil {
		return WALRecord{}, err
	}
	var payloadCRC uint32
	if err := binary.Read(reader, binary.BigEndian, &payloadCRC); err != nil {
		return WALRecord{}, err
	}
	values := make([]string, 4)
	for i := range values {
		var length uint16
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return WALRecord{}, err
		}
		value := make([]byte, length)
		if _, err := io.ReadFull(reader, value); err != nil {
			return WALRecord{}, err
		}
		values[i] = string(value)
	}
	var payloadLength uint32
	if err := binary.Read(reader, binary.BigEndian, &payloadLength); err != nil {
		return WALRecord{}, err
	}
	if uint64(payloadLength) > uint64(reader.Len()) {
		return WALRecord{}, errors.New("invalid WAL payload length")
	}
	payload := make([]byte, payloadLength)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return WALRecord{}, err
	}
	if reader.Len() != 0 || crc32.ChecksumIEEE(payload) != payloadCRC {
		return WALRecord{}, errors.New("invalid WAL payload checksum")
	}
	addr := netip.AddrFrom16(ip16)
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	return WALRecord{DatagramID: id, WALInput: WALInput{Protocol: Protocol(protocol), ReceivedAt: time.UnixMilli(receivedMS), Source: netip.AddrPortFrom(addr, port), ObservationDomainID: domain, RegistryVersion: registry, TenantID: values[0], ExporterID: values[1], TargetID: values[2], DeviceID: values[3], Payload: payload}}, nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
