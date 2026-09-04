package flowcollect

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

const qualityStateSchemaVersion = 1

func (q *QualityTracker) BuildJournalRecord(record WALRecord, decoded DecodedDatagram, collectorID string) (*flowpb.QualityJournalRecord, error) {
	if zeroDatagramID(record.DatagramID) || record.Segment == 0 || record.Offset < walHeaderSize || collectorID == "" {
		return nil, errors.New("quality journal requires a persisted WAL record")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	key := qualityKey(record, decoded)
	journal := &flowpb.QualityJournalRecord{
		StateSchemaVersion: qualityStateSchemaVersion,
		DatagramId:         bytes.Clone(record.DatagramID[:]),
		WalSegment:         record.Segment,
		WalOffset:          record.Offset,
		CollectorId:        collectorID,
		Decision:           qualityDecisionToProto(record.DatagramID, decoded),
	}
	if state := q.exporters[key]; state != nil {
		journal.ExporterState = exporterStateToProto(key, state)
	}
	if decoded.Protocol == ProtocolSFlow5 {
		seen := make(map[sourceQualityKey]struct{})
		for _, decodedRecord := range decoded.Records {
			sourceKey := sourceQualityKey{exporter: key, sourceType: decodedRecord.SourceIDType, sourceValue: decodedRecord.SourceIDValue}
			if _, exists := seen[sourceKey]; exists {
				continue
			}
			seen[sourceKey] = struct{}{}
			if state := q.sources[sourceKey]; state != nil {
				journal.SourceStates = append(journal.SourceStates, sourceStateToProto(sourceKey, state))
			}
		}
	}
	digest, err := qualityJournalDigest(journal)
	if err != nil {
		return nil, err
	}
	journal.PayloadSha256 = digest[:]
	return journal, nil
}

func (q *QualityTracker) BuildSnapshot(collectorID string, createdAt time.Time, pending []*flowpb.QualityDecision) (*flowpb.QualityStateSnapshot, error) {
	if collectorID == "" {
		return nil, errors.New("collector ID is required for quality snapshot")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	snapshot := &flowpb.QualityStateSnapshot{StateSchemaVersion: qualityStateSchemaVersion, CollectorId: collectorID, CreatedAtUnixMs: createdAt.UnixMilli()}
	for key, state := range q.exporters {
		snapshot.Exporters = append(snapshot.Exporters, exporterStateToProto(key, state))
	}
	for key, state := range q.sources {
		if q.exporters[key.exporter] != nil {
			snapshot.Sources = append(snapshot.Sources, sourceStateToProto(key, state))
		}
	}
	sort.Slice(snapshot.Exporters, func(i, j int) bool { return qualityKeyProtoLess(snapshot.Exporters[i].Key, snapshot.Exporters[j].Key) })
	sort.Slice(snapshot.Sources, func(i, j int) bool {
		left, right := snapshot.Sources[i], snapshot.Sources[j]
		if qualityKeyProtoLess(left.Exporter, right.Exporter) {
			return true
		}
		if qualityKeyProtoLess(right.Exporter, left.Exporter) {
			return false
		}
		if left.SourceIdType != right.SourceIdType {
			return left.SourceIdType < right.SourceIdType
		}
		return left.SourceIdValue < right.SourceIdValue
	})
	snapshot.PendingDecisions = make([]*flowpb.QualityDecision, 0, len(pending))
	for _, decision := range pending {
		if _, _, err := qualityDecisionFromProto(decision); err != nil {
			return nil, err
		}
		snapshot.PendingDecisions = append(snapshot.PendingDecisions, proto.Clone(decision).(*flowpb.QualityDecision))
	}
	sort.Slice(snapshot.PendingDecisions, func(i, j int) bool {
		return bytes.Compare(snapshot.PendingDecisions[i].DatagramId, snapshot.PendingDecisions[j].DatagramId) < 0
	})
	digest, err := qualitySnapshotDigest(snapshot)
	if err != nil {
		return nil, err
	}
	snapshot.PayloadSha256 = digest[:]
	return snapshot, nil
}

func (q *QualityTracker) RestoreSnapshot(snapshot *flowpb.QualityStateSnapshot, collectorID string) error {
	if err := validateQualitySnapshot(snapshot, collectorID, q.config); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.exporters = make(map[exporterQualityKey]*exporterQualityState, len(snapshot.Exporters))
	q.sources = make(map[sourceQualityKey]*sourceQualityState, len(snapshot.Sources))
	for _, encoded := range snapshot.Exporters {
		key, state, err := exporterStateFromProto(encoded)
		if err != nil {
			return err
		}
		if _, duplicate := q.exporters[key]; duplicate {
			return errors.New("quality snapshot contains duplicate exporter state")
		}
		q.exporters[key] = state
	}
	for _, encoded := range snapshot.Sources {
		key, state, err := sourceStateFromProto(encoded)
		if err != nil {
			return err
		}
		if q.exporters[key.exporter] == nil {
			return errors.New("quality source state references a missing exporter")
		}
		if _, duplicate := q.sources[key]; duplicate {
			return errors.New("quality snapshot contains duplicate source state")
		}
		q.sources[key] = state
	}
	return nil
}

func (q *QualityTracker) ApplyJournalRecord(journal *flowpb.QualityJournalRecord) error {
	if err := validateQualityJournalRecord(journal); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if journal.ExporterState != nil {
		key, state, err := exporterStateFromProto(journal.ExporterState)
		if err != nil {
			return err
		}
		if _, exists := q.exporters[key]; !exists && len(q.exporters) >= q.config.MaxExporters {
			return errors.New("quality journal exceeds exporter state capacity")
		}
		q.exporters[key] = state
	}
	for _, encoded := range journal.SourceStates {
		key, state, err := sourceStateFromProto(encoded)
		if err != nil {
			return err
		}
		if q.exporters[key.exporter] == nil {
			return errors.New("quality journal source references a missing exporter")
		}
		if _, exists := q.sources[key]; !exists && len(q.sources) >= q.config.MaxDataSources {
			return errors.New("quality journal exceeds source state capacity")
		}
		q.sources[key] = state
	}
	return nil
}

func (q *QualityTracker) RememberDecision(decision *flowpb.QualityDecision) error {
	id, observation, err := qualityDecisionFromProto(decision)
	if err != nil {
		return err
	}
	q.mu.Lock()
	q.observed[id] = observation
	q.mu.Unlock()
	return nil
}

func qualityKey(record WALRecord, decoded DecodedDatagram) exporterQualityKey {
	return exporterQualityKey{protocol: decoded.Protocol, source: record.Source.Addr().Unmap(), scope: decoded.SequenceScope, agent: decoded.AgentIP.Unmap(), subAgent: decoded.SubAgentID}
}

func qualityDecisionToProto(id DatagramID, decoded DecodedDatagram) *flowpb.QualityDecision {
	decision := &flowpb.QualityDecision{DatagramId: bytes.Clone(id[:]), ExporterEpoch: decoded.ExporterEpoch, RecordFlags: make([]uint64, len(decoded.Records)), RecordEpochs: make([]uint64, len(decoded.Records))}
	for index := range decoded.Records {
		decision.RecordFlags[index] = decoded.Records[index].QualityFlags
		decision.RecordEpochs[index] = decoded.Records[index].QualityEpoch
	}
	return decision
}

func qualityDecisionFromProto(decision *flowpb.QualityDecision) (DatagramID, qualityObservation, error) {
	if decision == nil || len(decision.DatagramId) != sha256.Size || len(decision.RecordFlags) != len(decision.RecordEpochs) {
		return DatagramID{}, qualityObservation{}, errors.New("quality decision is invalid")
	}
	var id DatagramID
	copy(id[:], decision.DatagramId)
	if zeroDatagramID(id) {
		return DatagramID{}, qualityObservation{}, errors.New("quality decision datagram ID is empty")
	}
	return id, qualityObservation{exporterEpoch: decision.ExporterEpoch, flags: append([]uint64(nil), decision.RecordFlags...), epochs: append([]uint64(nil), decision.RecordEpochs...)}, nil
}

func exporterKeyToProto(key exporterQualityKey) *flowpb.QualityExporterKey {
	return &flowpb.QualityExporterKey{Protocol: uint32(key.protocol), SourceIp: address16(key.source), SequenceScope: key.scope, AgentIp: address16(key.agent), SubAgentId: key.subAgent}
}

func exporterKeyFromProto(encoded *flowpb.QualityExporterKey) (exporterQualityKey, error) {
	if encoded == nil {
		return exporterQualityKey{}, errors.New("quality exporter key is missing")
	}
	protocol := Protocol(encoded.Protocol)
	if protocol < ProtocolSFlow5 || protocol > ProtocolIPFIX {
		return exporterQualityKey{}, errors.New("quality exporter protocol is invalid")
	}
	source, ok := netip.AddrFromSlice(encoded.SourceIp)
	if !ok {
		return exporterQualityKey{}, errors.New("quality exporter source IP is invalid")
	}
	agent := netip.Addr{}
	if len(encoded.AgentIp) > 0 {
		var valid bool
		agent, valid = netip.AddrFromSlice(encoded.AgentIp)
		if !valid {
			return exporterQualityKey{}, errors.New("quality exporter agent IP is invalid")
		}
		agent = agent.Unmap()
	}
	return exporterQualityKey{protocol: protocol, source: source.Unmap(), scope: encoded.SequenceScope, agent: agent, subAgent: encoded.SubAgentId}, nil
}

func exporterStateToProto(key exporterQualityKey, state *exporterQualityState) *flowpb.QualityExporterState {
	return &flowpb.QualityExporterState{
		Key: exporterKeyToProto(key), Initialized: state.initialized, ExpectedSequence: state.expectedSequence,
		Uptime: state.uptime, UptimeValid: state.uptimeValid, LastSeenUnixMs: timeToUnixMilli(state.lastSeen), Epoch: state.epoch,
		SamplingEpoch: state.samplingEpoch, SamplingRate: state.samplingRate, SamplingRateKnown: state.samplingRateKnown, Windows: windowsToProto(state.windows),
	}
}

func exporterStateFromProto(encoded *flowpb.QualityExporterState) (exporterQualityKey, *exporterQualityState, error) {
	if encoded == nil {
		return exporterQualityKey{}, nil, errors.New("quality exporter state is missing")
	}
	key, err := exporterKeyFromProto(encoded.GetKey())
	if err != nil {
		return exporterQualityKey{}, nil, err
	}
	if encoded.Epoch == 0 || encoded.SamplingEpoch == 0 || (encoded.Initialized && encoded.LastSeenUnixMs == 0) {
		return exporterQualityKey{}, nil, errors.New("quality exporter state is invalid")
	}
	return key, &exporterQualityState{initialized: encoded.Initialized, expectedSequence: encoded.ExpectedSequence, uptime: encoded.Uptime, uptimeValid: encoded.UptimeValid, lastSeen: unixMilliToTime(encoded.LastSeenUnixMs), epoch: encoded.Epoch, samplingEpoch: encoded.SamplingEpoch, samplingRate: encoded.SamplingRate, samplingRateKnown: encoded.SamplingRateKnown, windows: windowsFromProto(encoded.Windows)}, nil
}

func sourceStateToProto(key sourceQualityKey, state *sourceQualityState) *flowpb.QualitySourceState {
	return &flowpb.QualitySourceState{
		Exporter: exporterKeyToProto(key.exporter), SourceIdType: key.sourceType, SourceIdValue: key.sourceValue,
		Initialized: state.initialized, ExpectedSequence: state.expectedSequence, SamplePool: state.samplePool, Drops: state.drops,
		SamplingRate: state.samplingRate, RateKnown: state.rateKnown, LastSeenUnixMs: timeToUnixMilli(state.lastSeen), ExporterEpoch: state.exporterEpoch, Epoch: state.epoch, Windows: windowsToProto(state.windows),
	}
}

func sourceStateFromProto(encoded *flowpb.QualitySourceState) (sourceQualityKey, *sourceQualityState, error) {
	if encoded == nil {
		return sourceQualityKey{}, nil, errors.New("quality source state is missing")
	}
	exporter, err := exporterKeyFromProto(encoded.GetExporter())
	if err != nil {
		return sourceQualityKey{}, nil, err
	}
	if encoded.Epoch == 0 || encoded.ExporterEpoch == 0 || (encoded.Initialized && encoded.LastSeenUnixMs == 0) {
		return sourceQualityKey{}, nil, errors.New("quality source state is invalid")
	}
	key := sourceQualityKey{exporter: exporter, sourceType: encoded.SourceIdType, sourceValue: encoded.SourceIdValue}
	state := &sourceQualityState{initialized: encoded.Initialized, expectedSequence: encoded.ExpectedSequence, samplePool: encoded.SamplePool, drops: encoded.Drops, samplingRate: encoded.SamplingRate, rateKnown: encoded.RateKnown, lastSeen: unixMilliToTime(encoded.LastSeenUnixMs), exporterEpoch: encoded.ExporterEpoch, epoch: encoded.Epoch, windows: windowsFromProto(encoded.Windows)}
	return key, state, nil
}

func windowsToProto(windows qualityWindows) *flowpb.QualityWindows {
	return &flowpb.QualityWindows{TemplateUntilUnixMs: timeToUnixMilli(windows.template), GapUntilUnixMs: timeToUnixMilli(windows.gap), PoolResetUntilUnixMs: timeToUnixMilli(windows.poolReset), RateChangeUntilUnixMs: timeToUnixMilli(windows.rateChange), RestartUntilUnixMs: timeToUnixMilli(windows.restart), OutOfOrderUntilUnixMs: timeToUnixMilli(windows.outOfOrder)}
}

func windowsFromProto(windows *flowpb.QualityWindows) qualityWindows {
	if windows == nil {
		return qualityWindows{}
	}
	return qualityWindows{template: unixMilliToTime(windows.TemplateUntilUnixMs), gap: unixMilliToTime(windows.GapUntilUnixMs), poolReset: unixMilliToTime(windows.PoolResetUntilUnixMs), rateChange: unixMilliToTime(windows.RateChangeUntilUnixMs), restart: unixMilliToTime(windows.RestartUntilUnixMs), outOfOrder: unixMilliToTime(windows.OutOfOrderUntilUnixMs)}
}

func timeToUnixMilli(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}

func unixMilliToTime(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}

func qualityKeyProtoLess(left, right *flowpb.QualityExporterKey) bool {
	if left.Protocol != right.Protocol {
		return left.Protocol < right.Protocol
	}
	if comparison := bytes.Compare(left.SourceIp, right.SourceIp); comparison != 0 {
		return comparison < 0
	}
	if left.SequenceScope != right.SequenceScope {
		return left.SequenceScope < right.SequenceScope
	}
	if comparison := bytes.Compare(left.AgentIp, right.AgentIp); comparison != 0 {
		return comparison < 0
	}
	return left.SubAgentId < right.SubAgentId
}

func validateQualityJournalRecord(journal *flowpb.QualityJournalRecord) error {
	if journal == nil || journal.StateSchemaVersion != qualityStateSchemaVersion || journal.CollectorId == "" || len(journal.DatagramId) != sha256.Size || journal.WalSegment == 0 || journal.WalOffset < walHeaderSize || journal.Decision == nil || !bytes.Equal(journal.DatagramId, journal.Decision.DatagramId) || len(journal.PayloadSha256) != sha256.Size {
		return errors.New("quality journal record is invalid")
	}
	if _, _, err := qualityDecisionFromProto(journal.Decision); err != nil {
		return err
	}
	if journal.ExporterState != nil {
		if _, _, err := exporterStateFromProto(journal.ExporterState); err != nil {
			return err
		}
	}
	for _, state := range journal.SourceStates {
		if _, _, err := sourceStateFromProto(state); err != nil {
			return err
		}
	}
	digest, err := qualityJournalDigest(journal)
	if err != nil {
		return err
	}
	if !bytes.Equal(journal.PayloadSha256, digest[:]) {
		return errors.New("quality journal payload checksum mismatch")
	}
	return nil
}

func validateQualitySnapshot(snapshot *flowpb.QualityStateSnapshot, collectorID string, config QualityConfig) error {
	if snapshot == nil || snapshot.StateSchemaVersion != qualityStateSchemaVersion || snapshot.CollectorId != collectorID || snapshot.CreatedAtUnixMs == 0 || len(snapshot.PayloadSha256) != sha256.Size || len(snapshot.Exporters) > config.MaxExporters || len(snapshot.Sources) > config.MaxDataSources {
		return errors.New("quality snapshot is invalid")
	}
	for _, decision := range snapshot.PendingDecisions {
		if _, _, err := qualityDecisionFromProto(decision); err != nil {
			return err
		}
	}
	for _, state := range snapshot.Exporters {
		if _, _, err := exporterStateFromProto(state); err != nil {
			return err
		}
	}
	for _, state := range snapshot.Sources {
		if _, _, err := sourceStateFromProto(state); err != nil {
			return err
		}
	}
	digest, err := qualitySnapshotDigest(snapshot)
	if err != nil {
		return err
	}
	if !bytes.Equal(snapshot.PayloadSha256, digest[:]) {
		return errors.New("quality snapshot payload checksum mismatch")
	}
	return nil
}

func qualityJournalDigest(journal *flowpb.QualityJournalRecord) ([32]byte, error) {
	clone := proto.Clone(journal).(*flowpb.QualityJournalRecord)
	clone.PayloadSha256 = nil
	return qualityProtoDigest(clone)
}

func qualitySnapshotDigest(snapshot *flowpb.QualityStateSnapshot) ([32]byte, error) {
	clone := proto.Clone(snapshot).(*flowpb.QualityStateSnapshot)
	clone.PayloadSha256 = nil
	return qualityProtoDigest(clone)
}

func qualityProtoDigest(message proto.Message) ([32]byte, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshal quality state checksum payload: %w", err)
	}
	return sha256.Sum256(payload), nil
}
