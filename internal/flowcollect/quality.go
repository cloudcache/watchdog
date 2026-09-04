package flowcollect

import (
	"net/netip"
	"sync"
	"time"
)

const serialHalfRange uint32 = 1 << 31

// QualityTracker annotates decoded facts without changing their counters. Its
// state is deliberately bounded; saturation degrades to an explicit flag
// instead of blocking or dropping the flow data path.
type QualityTracker struct {
	config  QualityConfig
	metrics *Metrics

	mu         sync.Mutex
	exporters  map[exporterQualityKey]*exporterQualityState
	sources    map[sourceQualityKey]*sourceQualityState
	observed   map[DatagramID]qualityObservation
	lastExpiry time.Time
}

type exporterQualityKey struct {
	protocol Protocol
	source   netip.Addr
	scope    uint64
	agent    netip.Addr
	subAgent uint32
}

type sourceQualityKey struct {
	exporter    exporterQualityKey
	sourceType  uint32
	sourceValue uint32
}

type qualityWindows struct {
	template, gap, poolReset, rateChange, restart, outOfOrder time.Time
}

type exporterQualityState struct {
	initialized       bool
	expectedSequence  uint32
	uptime            uint32
	uptimeValid       bool
	lastSeen          time.Time
	epoch             uint64
	samplingEpoch     uint64
	samplingRate      uint64
	samplingRateKnown bool
	windows           qualityWindows
}

type sourceQualityState struct {
	initialized      bool
	expectedSequence uint32
	samplePool       uint32
	drops            uint32
	samplingRate     uint64
	rateKnown        bool
	lastSeen         time.Time
	exporterEpoch    uint64
	epoch            uint64
	windows          qualityWindows
}

type qualityObservation struct {
	exporterEpoch uint64
	flags         []uint64
	epochs        []uint64
}

type exporterObservation struct {
	flags     uint64
	epoch     uint64
	advance   bool
	saturated bool
}

type sampleGroupKey struct {
	index, sourceType, sourceValue, sequence uint32
}

type sampleObservation struct {
	flags uint64
	epoch uint64
}

func NewQualityTracker(config QualityConfig, metrics *Metrics) *QualityTracker {
	if metrics == nil {
		metrics = &Metrics{}
	}
	return &QualityTracker{
		config: config, metrics: metrics,
		exporters: make(map[exporterQualityKey]*exporterQualityState),
		sources:   make(map[sourceQualityKey]*sourceQualityState),
		observed:  make(map[DatagramID]qualityObservation),
	}
}

// Observe reuses a retained decision for a retried datagram ID. Call Remember
// after downstream failure so retry does not advance sequence state twice.
func (q *QualityTracker) Observe(record WALRecord, decoded DecodedDatagram) (DecodedDatagram, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if cached, ok := q.observed[record.DatagramID]; ok && !zeroDatagramID(record.DatagramID) {
		return applyQualityObservation(decoded, cached), false
	}

	key := exporterQualityKey{protocol: decoded.Protocol, source: record.Source.Addr().Unmap(), scope: decoded.SequenceScope, agent: decoded.AgentIP.Unmap(), subAgent: decoded.SubAgentID}
	exporter := q.observeExporter(key, record.ReceivedAt, decoded)
	decoded.ExporterEpoch = exporter.epoch

	if decoded.Protocol == ProtocolSFlow5 {
		q.observeSFlowSources(key, record.ReceivedAt, exporter, &decoded)
	} else {
		q.observeNetFlowSampling(key, record.ReceivedAt, exporter, &decoded)
	}
	for index := range decoded.Records {
		decoded.Records[index].QualityFlags |= exporter.flags
		if exporter.saturated {
			decoded.Records[index].QualityFlags |= QualityStateSaturated
		}
	}

	return decoded, true
}

// Remember retains a decision only after downstream processing fails. The hot
// success path therefore does not allocate retry metadata.
func (q *QualityTracker) Remember(id DatagramID, decoded DecodedDatagram) {
	if zeroDatagramID(id) {
		return
	}
	observation := qualityObservation{exporterEpoch: decoded.ExporterEpoch, flags: make([]uint64, len(decoded.Records)), epochs: make([]uint64, len(decoded.Records))}
	for index := range decoded.Records {
		observation.flags[index] = decoded.Records[index].QualityFlags
		observation.epochs[index] = decoded.Records[index].QualityEpoch
	}
	q.mu.Lock()
	q.observed[id] = observation
	q.mu.Unlock()
}

func (q *QualityTracker) Forget(id DatagramID) {
	q.mu.Lock()
	delete(q.observed, id)
	q.mu.Unlock()
}

func (q *QualityTracker) observeExporter(key exporterQualityKey, now time.Time, decoded DecodedDatagram) exporterObservation {
	state, ok := q.exporterState(key, now)
	if !ok {
		q.metrics.QualityStateSaturated.Add(1)
		return exporterObservation{flags: QualityStateSaturated, saturated: true}
	}
	if decoded.TemplateChanged {
		state.windows.template = now.Add(q.config.AnomalyWindow)
	}
	observation := exporterObservation{epoch: state.epoch, advance: true}
	if !state.initialized {
		state.initialized = true
		state.expectedSequence = decoded.DatagramSequence + decoded.SequenceIncrement
		state.uptime, state.uptimeValid = decoded.ExporterUptime, decoded.ExporterUptimeValid
		state.lastSeen = now
		observation.flags = state.windows.flags(now)
		return observation
	}
	if now.Before(state.lastSeen) {
		observation.advance = false
		observation.flags = state.windows.flags(now) | QualitySequenceOutOfOrder
		q.metrics.SequenceOutOfOrderEvents.Add(1)
		return observation
	}
	if state.uptimeValid && decoded.ExporterUptimeValid && counterReset(state.uptime, decoded.ExporterUptime) {
		state.epoch = nextEpoch(state.epoch)
		state.samplingEpoch = nextEpoch(state.samplingEpoch)
		state.samplingRateKnown = false
		state.expectedSequence = decoded.DatagramSequence + decoded.SequenceIncrement
		state.uptime = decoded.ExporterUptime
		state.lastSeen = now
		state.windows = qualityWindows{restart: now.Add(q.config.AnomalyWindow)}
		if decoded.TemplateChanged {
			state.windows.template = now.Add(q.config.AnomalyWindow)
		}
		q.metrics.ExporterRestartEvents.Add(1)
		observation.epoch = state.epoch
		observation.flags = state.windows.flags(now)
		return observation
	}

	delta := decoded.DatagramSequence - state.expectedSequence
	switch {
	case delta == 0:
		state.expectedSequence = decoded.DatagramSequence + decoded.SequenceIncrement
	case delta < serialHalfRange:
		state.expectedSequence = decoded.DatagramSequence + decoded.SequenceIncrement
		state.windows.gap = now.Add(q.config.AnomalyWindow)
		q.metrics.recordSequenceGap(key.protocol, false, uint64(delta))
	default:
		observation.advance = false
		state.windows.outOfOrder = now.Add(q.config.AnomalyWindow)
		q.metrics.SequenceOutOfOrderEvents.Add(1)
	}
	if observation.advance {
		state.uptime, state.uptimeValid = decoded.ExporterUptime, decoded.ExporterUptimeValid
	}
	state.lastSeen = now
	observation.epoch = state.epoch
	observation.flags = state.windows.flags(now)
	return observation
}

func (q *QualityTracker) observeNetFlowSampling(key exporterQualityKey, now time.Time, exporter exporterObservation, decoded *DecodedDatagram) {
	if exporter.saturated {
		return
	}
	state := q.exporters[key]
	if state == nil {
		return
	}
	rate, mixed := datagramSamplingRate(decoded.Records)
	if exporter.advance && rate > 0 {
		if state.samplingRateKnown && (state.samplingRate != rate || mixed) {
			state.samplingEpoch = nextEpoch(state.samplingEpoch)
			state.windows.rateChange = now.Add(q.config.AnomalyWindow)
			q.metrics.recordSamplingRateChange(key.protocol)
		}
		state.samplingRate, state.samplingRateKnown = rate, true
	}
	flags := state.windows.flags(now)
	epoch := composeQualityEpoch(state.epoch, state.samplingEpoch)
	for index := range decoded.Records {
		decoded.Records[index].QualityFlags |= flags
		decoded.Records[index].QualityEpoch = epoch
	}
}

func (q *QualityTracker) observeSFlowSources(key exporterQualityKey, now time.Time, exporter exporterObservation, decoded *DecodedDatagram) {
	var previous sampleGroupKey
	var observation sampleObservation
	havePrevious := false
	for index := range decoded.Records {
		record := &decoded.Records[index]
		groupKey := sampleGroupKey{record.SampleIndex, record.SourceIDType, record.SourceIDValue, record.SampleSequence}
		// The decoder appends all records from one sample contiguously. Advance
		// source state once, then project the same result onto its records.
		if !havePrevious || groupKey != previous {
			observation = q.observeSFlowSource(sourceQualityKey{exporter: key, sourceType: record.SourceIDType, sourceValue: record.SourceIDValue}, now, exporter, *record)
			previous, havePrevious = groupKey, true
		}
		record.QualityFlags |= observation.flags
		record.QualityEpoch = observation.epoch
	}
}

func (q *QualityTracker) observeSFlowSource(key sourceQualityKey, now time.Time, exporter exporterObservation, record DecodedRecord) sampleObservation {
	if exporter.saturated {
		return sampleObservation{flags: QualityStateSaturated, epoch: composeQualityEpoch(exporter.epoch, 0)}
	}
	state, ok := q.sourceState(key, now, exporter.epoch)
	if !ok {
		q.metrics.QualityStateSaturated.Add(1)
		return sampleObservation{flags: QualityStateSaturated, epoch: composeQualityEpoch(exporter.epoch, 0)}
	}
	flags := uint64(0)
	if !state.initialized || state.exporterEpoch != exporter.epoch {
		if state.initialized {
			state.epoch = nextEpoch(state.epoch)
			state.windows = qualityWindows{}
		}
		state.initialized = true
		state.expectedSequence = record.SampleSequence + 1
		state.samplePool = uint32(record.SamplePool)
		state.drops = uint32(record.ExporterDrops)
		state.samplingRate, state.rateKnown = record.SamplingRate, record.SamplingRate > 0
		state.exporterEpoch = exporter.epoch
		state.lastSeen = now
		return sampleObservation{flags: state.windows.flags(now), epoch: composeQualityEpoch(exporter.epoch, state.epoch)}
	}
	if !exporter.advance || now.Before(state.lastSeen) {
		flags |= QualitySequenceOutOfOrder
		return sampleObservation{flags: state.windows.flags(now) | flags, epoch: composeQualityEpoch(exporter.epoch, state.epoch)}
	}

	delta := record.SampleSequence - state.expectedSequence
	switch {
	case delta == 0:
	case delta < serialHalfRange:
		state.windows.gap = now.Add(q.config.AnomalyWindow)
		q.metrics.recordSequenceGap(key.exporter.protocol, true, uint64(delta))
	default:
		state.windows.outOfOrder = now.Add(q.config.AnomalyWindow)
		q.metrics.SequenceOutOfOrderEvents.Add(1)
		state.lastSeen = now
		return sampleObservation{flags: state.windows.flags(now), epoch: composeQualityEpoch(exporter.epoch, state.epoch)}
	}

	epochBoundary := false
	pool := uint32(record.SamplePool)
	if counterReset(state.samplePool, pool) {
		state.windows.poolReset = now.Add(q.config.AnomalyWindow)
		q.metrics.recordSamplePoolReset(key.exporter.protocol)
		epochBoundary = true
	}
	if record.SamplingRate > 0 && state.rateKnown && record.SamplingRate != state.samplingRate {
		state.windows.rateChange = now.Add(q.config.AnomalyWindow)
		q.metrics.recordSamplingRateChange(key.exporter.protocol)
		epochBoundary = true
	}
	if epochBoundary {
		state.epoch = nextEpoch(state.epoch)
	}
	if delta < serialHalfRange {
		state.expectedSequence = record.SampleSequence + 1
		state.samplePool = pool
		if dropDelta, valid := monotonicCounterDelta(state.drops, uint32(record.ExporterDrops)); valid {
			q.metrics.recordExporterDrops(key.exporter.protocol, uint64(dropDelta))
		}
		state.drops = uint32(record.ExporterDrops)
		if record.SamplingRate > 0 {
			state.samplingRate, state.rateKnown = record.SamplingRate, true
		}
	}
	state.lastSeen = now
	return sampleObservation{flags: state.windows.flags(now) | flags, epoch: composeQualityEpoch(exporter.epoch, state.epoch)}
}

func (q *QualityTracker) exporterState(key exporterQualityKey, now time.Time) (*exporterQualityState, bool) {
	if state, ok := q.exporters[key]; ok {
		if !state.lastSeen.IsZero() && now.Sub(state.lastSeen) > q.config.StateTTL {
			delete(q.exporters, key)
		} else {
			return state, true
		}
	}
	if len(q.exporters) >= q.config.MaxExporters {
		q.maybeExpire(now)
	}
	if len(q.exporters) >= q.config.MaxExporters {
		return nil, false
	}
	state := &exporterQualityState{epoch: 1, samplingEpoch: 1}
	q.exporters[key] = state
	return state, true
}

func (q *QualityTracker) sourceState(key sourceQualityKey, now time.Time, exporterEpoch uint64) (*sourceQualityState, bool) {
	if state, ok := q.sources[key]; ok {
		if !state.lastSeen.IsZero() && now.Sub(state.lastSeen) > q.config.StateTTL {
			delete(q.sources, key)
		} else {
			return state, true
		}
	}
	if len(q.sources) >= q.config.MaxDataSources {
		q.maybeExpire(now)
	}
	if len(q.sources) >= q.config.MaxDataSources {
		return nil, false
	}
	state := &sourceQualityState{epoch: 1, exporterEpoch: exporterEpoch}
	q.sources[key] = state
	return state, true
}

func (q *QualityTracker) maybeExpire(now time.Time) {
	interval := q.config.StateTTL / 4
	if interval > time.Minute {
		interval = time.Minute
	}
	if !q.lastExpiry.IsZero() && now.Sub(q.lastExpiry) < interval {
		return
	}
	q.expire(now)
	q.lastExpiry = now
}

func (q *QualityTracker) expire(now time.Time) {
	cutoff := now.Add(-q.config.StateTTL)
	for key, state := range q.exporters {
		if state.lastSeen.Before(cutoff) {
			delete(q.exporters, key)
		}
	}
	for key, state := range q.sources {
		if state.lastSeen.Before(cutoff) {
			delete(q.sources, key)
		}
	}
}

func (w qualityWindows) flags(now time.Time) uint64 {
	var flags uint64
	if now.Before(w.template) {
		flags |= QualityTemplateRecentlyLearned
	}
	if now.Before(w.gap) {
		flags |= QualitySequenceGapWindow
	}
	if now.Before(w.poolReset) {
		flags |= QualitySamplePoolResetWindow
	}
	if now.Before(w.rateChange) {
		flags |= QualitySamplingRateChange
	}
	if now.Before(w.restart) {
		flags |= QualityExporterRestartWindow
	}
	if now.Before(w.outOfOrder) {
		flags |= QualitySequenceOutOfOrder
	}
	return flags
}

func datagramSamplingRate(records []DecodedRecord) (uint64, bool) {
	var rate uint64
	for _, record := range records {
		if record.SamplingRate == 0 {
			continue
		}
		if rate == 0 {
			rate = record.SamplingRate
		} else if rate != record.SamplingRate {
			return rate, true
		}
	}
	return rate, false
}

func counterReset(previous, current uint32) bool {
	return current < previous && previous-current < serialHalfRange
}

func monotonicCounterDelta(previous, current uint32) (uint32, bool) {
	if current >= previous {
		return current - previous, true
	}
	if counterReset(previous, current) {
		return 0, false
	}
	return current - previous, true
}

func nextEpoch(epoch uint64) uint64 {
	epoch++
	if epoch == 0 {
		return 1
	}
	return epoch
}

func composeQualityEpoch(exporter, source uint64) uint64 {
	return uint64(uint32(exporter))<<32 | uint64(uint32(source))
}

func applyQualityObservation(decoded DecodedDatagram, observation qualityObservation) DecodedDatagram {
	decoded.ExporterEpoch = observation.exporterEpoch
	for index := range decoded.Records {
		if index < len(observation.flags) {
			decoded.Records[index].QualityFlags |= observation.flags[index]
			decoded.Records[index].QualityEpoch = observation.epochs[index]
		}
	}
	return decoded
}

func zeroDatagramID(id DatagramID) bool {
	return id == (DatagramID{})
}
