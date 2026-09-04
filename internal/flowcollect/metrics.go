package flowcollect

import (
	"sync/atomic"
	"time"
)

const (
	protocolMetricSlots = 5
	kafkaTopicSlots     = 4
	latencyBucketSlots  = 13
)

var latencyUpperBounds = [...]time.Duration{
	time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
}

type kafkaTopic uint8

const (
	kafkaTopicNormalized kafkaTopic = iota
	kafkaTopicCollectState
	kafkaTopicDecodeDLQ
	kafkaTopicQuarantine
)

type protocolMetricSet struct {
	received            atomic.Uint64
	decodedRecords      atomic.Uint64
	missingSamplingRate atomic.Uint64
	samplingRateChanges atomic.Uint64
	samplePoolResets    atomic.Uint64
	exporterDrops       atomic.Uint64
	sequenceGapDatagram atomic.Uint64
	sequenceGapSample   atomic.Uint64
	decodeInvalid       atomic.Uint64
	decodeTemplate      atomic.Uint64
	decodeRejected      atomic.Uint64
	normalizeRejected   atomic.Uint64
	normalizedSucceeded atomic.Uint64
	normalizedRejected  atomic.Uint64
}

type kafkaMetricSet struct {
	succeeded          atomic.Uint64
	failed             atomic.Uint64
	latencyCount       atomic.Uint64
	latencyNanoseconds atomic.Uint64
	latencyBuckets     [latencyBucketSlots]atomic.Uint64
}

type durationMetric struct {
	count       atomic.Uint64
	nanoseconds atomic.Uint64
	buckets     [latencyBucketSlots]atomic.Uint64
}

type Metrics struct {
	ReceivedDatagrams             atomic.Uint64
	ReceiveQueueDrops             atomic.Uint64
	InvalidDatagrams              atomic.Uint64
	QuarantinedDatagrams          atomic.Uint64
	QuarantineRateLimited         atomic.Uint64
	QuarantineQueueDrops          atomic.Uint64
	PublishedQuarantine           atomic.Uint64
	QuarantinePublishFailures     atomic.Uint64
	WALAppended                   atomic.Uint64
	WALHardStops                  atomic.Uint64
	DecodedDatagrams              atomic.Uint64
	DecodeFailures                atomic.Uint64
	NormalizeFailures             atomic.Uint64
	PublishFailures               atomic.Uint64
	TemplatePending               atomic.Uint64
	PublishedBatches              atomic.Uint64
	CollectStateCheckpoints       atomic.Uint64
	PublishedCollectStates        atomic.Uint64
	CollectStateFailures          atomic.Uint64
	CollectStateRestoreCandidates atomic.Int64
	CollectStateRestored          atomic.Int64
	CollectStateRestoreNanos      atomic.Int64
	PlanHistoryEntries            atomic.Int64
	PlanHistoryPruned             atomic.Uint64
	ReplayAttempts                atomic.Uint64
	AttemptJournalAppends         atomic.Uint64
	AttemptJournalFailures        atomic.Uint64
	AttemptStateRestores          atomic.Uint64
	AttemptStateCheckpoints       atomic.Uint64
	AttemptCheckpointFailures     atomic.Uint64
	DLQDatagrams                  atomic.Uint64
	DLQPublishFailures            atomic.Uint64
	SequenceGapEvents             atomic.Uint64
	MissingSequenceUnits          atomic.Uint64
	ExporterRestartEvents         atomic.Uint64
	SequenceOutOfOrderEvents      atomic.Uint64
	SamplePoolResetEvents         atomic.Uint64
	SamplingRateChangeEvents      atomic.Uint64
	ExporterDropSamples           atomic.Uint64
	QualityStateSaturated         atomic.Uint64
	QualityJournalAppends         atomic.Uint64
	QualityJournalFailures        atomic.Uint64
	QualityStateRestores          atomic.Uint64
	QualityStateCheckpoints       atomic.Uint64
	PublishedQualityCheckpoints   atomic.Uint64
	QualityRestoreCandidates      atomic.Int64
	QualityRestored               atomic.Int64
	QualityCheckpointFailures     atomic.Uint64
	ReceiveQueueDepth             atomic.Int64
	ReceiveQueueCapacity          atomic.Int64
	DecodeQueueDepth              atomic.Int64
	DecodeQueueCapacity           atomic.Int64
	QuarantineQueueDepth          atomic.Int64
	QuarantineQueueCapacity       atomic.Int64
	UDPQueueDropsSFlow            atomic.Uint64
	UDPQueueDropsNetFlow          atomic.Uint64
	UDPKernelDropsSFlow           atomic.Uint64
	UDPKernelDropsNetFlow         atomic.Uint64
	NormalizedBatchRecords        atomic.Int64
	NormalizedBatchBytes          atomic.Int64

	protocol [protocolMetricSlots]protocolMetricSet
	kafka    [kafkaTopicSlots]kafkaMetricSet
	walFsync durationMetric
}

type MetricSnapshot struct {
	ReceivedDatagrams           uint64
	ReceiveQueueDrops           uint64
	InvalidDatagrams            uint64
	QuarantinedDatagrams        uint64
	QuarantineRateLimited       uint64
	QuarantineQueueDrops        uint64
	PublishedQuarantine         uint64
	QuarantinePublishFailures   uint64
	WALAppended                 uint64
	WALHardStops                uint64
	DecodedDatagrams            uint64
	DecodeFailures              uint64
	NormalizeFailures           uint64
	PublishFailures             uint64
	TemplatePending             uint64
	PublishedBatches            uint64
	CollectStateCheckpoints     uint64
	PublishedCollectStates      uint64
	CollectStateFailures        uint64
	PlanHistoryPruned           uint64
	ReplayAttempts              uint64
	AttemptJournalAppends       uint64
	AttemptJournalFailures      uint64
	AttemptStateRestores        uint64
	AttemptStateCheckpoints     uint64
	AttemptCheckpointFailures   uint64
	DLQDatagrams                uint64
	DLQPublishFailures          uint64
	SequenceGapEvents           uint64
	MissingSequenceUnits        uint64
	ExporterRestartEvents       uint64
	SequenceOutOfOrderEvents    uint64
	SamplePoolResetEvents       uint64
	SamplingRateChangeEvents    uint64
	ExporterDropSamples         uint64
	QualityStateSaturated       uint64
	QualityJournalAppends       uint64
	QualityJournalFailures      uint64
	QualityStateRestores        uint64
	QualityStateCheckpoints     uint64
	PublishedQualityCheckpoints uint64
	QualityCheckpointFailures   uint64
}

func (m *Metrics) Snapshot() MetricSnapshot {
	return MetricSnapshot{
		ReceivedDatagrams: m.ReceivedDatagrams.Load(), ReceiveQueueDrops: m.ReceiveQueueDrops.Load(), InvalidDatagrams: m.InvalidDatagrams.Load(),
		QuarantinedDatagrams: m.QuarantinedDatagrams.Load(), WALAppended: m.WALAppended.Load(), WALHardStops: m.WALHardStops.Load(),
		DecodedDatagrams: m.DecodedDatagrams.Load(), DecodeFailures: m.DecodeFailures.Load(), NormalizeFailures: m.NormalizeFailures.Load(), PublishFailures: m.PublishFailures.Load(), TemplatePending: m.TemplatePending.Load(), PublishedBatches: m.PublishedBatches.Load(),
		CollectStateCheckpoints: m.CollectStateCheckpoints.Load(), PublishedCollectStates: m.PublishedCollectStates.Load(), CollectStateFailures: m.CollectStateFailures.Load(), ReplayAttempts: m.ReplayAttempts.Load(),
		PlanHistoryPruned: m.PlanHistoryPruned.Load(), AttemptJournalAppends: m.AttemptJournalAppends.Load(), AttemptJournalFailures: m.AttemptJournalFailures.Load(), AttemptStateRestores: m.AttemptStateRestores.Load(), AttemptStateCheckpoints: m.AttemptStateCheckpoints.Load(), AttemptCheckpointFailures: m.AttemptCheckpointFailures.Load(),
		QuarantineRateLimited: m.QuarantineRateLimited.Load(), QuarantineQueueDrops: m.QuarantineQueueDrops.Load(), PublishedQuarantine: m.PublishedQuarantine.Load(), QuarantinePublishFailures: m.QuarantinePublishFailures.Load(),
		DLQDatagrams: m.DLQDatagrams.Load(), DLQPublishFailures: m.DLQPublishFailures.Load(),
		SequenceGapEvents: m.SequenceGapEvents.Load(), MissingSequenceUnits: m.MissingSequenceUnits.Load(), ExporterRestartEvents: m.ExporterRestartEvents.Load(), SequenceOutOfOrderEvents: m.SequenceOutOfOrderEvents.Load(),
		SamplePoolResetEvents: m.SamplePoolResetEvents.Load(), SamplingRateChangeEvents: m.SamplingRateChangeEvents.Load(), ExporterDropSamples: m.ExporterDropSamples.Load(), QualityStateSaturated: m.QualityStateSaturated.Load(),
		QualityJournalAppends: m.QualityJournalAppends.Load(), QualityJournalFailures: m.QualityJournalFailures.Load(), QualityStateRestores: m.QualityStateRestores.Load(), QualityStateCheckpoints: m.QualityStateCheckpoints.Load(), PublishedQualityCheckpoints: m.PublishedQualityCheckpoints.Load(), QualityCheckpointFailures: m.QualityCheckpointFailures.Load(),
	}
}

func protocolMetricIndex(protocol Protocol) int {
	if protocol >= ProtocolSFlow5 && protocol <= ProtocolIPFIX {
		return int(protocol)
	}
	return 0
}

func protocolMetricName(index int) string {
	switch Protocol(index) {
	case ProtocolSFlow5:
		return "sflow5"
	case ProtocolNetFlow5:
		return "netflow5"
	case ProtocolNetFlow9:
		return "netflow9"
	case ProtocolIPFIX:
		return "ipfix"
	default:
		return "unknown"
	}
}

func kafkaTopicName(topic kafkaTopic) string {
	switch topic {
	case kafkaTopicCollectState:
		return "collect_state"
	case kafkaTopicDecodeDLQ:
		return "decode_dlq"
	case kafkaTopicQuarantine:
		return "quarantine"
	default:
		return "normalized"
	}
}

func (m *Metrics) recordReceived(protocol Protocol) {
	m.ReceivedDatagrams.Add(1)
	m.protocol[protocolMetricIndex(protocol)].received.Add(1)
}

func (m *Metrics) recordDecodedRecords(protocol Protocol, records int) {
	if records > 0 {
		m.protocol[protocolMetricIndex(protocol)].decodedRecords.Add(uint64(records))
	}
}

func (m *Metrics) recordDecodeError(protocol Protocol, reason string) {
	metric := &m.protocol[protocolMetricIndex(protocol)]
	switch reason {
	case "invalid_datagram":
		metric.decodeInvalid.Add(1)
	case "template_pending":
		metric.decodeTemplate.Add(1)
	case "normalize_rejected":
		metric.normalizeRejected.Add(1)
	default:
		metric.decodeRejected.Add(1)
	}
}

func (m *Metrics) recordMissingSamplingRate(protocol Protocol) {
	m.protocol[protocolMetricIndex(protocol)].missingSamplingRate.Add(1)
}

func (m *Metrics) recordNormalized(protocol Protocol, records int, err error) {
	metric := &m.protocol[protocolMetricIndex(protocol)]
	if err != nil {
		metric.normalizedRejected.Add(1)
		return
	}
	if records > 0 {
		metric.normalizedSucceeded.Add(uint64(records))
	}
}

func (m *Metrics) recordSamplingRateChange(protocol Protocol) {
	m.SamplingRateChangeEvents.Add(1)
	m.protocol[protocolMetricIndex(protocol)].samplingRateChanges.Add(1)
}

func (m *Metrics) recordSamplePoolReset(protocol Protocol) {
	m.SamplePoolResetEvents.Add(1)
	m.protocol[protocolMetricIndex(protocol)].samplePoolResets.Add(1)
}

func (m *Metrics) recordExporterDrops(protocol Protocol, drops uint64) {
	m.ExporterDropSamples.Add(drops)
	m.protocol[protocolMetricIndex(protocol)].exporterDrops.Add(drops)
}

func (m *Metrics) recordSequenceGap(protocol Protocol, sampleScope bool, missing uint64) {
	m.SequenceGapEvents.Add(1)
	m.MissingSequenceUnits.Add(missing)
	metric := &m.protocol[protocolMetricIndex(protocol)]
	if sampleScope {
		metric.sequenceGapSample.Add(1)
	} else {
		metric.sequenceGapDatagram.Add(1)
	}
}

func (m *Metrics) observeKafka(topic kafkaTopic, err error, duration time.Duration) {
	metric := &m.kafka[int(topic)]
	if err == nil {
		metric.succeeded.Add(1)
	} else {
		metric.failed.Add(1)
	}
	metric.latencyCount.Add(1)
	duration = max(duration, 0)
	metric.latencyNanoseconds.Add(uint64(duration))
	metric.latencyBuckets[durationBucket(duration)].Add(1)
}

func (m *Metrics) observeWALFsync(duration time.Duration) {
	duration = max(duration, 0)
	m.walFsync.count.Add(1)
	m.walFsync.nanoseconds.Add(uint64(duration))
	m.walFsync.buckets[durationBucket(duration)].Add(1)
}

func durationBucket(duration time.Duration) int {
	for index, upper := range latencyUpperBounds {
		if duration <= upper {
			return index
		}
	}
	return len(latencyUpperBounds)
}
