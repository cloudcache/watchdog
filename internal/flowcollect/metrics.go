package flowcollect

import "sync/atomic"

type Metrics struct {
	ReceivedDatagrams         atomic.Uint64
	ReceiveQueueDrops         atomic.Uint64
	InvalidDatagrams          atomic.Uint64
	QuarantinedDatagrams      atomic.Uint64
	QuarantineRateLimited     atomic.Uint64
	QuarantineQueueDrops      atomic.Uint64
	PublishedQuarantine       atomic.Uint64
	QuarantinePublishFailures atomic.Uint64
	WALAppended               atomic.Uint64
	WALHardStops              atomic.Uint64
	DecodedDatagrams          atomic.Uint64
	DecodeFailures            atomic.Uint64
	NormalizeFailures         atomic.Uint64
	PublishFailures           atomic.Uint64
	TemplatePending           atomic.Uint64
	PublishedBatches          atomic.Uint64
	CollectStateCheckpoints   atomic.Uint64
	PublishedCollectStates    atomic.Uint64
	CollectStateFailures      atomic.Uint64
	ReplayAttempts            atomic.Uint64
	DLQDatagrams              atomic.Uint64
	DLQPublishFailures        atomic.Uint64
	SequenceGapEvents         atomic.Uint64
	MissingSequenceUnits      atomic.Uint64
	ExporterRestartEvents     atomic.Uint64
	SequenceOutOfOrderEvents  atomic.Uint64
	SamplePoolResetEvents     atomic.Uint64
	SamplingRateChangeEvents  atomic.Uint64
	ExporterDropSamples       atomic.Uint64
	QualityStateSaturated     atomic.Uint64
	QualityJournalAppends     atomic.Uint64
	QualityJournalFailures    atomic.Uint64
	QualityStateRestores      atomic.Uint64
	QualityStateCheckpoints   atomic.Uint64
	QualityCheckpointFailures atomic.Uint64
}

type MetricSnapshot struct {
	ReceivedDatagrams         uint64
	ReceiveQueueDrops         uint64
	InvalidDatagrams          uint64
	QuarantinedDatagrams      uint64
	QuarantineRateLimited     uint64
	QuarantineQueueDrops      uint64
	PublishedQuarantine       uint64
	QuarantinePublishFailures uint64
	WALAppended               uint64
	WALHardStops              uint64
	DecodedDatagrams          uint64
	DecodeFailures            uint64
	NormalizeFailures         uint64
	PublishFailures           uint64
	TemplatePending           uint64
	PublishedBatches          uint64
	CollectStateCheckpoints   uint64
	PublishedCollectStates    uint64
	CollectStateFailures      uint64
	ReplayAttempts            uint64
	DLQDatagrams              uint64
	DLQPublishFailures        uint64
	SequenceGapEvents         uint64
	MissingSequenceUnits      uint64
	ExporterRestartEvents     uint64
	SequenceOutOfOrderEvents  uint64
	SamplePoolResetEvents     uint64
	SamplingRateChangeEvents  uint64
	ExporterDropSamples       uint64
	QualityStateSaturated     uint64
	QualityJournalAppends     uint64
	QualityJournalFailures    uint64
	QualityStateRestores      uint64
	QualityStateCheckpoints   uint64
	QualityCheckpointFailures uint64
}

func (m *Metrics) Snapshot() MetricSnapshot {
	return MetricSnapshot{
		ReceivedDatagrams: m.ReceivedDatagrams.Load(), ReceiveQueueDrops: m.ReceiveQueueDrops.Load(), InvalidDatagrams: m.InvalidDatagrams.Load(),
		QuarantinedDatagrams: m.QuarantinedDatagrams.Load(), WALAppended: m.WALAppended.Load(), WALHardStops: m.WALHardStops.Load(),
		DecodedDatagrams: m.DecodedDatagrams.Load(), DecodeFailures: m.DecodeFailures.Load(), NormalizeFailures: m.NormalizeFailures.Load(), PublishFailures: m.PublishFailures.Load(), TemplatePending: m.TemplatePending.Load(), PublishedBatches: m.PublishedBatches.Load(),
		CollectStateCheckpoints: m.CollectStateCheckpoints.Load(), PublishedCollectStates: m.PublishedCollectStates.Load(), CollectStateFailures: m.CollectStateFailures.Load(), ReplayAttempts: m.ReplayAttempts.Load(),
		QuarantineRateLimited: m.QuarantineRateLimited.Load(), QuarantineQueueDrops: m.QuarantineQueueDrops.Load(), PublishedQuarantine: m.PublishedQuarantine.Load(), QuarantinePublishFailures: m.QuarantinePublishFailures.Load(),
		DLQDatagrams: m.DLQDatagrams.Load(), DLQPublishFailures: m.DLQPublishFailures.Load(),
		SequenceGapEvents: m.SequenceGapEvents.Load(), MissingSequenceUnits: m.MissingSequenceUnits.Load(), ExporterRestartEvents: m.ExporterRestartEvents.Load(), SequenceOutOfOrderEvents: m.SequenceOutOfOrderEvents.Load(),
		SamplePoolResetEvents: m.SamplePoolResetEvents.Load(), SamplingRateChangeEvents: m.SamplingRateChangeEvents.Load(), ExporterDropSamples: m.ExporterDropSamples.Load(), QualityStateSaturated: m.QualityStateSaturated.Load(),
		QualityJournalAppends: m.QualityJournalAppends.Load(), QualityJournalFailures: m.QualityJournalFailures.Load(), QualityStateRestores: m.QualityStateRestores.Load(), QualityStateCheckpoints: m.QualityStateCheckpoints.Load(), QualityCheckpointFailures: m.QualityCheckpointFailures.Load(),
	}
}
