package flowcollect

import "sync/atomic"

type Metrics struct {
	ReceivedDatagrams       atomic.Uint64
	ReceiveQueueDrops       atomic.Uint64
	InvalidDatagrams        atomic.Uint64
	QuarantinedDatagrams    atomic.Uint64
	WALAppended             atomic.Uint64
	WALHardStops            atomic.Uint64
	DecodedDatagrams        atomic.Uint64
	DecodeFailures          atomic.Uint64
	NormalizeFailures       atomic.Uint64
	PublishFailures         atomic.Uint64
	TemplatePending         atomic.Uint64
	PublishedBatches        atomic.Uint64
	CollectStateCheckpoints atomic.Uint64
	PublishedCollectStates  atomic.Uint64
	CollectStateFailures    atomic.Uint64
	ReplayAttempts          atomic.Uint64
}

type MetricSnapshot struct {
	ReceivedDatagrams       uint64
	ReceiveQueueDrops       uint64
	InvalidDatagrams        uint64
	QuarantinedDatagrams    uint64
	WALAppended             uint64
	WALHardStops            uint64
	DecodedDatagrams        uint64
	DecodeFailures          uint64
	NormalizeFailures       uint64
	PublishFailures         uint64
	TemplatePending         uint64
	PublishedBatches        uint64
	CollectStateCheckpoints uint64
	PublishedCollectStates  uint64
	CollectStateFailures    uint64
	ReplayAttempts          uint64
}

func (m *Metrics) Snapshot() MetricSnapshot {
	return MetricSnapshot{
		ReceivedDatagrams: m.ReceivedDatagrams.Load(), ReceiveQueueDrops: m.ReceiveQueueDrops.Load(), InvalidDatagrams: m.InvalidDatagrams.Load(),
		QuarantinedDatagrams: m.QuarantinedDatagrams.Load(), WALAppended: m.WALAppended.Load(), WALHardStops: m.WALHardStops.Load(),
		DecodedDatagrams: m.DecodedDatagrams.Load(), DecodeFailures: m.DecodeFailures.Load(), NormalizeFailures: m.NormalizeFailures.Load(), PublishFailures: m.PublishFailures.Load(), TemplatePending: m.TemplatePending.Load(), PublishedBatches: m.PublishedBatches.Load(),
		CollectStateCheckpoints: m.CollectStateCheckpoints.Load(), PublishedCollectStates: m.PublishedCollectStates.Load(), CollectStateFailures: m.CollectStateFailures.Load(), ReplayAttempts: m.ReplayAttempts.Load(),
	}
}
