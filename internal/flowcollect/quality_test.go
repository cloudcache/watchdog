package flowcollect

import (
	"encoding/binary"
	"math"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

func TestQualityTrackerSFlowGapsRatePoolAndRestartEpochs(t *testing.T) {
	metrics := &Metrics{}
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 10, MaxDataSources: 10}, metrics)
	now := time.Unix(1000, 0)
	record := qualityWALRecord(1, now)

	first, fresh := tracker.Observe(record, sflowQualityDatagram(100, 1000, 10, 1000, 100, 2))
	if !fresh || first.ExporterEpoch != 1 || first.Records[0].QualityEpoch != composeQualityEpoch(1, 1) || first.Records[0].QualityFlags != 0 {
		t.Fatalf("unexpected initial state: fresh=%v datagram=%+v record=%+v", fresh, first, first.Records[0])
	}

	secondRecord := qualityWALRecord(2, now.Add(time.Second))
	second, _ := tracker.Observe(secondRecord, sflowQualityDatagram(102, 2000, 12, 3000, 200, 5))
	wantFlags := QualitySequenceGapWindow | QualitySamplingRateChange
	if second.Records[0].QualityFlags&wantFlags != wantFlags || second.Records[0].QualityEpoch != composeQualityEpoch(1, 2) {
		t.Fatalf("gap/rate change not projected: %+v", second.Records[0])
	}
	snapshot := metrics.Snapshot()
	if snapshot.SequenceGapEvents != 2 || snapshot.MissingSequenceUnits != 2 || snapshot.SamplingRateChangeEvents != 1 || snapshot.ExporterDropSamples != 3 {
		t.Fatalf("unexpected gap/rate/drop metrics: %+v", snapshot)
	}

	third, _ := tracker.Observe(qualityWALRecord(3, now.Add(2*time.Minute)), sflowQualityDatagram(103, 3000, 13, 100, 200, 6))
	if third.Records[0].QualityFlags != QualitySamplePoolResetWindow || third.Records[0].QualityEpoch != composeQualityEpoch(1, 3) {
		t.Fatalf("pool reset not isolated into a new epoch: %+v", third.Records[0])
	}

	fourth, _ := tracker.Observe(qualityWALRecord(4, now.Add(4*time.Minute)), sflowQualityDatagram(1, 10, 1, 10, 200, 0))
	if fourth.ExporterEpoch != 2 || fourth.Records[0].QualityFlags != QualityExporterRestartWindow || fourth.Records[0].QualityEpoch != composeQualityEpoch(2, 4) {
		t.Fatalf("exporter restart not isolated: datagram=%+v record=%+v", fourth, fourth.Records[0])
	}
	snapshot = metrics.Snapshot()
	if snapshot.SamplePoolResetEvents != 1 || snapshot.ExporterRestartEvents != 1 {
		t.Fatalf("unexpected reset/restart metrics: %+v", snapshot)
	}
}

func TestQualityTrackerRetryIsIdempotent(t *testing.T) {
	metrics := &Metrics{}
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 10, MaxDataSources: 10}, metrics)
	now := time.Unix(2000, 0)
	record := qualityWALRecord(1, now)
	diagram := sflowQualityDatagram(10, 1000, 20, 1000, 100, 0)
	first, fresh := tracker.Observe(record, diagram)
	tracker.Remember(record.DatagramID, first)
	retry, retryFresh := tracker.Observe(record, diagram)
	if !fresh || retryFresh || first.ExporterEpoch != retry.ExporterEpoch || first.Records[0].QualityEpoch != retry.Records[0].QualityEpoch || retry.Records[0].QualityFlags != first.Records[0].QualityFlags {
		t.Fatalf("retry changed quality decision: first=%+v retry=%+v", first, retry)
	}
	next, _ := tracker.Observe(qualityWALRecord(2, now.Add(time.Second)), sflowQualityDatagram(11, 2000, 21, 1100, 100, 0))
	if next.Records[0].QualityFlags != 0 || metrics.Snapshot().SequenceOutOfOrderEvents != 0 {
		t.Fatalf("retry advanced sequence state twice: record=%+v metrics=%+v", next.Records[0], metrics.Snapshot())
	}
}

func TestQualityTrackerAdvancesOneSFlowSampleOnce(t *testing.T) {
	metrics := &Metrics{}
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 10, MaxDataSources: 10}, metrics)
	now := time.Unix(2500, 0)
	first := sflowQualityDatagram(10, 1000, 20, 1000, 100, 0)
	first.Records = append(first.Records, first.Records[0])
	tracker.Observe(qualityWALRecord(1, now), first)
	second, _ := tracker.Observe(qualityWALRecord(2, now.Add(time.Second)), sflowQualityDatagram(11, 2000, 21, 1100, 100, 0))
	if second.Records[0].QualityFlags != 0 || metrics.Snapshot().SequenceOutOfOrderEvents != 0 {
		t.Fatalf("multiple records advanced one sample more than once: record=%+v metrics=%+v", second.Records[0], metrics.Snapshot())
	}
}

func TestQualityTrackerNetFlowSequenceUnitsAndSamplingEpoch(t *testing.T) {
	metrics := &Metrics{}
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 10, MaxDataSources: 10}, metrics)
	now := time.Unix(3000, 0)
	first := netflowQualityDatagram(100, 3, 1000, 100)
	tracked, _ := tracker.Observe(qualityWALRecord(1, now), first)
	if tracked.Records[0].QualityEpoch != composeQualityEpoch(1, 1) {
		t.Fatalf("unexpected initial quality epoch: %d", tracked.Records[0].QualityEpoch)
	}
	second := netflowQualityDatagram(105, 2, 2000, 200)
	tracked, _ = tracker.Observe(qualityWALRecord(2, now.Add(time.Second)), second)
	if tracked.Records[0].QualityFlags&(QualitySequenceGapWindow|QualitySamplingRateChange) != QualitySequenceGapWindow|QualitySamplingRateChange {
		t.Fatalf("NetFlow quality flags missing: %+v", tracked.Records[0])
	}
	if tracked.Records[0].QualityEpoch != composeQualityEpoch(1, 2) || metrics.Snapshot().MissingSequenceUnits != 2 {
		t.Fatalf("NetFlow gap/rate epoch mismatch: record=%+v metrics=%+v", tracked.Records[0], metrics.Snapshot())
	}
}

func TestQualityTrackerAcceptsUint32SequenceAndUptimeWrap(t *testing.T) {
	metrics := &Metrics{}
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 10, MaxDataSources: 10}, metrics)
	now := time.Unix(4000, 0)
	tracker.Observe(qualityWALRecord(1, now), netflowQualityDatagram(math.MaxUint32, 1, math.MaxUint32-5, 100))
	wrapped, _ := tracker.Observe(qualityWALRecord(2, now.Add(time.Second)), netflowQualityDatagram(0, 1, 5, 100))
	if wrapped.Records[0].QualityFlags != 0 || wrapped.ExporterEpoch != 1 || metrics.Snapshot().ExporterRestartEvents != 0 {
		t.Fatalf("uint32 wrap misclassified: datagram=%+v metrics=%+v", wrapped, metrics.Snapshot())
	}
}

func TestQualityTrackerSaturationFlagsButDoesNotReject(t *testing.T) {
	metrics := &Metrics{}
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 1, MaxDataSources: 1}, metrics)
	now := time.Unix(5000, 0)
	tracker.Observe(qualityWALRecord(1, now), netflowQualityDatagram(1, 1, 100, 100))
	secondRecord := qualityWALRecord(2, now.Add(time.Second))
	secondRecord.Source = netip.MustParseAddrPort("192.0.2.2:2055")
	second, _ := tracker.Observe(secondRecord, netflowQualityDatagram(1, 1, 100, 100))
	if second.Records[0].QualityFlags&QualityStateSaturated == 0 || metrics.Snapshot().QualityStateSaturated != 1 {
		t.Fatalf("bounded-state saturation was not explicit: record=%+v metrics=%+v", second.Records[0], metrics.Snapshot())
	}
}

func qualityWALRecord(id byte, receivedAt time.Time) WALRecord {
	return WALRecord{DatagramID: DatagramID{id}, WALInput: WALInput{Protocol: ProtocolSFlow5, ReceivedAt: receivedAt, Source: netip.MustParseAddrPort("192.0.2.1:6343")}}
}

func sflowQualityDatagram(datagramSequence, uptime, sampleSequence, pool uint32, rate uint64, drops uint32) DecodedDatagram {
	return DecodedDatagram{
		Protocol: ProtocolSFlow5, AgentIP: netip.MustParseAddr("198.51.100.1"), SubAgentID: 7,
		DatagramSequence: datagramSequence, SequenceIncrement: 1, ExporterUptime: uptime, ExporterUptimeValid: true,
		Records: []DecodedRecord{{SourceIDType: 0, SourceIDValue: 9, SampleIndex: 1, SampleSequence: sampleSequence, SamplePool: uint64(pool), SamplingRate: rate, ExporterDrops: uint64(drops)}},
	}
}

func netflowQualityDatagram(sequence, increment, uptime uint32, rate uint64) DecodedDatagram {
	return DecodedDatagram{
		Protocol: ProtocolNetFlow5, DatagramSequence: sequence, SequenceIncrement: increment, SequenceScope: 1,
		ExporterUptime: uptime, ExporterUptimeValid: true, Records: []DecodedRecord{{SamplingRate: rate}},
	}
}

func BenchmarkQualityTrackerParallel(b *testing.B) {
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 65536, MaxDataSources: 262144}, &Metrics{})
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			value := sequence.Add(1)
			var id DatagramID
			binary.BigEndian.PutUint64(id[:8], value)
			record := WALRecord{DatagramID: id, WALInput: WALInput{ReceivedAt: time.Unix(6000, int64(value)), Source: netip.MustParseAddrPort("192.0.2.1:2055")}}
			tracker.Observe(record, netflowQualityDatagram(uint32(value), 1, uint32(value), 100))
		}
	})
}

func BenchmarkQualityTrackerSFlow(b *testing.B) {
	tracker := NewQualityTracker(QualityConfig{StateTTL: time.Hour, AnomalyWindow: time.Minute, MaxExporters: 65536, MaxDataSources: 262144}, &Metrics{})
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		value := uint32(index + 1)
		var id DatagramID
		binary.BigEndian.PutUint64(id[:8], uint64(value))
		record := WALRecord{DatagramID: id, WALInput: WALInput{ReceivedAt: time.Unix(7000, int64(value)), Source: netip.MustParseAddrPort("192.0.2.1:6343")}}
		tracker.Observe(record, sflowQualityDatagram(value, value, value, value*100, 100, 0))
	}
}
