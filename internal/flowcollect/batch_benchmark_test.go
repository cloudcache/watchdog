package flowcollect

import (
	"net/netip"
	"testing"
	"time"
)

func BenchmarkBuildNormalizedBatch1024RecordsRandomShards(b *testing.B) {
	now := time.Unix(1700000000, 0)
	plan := validPlan(now)
	records := make([]DecodedRecord, 1024)
	for index := range records {
		records[index] = DecodedRecord{EventTime: now, SrcIP: netip.AddrFrom4([4]byte{10, 0, byte(index >> 8), byte(index)}), DstIP: netip.MustParseAddr("203.0.113.1"), SrcPort: uint32(10000 + index), DstPort: 443, IPProtocol: 6, RawBytes: 1500, RawPackets: 1, SamplingRate: 1000}
	}
	walRecord := WALRecord{DatagramID: DatagramID{1}, WALInput: WALInput{ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:6343")}}
	decoded := DecodedDatagram{Protocol: ProtocolSFlow5, Records: records}
	binding := SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", SamplingMode: SamplingModeSampled}
	config := NormalizedBatchCfg{MaxRecords: 1024, MaxBytes: 1 << 20, MaxWait: time.Millisecond}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := BuildNormalizedBatches(walRecord, decoded, binding, plan.CollectorID, plan, config, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildNormalizedBatch1024RecordsSingleShard(b *testing.B) {
	now := time.Unix(1700000000, 0)
	records := make([]DecodedRecord, 1024)
	for index := range records {
		records[index] = DecodedRecord{EventTime: now, SrcIP: netip.MustParseAddr("10.0.0.1"), DstIP: netip.MustParseAddr("203.0.113.1"), SrcPort: uint32(10000 + index), DstPort: 443, IPProtocol: 6, RawBytes: 1500, RawPackets: 1, SamplingRate: 1000}
	}
	benchmarkBuildNormalizedBatch(b, now, records)
}

func benchmarkBuildNormalizedBatch(b *testing.B, now time.Time, records []DecodedRecord) {
	plan := validPlan(now)
	walRecord := WALRecord{DatagramID: DatagramID{1}, WALInput: WALInput{ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:6343")}}
	decoded := DecodedDatagram{Protocol: ProtocolSFlow5, Records: records}
	binding := SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", SamplingMode: SamplingModeSampled}
	config := NormalizedBatchCfg{MaxRecords: 1024, MaxBytes: 1 << 20, MaxWait: time.Millisecond}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := BuildNormalizedBatches(walRecord, decoded, binding, plan.CollectorID, plan, config, 0); err != nil {
			b.Fatal(err)
		}
	}
}
