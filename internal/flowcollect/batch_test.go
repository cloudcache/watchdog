package flowcollect

import (
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

func TestBuildNormalizedBatchesPreservesDimensionsAndStableIdentity(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	binding := SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", SamplingMode: SamplingModeSampled}
	walRecord := WALRecord{DatagramID: DatagramID{1, 2, 3}, WALInput: WALInput{ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:6343")}}
	decoded := DecodedDatagram{Protocol: ProtocolSFlow5, AgentIP: netip.MustParseAddr("198.51.100.1"), SubAgentID: 7, DatagramSequence: 8, Records: []DecodedRecord{{EventTime: now, SrcIP: netip.MustParseAddr("10.0.0.1"), DstIP: netip.MustParseAddr("203.0.113.2"), SrcPort: 12345, DstPort: 443, IPProtocol: 6, RawBytes: 100, RawPackets: 1, SamplingRate: 1000, SrcAS: 64512, DstAS: 64513, SourceIDValue: 4, SampleSequence: 5, SamplePool: 6, ExporterDrops: 7}}}
	batches, err := BuildNormalizedBatches(walRecord, decoded, binding, "collector-a", plan, NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || len(batches[0].Records) != 1 {
		t.Fatalf("unexpected batches: %+v", batches)
	}
	batch, record := batches[0], batches[0].Records[0]
	if string(batch.AgentIp) != string(address16(netip.MustParseAddr("198.51.100.1"))) || batch.SubAgentId != 7 || record.SrcAs != 64512 || record.DstAs != 64513 || record.EstimatedBytes != 100000 || record.SamplePool != 6 {
		t.Fatalf("normalized fields lost: batch=%+v record=%+v", batch, record)
	}
	again, err := BuildNormalizedBatches(walRecord, decoded, binding, "collector-a", plan, NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if string(batch.NormalizedBatchId) != string(again[0].NormalizedBatchId) {
		t.Fatal("replay changed normalized batch identity")
	}
	encoded, err := proto.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip flowpb.NormalizedRecordBatch
	if err := proto.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.Records[0].DstAs != 64513 {
		t.Fatal("protobuf round trip lost ASN")
	}
}

func TestVirtualShardIsStable(t *testing.T) {
	got := VirtualShard("tenant-a", netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("2001:db8::1"))
	if got != 2603 {
		t.Fatalf("virtual shard changed: got %d", got)
	}
}
