package flowcollect

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
)

type recordingPublisher struct {
	batches []*flowpb.NormalizedRecordBatch
}

func (p *recordingPublisher) Publish(_ context.Context, batch *flowpb.NormalizedRecordBatch) error {
	p.batches = append(p.batches, batch)
	return nil
}
func (p *recordingPublisher) Close() error { return nil }

func TestRunnerProcessesDurableNetFlowAndAdvancesCheckpoint(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	packet := netflowlegacy.PacketNetFlowV5{Version: 5, SysUptime: 1000, UnixSecs: uint32(now.Unix()), SamplingInterval: 100, Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007101, DPkts: 2, DOctets: 1000, Proto: 17}}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow5, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	publisher := &recordingPublisher{}
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}}, Registry: registry, WAL: w, Decoder: decoder, Publisher: publisher}
	if err := runner.processRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(publisher.batches) != 1 || publisher.batches[0].Records[0].EstimatedBytes != 100000 {
		t.Fatalf("unexpected published batches: %+v", publisher.batches)
	}
	remaining := 0
	if err := w.Replay(func(WALRecord) error { remaining++; return nil }); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("published datagram remained replayable: %d", remaining)
	}
}
