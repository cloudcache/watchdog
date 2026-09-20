package flowworker

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	decoderutils "github.com/netsampler/goflow2/v3/decoders/utils"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestProcessorHandlesDecodedBatchBeforeOffsetSuccess(t *testing.T) {
	receivedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	packet := netflowlegacy.PacketNetFlowV5{
		Version: 5, UnixSecs: uint32(receivedAt.Unix()), SamplingInterval: 100,
		Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007102, Input: 3, Output: 4, DPkts: 2, DOctets: 100, SrcPort: 12345, DstPort: 443, Proto: 6}},
	}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var handled *RecordBatch
	processor, err := NewProcessor(time.Minute,
		func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return bindingFixture(), nil
		},
		func(_ context.Context, batch *RecordBatch) error { handled = batch; return nil },
		func(*kgo.Record, RejectReason, error) {
			t.Fatal("valid record was rejected")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	if err := processor.HandleRecord(context.Background(), workerRawRecord(t, flowpb.RawFlow_DECODER_NETFLOW, payload)); err != nil {
		t.Fatal(err)
	}
	if handled == nil || len(handled.Records) != 1 || handled.Protocol != uint32(flowplan.ProtocolNetFlow5) || handled.Records[0].EstimatedBytes != 10_000 {
		t.Fatalf("unexpected handled batch: %+v", handled)
	}
	if stats := processor.Stats(); stats.Datagrams != 1 || stats.Records != 1 || stats.Rejected != 0 || stats.RetryableErrors != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestProcessorPersistsCounterOnlySFlowDatagram(t *testing.T) {
	receivedAt := time.Date(2026, 9, 19, 2, 3, 4, 0, time.UTC)
	packet := sflow.Packet{
		Version: 5, IPVersion: 1, AgentIP: decoderutils.IPAddress{172, 57, 1, 2},
		SubAgentId: 7, SequenceNumber: 108, Uptime: 1000,
		Samples: []interface{}{sflow.CounterSample{
			Header: sflow.SampleHeader{SampleSequenceNumber: 40, SourceIdValue: 72},
			Records: []sflow.CounterRecord{{Data: sflow.IfCounters{
				IfIndex: 72, IfType: 6, IfSpeed: 25_000_000_000, IfDirection: 1, IfStatus: 3,
				IfInOctets: 101, IfInUcastPkts: 102, IfOutOctets: 201, IfOutUcastPkts: 202,
			}}},
		}},
	}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var handled *RecordBatch
	processor, err := NewProcessor(time.Minute,
		func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return bindingFixture(), nil
		},
		func(_ context.Context, batch *RecordBatch) error { handled = batch; return nil },
		func(*kgo.Record, RejectReason, error) { t.Fatal("valid counter datagram was rejected") },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	record := workerRawRecordAt(t, flowpb.RawFlow_DECODER_SFLOW, payload, receivedAt)
	if err := processor.HandleRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if handled == nil || handled.MessageDisposition != MessageDispositionPersisted || len(handled.Records) != 0 || len(handled.CounterRecords) != 1 {
		t.Fatalf("unexpected handled batch: %+v", handled)
	}
	counter := handled.CounterRecords[0]
	if counter.EventTimeUnixMS != receivedAt.UnixMilli() || counter.TargetID != "target-a" || counter.DeviceID != "device-a" || counter.IfIndex != 72 || counter.IfSpeed != 25_000_000_000 || counter.IfInOctets != 101 || counter.IfOutOctets != 201 {
		t.Fatalf("unexpected mapped counter: %+v", counter)
	}
	if stats := processor.Stats(); stats.Datagrams != 1 || stats.Records != 0 || stats.CounterRecords != 1 || stats.Rejected != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestProcessorAdvancesMissingTemplateAndCountsRejectedInput(t *testing.T) {
	receivedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	dataPacket := netflow.NFv9Packet{
		Version: 9, UnixSeconds: uint32(receivedAt.Unix()), SourceId: 42,
		FlowSets: []any{netflow.DataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 256}, Records: []netflow.DataRecord{{Values: []netflow.DataField{{Type: netflow.NFV9_FIELD_IN_BYTES, Value: []byte{0, 0, 0, 1}}}}}}},
	}
	data, err := dataPacket.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	rejectedCalls := 0
	var dispositions []MessageDisposition
	processor, err := NewProcessor(time.Minute,
		func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return bindingFixture(), nil
		},
		func(_ context.Context, batch *RecordBatch) error {
			dispositions = append(dispositions, batch.MessageDisposition)
			if len(batch.Records) != 0 {
				t.Fatal("non-persisted receipt batch carried records")
			}
			return nil
		},
		func(_ *kgo.Record, reason RejectReason, _ error) {
			rejectedCalls++
			if reason != RejectDecode {
				t.Fatalf("reject reason=%s", reason)
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	if err := processor.HandleRecord(context.Background(), workerRawRecord(t, flowpb.RawFlow_DECODER_NETFLOW, data)); err != nil {
		t.Fatalf("missing template must not block the partition: %v", err)
	}
	malformed := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 0, Offset: 2, Value: []byte{0xff}}
	if err := processor.HandleRecord(context.Background(), malformed); err != nil {
		t.Fatal(err)
	}
	if rejectedCalls != 1 {
		t.Fatalf("reject calls=%d", rejectedCalls)
	}
	if !reflect.DeepEqual(dispositions, []MessageDisposition{MessageDispositionTemplateMissing, MessageDispositionDecodeRejected}) {
		t.Fatalf("receipt dispositions=%v", dispositions)
	}
	if stats := processor.Stats(); stats.TemplateMissing != 1 || stats.Rejected != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestProcessorLeavesRetryableFailuresUncommittable(t *testing.T) {
	receivedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	packet := netflowlegacy.PacketNetFlowV5{Version: 5, UnixSecs: uint32(receivedAt.Unix()), Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007102, DPkts: 1, DOctets: 64}}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("registry unavailable")
	processor, err := NewProcessor(time.Minute,
		func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return flowplan.SourceBinding{}, want
		},
		func(context.Context, *RecordBatch) error { return nil },
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	if err := processor.HandleRecord(context.Background(), workerRawRecord(t, flowpb.RawFlow_DECODER_NETFLOW, payload)); !errors.Is(err, ErrBindingUnavailable) {
		t.Fatalf("error=%v", err)
	}
	if processor.Stats().RetryableErrors != 1 {
		t.Fatalf("stats=%+v", processor.Stats())
	}
}

func TestProcessorRestartReplayIsDeterministic(t *testing.T) {
	receivedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	packet := netflowlegacy.PacketNetFlowV5{
		Version: 5, UnixSecs: uint32(receivedAt.Unix()), SamplingInterval: 100,
		Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007102, DPkts: 1, DOctets: 64}},
	}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	record := workerRawRecord(t, flowpb.RawFlow_DECODER_NETFLOW, payload)
	var results []*RecordBatch
	for range 2 {
		processor, err := NewProcessor(time.Minute,
			func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
				return bindingFixture(), nil
			},
			func(_ context.Context, batch *RecordBatch) error { results = append(results, batch); return nil },
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := processor.HandleRecord(context.Background(), record); err != nil {
			processor.Close()
			t.Fatal(err)
		}
		processor.Close()
	}
	if len(results) != 2 || !reflect.DeepEqual(results[0], results[1]) {
		t.Fatalf("restart replay changed output: first=%+v second=%+v", results[0], results[1])
	}
}

func TestBatchProcessorWritesOneOrderedPartitionGroup(t *testing.T) {
	receivedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	packet := netflowlegacy.PacketNetFlowV5{
		Version: 5, UnixSecs: uint32(receivedAt.Unix()), SamplingInterval: 100,
		Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007102, DPkts: 1, DOctets: 64}},
	}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	first := workerRawRecord(t, flowpb.RawFlow_DECODER_NETFLOW, payload)
	second := workerRawRecord(t, flowpb.RawFlow_DECODER_NETFLOW, payload)
	second.Offset = first.Offset + 1
	groups := 0
	processor, err := NewBatchProcessor(time.Minute,
		func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return bindingFixture(), nil
		},
		func(_ context.Context, batches []*RecordBatch) error {
			groups++
			if len(batches) != 2 || batches[0].KafkaOffset != first.Offset || batches[1].KafkaOffset != second.Offset {
				t.Fatalf("unexpected durable group: %+v", batches)
			}
			return nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	if err := processor.HandleRecords(context.Background(), []*kgo.Record{first, second}); err != nil {
		t.Fatal(err)
	}
	if groups != 1 || processor.Stats().Records != 2 {
		t.Fatalf("groups=%d stats=%+v", groups, processor.Stats())
	}
	if err := processor.HandleRecords(context.Background(), []*kgo.Record{second, first}); err == nil {
		t.Fatal("out-of-order offsets were accepted")
	}
}

func workerRawRecord(t testing.TB, decoder flowpb.RawFlow_Decoder, payload []byte) *kgo.Record {
	return workerRawRecordAt(t, decoder, payload, time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC))
}

func workerRawRecordAt(t testing.TB, decoder flowpb.RawFlow_Decoder, payload []byte, receivedAt time.Time) *kgo.Record {
	t.Helper()
	raw, err := flowstream.NewRawFlow("collector-a", "netflow", 7, receivedAt, netip.MustParseAddrPort("192.0.2.1:9999"), decoder, payload)
	if err != nil {
		t.Fatal(err)
	}
	value, err := proto.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 0, Offset: 1, Value: value}
}

func TestRateLimitedRejectObserverIsInstalledByDefault(t *testing.T) {
	// A nil observer must be replaced by the rate-limited default so rejections
	// are not silent; the default must tolerate a nil record and repeat calls.
	resolver := func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
		return bindingFixture(), nil
	}
	p, err := NewBatchProcessorForStream(time.Minute, "kafka:test", resolver, func(context.Context, []*RecordBatch) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.onRejected == nil {
		t.Fatal("nil observer was not replaced by a default")
	}
	rec := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 0, Offset: 1}
	p.onRejected(rec, RejectMapping, errors.New("cause"))
	p.onRejected(rec, RejectMapping, errors.New("cause")) // rate-limited, must not panic
	p.onRejected(nil, RejectDecode, nil)                  // nil record must be ignored
}
