// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	decoderutils "github.com/netsampler/goflow2/v3/decoders/utils"
	goflowpb "github.com/netsampler/goflow2/v3/pb"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestDecoderHandlesSFlowAndNetFlowV5(t *testing.T) {
	sflowPacket := sflow.Packet{
		Version: 5, IPVersion: 1, AgentIP: decoderutils.IPAddress{192, 0, 2, 9}, SubAgentId: 7, SequenceNumber: 99, Uptime: 1000,
		Samples: []interface{}{sflow.FlowSample{
			Header: sflow.SampleHeader{SampleSequenceNumber: 12, SourceIdValue: 44}, SamplingRate: 1000, SamplePool: 9000, Drops: 3, Input: 10, Output: 20,
			Records: []sflow.FlowRecord{{Data: sflow.SampledIPv4{SampledIPBase: sflow.SampledIPBase{Length: 128, Protocol: 6, SrcIP: decoderutils.IPAddress{10, 0, 0, 1}, DstIP: decoderutils.IPAddress{203, 0, 113, 2}, SrcPort: 12345, DstPort: 443, TcpFlags: 0x12}}}},
		}},
	}
	sflowPayload, err := sflowPacket.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	netflowV5Packet := netflowlegacy.PacketNetFlowV5{
		Version: 5, SysUptime: 10000, UnixSecs: uint32(time.Now().Unix()), FlowSequence: 88, SamplingInterval: 500,
		Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007102, Input: 3, Output: 4, DPkts: 2, DOctets: 1500, First: 9000, Last: 9500, SrcPort: 12345, DstPort: 443, TCPFlags: 0x12, Proto: 6, SrcAS: 64512, DstAS: 64513}},
	}
	netflowV5Payload, err := netflowV5Packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	sflowBatch, err := decoder.DecodeValue(rawFlowValue(t, flowpb.RawFlow_DECODER_SFLOW, sflowPayload))
	if err != nil {
		t.Fatal(err)
	}
	if len(sflowBatch.Records) != 1 {
		t.Fatalf("sFlow records=%d", len(sflowBatch.Records))
	}
	sflowRecord := sflowBatch.Records[0]
	if sflowBatch.FlowType != goflowpb.FlowMessage_SFLOW_5 || sflowBatch.ObservationDomainID != 0 {
		t.Fatalf("sFlow protocol metadata = %s/%d", sflowBatch.FlowType, sflowBatch.ObservationDomainID)
	}
	if sflowBatch.SubAgentID != 7 || sflowBatch.DatagramSequence != 99 || sflowBatch.AgentIP != netip.MustParseAddr("192.0.2.9") || len(sflowBatch.RecordMetadata) != 1 {
		t.Fatalf("unexpected sFlow datagram metadata: %+v", sflowBatch)
	}
	sflowMetadata := sflowBatch.RecordMetadata[0]
	if !sflowMetadata.Present || sflowMetadata.SourceIDType != 0 || sflowMetadata.SourceIDValue != 44 || sflowMetadata.SampleSequence != 12 || sflowMetadata.SamplePool != 9000 || sflowMetadata.ExporterDrops != 3 {
		t.Fatalf("unexpected sFlow sample metadata: %+v", sflowMetadata)
	}
	if sflowRecord.SamplingRate != 1000 || sflowRecord.Bytes != 128 || sflowRecord.Packets != 1 || sflowRecord.InIf != 10 || sflowRecord.OutIf != 20 || sflowRecord.DstPort != 443 || netip.MustParseAddr("10.0.0.1") != addressFromBytes(t, sflowRecord.SrcAddr) {
		t.Fatalf("unexpected sFlow record: %+v", sflowRecord)
	}

	netflowBatch, err := decoder.DecodeValue(rawFlowValue(t, flowpb.RawFlow_DECODER_NETFLOW, netflowV5Payload))
	if err != nil {
		t.Fatal(err)
	}
	if len(netflowBatch.Records) != 1 {
		t.Fatalf("NetFlow v5 records=%d", len(netflowBatch.Records))
	}
	netflowRecord := netflowBatch.Records[0]
	if netflowBatch.FlowType != goflowpb.FlowMessage_NETFLOW_V5 || netflowBatch.ObservationDomainID != 0 {
		t.Fatalf("NetFlow v5 protocol metadata = %s/%d", netflowBatch.FlowType, netflowBatch.ObservationDomainID)
	}
	if netflowRecord.SamplingRate != 500 || netflowRecord.Bytes != 1500 || netflowRecord.Packets != 2 || netflowRecord.SrcAs != 64512 || netflowRecord.DstAs != 64513 || netflowRecord.DstPort != 443 {
		t.Fatalf("unexpected NetFlow v5 record: %+v", netflowRecord)
	}
}

func TestPartitionDecodersHandleNetFlowV9AndIPFIX(t *testing.T) {
	v9Template, v9Data := netFlowV9Fixture(t)
	ipfixCombined := ipfixFixture(t)
	decoders := NewPartitionDecoders(time.Minute)
	defer decoders.Close()

	templateBatch, err := decoders.DecodeRecord(rawKafkaRecord(t, 2, flowpb.RawFlow_DECODER_NETFLOW, v9Template))
	if err != nil {
		t.Fatal(err)
	}
	if len(templateBatch.Records) != 0 {
		t.Fatalf("template produced %d records", len(templateBatch.Records))
	}
	if templateBatch.FlowType != goflowpb.FlowMessage_NETFLOW_V9 || templateBatch.ObservationDomainID != 42 {
		t.Fatalf("NetFlow v9 protocol metadata = %s/%d", templateBatch.FlowType, templateBatch.ObservationDomainID)
	}
	v9Batch, err := decoders.DecodeRecord(rawKafkaRecord(t, 2, flowpb.RawFlow_DECODER_NETFLOW, v9Data))
	if err != nil {
		t.Fatal(err)
	}
	assertMappedRecord(t, v9Batch, 1000, 2, 64512, 64513)

	ipfixBatch, err := decoders.DecodeRecord(rawKafkaRecord(t, 3, flowpb.RawFlow_DECODER_NETFLOW, ipfixCombined))
	if err != nil {
		t.Fatal(err)
	}
	assertMappedRecord(t, ipfixBatch, 1000, 2, 64512, 64513)
	if ipfixBatch.FlowType != goflowpb.FlowMessage_IPFIX || ipfixBatch.ObservationDomainID != 84 {
		t.Fatalf("IPFIX protocol metadata = %s/%d", ipfixBatch.FlowType, ipfixBatch.ObservationDomainID)
	}
}

func TestDecoderHandlesIPv6IPFIX(t *testing.T) {
	decoders := NewPartitionDecoders(time.Minute)
	defer decoders.Close()
	batch, err := decoders.DecodeRecord(rawKafkaRecord(t, 4, flowpb.RawFlow_DECODER_NETFLOW, ipfixIPv6Fixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("records=%d", len(batch.Records))
	}
	record := batch.Records[0]
	if addressFromBytes(t, record.SrcAddr) != netip.MustParseAddr("2001:db8::1") || addressFromBytes(t, record.DstAddr) != netip.MustParseAddr("2001:db8:1::2") || record.Bytes != 4096 {
		t.Fatalf("unexpected IPv6 record: %+v", record)
	}
}

func TestPartitionDecodersDoNotShareTemplateState(t *testing.T) {
	template, data := netFlowV9Fixture(t)
	decoders := NewPartitionDecoders(time.Minute)
	defer decoders.Close()
	if _, err := decoders.DecodeRecord(rawKafkaRecord(t, 1, flowpb.RawFlow_DECODER_NETFLOW, template)); err != nil {
		t.Fatal(err)
	}
	if _, err := decoders.DecodeRecord(rawKafkaRecord(t, 2, flowpb.RawFlow_DECODER_NETFLOW, data)); err == nil {
		t.Fatal("NetFlow data used a template from another Kafka partition")
	}
}

func TestDecoderRejectsMalformedRawEnvelope(t *testing.T) {
	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	if _, err := decoder.DecodeValue([]byte{0xff}); err == nil {
		t.Fatal("malformed raw protobuf was accepted")
	}
}

func TestPartitionDecodersProcessPartitionsConcurrently(t *testing.T) {
	packet := netflowlegacy.PacketNetFlowV5{Version: 5, UnixSecs: uint32(time.Now().Unix()), Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007102, DPkts: 1, DOctets: 64}}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	value := rawFlowValue(t, flowpb.RawFlow_DECODER_NETFLOW, payload)
	decoders := NewPartitionDecoders(time.Minute)
	defer decoders.Close()
	var wait sync.WaitGroup
	errorsFound := make(chan error, 8)
	for partition := range int32(8) {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 25 {
				batch, err := decoders.DecodeRecord(&kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: partition, Value: value})
				if err != nil {
					errorsFound <- err
					return
				}
				if len(batch.Records) != 1 {
					errorsFound <- errors.New("decoded record count mismatch")
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
}

func BenchmarkDecodeNetFlowV5(b *testing.B) {
	const recordsPerDatagram = 30
	records := make([]netflowlegacy.RecordsNetFlowV5, recordsPerDatagram)
	for index := range records {
		records[index] = netflowlegacy.RecordsNetFlowV5{SrcAddr: netflowlegacy.IPAddress(0x0a000001 + uint32(index)), DstAddr: 0xcb007102, Input: 3, Output: 4, DPkts: 2, DOctets: 1500, First: 9000, Last: 9500, SrcPort: 12345, DstPort: 443, TCPFlags: 0x12, Proto: 6, SrcAS: 64512, DstAS: 64513}
	}
	packet := netflowlegacy.PacketNetFlowV5{Version: 5, SysUptime: 10000, UnixSecs: uint32(time.Now().Unix()), FlowSequence: 88, SamplingInterval: 500, Records: records}
	payload, err := packet.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	value := rawFlowValue(b, flowpb.RawFlow_DECODER_NETFLOW, payload)
	decoder, err := NewDecoder(time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	defer decoder.Close()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		batch, err := decoder.DecodeValue(value)
		if err != nil {
			b.Fatal(err)
		}
		if len(batch.Records) != recordsPerDatagram {
			b.Fatalf("records=%d", len(batch.Records))
		}
	}
	b.ReportMetric(float64(b.N*recordsPerDatagram)/b.Elapsed().Seconds(), "records/s")
}

func rawFlowValue(t testing.TB, decoder flowpb.RawFlow_Decoder, payload []byte) []byte {
	t.Helper()
	raw, err := NewRawFlow("collector-a", "flow", 7, time.Unix(1_800_000_000, 0), netip.MustParseAddrPort("192.0.2.1:9999"), decoder, payload)
	if err != nil {
		t.Fatal(err)
	}
	value, err := proto.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func rawKafkaRecord(t testing.TB, partition int32, decoder flowpb.RawFlow_Decoder, payload []byte) *kgo.Record {
	t.Helper()
	return &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: partition, Value: rawFlowValue(t, decoder, payload)}
}

func netFlowV9Fixture(t testing.TB) ([]byte, []byte) {
	t.Helper()
	header := netflow.NFv9Packet{Version: 9, SystemUptime: 1000, UnixSeconds: uint32(time.Now().Unix()), SourceId: 42}
	template := header
	template.SequenceNumber = 12
	template.FlowSets = []interface{}{netflow.TemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 0}, Records: []netflow.TemplateRecord{{TemplateId: 256, Fields: []netflow.Field{
		{Type: netflow.NFV9_FIELD_IN_BYTES, Length: 4}, {Type: netflow.NFV9_FIELD_IN_PKTS, Length: 4}, {Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR, Length: 4}, {Type: netflow.NFV9_FIELD_IPV4_DST_ADDR, Length: 4}, {Type: netflow.NFV9_FIELD_SRC_AS, Length: 4}, {Type: netflow.NFV9_FIELD_DST_AS, Length: 4},
	}}}}}
	data := header
	data.SequenceNumber = 13
	data.FlowSets = []interface{}{netflow.DataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 256}, Records: []netflow.DataRecord{{Values: []netflow.DataField{
		{Type: netflow.NFV9_FIELD_IN_BYTES, Value: []byte{0, 0, 3, 232}}, {Type: netflow.NFV9_FIELD_IN_PKTS, Value: []byte{0, 0, 0, 2}}, {Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR, Value: []byte{10, 0, 0, 1}}, {Type: netflow.NFV9_FIELD_IPV4_DST_ADDR, Value: []byte{203, 0, 113, 1}}, {Type: netflow.NFV9_FIELD_SRC_AS, Value: []byte{0, 0, 252, 0}}, {Type: netflow.NFV9_FIELD_DST_AS, Value: []byte{0, 0, 252, 1}},
	}}}}}
	templatePayload, err := template.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	dataPayload, err := data.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return templatePayload, dataPayload
}

func ipfixFixture(t testing.TB) []byte {
	t.Helper()
	packet := netflow.IPFIXPacket{Version: 10, ExportTime: uint32(time.Now().Unix()), SequenceNumber: 13, ObservationDomainId: 84, FlowSets: []interface{}{
		netflow.TemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 2}, Records: []netflow.TemplateRecord{{TemplateId: 300, Fields: []netflow.Field{
			{Type: netflow.IPFIX_FIELD_octetDeltaCount, Length: 8}, {Type: netflow.IPFIX_FIELD_packetDeltaCount, Length: 8}, {Type: netflow.IPFIX_FIELD_sourceIPv4Address, Length: 4}, {Type: netflow.IPFIX_FIELD_destinationIPv4Address, Length: 4}, {Type: netflow.IPFIX_FIELD_bgpSourceAsNumber, Length: 4}, {Type: netflow.IPFIX_FIELD_bgpDestinationAsNumber, Length: 4},
			{PenProvided: true, Type: 4000, Length: 2, Pen: 32473},
		}}}},
		netflow.DataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 300}, Records: []netflow.DataRecord{{Values: []netflow.DataField{
			{Type: netflow.IPFIX_FIELD_octetDeltaCount, Value: []byte{0, 0, 0, 0, 0, 0, 3, 232}}, {Type: netflow.IPFIX_FIELD_packetDeltaCount, Value: []byte{0, 0, 0, 0, 0, 0, 0, 2}}, {Type: netflow.IPFIX_FIELD_sourceIPv4Address, Value: []byte{10, 0, 0, 1}}, {Type: netflow.IPFIX_FIELD_destinationIPv4Address, Value: []byte{203, 0, 113, 1}}, {Type: netflow.IPFIX_FIELD_bgpSourceAsNumber, Value: []byte{0, 0, 252, 0}}, {Type: netflow.IPFIX_FIELD_bgpDestinationAsNumber, Value: []byte{0, 0, 252, 1}},
			{PenProvided: true, Type: 4000, Pen: 32473, Value: []byte{0x12, 0x34}},
		}}}},
	}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func ipfixIPv6Fixture(t testing.TB) []byte {
	t.Helper()
	source := netip.MustParseAddr("2001:db8::1").As16()
	destination := netip.MustParseAddr("2001:db8:1::2").As16()
	packet := netflow.IPFIXPacket{Version: 10, ExportTime: uint32(time.Now().Unix()), SequenceNumber: 14, ObservationDomainId: 85, FlowSets: []interface{}{
		netflow.TemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 2}, Records: []netflow.TemplateRecord{{TemplateId: 301, Fields: []netflow.Field{
			{Type: netflow.IPFIX_FIELD_octetDeltaCount, Length: 8}, {Type: netflow.IPFIX_FIELD_sourceIPv6Address, Length: 16}, {Type: netflow.IPFIX_FIELD_destinationIPv6Address, Length: 16},
		}}}},
		netflow.DataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 301}, Records: []netflow.DataRecord{{Values: []netflow.DataField{
			{Type: netflow.IPFIX_FIELD_octetDeltaCount, Value: []byte{0, 0, 0, 0, 0, 0, 16, 0}}, {Type: netflow.IPFIX_FIELD_sourceIPv6Address, Value: source[:]}, {Type: netflow.IPFIX_FIELD_destinationIPv6Address, Value: destination[:]},
		}}}},
	}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func FuzzInspectFlowProtocolNeverPanics(f *testing.F) {
	f.Add(uint8(flowpb.RawFlow_DECODER_NETFLOW), []byte{0, 9})
	f.Add(uint8(flowpb.RawFlow_DECODER_SFLOW), []byte{0, 0, 0, 5})
	f.Add(uint8(255), []byte{0xff})
	f.Fuzz(func(t *testing.T, decoder uint8, payload []byte) {
		_, _, _ = inspectFlowProtocol(flowpb.RawFlow_Decoder(decoder), payload)
	})
}

func assertMappedRecord(t testing.TB, batch DecodedBatch, bytes, packets uint64, srcAS, dstAS uint32) {
	t.Helper()
	if batch.CollectorID != "collector-a" || batch.RegistryVersion != 7 || len(batch.Records) != 1 {
		t.Fatalf("unexpected decoded batch: %+v", batch)
	}
	record := batch.Records[0]
	if record.Bytes != bytes || record.Packets != packets || record.SrcAs != srcAS || record.DstAs != dstAS || addressFromBytes(t, record.SrcAddr) != netip.MustParseAddr("10.0.0.1") || addressFromBytes(t, record.DstAddr) != netip.MustParseAddr("203.0.113.1") {
		t.Fatalf("unexpected mapped record: %+v", record)
	}
}

func addressFromBytes(t testing.TB, value []byte) netip.Addr {
	t.Helper()
	address, ok := netip.AddrFromSlice(value)
	if !ok {
		t.Fatalf("invalid address bytes %x", value)
	}
	return address.Unmap()
}
