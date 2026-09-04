package flowcollect

import (
	"net/netip"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/netsampler/goflow2/v3/decoders/sflow"
	decoderutils "github.com/netsampler/goflow2/v3/decoders/utils"
)

func TestDecoderPreservesSFlowSampleState(t *testing.T) {
	packet := sflow.Packet{
		Version: 5, IPVersion: 1, AgentIP: decoderutils.IPAddress{192, 0, 2, 1}, SubAgentId: 7, SequenceNumber: 99, Uptime: 1000,
		Samples: []interface{}{sflow.FlowSample{
			Header: sflow.SampleHeader{SampleSequenceNumber: 12, SourceIdType: 0, SourceIdValue: 44}, SamplingRate: 1000, SamplePool: 9000, Drops: 3, Input: 10, Output: 20,
			Records: []sflow.FlowRecord{{Data: sflow.SampledIPv4{SampledIPBase: sflow.SampledIPBase{Length: 128, Protocol: 6, SrcIP: decoderutils.IPAddress{10, 0, 0, 1}, DstIP: decoderutils.IPAddress{203, 0, 113, 2}, SrcPort: 12345, DstPort: 443, TcpFlags: 0x12}}}},
		}},
	}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.Decode(decodeWALRecord(ProtocolSFlow5, payload))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.AgentIP.String() != "192.0.2.1" || decoded.SubAgentID != 7 || decoded.DatagramSequence != 99 || len(decoded.Records) != 1 {
		t.Fatalf("unexpected datagram: %+v", decoded)
	}
	record := decoded.Records[0]
	if record.SamplingRate != 1000 || record.SampleSequence != 12 || record.SamplePool != 9000 || record.ExporterDrops != 3 || record.SourceIDValue != 44 {
		t.Fatalf("sample state lost: %+v", record)
	}
	if record.SrcIP.String() != "10.0.0.1" || record.DstIP.String() != "203.0.113.2" || record.DstPort != 443 {
		t.Fatalf("flow fields lost: %+v", record)
	}
}

func TestDecoderPreservesNetFlowV5ASNAndSampling(t *testing.T) {
	packet := netflowlegacy.PacketNetFlowV5{Version: 5, SysUptime: 10000, UnixSecs: uint32(time.Now().Unix()), FlowSequence: 88, SamplingInterval: 500, Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007102, Input: 3, Output: 4, DPkts: 2, DOctets: 1500, First: 9000, Last: 9500, SrcPort: 12345, DstPort: 443, TCPFlags: 0x12, Proto: 6, SrcAS: 64512, DstAS: 64513}}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow5, payload))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.DatagramSequence != 88 || len(decoded.Records) != 1 {
		t.Fatalf("unexpected datagram: %+v", decoded)
	}
	record := decoded.Records[0]
	if record.SrcAS != 64512 || record.DstAS != 64513 || record.SamplingRate != 500 || record.RawBytes != 1500 || record.RawPackets != 2 {
		t.Fatalf("NetFlow fields lost: %+v", record)
	}
}

func TestDecoderHandlesNetFlowV9TemplateAndData(t *testing.T) {
	packet := netflow.NFv9Packet{Version: 9, SystemUptime: 1000, UnixSeconds: uint32(time.Now().Unix()), SequenceNumber: 12, SourceId: 42, FlowSets: []interface{}{
		netflow.TemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 0}, Records: []netflow.TemplateRecord{{TemplateId: 256, Fields: []netflow.Field{{Type: netflow.NFV9_FIELD_IN_BYTES, Length: 4}, {Type: netflow.NFV9_FIELD_IN_PKTS, Length: 4}, {Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR, Length: 4}, {Type: netflow.NFV9_FIELD_IPV4_DST_ADDR, Length: 4}, {Type: netflow.NFV9_FIELD_SRC_AS, Length: 4}, {Type: netflow.NFV9_FIELD_DST_AS, Length: 4}}}}},
		netflow.DataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 256}, Records: []netflow.DataRecord{{Values: []netflow.DataField{{Type: netflow.NFV9_FIELD_IN_BYTES, Value: []byte{0, 0, 3, 232}}, {Type: netflow.NFV9_FIELD_IN_PKTS, Value: []byte{0, 0, 0, 2}}, {Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR, Value: []byte{10, 0, 0, 1}}, {Type: netflow.NFV9_FIELD_IPV4_DST_ADDR, Value: []byte{203, 0, 113, 1}}, {Type: netflow.NFV9_FIELD_SRC_AS, Value: []byte{0, 0, 252, 0}}, {Type: netflow.NFV9_FIELD_DST_AS, Value: []byte{0, 0, 252, 1}}}}}},
	}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, payload))
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.TemplateChanged || decoded.ObservationDomainID != 42 || len(decoded.Records) != 1 {
		t.Fatalf("unexpected v9 datagram: %+v", decoded)
	}
	if decoded.Records[0].RawBytes != 1000 || decoded.Records[0].SrcAS != 64512 || decoded.Records[0].DstAS != 64513 {
		t.Fatalf("v9 fields lost: %+v", decoded.Records[0])
	}
}

func TestDecoderHandlesIPFIXTemplateAndData(t *testing.T) {
	packet := netflow.IPFIXPacket{Version: 10, ExportTime: uint32(time.Now().Unix()), SequenceNumber: 13, ObservationDomainId: 84, FlowSets: []interface{}{
		netflow.TemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 2}, Records: []netflow.TemplateRecord{{TemplateId: 300, Fields: []netflow.Field{{Type: netflow.IPFIX_FIELD_octetDeltaCount, Length: 8}, {Type: netflow.IPFIX_FIELD_packetDeltaCount, Length: 8}, {Type: netflow.IPFIX_FIELD_sourceIPv4Address, Length: 4}, {Type: netflow.IPFIX_FIELD_destinationIPv4Address, Length: 4}, {Type: netflow.IPFIX_FIELD_bgpSourceAsNumber, Length: 4}, {Type: netflow.IPFIX_FIELD_bgpDestinationAsNumber, Length: 4}}}}},
		netflow.DataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 300}, Records: []netflow.DataRecord{{Values: []netflow.DataField{{Type: netflow.IPFIX_FIELD_octetDeltaCount, Value: []byte{0, 0, 0, 0, 0, 0, 3, 232}}, {Type: netflow.IPFIX_FIELD_packetDeltaCount, Value: []byte{0, 0, 0, 0, 0, 0, 0, 2}}, {Type: netflow.IPFIX_FIELD_sourceIPv4Address, Value: []byte{10, 0, 0, 1}}, {Type: netflow.IPFIX_FIELD_destinationIPv4Address, Value: []byte{203, 0, 113, 1}}, {Type: netflow.IPFIX_FIELD_bgpSourceAsNumber, Value: []byte{0, 0, 252, 0}}, {Type: netflow.IPFIX_FIELD_bgpDestinationAsNumber, Value: []byte{0, 0, 252, 1}}}}}},
	}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, payload))
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.TemplateChanged || decoded.ObservationDomainID != 84 || len(decoded.Records) != 1 {
		t.Fatalf("unexpected IPFIX datagram: %+v", decoded)
	}
	if decoded.Records[0].RawBytes != 1000 || decoded.Records[0].SrcAS != 64512 || decoded.Records[0].DstAS != 64513 {
		t.Fatalf("IPFIX fields lost: %+v", decoded.Records[0])
	}
}

func decodeWALRecord(protocol Protocol, payload []byte) WALRecord {
	return WALRecord{WALInput: WALInput{Protocol: protocol, ReceivedAt: time.Now(), Source: netip.MustParseAddrPort("192.0.2.1:9999"), Payload: payload}}
}
