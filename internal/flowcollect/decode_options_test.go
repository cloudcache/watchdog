package flowcollect

import (
	"context"
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

func TestDecoderLearnsNetFlowV9SamplingFromOptions(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	options := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowV9OptionsPacket(42, 1000)))
	decoded, err := decoder.Decode(options)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.TemplateChanged || !decoded.CollectStateChanged || decoded.SequenceIncrement != 1 || len(decoded.Records) != 0 {
		t.Fatalf("unexpected v9 options result: %+v", decoded)
	}

	flow := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(true)))
	decoded, err = decoder.Decode(flow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 1000 {
		t.Fatalf("v9 options sampling rate was not applied: %+v", decoded)
	}
}

func TestDecoderRefreshesNetFlowV9OptionsState(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	source := netip.MustParseAddr("192.0.2.1")

	if _, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowV9OptionsPacket(42, 1000)))); err != nil {
		t.Fatal(err)
	}
	_, _, firstGeneration, err := decoder.SnapshotState(ProtocolNetFlow9, source, 42)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowV9OptionsPacket(42, 2000)))); err != nil {
		t.Fatal(err)
	}
	_, _, secondGeneration, err := decoder.SnapshotState(ProtocolNetFlow9, source, 42)
	if err != nil {
		t.Fatal(err)
	}
	if secondGeneration <= firstGeneration {
		t.Fatalf("options refresh did not advance state generation: first=%d second=%d", firstGeneration, secondGeneration)
	}

	decoded, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(true))))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 2000 {
		t.Fatalf("refreshed sampling rate was not applied: %+v", decoded)
	}
}

func TestDecoderReplaysOutOfOrderNetFlowV9OptionsData(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	templateOnly, dataOnly := splitNetFlowV9OptionsFixture(t, netflowV9OptionsPacket(42, 3000))

	if _, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, dataOnly)); !errors.Is(err, ErrTemplatePending) {
		t.Fatalf("options data before template error=%v, want ErrTemplatePending", err)
	}
	if decoded, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, templateOnly)); err != nil || !decoded.TemplateChanged || !decoded.CollectStateChanged {
		t.Fatalf("options template did not establish state: decoded=%+v err=%v", decoded, err)
	}
	if decoded, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, dataOnly)); err != nil || !decoded.CollectStateChanged {
		t.Fatalf("replayed options data did not update sampling state: decoded=%+v err=%v", decoded, err)
	}
	decoded, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(true))))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 3000 {
		t.Fatalf("replayed options sampling rate was not applied: %+v", decoded)
	}
}

func TestDecoderPreservesTemplateStateWhenOptionsMappingFails(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	packet := netflowV9UnsupportedShortOptionsPacket(42, 1000)

	decoded, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, packet)))
	if err == nil {
		t.Fatal("unsupported short options value unexpectedly mapped")
	}
	if !decoded.TemplateChanged || !decoded.CollectStateChanged || decoded.ObservationDomainID != 42 {
		t.Fatalf("template state was lost on producer mapping failure: %+v err=%v", decoded, err)
	}
	if _, _, generation, snapshotErr := decoder.SnapshotState(ProtocolNetFlow9, netip.MustParseAddr("192.0.2.1"), 42); snapshotErr != nil || generation == 0 {
		t.Fatalf("learned options template was not checkpointable: generation=%d err=%v", generation, snapshotErr)
	}
}

func TestRunnerCheckpointsTemplateBeforeOptionsDecodeRejection(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", ObservationDomainID: &domain, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModePreScaled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	record, err := wal.Append(WALInput{Protocol: ProtocolNetFlow9, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), ObservationDomainID: domain, RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", Payload: mustMarshalNFv9(t, netflowV9UnsupportedShortOptionsPacket(42, 1000))})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	state, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	runner := &Runner{Config: DefaultConfig(), Registry: registry, WAL: wal, Decoder: decoder, State: state, Publisher: publisher}
	if err := runner.processRecord(context.Background(), record, 0); err == nil {
		t.Fatal("options mapping rejection unexpectedly succeeded")
	}
	if len(publisher.events) != 1 || publisher.events[0] != "state" || len(publisher.states) != 1 {
		t.Fatalf("template state was not published before decode rejection: events=%v states=%d", publisher.events, len(publisher.states))
	}
	if replayCount(t, wal) != 1 {
		t.Fatal("decode-rejected WAL record was acknowledged before DLQ")
	}
}

func TestDecoderLearnsIPFIXSamplingAndCountsOptionsRecords(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	options := decodeWALRecord(ProtocolIPFIX, mustMarshalIPFIX(t, ipfixOptionsPacket(84, 2000)))
	decoded, err := decoder.Decode(options)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.TemplateChanged || !decoded.CollectStateChanged || decoded.SequenceIncrement != 1 || len(decoded.Records) != 0 {
		t.Fatalf("unexpected IPFIX options result: %+v", decoded)
	}

	flow := decodeWALRecord(ProtocolIPFIX, mustMarshalIPFIX(t, ipfixDataTemplatePacket(84, true)))
	decoded, err = decoder.Decode(flow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 2000 || decoded.SequenceIncrement != 1 {
		t.Fatalf("IPFIX options sampling rate was not applied: %+v", decoded)
	}
}

func TestDecoderRefreshesIPFIXOptionsState(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	source := netip.MustParseAddr("192.0.2.1")

	if _, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, mustMarshalIPFIX(t, ipfixOptionsPacket(84, 1000)))); err != nil {
		t.Fatal(err)
	}
	_, _, firstGeneration, err := decoder.SnapshotState(ProtocolIPFIX, source, 84)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, mustMarshalIPFIX(t, ipfixOptionsPacket(84, 2000)))); err != nil {
		t.Fatal(err)
	}
	_, _, secondGeneration, err := decoder.SnapshotState(ProtocolIPFIX, source, 84)
	if err != nil {
		t.Fatal(err)
	}
	if secondGeneration <= firstGeneration {
		t.Fatalf("IPFIX options refresh did not advance state generation: first=%d second=%d", firstGeneration, secondGeneration)
	}

	decoded, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, mustMarshalIPFIX(t, ipfixDataTemplatePacket(84, true))))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 2000 {
		t.Fatalf("refreshed IPFIX sampling rate was not applied: %+v", decoded)
	}
}

func TestDecoderReplaysOutOfOrderIPFIXOptionsData(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	templateOnly, dataOnly := splitIPFIXOptionsFixture(t, ipfixOptionsPacket(84, 3000))

	if _, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, dataOnly)); !errors.Is(err, ErrTemplatePending) {
		t.Fatalf("IPFIX options data before template error=%v, want ErrTemplatePending", err)
	}
	if decoded, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, templateOnly)); err != nil || !decoded.TemplateChanged || !decoded.CollectStateChanged {
		t.Fatalf("IPFIX options template did not establish state: decoded=%+v err=%v", decoded, err)
	}
	if decoded, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, dataOnly)); err != nil || !decoded.CollectStateChanged || decoded.SequenceIncrement != 1 {
		t.Fatalf("replayed IPFIX options data did not update sampling state: decoded=%+v err=%v", decoded, err)
	}
	decoded, err := decoder.Decode(decodeWALRecord(ProtocolIPFIX, mustMarshalIPFIX(t, ipfixDataTemplatePacket(84, true))))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 3000 {
		t.Fatalf("replayed IPFIX options sampling rate was not applied: %+v", decoded)
	}
}

func TestDecoderReplaysOutOfOrderDataAfterTemplateArrival(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	data := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket()))
	if _, err := decoder.Decode(data); !errors.Is(err, ErrTemplatePending) {
		t.Fatalf("data before template error=%v, want ErrTemplatePending", err)
	}
	template := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(false)))
	if decoded, err := decoder.Decode(template); err != nil || !decoded.CollectStateChanged {
		t.Fatalf("template arrival did not establish state: decoded=%+v err=%v", decoded, err)
	}
	decoded, err := decoder.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].RawBytes != 1000 {
		t.Fatalf("replayed data did not decode: %+v", decoded)
	}
}

func TestDecoderOptionsStateIsIsolatedBySourceAndDomain(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	if _, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowV9OptionsPacket(42, 1000)))); err != nil {
		t.Fatal(err)
	}
	otherSource := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(true)))
	otherSource.Source = netip.MustParseAddrPort("192.0.2.2:9999")
	decoded, err := decoder.Decode(otherSource)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 0 {
		t.Fatalf("sampling state leaked across transport sources: %+v", decoded)
	}

	otherDomainPacket := netflowTemplatePacket(true)
	otherDomainPacket.SourceId = 43
	decoded, err = decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, otherDomainPacket)))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 0 {
		t.Fatalf("sampling state leaked across observation domains: %+v", decoded)
	}
}

func TestDecoderRestoresOptionsDerivedSamplingState(t *testing.T) {
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	source := netip.MustParseAddr("192.0.2.1")
	if _, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowV9OptionsPacket(42, 4000)))); err != nil {
		decoder.Close()
		t.Fatal(err)
	}
	if _, err := decoder.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(false)))); err != nil {
		decoder.Close()
		t.Fatal(err)
	}
	templates, sampling, generation, err := decoder.SnapshotState(ProtocolNetFlow9, source, 42)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}

	restored, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.RestoreStateRevision(ProtocolNetFlow9, source, 42, generation, templates, sampling); err != nil {
		t.Fatal(err)
	}
	decoded, err := restored.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket())))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].SamplingRate != 4000 {
		t.Fatalf("options-derived state did not survive restore: %+v", decoded)
	}
}

func netflowV9OptionsPacket(domain, rate uint32) netflow.NFv9Packet {
	return netflow.NFv9Packet{
		Version: 9, SystemUptime: 1000, UnixSeconds: uint32(time.Now().Unix()), SequenceNumber: 10, SourceId: domain,
		FlowSets: []interface{}{
			netflow.NFv9OptionsTemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 1}, Records: []netflow.NFv9OptionsTemplateRecord{{TemplateId: 257, Scopes: []netflow.Field{{Type: 1, Length: 4}}, Options: []netflow.Field{{Type: netflow.NFV9_FIELD_SAMPLING_INTERVAL, Length: 4}}}}},
			netflow.OptionsDataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 257}, Records: []netflow.OptionsDataRecord{{ScopesValues: []netflow.DataField{{Type: 1, Value: uint32Bytes(domain)}}, OptionsValues: []netflow.DataField{{Type: netflow.NFV9_FIELD_SAMPLING_INTERVAL, Value: uint32Bytes(rate)}}}}},
		},
	}
}

func netflowV9UnsupportedShortOptionsPacket(domain, rate uint32) netflow.NFv9Packet {
	packet := netflowV9OptionsPacket(domain, rate)
	optionTemplate := packet.FlowSets[0].(netflow.NFv9OptionsTemplateFlowSet)
	optionTemplate.Records[0].Options[0].Length = 2
	packet.FlowSets[0] = optionTemplate
	optionData := packet.FlowSets[1].(netflow.OptionsDataFlowSet)
	optionData.Records[0].OptionsValues[0].Value = []byte{byte(rate >> 8), byte(rate)}
	packet.FlowSets[1] = optionData
	return packet
}

func ipfixOptionsPacket(domain, rate uint32) netflow.IPFIXPacket {
	return netflow.IPFIXPacket{
		Version: 10, ExportTime: uint32(time.Now().Unix()), SequenceNumber: 20, ObservationDomainId: domain,
		FlowSets: []interface{}{
			netflow.IPFIXOptionsTemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 3}, Records: []netflow.IPFIXOptionsTemplateRecord{{TemplateId: 301, Scopes: []netflow.Field{{Type: netflow.IPFIX_FIELD_observationDomainId, Length: 4}}, Options: []netflow.Field{{Type: netflow.IPFIX_FIELD_samplingInterval, Length: 4}}}}},
			netflow.OptionsDataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 301}, Records: []netflow.OptionsDataRecord{{ScopesValues: []netflow.DataField{{Type: netflow.IPFIX_FIELD_observationDomainId, Value: uint32Bytes(domain)}}, OptionsValues: []netflow.DataField{{Type: netflow.IPFIX_FIELD_samplingInterval, Value: uint32Bytes(rate)}}}}},
		},
	}
}

func ipfixDataTemplatePacket(domain uint32, withData bool) netflow.IPFIXPacket {
	sets := []interface{}{netflow.TemplateFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 2}, Records: []netflow.TemplateRecord{{TemplateId: 300, Fields: []netflow.Field{{Type: netflow.IPFIX_FIELD_octetDeltaCount, Length: 8}, {Type: netflow.IPFIX_FIELD_packetDeltaCount, Length: 8}, {Type: netflow.IPFIX_FIELD_sourceIPv4Address, Length: 4}, {Type: netflow.IPFIX_FIELD_destinationIPv4Address, Length: 4}}}}}}
	if withData {
		sets = append(sets, netflow.DataFlowSet{FlowSetHeader: netflow.FlowSetHeader{Id: 300}, Records: []netflow.DataRecord{{Values: []netflow.DataField{{Type: netflow.IPFIX_FIELD_octetDeltaCount, Value: []byte{0, 0, 0, 0, 0, 0, 3, 232}}, {Type: netflow.IPFIX_FIELD_packetDeltaCount, Value: []byte{0, 0, 0, 0, 0, 0, 0, 2}}, {Type: netflow.IPFIX_FIELD_sourceIPv4Address, Value: []byte{10, 0, 0, 1}}, {Type: netflow.IPFIX_FIELD_destinationIPv4Address, Value: []byte{203, 0, 113, 1}}}}}})
	}
	return netflow.IPFIXPacket{Version: 10, ExportTime: uint32(time.Now().Unix()), SequenceNumber: 21, ObservationDomainId: domain, FlowSets: sets}
}

func mustMarshalIPFIX(t *testing.T, packet netflow.IPFIXPacket) []byte {
	t.Helper()
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func uint32Bytes(value uint32) []byte {
	return []byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
}

func splitNetFlowV9OptionsFixture(t *testing.T, packet netflow.NFv9Packet) ([]byte, []byte) {
	t.Helper()
	full := mustMarshalNFv9(t, packet)
	const headerLength = 20
	if len(full) < headerLength+8 {
		t.Fatalf("encoded NetFlow v9 options fixture is too short: %d", len(full))
	}
	firstLength := int(binary.BigEndian.Uint16(full[headerLength+2 : headerLength+4]))
	if firstLength < 4 || headerLength+firstLength >= len(full) {
		t.Fatalf("invalid first flow-set length: %d", firstLength)
	}
	makePacket := func(flowSet []byte) []byte {
		out := append([]byte(nil), full[:headerLength]...)
		binary.BigEndian.PutUint16(out[2:4], 1)
		return append(out, flowSet...)
	}
	return makePacket(full[headerLength : headerLength+firstLength]), makePacket(full[headerLength+firstLength:])
}

func splitIPFIXOptionsFixture(t *testing.T, packet netflow.IPFIXPacket) ([]byte, []byte) {
	t.Helper()
	full := mustMarshalIPFIX(t, packet)
	const headerLength = 16
	if len(full) < headerLength+8 {
		t.Fatalf("encoded IPFIX options fixture is too short: %d", len(full))
	}
	firstLength := int(binary.BigEndian.Uint16(full[headerLength+2 : headerLength+4]))
	if firstLength < 4 || headerLength+firstLength >= len(full) {
		t.Fatalf("invalid first IPFIX set length: %d", firstLength)
	}
	makePacket := func(flowSet []byte) []byte {
		out := append([]byte(nil), full[:headerLength]...)
		binary.BigEndian.PutUint16(out[2:4], uint16(headerLength+len(flowSet)))
		return append(out, flowSet...)
	}
	return makePacket(full[headerLength : headerLength+firstLength]), makePacket(full[headerLength+firstLength:])
}
