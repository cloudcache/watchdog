package flowcollect

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

func TestCollectStateCheckpointRestoresTemplateAndSamplingRate(t *testing.T) {
	dir := t.TempDir()
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	templateRecord := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(false)))
	templateRecord.DatagramID = DatagramID{1, 2, 3}
	templateRecord.RegistryVersion = 1
	decoded, err := decoder.Decode(templateRecord)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.TemplateChanged || !decoded.CollectStateChanged {
		t.Fatalf("template state change was not detected: %+v", decoded)
	}
	flowContext := netflow.FlowContext{RouterKey: templateRecord.Source.Addr().String()}
	if err := decoder.sampling.Set(flowContext, 9, 42, 1000); err != nil {
		t.Fatal(err)
	}
	state, err := BuildCollectState(templateRecord, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a"}, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenCollectStateStore(dir, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(state); err != nil {
		t.Fatal(err)
	}
	decoder.Close()

	restored, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := OpenCollectStateStore(dir, "collector-a", restored); err != nil {
		t.Fatal(err)
	}
	dataRecord := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket()))
	data, err := restored.Decode(dataRecord)
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Records) != 1 || data.Records[0].RawBytes != 1000 || data.Records[0].SamplingRate != 1000 {
		t.Fatalf("restored decoder produced unexpected data: %+v", data)
	}
}

func TestCollectStateCheckpointCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	decoded := DecodedDatagram{Protocol: ProtocolNetFlow9, ObservationDomainID: 42, CollectStateChanged: true}
	record := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(false)))
	record.DatagramID = DatagramID{4, 5, 6}
	record.RegistryVersion = 1
	if _, err := decoder.Decode(record); err != nil {
		t.Fatal(err)
	}
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a"}, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenCollectStateStore(dir, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(state); err != nil {
		t.Fatal(err)
	}
	decoder.Close()
	files, err := filepath.Glob(filepath.Join(dir, "*.state"))
	if err != nil || len(files) != 1 {
		t.Fatalf("checkpoint files=%v err=%v", files, err)
	}
	file, err := os.OpenFile(files[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, collectStateHeaderSize); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	restored, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := OpenCollectStateStore(dir, "collector-a", restored); err == nil {
		t.Fatal("corrupt collect-state checkpoint was accepted")
	}
}

func TestCollectStateCheckpointDoesNotRestoreExpiredTemplate(t *testing.T) {
	dir := t.TempDir()
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	record := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(false)))
	record.DatagramID = DatagramID{7, 8, 9}
	record.RegistryVersion = 1
	record.ReceivedAt = time.Now().Add(-defaultDecoderStateTTL - time.Minute)
	decoded, err := decoder.Decode(record)
	if err != nil {
		t.Fatal(err)
	}
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a"}, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenCollectStateStore(dir, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(state); err != nil {
		t.Fatal(err)
	}
	decoder.Close()

	restored, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := OpenCollectStateStore(dir, "collector-a", restored); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket()))); !errors.Is(err, ErrTemplatePending) {
		t.Fatalf("expired template was restored: %v", err)
	}
}

func netflowTemplatePacket(withData bool) netflow.NFv9Packet {
	sets := []interface{}{
		netflow.TemplateFlowSet{
			FlowSetHeader: netflow.FlowSetHeader{Id: 0},
			Records: []netflow.TemplateRecord{{
				TemplateId: 256,
				Fields: []netflow.Field{
					{Type: netflow.NFV9_FIELD_IN_BYTES, Length: 4},
					{Type: netflow.NFV9_FIELD_IN_PKTS, Length: 4},
					{Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR, Length: 4},
					{Type: netflow.NFV9_FIELD_IPV4_DST_ADDR, Length: 4},
				},
			}},
		},
	}
	if withData {
		sets = append(sets, netflowDataSet())
	}
	return netflow.NFv9Packet{Version: 9, SystemUptime: 1000, UnixSeconds: uint32(time.Now().Unix()), SequenceNumber: 12, SourceId: 42, FlowSets: sets}
}

func netflowDataPacket() netflow.NFv9Packet {
	return netflow.NFv9Packet{Version: 9, SystemUptime: 1000, UnixSeconds: uint32(time.Now().Unix()), SequenceNumber: 13, SourceId: 42, FlowSets: []interface{}{netflowDataSet()}}
}

func netflowDataSet() netflow.DataFlowSet {
	return netflow.DataFlowSet{
		FlowSetHeader: netflow.FlowSetHeader{Id: 256},
		Records: []netflow.DataRecord{
			{Values: []netflow.DataField{
				{Type: netflow.NFV9_FIELD_IN_BYTES, Value: []byte{0, 0, 3, 232}},
				{Type: netflow.NFV9_FIELD_IN_PKTS, Value: []byte{0, 0, 0, 2}},
				{Type: netflow.NFV9_FIELD_IPV4_SRC_ADDR, Value: []byte{10, 0, 0, 1}},
				{Type: netflow.NFV9_FIELD_IPV4_DST_ADDR, Value: []byte{203, 0, 113, 1}},
			}},
		},
	}
}

func mustMarshalNFv9(t *testing.T, packet netflow.NFv9Packet) []byte {
	t.Helper()
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
