package flowcollect

import (
	"bytes"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"google.golang.org/protobuf/proto"
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
	registry := collectStateRegistry(t, "collector-a", 1, templateRecord.Source.Addr())
	store, err := OpenCollectStateStore(dir, "collector-a", registry, decoder)
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
	if _, err := OpenCollectStateStore(dir, "collector-a", registry, restored); err != nil {
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
	registry := collectStateRegistry(t, "collector-a", 1, record.Source.Addr())
	store, err := OpenCollectStateStore(dir, "collector-a", registry, decoder)
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
	if _, err := OpenCollectStateStore(dir, "collector-a", registry, restored); err == nil {
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
	registry := collectStateRegistry(t, "collector-a", 1, record.Source.Addr())
	store, err := OpenCollectStateStore(dir, "collector-a", registry, decoder)
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
	if _, err := OpenCollectStateStore(dir, "collector-a", registry, restored); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket()))); !errors.Is(err, ErrTemplatePending) {
		t.Fatalf("expired template was restored: %v", err)
	}
}

func TestCollectStateRecoveryUsesExactHistoricalPlanForRemovedSource(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	historical := collectStateRegistry(t, "collector-a", 1, record.Source.Addr())
	state, err := BuildCollectState(record, decoded, historical.Plan().Sources[0], "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := OpenCollectStateStore(dir, "collector-a", historical, decoder)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(state); err != nil {
		t.Fatal(err)
	}
	decoder.Close()

	activePlan := historical.Plan()
	activePlan.Revision = 2
	activePlan.Sources = nil
	active, err := CompilePlan(activePlan, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	history := testPlanHistory(active, historical, active)
	restored, _ := NewDecoder()
	defer restored.Close()
	recovered, err := OpenCollectStateStoreWithRecovery(dir, "collector-a", active, history, restored, nil)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.RestoredCount() != 1 {
		t.Fatalf("restored states=%d, want 1", recovered.RestoredCount())
	}
	if _, err := restored.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket()))); err != nil {
		t.Fatalf("historical template was not restored: %v", err)
	}
}

func TestCollectStateRecoveryRejectsHistoricalStateWhenTupleIsRebound(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	historical := collectStateRegistry(t, "collector-a", 1, record.Source.Addr())
	state, err := BuildCollectState(record, decoded, historical.Plan().Sources[0], "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	activePlan := historical.Plan()
	activePlan.Revision = 2
	activePlan.Sources = append([]SourceBinding(nil), activePlan.Sources...)
	activePlan.Sources[0].TenantID = "tenant-b"
	activePlan.Sources[0].ExporterID = "exporter-b"
	activePlan.Sources[0].OwnershipEpoch = 2
	active, err := CompilePlan(activePlan, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	history := testPlanHistory(active, historical, active)
	eligible, err := authorizeCollectStateRecovery(state, active, history, true)
	if err != nil {
		t.Fatal(err)
	}
	if eligible {
		t.Fatal("historical state crossed an active tuple rebind")
	}
}

func TestCollectStateV2IdentitySurvivesOwnerChangeAndEpochFencesWrites(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	defer decoder.Close()
	binding := SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 7}
	first, err := BuildCollectState(record, decoded, binding, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildCollectState(record, decoded, binding, "collector-b", decoder)
	if err != nil {
		t.Fatal(err)
	}
	if first.StateSchemaVersion != 2 || !bytes.Equal(first.StateIdentityKey, second.StateIdentityKey) || !bytes.Equal(first.StateKey, second.StateKey) {
		t.Fatalf("logical or compacted key changed with collector owner: first=%x/%x second=%x/%x", first.StateIdentityKey, first.StateKey, second.StateIdentityKey, second.StateKey)
	}
	third, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 8}, "collector-b", decoder)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.StateIdentityKey, third.StateIdentityKey) || bytes.Equal(first.StateKey, third.StateKey) {
		t.Fatal("ownership epoch did not isolate the stale owner's compacted key")
	}
}

func TestRestoreRemoteCollectStateAcrossOwnershipEpoch(t *testing.T) {
	now := time.Now()
	oldDecoder, record, decoded := collectStateFixture(t)
	flowContext := netflow.FlowContext{RouterKey: record.Source.Addr().String()}
	if err := oldDecoder.sampling.Set(flowContext, 9, 42, 1000); err != nil {
		t.Fatal(err)
	}
	oldState, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 4}, "collector-a", oldDecoder)
	if err != nil {
		t.Fatal(err)
	}
	oldDecoder.Close()

	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.CollectorID = "collector-b"
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: record.Source.Addr().String() + "/32", ObservationDomainID: uint64Pointer(42), TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: 5, SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	count, err := RestoreRemoteCollectStates(restored, registry, []CollectStateRecord{{State: oldState, Partition: 2, Offset: 41}}, now)
	if err != nil || count != 1 {
		t.Fatalf("remote restore count=%d err=%v", count, err)
	}
	data, err := restored.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket())))
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Records) != 1 || data.Records[0].SamplingRate != 1000 {
		t.Fatalf("remote decoder state was not restored: %+v", data)
	}
	if err := restored.sampling.Set(flowContext, 9, 42, 2000); err != nil {
		t.Fatal(err)
	}
	record.DatagramID = DatagramID{9, 9, 9}
	current, err := BuildCollectState(record, decoded, plan.Sources[0], plan.CollectorID, restored)
	if err != nil {
		t.Fatal(err)
	}
	if current.OwnershipEpoch != 5 || current.StateGeneration <= oldState.StateGeneration {
		t.Fatalf("restored generation did not advance under the new epoch: old=%d new=%d", oldState.StateGeneration, current.StateGeneration)
	}
}

func TestRestoreRemoteCollectStateRejectsEpochReuseAcrossCollectors(t *testing.T) {
	now := time.Now()
	decoder, record, decoded := collectStateFixture(t)
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 4}, "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.CollectorID = "collector-b"
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: record.Source.Addr().String() + "/32", ObservationDomainID: uint64Pointer(42), TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: 4, SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	restored, _ := NewDecoder()
	defer restored.Close()
	if _, err := RestoreRemoteCollectStates(restored, registry, []CollectStateRecord{{State: state}}, now); err == nil {
		t.Fatal("ownership epoch reuse across collectors was accepted")
	}
}

func TestRestoreRemoteCollectStatePrefersOwnershipEpochOverOffsetAndGeneration(t *testing.T) {
	now := time.Now()
	oldDecoder, record, decoded := collectStateFixture(t)
	flowContext := netflow.FlowContext{RouterKey: record.Source.Addr().String()}
	if err := oldDecoder.sampling.Set(flowContext, 9, 42, 1000); err != nil {
		t.Fatal(err)
	}
	stale, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 4}, "collector-a", oldDecoder)
	oldDecoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	stale.StateGeneration = 999
	staleDigest, err := collectStateDigest(stale)
	if err != nil {
		t.Fatal(err)
	}
	stale.PayloadSha256 = staleDigest[:]

	currentDecoder, _, currentDecoded := collectStateFixture(t)
	if err := currentDecoder.sampling.Set(flowContext, 9, 42, 2000); err != nil {
		t.Fatal(err)
	}
	current, err := BuildCollectState(record, currentDecoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 5}, "collector-b", currentDecoder)
	currentDecoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.CollectorID = "collector-b"
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: record.Source.Addr().String() + "/32", ObservationDomainID: uint64Pointer(42), TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: 5, SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	restored, _ := NewDecoder()
	defer restored.Close()
	count, err := RestoreRemoteCollectStates(restored, registry, []CollectStateRecord{{State: current, Partition: 1, Offset: 10}, {State: stale, Partition: 1, Offset: 999}}, now)
	if err != nil || count != 1 {
		t.Fatalf("restore count=%d err=%v", count, err)
	}
	data, err := restored.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket())))
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Records) != 1 || data.Records[0].SamplingRate != 2000 {
		t.Fatalf("stale owner won restore ordering: %+v", data)
	}
}

func TestRestoreRemoteCollectStateRejectsSameGenerationConflict(t *testing.T) {
	now := time.Now()
	decoder, record, decoded := collectStateFixture(t)
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 4}, "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	conflict := proto.Clone(state).(*flowpb.CollectState)
	conflict.TemplatesJson = []byte("{}")
	digest, err := collectStateDigest(conflict)
	if err != nil {
		t.Fatal(err)
	}
	conflict.PayloadSha256 = digest[:]
	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.CollectorID = "collector-a"
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: record.Source.Addr().String() + "/32", ObservationDomainID: uint64Pointer(42), TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: 4, SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	restored, _ := NewDecoder()
	defer restored.Close()
	if _, err := RestoreRemoteCollectStates(restored, registry, []CollectStateRecord{{State: state, Offset: 10}, {State: conflict, Offset: 11}}, now); err == nil {
		t.Fatal("conflicting payloads with the same epoch and generation were accepted")
	}
}

func TestCollectStateV1RestoresOnlyForSameCollector(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a"}, "collector-a", decoder)
	if err != nil {
		t.Fatal(err)
	}
	state.StateSchemaVersion = collectStateSchemaV1
	state.StateIdentityKey = nil
	state.OwnershipEpoch = 0
	state.StateGeneration = 0
	legacyKey := makeCollectStateKeyV1(state.CollectorId, Protocol(state.Protocol), addressFrom16(state.SourceIp), state.ObservationDomainId)
	state.StateKey = legacyKey[:]
	var datagramID DatagramID
	copy(datagramID[:], state.DatagramId)
	legacyID := makeCollectStateID(datagramID, legacyKey)
	state.StateId = legacyID[:]
	digest, err := collectStateDigest(state)
	if err != nil {
		t.Fatal(err)
	}
	state.PayloadSha256 = digest[:]
	dir := t.TempDir()
	registryA := collectStateRegistry(t, "collector-a", 1, record.Source.Addr())
	store, err := OpenCollectStateStore(dir, "collector-a", registryA, decoder)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(state); err != nil {
		t.Fatal(err)
	}
	decoder.Close()
	restored, _ := NewDecoder()
	defer restored.Close()
	registryB := collectStateRegistry(t, "collector-b", 2, record.Source.Addr())
	if _, err := OpenCollectStateStore(dir, "collector-b", registryB, restored); err == nil {
		t.Fatal("schema-v1 state crossed collector ownership")
	}
}

func collectStateFixture(t *testing.T) (*Decoder, WALRecord, DecodedDatagram) {
	t.Helper()
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	record := decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowTemplatePacket(false)))
	record.DatagramID = DatagramID{1, 2, 3}
	record.RegistryVersion = 1
	decoded, err := decoder.Decode(record)
	if err != nil {
		decoder.Close()
		t.Fatal(err)
	}
	return decoder, record, decoded
}

func collectStateRegistry(t *testing.T, collectorID string, ownershipEpoch uint64, source netip.Addr) *Registry {
	t.Helper()
	now := time.Now()
	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.CollectorID = collectorID
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: source.String() + "/32", ObservationDomainID: uint64Pointer(42), TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: ownershipEpoch, SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func uint64Pointer(value uint64) *uint64 { return &value }

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
