package flowcollect

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
)

type recordingPublisher struct {
	batches        []*flowpb.NormalizedRecordBatch
	states         []*flowpb.CollectState
	decodeFailures []*flowpb.DecodeFailure
	quarantines    []*flowpb.QuarantineEvent
	events         []string
	dlqPublished   chan struct{}
}

func (p *recordingPublisher) Publish(_ context.Context, batch *flowpb.NormalizedRecordBatch) error {
	p.batches = append(p.batches, batch)
	p.events = append(p.events, "data")
	return nil
}
func (p *recordingPublisher) PublishCollectState(_ context.Context, state *flowpb.CollectState) error {
	p.states = append(p.states, state)
	p.events = append(p.events, "state")
	return nil
}
func (p *recordingPublisher) PublishDecodeFailure(_ context.Context, failure *flowpb.DecodeFailure) error {
	p.decodeFailures = append(p.decodeFailures, failure)
	p.events = append(p.events, "dlq")
	if p.dlqPublished != nil {
		p.dlqPublished <- struct{}{}
	}
	return nil
}
func (p *recordingPublisher) PublishQuarantine(_ context.Context, event *flowpb.QuarantineEvent) error {
	p.quarantines = append(p.quarantines, event)
	p.events = append(p.events, "quarantine")
	return nil
}
func (p *recordingPublisher) Close() error { return nil }

type failDataOncePublisher struct {
	recordingPublisher
	fail bool
}

type failDLQOncePublisher struct {
	recordingPublisher
	fail bool
}

type signalingPublisher struct {
	recordingPublisher
	published chan struct{}
}

func (p *signalingPublisher) Publish(ctx context.Context, batch *flowpb.NormalizedRecordBatch) error {
	if err := p.recordingPublisher.Publish(ctx, batch); err != nil {
		return err
	}
	close(p.published)
	return nil
}

func (p *failDLQOncePublisher) PublishDecodeFailure(ctx context.Context, failure *flowpb.DecodeFailure) error {
	if p.fail {
		p.fail = false
		return errors.New("injected DLQ publish failure")
	}
	return p.recordingPublisher.PublishDecodeFailure(ctx, failure)
}

func (p *failDataOncePublisher) Publish(ctx context.Context, batch *flowpb.NormalizedRecordBatch) error {
	if p.fail {
		p.fail = false
		p.events = append(p.events, "data-failed")
		return errors.New("injected normalized publish failure")
	}
	return p.recordingPublisher.Publish(ctx, batch)
}

func TestRunnerCheckpointsAndPublishesCollectStateBeforeData(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", ObservationDomainID: &domain, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", SamplingMode: SamplingModePreScaled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	payload := mustMarshalNFv9(t, netflowTemplatePacket(true))
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow9, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), ObservationDomainID: domain, RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	stateStore, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}}, Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Publisher: publisher}
	if err := runner.processRecord(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}
	if len(publisher.states) != 1 || len(publisher.batches) != 1 || len(publisher.events) != 2 || publisher.events[0] != "state" || publisher.events[1] != "data" {
		t.Fatalf("collect-state/data publish order is unsafe: events=%v states=%d batches=%d", publisher.events, len(publisher.states), len(publisher.batches))
	}
	remaining := 0
	if err := w.Replay(func(WALRecord) error { remaining++; return nil }); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("state and data children were not fully acknowledged: %d", remaining)
	}
}

func TestRunnerAcknowledgesTemplateOnlyAfterCollectStatePublish(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", ObservationDomainID: &domain, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModePreScaled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow9, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), ObservationDomainID: domain, RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", Payload: mustMarshalNFv9(t, netflowTemplatePacket(false))})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	stateStore, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}}, Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Publisher: publisher}
	if err := runner.processRecord(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}
	if len(publisher.states) != 1 || len(publisher.batches) != 0 || len(publisher.events) != 1 || publisher.events[0] != "state" {
		t.Fatalf("template-only datagram was not safely published: events=%v states=%d batches=%d", publisher.events, len(publisher.states), len(publisher.batches))
	}
	assertReplayCount(t, w, 0)
}

func TestRunnerKeepsMissingTemplateDataInWAL(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", ObservationDomainID: &domain, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModePreScaled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow9, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), ObservationDomainID: domain, RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", Payload: mustMarshalNFv9(t, netflowDataPacket())})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	stateStore, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}}, Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Publisher: publisher}
	if err := runner.processRecord(context.Background(), record, 0); !errors.Is(err, ErrTemplatePending) {
		t.Fatalf("missing template error=%v, want ErrTemplatePending", err)
	}
	if len(publisher.states) != 0 || len(publisher.batches) != 0 {
		t.Fatalf("missing-template data was published: states=%d batches=%d", len(publisher.states), len(publisher.batches))
	}
	assertReplayCount(t, w, 1)
}

func TestDecodeWorkerAffinityIsStablePerExporterDomain(t *testing.T) {
	record := WALRecord{WALInput: WALInput{Protocol: ProtocolIPFIX, Source: netip.MustParseAddrPort("192.0.2.9:2055"), ObservationDomainID: 42}}
	want := decodeWorkerIndex(record, 16)
	for index := 0; index < 100; index++ {
		if got := decodeWorkerIndex(record, 16); got != want {
			t.Fatalf("worker affinity changed: got %d want %d", got, want)
		}
	}
	seen := map[int]struct{}{want: {}}
	for domain := uint64(43); domain < 80; domain++ {
		record.ObservationDomainID = domain
		seen[decodeWorkerIndex(record, 16)] = struct{}{}
	}
	if len(seen) == 1 {
		t.Fatal("worker affinity ignored observation domain")
	}
}

func TestRunnerRetrySkipsAcknowledgedCollectStateChild(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", ObservationDomainID: &domain, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModePreScaled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow9, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), ObservationDomainID: domain, RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", Payload: mustMarshalNFv9(t, netflowTemplatePacket(true))})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	stateStore, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &failDataOncePublisher{fail: true}
	qualityConfig := testQualityStateConfig()
	metrics := &Metrics{}
	quality := NewQualityTracker(qualityConfig, metrics)
	qualityState, err := OpenQualityStateStore(t.TempDir(), plan.CollectorID, qualityConfig, quality, w, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer qualityState.Close()
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}, Quality: qualityConfig}, Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Publisher: publisher, Metrics: metrics, Quality: quality, QualityState: qualityState}
	if err := runner.processRecord(context.Background(), record, 0); err == nil {
		t.Fatal("injected publish failure was not returned")
	}
	if err := runner.processRecord(context.Background(), record, 1); err != nil {
		t.Fatal(err)
	}
	if len(publisher.states) != 1 || len(publisher.batches) != 1 {
		t.Fatalf("retry duplicated acknowledged child: states=%d batches=%d events=%v", len(publisher.states), len(publisher.batches), publisher.events)
	}
	if publisher.batches[0].ReplayGeneration != 1 {
		t.Fatalf("retry generation=%d, want 1", publisher.batches[0].ReplayGeneration)
	}
	if got := metrics.Snapshot().QualityJournalAppends; got != 1 {
		t.Fatalf("retry appended quality state %d times, want 1", got)
	}
	if len(qualityState.pending) != 0 {
		t.Fatalf("successful retry retained %d pending quality decisions", len(qualityState.pending))
	}
}

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
	stateStore, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}}, Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Publisher: publisher}
	if err := runner.processRecord(context.Background(), record, 0); err != nil {
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

func TestRunnerUsesHistoricalPlanBindingAndPartitionMap(t *testing.T) {
	now := time.Now()
	historicalPlan := validPlan(now)
	historicalPlan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", SamplingMode: SamplingModeSampled, Enabled: true}}
	historicalPlan.PartitionMapVersion = 11
	for index := range historicalPlan.PartitionMap {
		historicalPlan.PartitionMap[index] = 3
	}
	historical, err := CompilePlan(historicalPlan, now)
	if err != nil {
		t.Fatal(err)
	}
	activePlan := historicalPlan
	activePlan.Revision = 2
	activePlan.PartitionMapVersion = 12
	activePlan.Sources = nil
	activePlan.PartitionMap = append([]uint32(nil), activePlan.PartitionMap...)
	for index := range activePlan.PartitionMap {
		activePlan.PartitionMap[index] = 7
	}
	active, err := CompilePlan(activePlan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), historicalPlan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	packet := netflowlegacy.PacketNetFlowV5{Version: 5, SysUptime: 1000, UnixSecs: uint32(now.Unix()), SamplingInterval: 100, Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007101, DPkts: 2, DOctets: 1000, Proto: 17}}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow5, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), RegistryVersion: historicalPlan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	decoder, _ := NewDecoder()
	defer decoder.Close()
	stateStore, err := OpenCollectStateStore(t.TempDir(), activePlan.CollectorID, active, decoder)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}}, Registry: active, Plans: testPlanHistory(active, historical, active), WAL: w, Decoder: decoder, State: stateStore, Publisher: publisher}
	if err := runner.processRecord(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}
	if len(publisher.batches) != 1 || publisher.batches[0].PartitionMapVersion != 11 || publisher.batches[0].PhysicalPartition != 3 {
		t.Fatalf("old WAL used the wrong plan: %+v", publisher.batches)
	}
}

func TestRunnerOverlapsQualityFsyncWithKafkaButWaitsBeforeTerminalACK(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	decoder, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	stateStore, err := OpenCollectStateStore(t.TempDir(), plan.CollectorID, registry, decoder)
	if err != nil {
		t.Fatal(err)
	}
	qualityConfig := testQualityStateConfig()
	qualityConfig.JournalFsync = time.Hour
	metrics := &Metrics{}
	quality := NewQualityTracker(qualityConfig, metrics)
	qualityState, err := OpenQualityStateStore(t.TempDir(), plan.CollectorID, qualityConfig, quality, w, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer qualityState.Close()
	packet := netflowlegacy.PacketNetFlowV5{Version: 5, SysUptime: 1000, UnixSecs: uint32(now.Unix()), SamplingInterval: 100, Records: []netflowlegacy.RecordsNetFlowV5{{SrcAddr: 0x0a000001, DstAddr: 0xcb007101, DPkts: 2, DOctets: 1000, Proto: 17}}}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow5, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &signalingPublisher{published: make(chan struct{})}
	runner := &Runner{Config: Config{NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond}, Quality: qualityConfig}, Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Publisher: publisher, Metrics: metrics, Quality: quality, QualityState: qualityState}
	result := make(chan error, 1)
	go func() { result <- runner.processRecord(context.Background(), record, 0) }()
	select {
	case <-publisher.published:
	case <-time.After(time.Second):
		t.Fatal("Kafka publish waited for the quality fsync interval")
	}
	if w.ChildAcknowledged(record.DatagramID, 0, 1) {
		t.Fatal("terminal WAL acknowledgement preceded quality durability")
	}
	select {
	case err := <-result:
		t.Fatalf("record completed before quality durability: %v", err)
	default:
	}
	if err := qualityState.Sync(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("record did not complete after quality state became durable")
	}
	if !w.ChildAcknowledged(record.DatagramID, 0, 1) {
		t.Fatal("terminal WAL acknowledgement was not written")
	}
}

func assertReplayCount(t *testing.T, w *WAL, want int) {
	t.Helper()
	got := 0
	if err := w.Replay(func(WALRecord) error { got++; return nil }); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("replayable datagrams=%d, want %d", got, want)
	}
}
