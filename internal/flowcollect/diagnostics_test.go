package flowcollect

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"
)

func TestDecodeFailurePayloadIsOptInAndBounded(t *testing.T) {
	record := WALRecord{
		DatagramID: DatagramID{1, 2, 3},
		WALInput: WALInput{
			Protocol:            ProtocolIPFIX,
			ReceivedAt:          time.Unix(100, 0),
			Source:              netip.MustParseAddrPort("192.0.2.1:2055"),
			ObservationDomainID: 42,
			RegistryVersion:     7,
			TenantID:            "tenant-a",
			ExporterID:          "exporter-a",
			Payload:             []byte{1, 2, 3, 4, 5},
		},
	}
	failure := BuildDecodeFailure(record, "collector-a", decodeRejectedCode, errors.New("bad\npacket"), 3, 0)
	if len(failure.Payload) != 0 || !failure.PayloadTruncated || len(failure.PayloadSha256) != 32 || failure.ErrorSummary != "bad packet" {
		t.Fatalf("unexpected metadata-only failure: %+v", failure)
	}
	withPayload := BuildDecodeFailure(record, "collector-a", decodeRejectedCode, errors.New("bad packet"), 3, 3)
	if !bytes.Equal(withPayload.Payload, []byte{1, 2, 3}) || !withPayload.PayloadTruncated {
		t.Fatalf("diagnostic payload limit not enforced: %+v", withPayload)
	}
	again := BuildDecodeFailure(record, "collector-a", decodeRejectedCode, errors.New("different detail"), 4, 0)
	if !bytes.Equal(failure.EventId, again.EventId) {
		t.Fatal("stable DLQ event ID changed across retries")
	}
}

func TestQuarantineLimiterBoundsGlobalAndPerSourceRate(t *testing.T) {
	limiter := newQuarantineLimiter(3, 2)
	now := time.Unix(100, 0)
	first := netip.MustParseAddr("192.0.2.1")
	second := netip.MustParseAddr("192.0.2.2")
	if !limiter.Allow(first, now) || !limiter.Allow(first, now) || limiter.Allow(first, now) {
		t.Fatal("per-source quarantine limit was not enforced")
	}
	if !limiter.Allow(second, now) || limiter.Allow(second, now) {
		t.Fatal("global quarantine limit was not enforced")
	}
	if !limiter.Allow(first, now.Add(time.Second)) {
		t.Fatal("quarantine window did not reset")
	}
}

func TestDecodeLoopPublishesDLQBeforeAcknowledgingPoisonDatagram(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	payload := []byte{0, 5}
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow5, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", Payload: payload})
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
	attempts, err := OpenAttemptStore(t.TempDir(), plan.CollectorID, DefaultConfig().Diagnostics, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer attempts.Close()
	publisher := &recordingPublisher{dlqPublished: make(chan struct{}, 1)}
	runner := &Runner{
		Config: Config{
			NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond},
			Diagnostics:     DiagnosticsConfig{DecodeMaxAttempts: 2, RetryInitial: time.Millisecond, RetryMax: 2 * time.Millisecond},
		},
		Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Attempts: attempts, Publisher: publisher,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	queue := make(chan WALRecord, 1)
	go func() {
		defer close(done)
		runner.decodeLoop(ctx, queue)
	}()
	queue <- record
	select {
	case <-publisher.dlqPublished:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("poison datagram was not published to DLQ")
	}
	deadline := time.Now().Add(time.Second)
	for replayCount(t, w) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if len(publisher.decodeFailures) != 1 {
		t.Fatalf("DLQ records=%d, want 1", len(publisher.decodeFailures))
	}
	failure := publisher.decodeFailures[0]
	if failure.ErrorCode != decodeRejectedCode || failure.Attempts != 2 || len(failure.Payload) != 0 {
		t.Fatalf("unexpected DLQ failure: %+v", failure)
	}
	if replayCount(t, w) != 0 {
		t.Fatal("poison datagram was acknowledged before a durable DLQ outcome")
	}
	if snapshot := runner.metrics().Snapshot(); snapshot.DLQDatagrams != 1 || snapshot.ReplayAttempts != 1 {
		t.Fatalf("unexpected diagnostic metrics: %+v", snapshot)
	}
}

func TestNormalizeFailurePublishesCollectStateBeforeDLQ(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", ObservationDomainID: &domain, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModeSampled, Enabled: true}}
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
	attempts, err := OpenAttemptStore(t.TempDir(), plan.CollectorID, DefaultConfig().Diagnostics, w, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer attempts.Close()
	publisher := &recordingPublisher{dlqPublished: make(chan struct{}, 1)}
	runner := &Runner{
		Config: Config{
			NormalizedBatch: NormalizedBatchCfg{MaxRecords: 100, MaxBytes: 1 << 20, MaxWait: time.Millisecond},
			Diagnostics:     DiagnosticsConfig{DecodeMaxAttempts: 1, RetryInitial: time.Millisecond, RetryMax: time.Millisecond},
		},
		Registry: registry, WAL: w, Decoder: decoder, State: stateStore, Attempts: attempts, Publisher: publisher,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	queue := make(chan WALRecord, 1)
	go func() {
		defer close(done)
		runner.decodeLoop(ctx, queue)
	}()
	queue <- record
	select {
	case <-publisher.dlqPublished:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("normalize failure was not published to DLQ")
	}
	deadline := time.Now().Add(time.Second)
	for replayCount(t, w) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if len(publisher.events) != 2 || publisher.events[0] != "state" || publisher.events[1] != "dlq" {
		t.Fatalf("unsafe normalize-failure order: %v", publisher.events)
	}
	if len(publisher.decodeFailures) != 1 || publisher.decodeFailures[0].ErrorCode != normalizeRejectedCode {
		t.Fatalf("unexpected normalize DLQ record: %+v", publisher.decodeFailures)
	}
	if replayCount(t, w) != 0 {
		t.Fatal("normalize failure remained in WAL after state and DLQ acknowledgements")
	}
}

func TestDLQPublishFailureDoesNotAcknowledgeWAL(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", SamplingMode: SamplingModeSampled, Enabled: true}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(t.TempDir(), plan.CollectorID, testWALConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	record, err := w.Append(WALInput{Protocol: ProtocolNetFlow5, ReceivedAt: now, Source: netip.MustParseAddrPort("192.0.2.1:2055"), RegistryVersion: plan.Revision, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", Payload: []byte{0, 5}})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &failDLQOncePublisher{fail: true}
	runner := &Runner{Config: Config{Diagnostics: DiagnosticsConfig{}}, Registry: registry, WAL: w, Publisher: publisher}
	if err := runner.deadLetter(context.Background(), record, decodeRejectedCode, errors.New("bad packet"), 3); err == nil {
		t.Fatal("injected DLQ publish failure was not returned")
	}
	if replayCount(t, w) != 1 {
		t.Fatal("WAL was acknowledged despite DLQ publish failure")
	}
	if err := runner.deadLetter(context.Background(), record, decodeRejectedCode, errors.New("bad packet"), 4); err != nil {
		t.Fatal(err)
	}
	if replayCount(t, w) != 0 || len(publisher.decodeFailures) != 1 {
		t.Fatalf("successful DLQ retry did not advance WAL: replay=%d failures=%d", replayCount(t, w), len(publisher.decodeFailures))
	}
}

func replayCount(t *testing.T, w *WAL) int {
	t.Helper()
	count := 0
	if err := w.Replay(func(WALRecord) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	return count
}
