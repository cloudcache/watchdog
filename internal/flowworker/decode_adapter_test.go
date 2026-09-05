package flowworker

import (
	"errors"
	"math"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	goflowpb "github.com/netsampler/goflow2/v3/pb"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDecodeAdapterMapsBindingAndAuthoritativeCounters(t *testing.T) {
	receivedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	decoded := decodedFixture(receivedAt)
	decoded.ObservationDomainID = 42
	decoded.Records[0].SamplingRate = 1000
	decoded.Records[0].TimeFlowStartNs = uint64(receivedAt.Add(-3 * time.Second).UnixNano())
	decoded.Records[0].TimeFlowEndNs = uint64(receivedAt.Add(-time.Second).UnixNano())

	adapter := DecodeAdapter{ResolveBinding: func(collectorID string, registryVersion uint64, protocol flowplan.Protocol, source netip.Addr, domain uint64) (flowplan.SourceBinding, error) {
		if collectorID != "collector-a" || registryVersion != 7 || protocol != flowplan.ProtocolNetFlow9 || source != netip.MustParseAddr("192.0.2.1") || domain != 42 {
			t.Fatalf("unexpected binding lookup: %s/%d/%d/%s/%d", collectorID, registryVersion, protocol, source, domain)
		}
		return bindingFixture(), nil
	}}
	kafkaRecord := &kgo.Record{Topic: "watchdog.flow.raw-v1", Partition: 3, Offset: 19}
	batch, err := adapter.Map(kafkaRecord, decoded)
	if err != nil {
		t.Fatal(err)
	}
	wantSourceID := kafkaSourceID(kafkaRecord.Topic, kafkaRecord.Partition, kafkaRecord.Offset)
	if string(batch.SourceID) != string(wantSourceID[:]) || batch.KafkaPartition != 3 || batch.KafkaOffset != 19 || batch.TenantID != "tenant-a" || batch.ExporterEpoch != 9 {
		t.Fatalf("unexpected batch identity: %+v", batch)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("records=%d", len(batch.Records))
	}
	record := batch.Records[0]
	if record.SamplingMode != SamplingSampled || record.SamplingSource != SamplingSourceProtocol || record.SamplingRate != 1000 || !record.EstimatedValid || record.EstimatedBytes != 100_000 || record.EstimatedPackets != 2_000 {
		t.Fatalf("unexpected sampled counters: %+v", record)
	}
	if record.ObservationIfIndex != 3 || record.ObservationDirection != uint32(ObservationIngress) || record.EventTimeUnixMS != receivedAt.Add(-time.Second).UnixMilli() || record.FlowDurationMS != 2000 {
		t.Fatalf("unexpected observation/time mapping: %+v", record)
	}
}

func TestDecodeAdapterSamplingFallbackUnknownAndPreScaled(t *testing.T) {
	domain, ifIndex := uint64(42), uint32(3)
	tests := []struct {
		name        string
		binding     flowplan.SourceBinding
		wantMode    uint32
		wantRate    uint64
		wantSource  uint32
		wantBytes   uint64
		wantValid   bool
		wantQuality uint64
	}{
		{
			name:     "plan fallback",
			binding:  withSamplingRules(bindingFixture(), flowplan.SamplingRule{ObservationDomainID: &domain, IfIndex: &ifIndex, Mode: flowplan.SamplingModeSampled, Rate: 100}),
			wantMode: SamplingSampled, wantRate: 100, wantSource: SamplingSourcePlanRule, wantBytes: 10_000, wantValid: true, wantQuality: QualitySamplingPlanFallback | QualityEventTimeFallback,
		},
		{
			name:     "unknown",
			binding:  bindingFixture(),
			wantMode: SamplingUnknown, wantQuality: QualityEventTimeFallback,
		},
		{
			name: "exporter default",
			binding: func() flowplan.SourceBinding {
				value := bindingFixture()
				value.DefaultSamplingRate = 250
				return value
			}(),
			wantMode: SamplingSampled, wantRate: 250, wantSource: SamplingSourceExporterDefault, wantBytes: 25_000, wantValid: true, wantQuality: QualitySamplingPlanFallback | QualityEventTimeFallback,
		},
		{
			name: "pre-scaled",
			binding: func() flowplan.SourceBinding {
				value := bindingFixture()
				value.SamplingMode = flowplan.SamplingModePreScaled
				return value
			}(),
			wantMode: SamplingPreScaled, wantRate: 0, wantSource: SamplingSourceCounterMode, wantBytes: 100, wantValid: true, wantQuality: QualityEventTimeFallback,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded := decodedFixture(time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC))
			decoded.ObservationDomainID = 42
			adapter := DecodeAdapter{ResolveBinding: func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
				return test.binding, nil
			}}
			batch, err := adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 1}, decoded)
			if err != nil {
				t.Fatal(err)
			}
			record := batch.Records[0]
			if record.SamplingMode != test.wantMode || record.SamplingRate != test.wantRate || record.SamplingSource != test.wantSource || record.EstimatedBytes != test.wantBytes || record.EstimatedValid != test.wantValid || record.QualityFlags != test.wantQuality {
				t.Fatalf("unexpected counters: %+v", record)
			}
		})
	}
}

func TestDecodeAdapterRetainsOverflowAndUnavailableSelectorAsQuality(t *testing.T) {
	receivedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	t.Run("overflow", func(t *testing.T) {
		decoded := decodedFixture(receivedAt)
		decoded.Records[0].Bytes = math.MaxUint64
		decoded.Records[0].SamplingRate = 2
		adapter := DecodeAdapter{ResolveBinding: func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return bindingFixture(), nil
		}}
		batch, err := adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 1}, decoded)
		if err != nil {
			t.Fatal(err)
		}
		record := batch.Records[0]
		if record.EstimatedValid || record.QualityFlags&QualityCounterOverflow == 0 || record.RawBytes != math.MaxUint64 {
			t.Fatalf("overflow was not retained safely: %+v", record)
		}
		if err := validateRecord(batch, record, EnrichmentLimits{MaxFutureSkew: time.Minute}); err != nil {
			t.Fatalf("explicit overflow should pass structural validation: %v", err)
		}
	})

	t.Run("unavailable sflow selector", func(t *testing.T) {
		sourceType, sourceValue := uint32(0), uint32(44)
		binding := bindingFixture()
		binding.SamplingRules = []flowplan.SamplingRule{{SourceIDType: &sourceType, SourceIDValue: &sourceValue, Mode: flowplan.SamplingModeSampled, Rate: 1000}}
		decoded := decodedFixture(receivedAt)
		decoded.FlowType = goflowpb.FlowMessage_SFLOW_5
		decoded.Records[0].Type = goflowpb.FlowMessage_SFLOW_5
		adapter := DecodeAdapter{ResolveBinding: func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return binding, nil
		}}
		batch, err := adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 2}, decoded)
		if err != nil {
			t.Fatal(err)
		}
		record := batch.Records[0]
		if record.SamplingMode != SamplingUnknown || record.EstimatedValid || record.QualityFlags&QualitySamplingSelectorUnavailable == 0 {
			t.Fatalf("unobservable selector was not explicit: %+v", record)
		}
	})

	t.Run("available sflow selector", func(t *testing.T) {
		subAgent, sourceType, sourceValue := uint32(7), uint32(0), uint32(44)
		binding := bindingFixture()
		binding.SamplingRules = []flowplan.SamplingRule{{SubAgentID: &subAgent, SourceIDType: &sourceType, SourceIDValue: &sourceValue, Mode: flowplan.SamplingModeSampled, Rate: 1000}}
		decoded := decodedFixture(receivedAt)
		decoded.FlowType = goflowpb.FlowMessage_SFLOW_5
		decoded.SubAgentID = 7
		decoded.DatagramSequence = 99
		decoded.AgentIP = netip.MustParseAddr("192.0.2.9")
		decoded.Records[0].Type = goflowpb.FlowMessage_SFLOW_5
		decoded.RecordMetadata = []flowstream.DecodedRecordMetadata{{Present: true, SubAgentID: 7, SourceIDType: 0, SourceIDValue: 44, SampleSequence: 12, SamplePool: 9000, ExporterDrops: 3}}
		adapter := DecodeAdapter{ResolveBinding: func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return binding, nil
		}}
		batch, err := adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 3}, decoded)
		if err != nil {
			t.Fatal(err)
		}
		record := batch.Records[0]
		if batch.SubAgentID != 7 || batch.DatagramSequence != 99 || len(batch.AgentIP) != 16 || record.SamplingMode != SamplingSampled || record.SamplingRate != 1000 || !record.EstimatedValid || record.EstimatedBytes != 100_000 || record.SourceIDValue != 44 || record.SamplePool != 9000 || record.ExporterDrops != 3 {
			t.Fatalf("sFlow metadata/rule was not mapped: batch=%+v record=%+v", batch, record)
		}
	})
}

func TestDecodeAdapterRejectsUnboundAndInvalidRecords(t *testing.T) {
	decoded := decodedFixture(time.Now().UTC())
	adapter := DecodeAdapter{ResolveBinding: func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
		return flowplan.SourceBinding{}, errors.New("revision expired")
	}}
	if _, err := adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 1}, decoded); !errors.Is(err, ErrBindingUnavailable) {
		t.Fatalf("binding error=%v", err)
	}
	decoded.Records[0].DstAddr = []byte{1, 2, 3}
	adapter.ResolveBinding = func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
		return bindingFixture(), nil
	}
	if _, err := adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 1}, decoded); !errors.Is(err, ErrDecodedFlowInvalid) {
		t.Fatalf("invalid record error=%v", err)
	}
}

func FuzzDecodeAdapterNeverPanics(f *testing.F) {
	f.Add([]byte{10, 0, 0, 1}, []byte{203, 0, 113, 2}, uint32(443), uint32(6), uint64(100), uint64(1000))
	f.Add([]byte{1}, []byte{}, uint32(math.MaxUint16+1), uint32(math.MaxUint8+1), uint64(math.MaxUint64), uint64(2))
	f.Fuzz(func(t *testing.T, source, destination []byte, destinationPort, protocol uint32, rawBytes, rate uint64) {
		decoded := decodedFixture(time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC))
		decoded.Records[0].SrcAddr = source
		decoded.Records[0].DstAddr = destination
		decoded.Records[0].DstPort = destinationPort
		decoded.Records[0].Proto = protocol
		decoded.Records[0].Bytes = rawBytes
		decoded.Records[0].SamplingRate = rate
		adapter := DecodeAdapter{ResolveBinding: func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return bindingFixture(), nil
		}}
		_, _ = adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 1}, decoded)
	})
}

func FuzzDecodeAdapterCounterConservation(f *testing.F) {
	f.Add(uint64(100), uint64(2), uint64(1000), false)
	f.Add(uint64(math.MaxUint64), uint64(1), uint64(2), false)
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(math.MaxUint64), true)
	f.Fuzz(func(t *testing.T, rawBytes, rawPackets, rate uint64, preScaled bool) {
		decoded := decodedFixture(time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC))
		decoded.Records[0].Bytes = rawBytes
		decoded.Records[0].Packets = rawPackets
		binding := bindingFixture()
		if preScaled {
			binding.SamplingMode = flowplan.SamplingModePreScaled
			decoded.Records[0].SamplingRate = rate
		} else {
			if rate == 0 {
				rate = 1
			}
			decoded.Records[0].SamplingRate = rate
		}
		adapter := DecodeAdapter{ResolveBinding: func(string, uint64, flowplan.Protocol, netip.Addr, uint64) (flowplan.SourceBinding, error) {
			return binding, nil
		}}
		batch, err := adapter.Map(&kgo.Record{Topic: "raw", Partition: 0, Offset: 1}, decoded)
		if err != nil {
			t.Fatal(err)
		}
		record := batch.Records[0]
		if record.RawBytes != rawBytes || record.RawPackets != rawPackets {
			t.Fatalf("raw counters changed: bytes=%d/%d packets=%d/%d", record.RawBytes, rawBytes, record.RawPackets, rawPackets)
		}
		if preScaled {
			if !record.EstimatedValid || record.EstimatedBytes != rawBytes || record.EstimatedPackets != rawPackets {
				t.Fatalf("pre-scaled counters were changed: %+v", record)
			}
			return
		}
		overflow := (rawBytes != 0 && rate > math.MaxUint64/rawBytes) || (rawPackets != 0 && rate > math.MaxUint64/rawPackets)
		if overflow {
			if record.EstimatedValid || record.QualityFlags&QualityCounterOverflow == 0 {
				t.Fatalf("overflow was not explicit: %+v", record)
			}
			return
		}
		if !record.EstimatedValid || record.EstimatedBytes != rawBytes*rate || record.EstimatedPackets != rawPackets*rate {
			t.Fatalf("sampled counters do not conserve raw*rate: %+v", record)
		}
	})
}

func decodedFixture(receivedAt time.Time) flowstream.DecodedBatch {
	return flowstream.DecodedBatch{
		CollectorID: "collector-a", RegistryVersion: 7, ReceivedAt: receivedAt,
		Source: netip.MustParseAddrPort("192.0.2.1:9999"), FlowType: goflowpb.FlowMessage_NETFLOW_V9,
		Records: []*goflowpb.FlowMessage{{
			Type: goflowpb.FlowMessage_NETFLOW_V9, TimeReceivedNs: uint64(receivedAt.UnixNano()),
			SrcAddr: netip.MustParseAddr("10.0.0.1").AsSlice(), DstAddr: netip.MustParseAddr("203.0.113.2").AsSlice(),
			SrcPort: 12345, DstPort: 443, Proto: 6, TcpFlags: 0x12, InIf: 3, OutIf: 4,
			Bytes: 100, Packets: 2, SrcAs: 64512, DstAs: 64513,
		}},
	}
}

func bindingFixture() flowplan.SourceBinding {
	return flowplan.SourceBinding{
		Enabled: true, TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", DeviceID: "device-a",
		OwnershipEpoch: 9, SamplingMode: flowplan.SamplingModeSampled,
		Observations: map[uint32]flowplan.Observation{3: {Direction: uint32(ObservationIngress)}},
	}
}

func withSamplingRules(binding flowplan.SourceBinding, rules ...flowplan.SamplingRule) flowplan.SourceBinding {
	binding.SamplingRules = rules
	return binding
}
