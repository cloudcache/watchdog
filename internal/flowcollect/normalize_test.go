package flowcollect

import (
	"math"
	"testing"
)

func TestNormalizeCountersDoesNotTreatMissingRateAsOne(t *testing.T) {
	binding := SourceBinding{SamplingMode: SamplingModeSampled}
	if _, err := NormalizeCounters(binding, DecodedDatagram{}, DecodedRecord{RawBytes: 100}); err == nil {
		t.Fatal("missing sampling rate was accepted")
	}
}

func TestNormalizeCountersUsesSpecificSamplingRule(t *testing.T) {
	sourceType, sourceValue, ifIndex := uint32(0), uint32(9), uint32(10)
	binding := SourceBinding{SamplingMode: SamplingModeSampled, SamplingRules: []SamplingRule{{SourceIDType: &sourceType, SourceIDValue: &sourceValue, IfIndex: &ifIndex, Mode: SamplingModeSampled, Rate: 1000}}}
	values, err := NormalizeCounters(binding, DecodedDatagram{}, DecodedRecord{SourceIDValue: 9, InIf: 10, RawBytes: 100, RawPackets: 1, SamplingRate: 100})
	if err != nil {
		t.Fatal(err)
	}
	if values.EstimatedBytes != 100000 || values.EstimatedPackets != 1000 || values.QualityFlags&QualitySamplingRateOverridden == 0 {
		t.Fatalf("unexpected values: %+v", values)
	}
}

func TestNormalizeCountersChecksOverflow(t *testing.T) {
	binding := SourceBinding{SamplingMode: SamplingModeSampled}
	if _, err := NormalizeCounters(binding, DecodedDatagram{}, DecodedRecord{RawBytes: math.MaxUint64, SamplingRate: 2}); err == nil {
		t.Fatal("overflow was accepted")
	}
}

func TestPreScaledCountersAreNotMultiplied(t *testing.T) {
	binding := SourceBinding{SamplingMode: SamplingModePreScaled}
	values, err := NormalizeCounters(binding, DecodedDatagram{}, DecodedRecord{RawBytes: 100, RawPackets: 4, SamplingRate: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if values.EstimatedBytes != 100 || values.EstimatedPackets != 4 {
		t.Fatalf("unexpected values: %+v", values)
	}
}
