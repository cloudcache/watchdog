package flowcollect

import (
	"errors"
	"fmt"
	"math"
)

const QualitySamplingRateOverridden uint64 = 1 << 1

type NormalizedValues struct {
	SamplingMode     SamplingMode
	SamplingRate     uint64
	EstimatedBytes   uint64
	EstimatedPackets uint64
	QualityFlags     uint64
}

func NormalizeCounters(binding SourceBinding, datagram DecodedDatagram, record DecodedRecord) (NormalizedValues, error) {
	mode, configuredRate, configured, err := selectSampling(binding, datagram, record)
	if err != nil {
		return NormalizedValues{}, err
	}
	values := NormalizedValues{SamplingMode: mode, SamplingRate: record.SamplingRate}
	switch mode {
	case SamplingModePreScaled:
		values.EstimatedBytes = record.RawBytes
		values.EstimatedPackets = record.RawPackets
		return values, nil
	case SamplingModeSampled:
		rate := record.SamplingRate
		if configuredRate > 0 && configuredRate != rate {
			rate = configuredRate
			values.QualityFlags |= QualitySamplingRateOverridden
		}
		if configured && configuredRate == rate {
			values.QualityFlags |= QualitySamplingRateOverridden
		}
		if rate == 0 {
			return NormalizedValues{}, errors.New("sampled flow has no authoritative sampling rate")
		}
		bytes, ok := checkedMultiply(record.RawBytes, rate)
		if !ok {
			return NormalizedValues{}, errors.New("estimated byte count overflow")
		}
		packets, ok := checkedMultiply(record.RawPackets, rate)
		if !ok {
			return NormalizedValues{}, errors.New("estimated packet count overflow")
		}
		values.SamplingRate, values.EstimatedBytes, values.EstimatedPackets = rate, bytes, packets
		return values, nil
	default:
		return NormalizedValues{}, errors.New("unknown sampling mode")
	}
}

func selectSampling(binding SourceBinding, datagram DecodedDatagram, record DecodedRecord) (SamplingMode, uint64, bool, error) {
	mode, rate := binding.SamplingMode, uint64(0)
	bestSpecificity := -1
	for _, rule := range binding.SamplingRules {
		if rule.ObservationDomainID != nil && *rule.ObservationDomainID != datagram.ObservationDomainID {
			continue
		}
		if rule.SubAgentID != nil && *rule.SubAgentID != datagram.SubAgentID {
			continue
		}
		if rule.SourceIDType != nil && *rule.SourceIDType != record.SourceIDType {
			continue
		}
		if rule.SourceIDValue != nil && *rule.SourceIDValue != record.SourceIDValue {
			continue
		}
		if rule.IfIndex != nil && *rule.IfIndex != record.InIf && *rule.IfIndex != record.OutIf {
			continue
		}
		specificity := 0
		if rule.ObservationDomainID != nil {
			specificity++
		}
		if rule.SubAgentID != nil {
			specificity++
		}
		if rule.SourceIDType != nil {
			specificity++
		}
		if rule.SourceIDValue != nil {
			specificity++
		}
		if rule.IfIndex != nil {
			specificity++
		}
		if specificity < bestSpecificity {
			continue
		}
		if specificity == bestSpecificity && (mode != rule.Mode || rate != rule.Rate) {
			return 0, 0, false, fmt.Errorf("ambiguous sampling rules at specificity %d", specificity)
		}
		mode, rate, bestSpecificity = rule.Mode, rule.Rate, specificity
	}
	return mode, rate, bestSpecificity >= 0, nil
}

func checkedMultiply(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}
