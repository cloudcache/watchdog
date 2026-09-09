package watchdog

import (
	"context"
	"errors"

	"github.com/cloudcache/watchdog/internal/snmpch"
)

type ClickHouseSNMPRawWriter struct {
	Store interface {
		WriteSamples(context.Context, []snmpch.Sample) error
	}
	AgentID string
}

func (w ClickHouseSNMPRawWriter) WriteSNMPRawSamples(ctx context.Context, samples []SNMPRawSample) error {
	if w.Store == nil {
		return errors.New("ClickHouse SNMP telemetry writer is not configured")
	}
	rows := make([]snmpch.Sample, 0, len(samples))
	for _, sample := range samples {
		if err := validateRawSample(sample); err != nil {
			return err
		}
		if (sample.ValueType == SNMPCollectorValueCounter32 || sample.ValueType == SNMPCollectorValueCounter64) && !sample.CounterValid {
			return errors.New("ClickHouse SNMP counter sample is missing its exact integer value")
		}
		if sample.ValueType == SNMPCollectorValueString || sample.ValueType == SNMPCollectorValueMACAddr || sample.ValueType == SNMPCollectorValueIPAddr {
			continue
		}
		kind := "gauge"
		if sample.CounterValid {
			kind = "counter"
		}
		rows = append(rows, snmpch.Sample{ObservedAt: sample.SampledAt, DeviceID: string(sample.DeviceID), AgentID: w.AgentID, EntityKind: string(sample.EntityType), EntityID: string(sample.EntityID), RecipeID: string(sample.RecipeID), Metric: sample.MetricName, ValueKind: kind, GaugeValue: sample.FloatValue, CounterValue: sample.CounterValue, CounterWidth: sample.CounterWidth, IntervalMS: sample.IntervalMS, QualityFlags: sample.QualityFlags, PollSequence: sample.PollSequence, SourceRunID: sample.SourceRunID, SampleIndex: sample.SampleIndex})
	}
	return w.Store.WriteSamples(ctx, rows)
}
