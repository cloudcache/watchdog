package snmpdomain

import (
	"context"
	"errors"

	"github.com/cloudcache/watchdog/internal/snmpch"
)

type ClickHouseRawWriter struct {
	Store interface {
		WriteSamples(context.Context, []snmpch.Sample) error
	}
	AgentID string
}

func (w ClickHouseRawWriter) WriteRawSamples(ctx context.Context, samples []RawSample) error {
	if w.Store == nil {
		return errors.New("ClickHouse SNMP telemetry writer is not configured")
	}
	rows := make([]snmpch.Sample, 0, len(samples))
	for _, sample := range samples {
		if err := validateRawSample(sample); err != nil {
			return err
		}
		if (sample.ValueType == ValueCounter32 || sample.ValueType == ValueCounter64) && !sample.CounterValid {
			return errors.New("ClickHouse SNMP counter sample is missing its exact integer value")
		}
		if sample.ValueType == ValueString || sample.ValueType == ValueMACAddr || sample.ValueType == ValueIPAddr {
			continue
		}
		kind := "gauge"
		if sample.CounterValid {
			kind = "counter"
		}
		rows = append(rows, snmpch.Sample{ObservedAt: sample.SampledAt, DeviceID: sample.DeviceID, AgentID: w.AgentID, EntityKind: string(sample.EntityType), EntityID: sample.EntityID, RecipeID: sample.RecipeID, Metric: sample.MetricName, ValueKind: kind, GaugeValue: sample.FloatValue, CounterValue: sample.CounterValue, CounterWidth: sample.CounterWidth, IntervalMS: sample.IntervalMS, QualityFlags: sample.QualityFlags, PollSequence: sample.PollSequence, SourceRunID: sample.SourceRunID, SampleIndex: sample.SampleIndex})
	}
	return w.Store.WriteSamples(ctx, rows)
}
