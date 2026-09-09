package snmpch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const writeChunkSize = 2000

type Sample struct {
	ObservedAt                                                           time.Time
	DeviceID, AgentID, EntityKind, EntityID, RecipeID, Metric, ValueKind string
	GaugeValue                                                           float64
	CounterValue                                                         uint64
	CounterWidth                                                         uint8
	IntervalMS, QualityFlags                                             uint32
	PollSequence                                                         uint64
	SourceRunID                                                          string
	SampleIndex                                                          uint32
}

func (s *Store) WriteSamples(ctx context.Context, samples []Sample) error {
	if s == nil || s.exec == nil {
		return errors.New("SNMP ClickHouse store is not initialized")
	}
	for start := 0; start < len(samples); start += writeChunkSize {
		end := min(start+writeChunkSize, len(samples))
		if err := s.writeChunk(ctx, samples[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) writeChunk(ctx context.Context, samples []Sample) error {
	if len(samples) == 0 {
		return nil
	}
	first, last := samples[0], samples[len(samples)-1]
	for index, sample := range samples {
		if sample.SourceRunID != first.SourceRunID || sample.PollSequence != first.PollSequence ||
			sample.SampleIndex != first.SampleIndex+uint32(index) {
			return errors.New("SNMP batch must contain one poll with consecutive sample indexes")
		}
	}
	input, err := sampleInput(samples)
	if err != nil {
		return err
	}
	token := fmt.Sprintf("snmp:%s:%d:%d:%d", first.SourceRunID, first.PollSequence, first.SampleIndex, last.SampleIndex)
	query := ch.Query{
		Body:  input.Into("snmp_samples"),
		Input: input,
		Settings: []ch.Setting{
			{Key: "async_insert", Value: "0", Important: true},
			{Key: "wait_for_async_insert", Value: "1", Important: true},
			{Key: "insert_deduplication_token", Value: token, Important: true},
		},
	}
	delay := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err = s.exec.Do(ctx, query)
		if err == nil {
			return nil
		}
		if attempt == 4 || ctx.Err() != nil {
			return fmt.Errorf("insert SNMP ClickHouse batch: %w", errors.Join(err, ctx.Err()))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		delay *= 2
	}
}

func sampleInput(samples []Sample) (proto.Input, error) {
	observed := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	ingested := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	var device, agent, entityID, recipe, run proto.ColStr
	entity := new(proto.ColStr).LowCardinality()
	metric := new(proto.ColStr).LowCardinality()
	kind := new(proto.ColStr).LowCardinality()
	var gauge proto.ColFloat64
	var counter, poll proto.ColUInt64
	var width proto.ColUInt8
	var interval, quality, index proto.ColUInt32
	now := time.Now().UTC()
	for _, sample := range samples {
		if sample.ObservedAt.IsZero() || sample.DeviceID == "" || sample.EntityKind == "" || sample.RecipeID == "" || sample.Metric == "" || sample.SourceRunID == "" || (sample.ValueKind != "gauge" && sample.ValueKind != "counter") {
			return nil, errors.New("invalid SNMP sample")
		}
		if sample.ValueKind == "counter" && sample.CounterWidth != 32 && sample.CounterWidth != 64 {
			return nil, errors.New("SNMP counter width must be 32 or 64")
		}
		observed.Append(sample.ObservedAt.UTC())
		ingested.Append(now)
		device.Append(sample.DeviceID)
		agent.Append(sample.AgentID)
		entity.Append(sample.EntityKind)
		entityID.Append(sample.EntityID)
		recipe.Append(sample.RecipeID)
		metric.Append(sample.Metric)
		kind.Append(sample.ValueKind)
		gauge.Append(sample.GaugeValue)
		counter.Append(sample.CounterValue)
		width.Append(sample.CounterWidth)
		interval.Append(sample.IntervalMS)
		quality.Append(sample.QualityFlags)
		poll.Append(sample.PollSequence)
		run.Append(sample.SourceRunID)
		index.Append(sample.SampleIndex)
	}
	return proto.Input{
		{Name: "observed_at", Data: observed}, {Name: "ingested_at", Data: ingested},
		{Name: "device_id", Data: &device}, {Name: "agent_id", Data: &agent},
		{Name: "entity_kind", Data: entity}, {Name: "entity_id", Data: &entityID},
		{Name: "recipe_id", Data: &recipe}, {Name: "metric", Data: metric},
		{Name: "value_kind", Data: kind}, {Name: "gauge_value", Data: &gauge},
		{Name: "counter_value", Data: &counter}, {Name: "counter_width", Data: &width},
		{Name: "interval_ms", Data: &interval}, {Name: "quality_flags", Data: &quality},
		{Name: "poll_sequence", Data: &poll}, {Name: "source_run_id", Data: &run},
		{Name: "sample_index", Data: &index},
	}, nil
}
