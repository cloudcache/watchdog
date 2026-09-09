package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type SNMPRawSampleWriter interface {
	WriteSNMPRawSamples(ctx context.Context, samples []SNMPRawSample) error
}

type SNMPPollJob struct {
	TenantID     ID
	TargetID     ID
	DeviceID     ID
	Target       SNMPCollectorTarget
	Profile      SNMPProfile
	Recipes      []SNMPCollectionRecipe
	SampledAt    time.Time
	SourceRunID  string
	PollSequence uint64
}

type SNMPPollResult struct {
	SampledAt        time.Time
	SampleCount      int
	MissingRecipeIDs []ID
}

type SNMPPoller struct {
	Query   SNMPCollectorQueryEngine
	Writer  SNMPRawSampleWriter
	MaxOids int
}

func (p SNMPPoller) Poll(ctx context.Context, job SNMPPollJob) (SNMPPollResult, error) {
	if p.Query == nil {
		return SNMPPollResult{}, errors.New("snmp poller query engine is required")
	}
	if p.Writer == nil {
		return SNMPPollResult{}, errors.New("snmp poller raw sample writer is required")
	}
	sampledAt := job.SampledAt
	if sampledAt.IsZero() {
		sampledAt = time.Now().UTC()
	}
	result := SNMPPollResult{SampledAt: sampledAt}
	runID := strings.TrimSpace(job.SourceRunID)
	if runID == "" {
		runID = uuid.NewString()
	}
	pollSequence := job.PollSequence
	if pollSequence == 0 {
		pollSequence = uint64(sampledAt.UnixMilli())
	}
	groups := groupRecipesByContext(job.Recipes)
	var samples []SNMPRawSample
	for contextName, recipes := range groups {
		oids := make([]string, 0, len(recipes))
		recipesByOID := make(map[string][]SNMPCollectionRecipe, len(recipes))
		for _, recipe := range recipes {
			if !recipe.Enabled || strings.TrimSpace(recipe.NumericOID) == "" {
				continue
			}
			oid := strings.TrimPrefix(strings.TrimSpace(recipe.NumericOID), ".")
			oids = append(oids, oid)
			recipesByOID[oid] = append(recipesByOID[oid], recipe)
		}
		if len(oids) == 0 {
			continue
		}
		response, err := p.Query.Get(ctx, SNMPCollectorGetRequest{
			Target:  job.Target,
			Profile: job.Profile,
			Context: contextName,
			OIDs:    oids,
			Flags: SNMPCollectorQueryFlags{
				MaxOids: p.maxOids(),
			},
		})
		if err != nil {
			return result, err
		}
		seen := make(map[ID]struct{}, len(recipes))
		for _, vb := range response.VarBinds {
			oid := strings.TrimPrefix(vb.OID, ".")
			for _, recipe := range recipesByOID[oid] {
				sample, ok := rawSampleFromRecipe(job, recipe, vb, sampledAt)
				if !ok {
					continue
				}
				if err := validateRawSample(sample); err != nil {
					return result, err
				}
				samples = append(samples, sample)
				samples[len(samples)-1].SourceRunID = runID
				samples[len(samples)-1].PollSequence = pollSequence
				samples[len(samples)-1].SampleIndex = uint32(len(samples) - 1)
				seen[recipe.ID] = struct{}{}
			}
		}
		for _, recipe := range recipes {
			if !recipe.Enabled {
				continue
			}
			if _, ok := seen[recipe.ID]; !ok {
				result.MissingRecipeIDs = append(result.MissingRecipeIDs, recipe.ID)
			}
		}
	}
	if len(samples) > 0 {
		if err := p.Writer.WriteSNMPRawSamples(ctx, samples); err != nil {
			return result, err
		}
	}
	result.SampleCount = len(samples)
	return result, nil
}

func (p SNMPPoller) maxOids() int {
	if p.MaxOids > 0 {
		return p.MaxOids
	}
	return 60
}

func groupRecipesByContext(recipes []SNMPCollectionRecipe) map[string][]SNMPCollectionRecipe {
	groups := make(map[string][]SNMPCollectionRecipe)
	for _, recipe := range recipes {
		groups[recipe.ContextName] = append(groups[recipe.ContextName], recipe)
	}
	return groups
}

func rawSampleFromRecipe(job SNMPPollJob, recipe SNMPCollectionRecipe, vb SNMPCollectorVarBind, sampledAt time.Time) (SNMPRawSample, bool) {
	sample := SNMPRawSample{
		TenantID:   firstSNMPCollectorID(job.TenantID, recipe.TenantID),
		TargetID:   job.TargetID,
		DeviceID:   firstSNMPCollectorID(job.DeviceID, recipe.DeviceID),
		EntityType: recipe.EntityType,
		EntityID:   recipe.EntityID,
		RecipeID:   recipe.ID,
		MetricName: recipe.MetricName,
		ValueType:  recipe.ValueType,
		SampledAt:  sampledAt,
		IntervalMS: recipe.SampleIntervalSeconds * 1000,
		Labels:     rawSampleLabels(recipe),
	}
	if recipe.ValueType == SNMPCollectorValueCounter32 || recipe.ValueType == SNMPCollectorValueCounter64 {
		value, ok := snmpCollectorUint64Value(vb.Value)
		if !ok {
			return sample, false
		}
		sample.CounterValue, sample.CounterValid = value, true
		// FloatValue remains populated for the retired Prometheus renderer and
		// diagnostic callers. ClickHouse writes CounterValue directly, so counters
		// above 2^53 never pass through this lossy compatibility field.
		sample.FloatValue = float64(value)
		if recipe.ValueType == SNMPCollectorValueCounter32 {
			sample.CounterWidth = 32
		} else {
			sample.CounterWidth = 64
		}
		return sample, true
	}
	if recipe.ValueType == SNMPCollectorValueString || recipe.ValueType == SNMPCollectorValueMACAddr || recipe.ValueType == SNMPCollectorValueIPAddr {
		sample.StringValue = snmpCollectorStringValue(vb.Value)
		return sample, true
	}
	value, ok := snmpCollectorFloatValue(vb.Value)
	if !ok {
		if parsed, parsedOK := snmpCollectorFloatValue(snmpCollectorStringValue(vb.Value)); parsedOK {
			value = parsed
			ok = true
		}
	}
	if !ok {
		sample.StringValue = snmpCollectorStringValue(vb.Value)
		return sample, sample.StringValue != ""
	}
	if recipe.HasDivisor && recipe.Divisor != 0 {
		value /= recipe.Divisor
	}
	if recipe.HasMultiplier {
		value *= recipe.Multiplier
	}
	sample.FloatValue = value
	return sample, true
}

func snmpCollectorUint64Value(value any) (uint64, bool) {
	switch v := value.(type) {
	case uint64:
		return v, true
	case uint32:
		return uint64(v), true
	case uint:
		return uint64(v), true
	case int:
		if v >= 0 {
			return uint64(v), true
		}
	case int32:
		if v >= 0 {
			return uint64(v), true
		}
	case int64:
		if v >= 0 {
			return uint64(v), true
		}
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		return parsed, err == nil
	case []byte:
		parsed, err := strconv.ParseUint(strings.TrimSpace(string(v)), 10, 64)
		return parsed, err == nil
	}
	return 0, false
}

func rawSampleLabels(recipe SNMPCollectionRecipe) map[string]string {
	labels := make(map[string]string, len(recipe.Labels)+6)
	for key, value := range recipe.Labels {
		if value != "" {
			labels[key] = value
		}
	}
	labels["device_id"] = string(recipe.DeviceID)
	labels["recipe_id"] = string(recipe.ID)
	labels["entity_type"] = string(recipe.EntityType)
	labels["entity_id"] = string(recipe.EntityID)
	labels["module"] = recipe.ModuleName
	return labels
}

func firstSNMPCollectorID(values ...ID) ID {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func validateRawSample(sample SNMPRawSample) error {
	if sample.DeviceID == "" {
		return fmt.Errorf("raw sample %s missing device_id", sample.MetricName)
	}
	if sample.RecipeID == "" {
		return fmt.Errorf("raw sample %s missing recipe_id", sample.MetricName)
	}
	return nil
}
