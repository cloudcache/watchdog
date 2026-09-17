package snmpdomain

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type RawSampleWriter interface {
	WriteRawSamples(context.Context, []RawSample) error
}

type PollJob struct {
	TargetID     string
	DeviceID     string
	Target       QueryTarget
	Profile      Profile
	Recipes      []Recipe
	SampledAt    time.Time
	SourceRunID  string
	PollSequence uint64
}

type PollResult struct {
	SampledAt        time.Time
	SampleCount      int
	MissingRecipeIDs []string
}

type Poller struct {
	Query   QueryEngine
	Writer  RawSampleWriter
	MaxOids int
}

func (p Poller) Poll(ctx context.Context, job PollJob) (PollResult, error) {
	if p.Query == nil {
		return PollResult{}, errors.New("snmp poller query engine is required")
	}
	if p.Writer == nil {
		return PollResult{}, errors.New("snmp poller raw sample writer is required")
	}
	sampledAt := job.SampledAt.UTC()
	if sampledAt.IsZero() {
		sampledAt = time.Now().UTC()
	}
	result := PollResult{SampledAt: sampledAt}
	runID := strings.TrimSpace(job.SourceRunID)
	if runID == "" {
		runID = uuid.NewString()
	}
	pollSequence := job.PollSequence
	if pollSequence == 0 {
		pollSequence = uint64(sampledAt.UnixMilli())
	}
	groups := make(map[string][]Recipe)
	for _, recipe := range job.Recipes {
		groups[recipe.ContextName] = append(groups[recipe.ContextName], recipe)
	}
	var samples []RawSample
	for contextName, recipes := range groups {
		oids := make([]string, 0, len(recipes))
		byOID := make(map[string][]Recipe, len(recipes))
		for _, recipe := range recipes {
			if !recipe.Enabled || strings.TrimSpace(recipe.NumericOID) == "" {
				continue
			}
			oid := strings.TrimPrefix(strings.TrimSpace(recipe.NumericOID), ".")
			oids = append(oids, oid)
			byOID[oid] = append(byOID[oid], recipe)
		}
		if len(oids) == 0 {
			continue
		}
		response, err := p.Query.Get(ctx, GetRequest{Target: job.Target, Profile: job.Profile, Context: contextName, OIDs: oids, Flags: QueryFlags{MaxOids: p.maxOids()}})
		if err != nil {
			return result, err
		}
		seen := make(map[string]struct{}, len(recipes))
		collect := func(response QueryResponse, recipesByOID map[string][]Recipe) error {
			for _, vb := range response.VarBinds {
				for _, recipe := range recipesByOID[strings.TrimPrefix(vb.OID, ".")] {
					sample, ok := sampleFromRecipe(job, recipe, vb, sampledAt)
					if !ok {
						continue
					}
					if err := validateRawSample(sample); err != nil {
						return err
					}
					sample.SourceRunID = runID
					sample.PollSequence = pollSequence
					sample.SampleIndex = uint32(len(samples))
					samples = append(samples, sample)
					seen[recipe.ID] = struct{}{}
				}
			}
			return nil
		}
		if err := collect(response, byOID); err != nil {
			return result, err
		}
		// A large device may time out only one GET chunk. Retry the missing
		// high-capacity interface counters first, using their original OIDs,
		// before considering the 32-bit compatibility counter. Otherwise a
		// transient timeout can switch widths and manufacture a false delta.
		primaryRetryOIDs := make([]string, 0)
		primaryRetryByOID := make(map[string][]Recipe)
		for _, recipe := range recipes {
			if !recipe.Enabled {
				continue
			}
			if _, ok := seen[recipe.ID]; ok {
				continue
			}
			if _, ok := fallbackCounterRecipe(recipe); !ok {
				continue
			}
			oid := strings.TrimPrefix(strings.TrimSpace(recipe.NumericOID), ".")
			if _, exists := primaryRetryByOID[oid]; !exists {
				primaryRetryOIDs = append(primaryRetryOIDs, oid)
			}
			primaryRetryByOID[oid] = append(primaryRetryByOID[oid], recipe)
		}
		primaryRetryReturned := make(map[string]struct{}, len(primaryRetryOIDs))
		if len(primaryRetryOIDs) > 0 {
			primaryRetry, retryErr := p.Query.Get(ctx, GetRequest{Target: job.Target, Profile: job.Profile, Context: contextName, OIDs: primaryRetryOIDs, Flags: QueryFlags{MaxOids: p.maxOids()}})
			if retryErr == nil {
				for _, vb := range primaryRetry.VarBinds {
					primaryRetryReturned[strings.TrimPrefix(vb.OID, ".")] = struct{}{}
				}
				if err := collect(primaryRetry, primaryRetryByOID); err != nil {
					return result, err
				}
			}
		}
		fallbackOIDs := make([]string, 0)
		fallbackByOID := make(map[string][]Recipe)
		for _, recipe := range recipes {
			if !recipe.Enabled {
				continue
			}
			if _, ok := seen[recipe.ID]; ok {
				continue
			}
			fallback, ok := fallbackCounterRecipe(recipe)
			if !ok {
				continue
			}
			// Only fall back after the device explicitly returned the primary
			// OID without a usable value. An OID omitted by a failed chunk is a
			// transport failure, not evidence that the HC counter is unsupported.
			primaryOID := strings.TrimPrefix(strings.TrimSpace(recipe.NumericOID), ".")
			if _, ok := primaryRetryReturned[primaryOID]; !ok {
				continue
			}
			if _, exists := fallbackByOID[fallback.NumericOID]; !exists {
				fallbackOIDs = append(fallbackOIDs, fallback.NumericOID)
			}
			fallbackByOID[fallback.NumericOID] = append(fallbackByOID[fallback.NumericOID], fallback)
		}
		if len(fallbackOIDs) > 0 {
			fallbackResponse, fallbackErr := p.Query.Get(ctx, GetRequest{Target: job.Target, Profile: job.Profile, Context: contextName, OIDs: fallbackOIDs, Flags: QueryFlags{MaxOids: p.maxOids()}})
			if fallbackErr == nil {
				if err := collect(fallbackResponse, fallbackByOID); err != nil {
					return result, err
				}
			}
		}
		for _, recipe := range recipes {
			if recipe.Enabled {
				if _, ok := seen[recipe.ID]; !ok {
					result.MissingRecipeIDs = append(result.MissingRecipeIDs, recipe.ID)
				}
			}
		}
	}
	if len(samples) > 0 {
		if err := p.Writer.WriteRawSamples(ctx, samples); err != nil {
			return result, err
		}
	}
	result.SampleCount = len(samples)
	return result, nil
}

func fallbackCounterRecipe(recipe Recipe) (Recipe, bool) {
	if recipe.ValueType != ValueCounter64 {
		return Recipe{}, false
	}
	fallbackOID := strings.TrimPrefix(strings.TrimSpace(recipe.Options["fallback_numeric_oid"]), ".")
	if fallbackOID == "" && strings.TrimSpace(recipe.OIDIndex) != "" {
		switch recipe.MetricName {
		case MetricSNMPIfInOctetsTotal:
			fallbackOID = snmpOIDIfInOctets + "." + recipe.OIDIndex
		case MetricSNMPIfOutOctetsTotal:
			fallbackOID = snmpOIDIfOutOctets + "." + recipe.OIDIndex
		}
	}
	if fallbackOID == "" || fallbackOID == strings.TrimPrefix(strings.TrimSpace(recipe.NumericOID), ".") {
		return Recipe{}, false
	}
	fallback := recipe
	fallback.NumericOID = fallbackOID
	fallback.ValueType = ValueCounter32
	return fallback, true
}

func (p Poller) maxOids() int {
	if p.MaxOids > 0 {
		return p.MaxOids
	}
	return 60
}

func sampleFromRecipe(job PollJob, recipe Recipe, vb VarBind, sampledAt time.Time) (RawSample, bool) {
	sample := RawSample{TargetID: job.TargetID, DeviceID: firstID(job.DeviceID, recipe.DeviceID), EntityType: recipe.EntityType, EntityID: recipe.EntityID, RecipeID: recipe.ID, MetricName: recipe.MetricName, ValueType: recipe.ValueType, SampledAt: sampledAt, IntervalMS: recipe.SampleIntervalSeconds * 1000, Labels: sampleLabels(recipe)}
	if recipe.ValueType == ValueCounter32 || recipe.ValueType == ValueCounter64 {
		value, ok := uint64Value(vb.Value)
		if !ok {
			return sample, false
		}
		sample.CounterValue, sample.CounterValid, sample.FloatValue = value, true, float64(value)
		if recipe.ValueType == ValueCounter32 {
			sample.CounterWidth = 32
		} else {
			sample.CounterWidth = 64
		}
		return sample, true
	}
	if recipe.ValueType == ValueString || recipe.ValueType == ValueMACAddr || recipe.ValueType == ValueIPAddr {
		sample.StringValue = stringValue(vb.Value)
		return sample, true
	}
	value, ok := floatValue(vb.Value)
	if !ok {
		if parsed, parsedOK := floatValue(stringValue(vb.Value)); parsedOK {
			value, ok = parsed, true
		}
	}
	if !ok {
		sample.StringValue = stringValue(vb.Value)
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

func uint64Value(value any) (uint64, bool) {
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

func floatValue(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case uint:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case int32:
		return float64(v), true
	case float64:
		return v, true
	case []byte:
		parsed, err := strconv.ParseFloat(string(v), 64)
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func stringValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func sampleLabels(recipe Recipe) map[string]string {
	labels := make(map[string]string, len(recipe.Labels)+6)
	for key, value := range recipe.Labels {
		if value != "" {
			labels[key] = value
		}
	}
	labels["device_id"] = recipe.DeviceID
	labels["recipe_id"] = recipe.ID
	labels["entity_type"] = string(recipe.EntityType)
	labels["entity_id"] = recipe.EntityID
	labels["module"] = recipe.ModuleName
	return labels
}

func firstID(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func validateRawSample(sample RawSample) error {
	if sample.DeviceID == "" {
		return fmt.Errorf("raw sample %s missing device_id", sample.MetricName)
	}
	if sample.RecipeID == "" {
		return fmt.Errorf("raw sample %s missing recipe_id", sample.MetricName)
	}
	return nil
}
