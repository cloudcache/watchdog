package watchdog

import (
	"context"
	"errors"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

type SNMPPollRunner struct {
	Collector         SNMPPollRecipeRepository
	Network           SNMPPollDeviceRepository
	Targets           SNMPPollTargetRepository
	SNMP              SNMPPollProfileRepository
	Poller            SNMPPoller
	Now               func() time.Time
	GlobalConcurrency int
}

// The poll runner intentionally depends on its four read/write operations,
// rather than the former management-wide repositories. This lets the v2 Gin
// runtime reuse the proven poll orchestration without reintroducing legacy
// tenant stores or their unrelated CRUD surface.
type SNMPPollRecipeRepository interface {
	ListDueSNMPDevices(context.Context, ID, int, time.Time) ([]ID, error)
	ListSNMPCollectionRecipesByDevice(context.Context, ID, ID) ([]SNMPCollectionRecipe, error)
	MarkSNMPRecipePollResult(context.Context, ID, time.Time, string) error
}

type SNMPPollDeviceRepository interface {
	GetDevice(context.Context, ID, ID) (NetworkDevice, error)
}

type SNMPPollTargetRepository interface {
	GetTarget(context.Context, ID, ID) (Target, error)
}

type SNMPPollProfileRepository interface {
	GetSNMPProfile(context.Context, ID, ID) (SNMPProfile, error)
}

type SNMPPollRunnerResult struct {
	RecipeCount int
	DeviceCount int
	SampleCount int
	FailedCount int
}

func (r SNMPPollRunner) RunDue(ctx context.Context, tenantID ID, limit int) (SNMPPollRunnerResult, error) {
	if r.Collector == nil {
		return SNMPPollRunnerResult{}, errors.New("snmp collector repository is required")
	}
	if r.Network == nil {
		return SNMPPollRunnerResult{}, errors.New("network repository is required")
	}
	if r.Targets == nil {
		return SNMPPollRunnerResult{}, errors.New("target repository is required")
	}
	if r.SNMP == nil {
		return SNMPPollRunnerResult{}, errors.New("snmp repository is required")
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}

	deviceIDs, err := r.Collector.ListDueSNMPDevices(ctx, tenantID, limit, now)
	if err != nil {
		return SNMPPollRunnerResult{}, err
	}
	result := SNMPPollRunnerResult{DeviceCount: len(deviceIDs)}

	concurrency := r.GlobalConcurrency
	if concurrency <= 0 {
		concurrency = 32
	}
	if len(deviceIDs) < concurrency {
		concurrency = len(deviceIDs)
	}

	type deviceResult = devicePollResult
	results := make([]deviceResult, len(deviceIDs))

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, deviceID := range deviceIDs {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(idx int, devID ID) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("snmp poll panic device=%s stack=%s", devID, debug.Stack())
					results[idx] = devicePollResult{}
				}
			}()

			recipes, err := r.Collector.ListSNMPCollectionRecipesByDevice(ctx, tenantID, devID)
			if err != nil {
				results[idx] = devicePollResult{}
				return
			}
			recipes = dueSNMPRecipes(recipes, now)
			if len(recipes) == 0 {
				return
			}
			dr := r.pollDevice(ctx, tenantID, devID, recipes, now)
			dr.recipeCount = len(recipes)
			results[idx] = dr
		}(i, deviceID)
	}
	wg.Wait()

	for _, dr := range results {
		result.RecipeCount += dr.recipeCount
		result.SampleCount += dr.samples
		result.FailedCount += dr.failed
	}
	return result, nil
}

func dueSNMPRecipes(recipes []SNMPCollectionRecipe, now time.Time) []SNMPCollectionRecipe {
	due := make([]SNMPCollectionRecipe, 0, len(recipes))
	for _, recipe := range recipes {
		interval := time.Duration(recipe.SampleIntervalSeconds) * time.Second
		if interval <= 0 {
			interval = time.Minute
		}
		if recipe.LastPolledAt.IsZero() || !recipe.LastPolledAt.Add(interval).After(now) {
			due = append(due, recipe)
		}
	}
	return due
}

type devicePollResult struct {
	recipeCount int
	samples     int
	failed      int
}

func (r SNMPPollRunner) pollDevice(ctx context.Context, tenantID, deviceID ID, recipes []SNMPCollectionRecipe, now time.Time) devicePollResult {
	result := devicePollResult{recipeCount: len(recipes)}

	pollStart := time.Now()
	job, err := r.pollJob(ctx, tenantID, deviceID, recipes, now)
	if err != nil {
		result.failed = len(recipes)
		r.markRecipes(ctx, recipes, now, err.Error())
		return result
	}
	pollResult, err := r.Poller.Poll(ctx, job)
	if err != nil {
		result.failed = len(recipes)
		r.markRecipes(ctx, recipes, now, err.Error())
		return result
	}
	result.samples = pollResult.SampleCount
	log.Printf("snmp poll device=%s recipes=%d samples=%d elapsed=%dms", deviceID, len(recipes), pollResult.SampleCount, time.Since(pollStart).Milliseconds())
	missing := map[ID]struct{}{}
	for _, recipeID := range pollResult.MissingRecipeIDs {
		missing[recipeID] = struct{}{}
	}
	for _, recipe := range recipes {
		lastError := ""
		if _, ok := missing[recipe.ID]; ok {
			lastError = "missing snmp response"
			result.failed++
		}
		_ = r.Collector.MarkSNMPRecipePollResult(ctx, recipe.ID, pollResult.SampledAt, lastError)
	}
	return result
}

func (r SNMPPollRunner) pollJob(ctx context.Context, tenantID ID, deviceID ID, recipes []SNMPCollectionRecipe, sampledAt time.Time) (SNMPPollJob, error) {
	device, err := r.Network.GetDevice(ctx, tenantID, deviceID)
	if err != nil {
		return SNMPPollJob{}, err
	}
	target, err := r.Targets.GetTarget(ctx, tenantID, device.TargetID)
	if err != nil {
		return SNMPPollJob{}, err
	}
	profile, err := r.SNMP.GetSNMPProfile(ctx, tenantID, device.SNMPProfileID)
	if err != nil {
		return SNMPPollJob{}, err
	}
	profile = ApplyDeviceSNMPOverrides(profile, device)
	targetPort := uint16(0)
	if device.SNMPPort != 0 {
		targetPort = device.SNMPPort
	}
	return SNMPPollJob{
		TenantID:  tenantID,
		TargetID:  device.TargetID,
		DeviceID:  device.ID,
		Target:    SNMPCollectorTarget{Host: target.Host, Port: targetPort},
		Profile:   profile,
		Recipes:   recipes,
		SampledAt: sampledAt,
	}, nil
}

func (r SNMPPollRunner) markRecipes(ctx context.Context, recipes []SNMPCollectionRecipe, polledAt time.Time, lastError string) error {
	for _, recipe := range recipes {
		if err := r.Collector.MarkSNMPRecipePollResult(ctx, recipe.ID, polledAt, lastError); err != nil {
			return err
		}
	}
	return nil
}

func snmpRecipesByDevice(recipes []SNMPCollectionRecipe) map[ID][]SNMPCollectionRecipe {
	groups := make(map[ID][]SNMPCollectionRecipe)
	for _, recipe := range recipes {
		if recipe.DeviceID == "" {
			continue
		}
		groups[recipe.DeviceID] = append(groups[recipe.DeviceID], recipe)
	}
	return groups
}
