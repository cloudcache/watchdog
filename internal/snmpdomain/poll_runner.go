package snmpdomain

import (
	"context"
	"errors"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

type PollRecipeRepository interface {
	ListDueDevices(context.Context, int, time.Time) ([]string, error)
	ListRecipesByDevice(context.Context, string) ([]Recipe, error)
	MarkRecipePollResult(context.Context, string, time.Time, string) error
}

type PollDeviceStatusRepository interface {
	MarkDevicePollResult(context.Context, string, time.Time, string) error
}

type PollDeviceRepository interface {
	GetDevice(context.Context, string) (Device, error)
}
type PollTargetRepository interface {
	GetTarget(context.Context, string) (Target, error)
}
type PollProfileRepository interface {
	GetProfile(context.Context, string) (Profile, error)
}

type PollRunner struct {
	Recipes           PollRecipeRepository
	Devices           PollDeviceRepository
	Targets           PollTargetRepository
	Profiles          PollProfileRepository
	Poller            Poller
	Now               func() time.Time
	GlobalConcurrency int
}

type PollRunnerResult struct {
	RecipeCount int
	DeviceCount int
	SampleCount int
	FailedCount int
}

type devicePollResult struct{ recipeCount, samples, failed int }

func (r PollRunner) RunDue(ctx context.Context, limit int) (PollRunnerResult, error) {
	if r.Recipes == nil {
		return PollRunnerResult{}, errors.New("snmp recipe repository is required")
	}
	if r.Devices == nil {
		return PollRunnerResult{}, errors.New("snmp device repository is required")
	}
	if r.Targets == nil {
		return PollRunnerResult{}, errors.New("snmp target repository is required")
	}
	if r.Profiles == nil {
		return PollRunnerResult{}, errors.New("snmp profile repository is required")
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	deviceIDs, err := r.Recipes.ListDueDevices(ctx, limit, now)
	if err != nil {
		return PollRunnerResult{}, err
	}
	result := PollRunnerResult{DeviceCount: len(deviceIDs)}
	concurrency := r.GlobalConcurrency
	if concurrency <= 0 {
		concurrency = 32
	}
	if len(deviceIDs) < concurrency {
		concurrency = len(deviceIDs)
	}
	results := make([]devicePollResult, len(deviceIDs))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, deviceID := range deviceIDs {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(index int, id string) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("snmp poll panic device=%s stack=%s", id, debug.Stack())
				}
			}()
			recipes, listErr := r.Recipes.ListRecipesByDevice(ctx, id)
			if listErr != nil {
				return
			}
			recipes = dueRecipes(recipes, now)
			if len(recipes) == 0 {
				return
			}
			results[index] = r.pollDevice(ctx, id, recipes, now)
			results[index].recipeCount = len(recipes)
		}(i, deviceID)
	}
	wg.Wait()
	for _, item := range results {
		result.RecipeCount += item.recipeCount
		result.SampleCount += item.samples
		result.FailedCount += item.failed
	}
	return result, nil
}

func dueRecipes(recipes []Recipe, now time.Time) []Recipe {
	due := make([]Recipe, 0, len(recipes))
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

func (r PollRunner) pollDevice(ctx context.Context, deviceID string, recipes []Recipe, now time.Time) devicePollResult {
	result := devicePollResult{recipeCount: len(recipes)}
	pollStart := time.Now()
	job, err := r.pollJob(ctx, deviceID, recipes, now)
	if err != nil {
		return r.failDevice(ctx, deviceID, recipes, now, err)
	}
	pollResult, err := r.Poller.Poll(ctx, job)
	if err != nil {
		return r.failDevice(ctx, deviceID, recipes, now, err)
	}
	result.samples = pollResult.SampleCount
	log.Printf("snmp poll device=%s recipes=%d samples=%d elapsed=%dms", deviceID, len(recipes), pollResult.SampleCount, time.Since(pollStart).Milliseconds())
	missing := make(map[string]struct{}, len(pollResult.MissingRecipeIDs))
	for _, id := range pollResult.MissingRecipeIDs {
		missing[id] = struct{}{}
	}
	for _, recipe := range recipes {
		lastError := ""
		if _, ok := missing[recipe.ID]; ok {
			lastError = "missing snmp response"
			result.failed++
		}
		_ = r.Recipes.MarkRecipePollResult(ctx, recipe.ID, pollResult.SampledAt, lastError)
	}
	r.markDevice(ctx, deviceID, pollResult.SampledAt, "")
	return result
}

func (r PollRunner) failDevice(ctx context.Context, deviceID string, recipes []Recipe, now time.Time, err error) devicePollResult {
	for _, recipe := range recipes {
		_ = r.Recipes.MarkRecipePollResult(ctx, recipe.ID, now, err.Error())
	}
	r.markDevice(ctx, deviceID, now, err.Error())
	return devicePollResult{recipeCount: len(recipes), failed: len(recipes)}
}

func (r PollRunner) markDevice(ctx context.Context, deviceID string, at time.Time, lastError string) {
	if repository, ok := r.Recipes.(PollDeviceStatusRepository); ok {
		_ = repository.MarkDevicePollResult(ctx, deviceID, at, lastError)
	}
}

func (r PollRunner) pollJob(ctx context.Context, deviceID string, recipes []Recipe, sampledAt time.Time) (PollJob, error) {
	device, err := r.Devices.GetDevice(ctx, deviceID)
	if err != nil {
		return PollJob{}, err
	}
	target, err := r.Targets.GetTarget(ctx, device.TargetID)
	if err != nil {
		return PollJob{}, err
	}
	profile, err := r.Profiles.GetProfile(ctx, device.SNMPProfileID)
	if err != nil {
		return PollJob{}, err
	}
	profile = ApplyDeviceOverrides(profile, device)
	return PollJob{TargetID: device.TargetID, DeviceID: device.ID, Target: QueryTarget{Host: target.Host, Port: device.SNMPPort}, Profile: profile, Recipes: recipes, SampledAt: sampledAt}, nil
}
