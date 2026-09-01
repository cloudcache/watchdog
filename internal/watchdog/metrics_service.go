package watchdog

import (
	"context"
	"errors"
	"time"
)

type MetricsQueryClient interface {
	QueryRange(ctx context.Context, query RangeQuery) (VictoriaMetricsResponse, error)
}

type SeriesCleaner interface {
	DeleteSeries(ctx context.Context, matchers []string) error
}

type MetricsImportClient interface {
	ImportPrometheus(ctx context.Context, payload []byte) error
}

type MetricsService struct {
	Client   MetricsQueryClient
	Importer MetricsImportClient
	Now      func() time.Time
}

type FixedTimeWindow string

const (
	FixedWindowFiveMinutes    FixedTimeWindow = "5m"
	FixedWindowTenMinutes     FixedTimeWindow = "10m"
	FixedWindowFifteenMinutes FixedTimeWindow = "15m"
	FixedWindowThirtyMinutes  FixedTimeWindow = "30m"
	FixedWindowHour           FixedTimeWindow = "1h"
	FixedWindowDay            FixedTimeWindow = "24h"
	FixedWindowWeek           FixedTimeWindow = "7d"
	FixedWindowMonth          FixedTimeWindow = "30d"
)

func (s MetricsService) QueryRealtime(ctx context.Context, req MetricsQueryRequest, selector string) (VictoriaMetricsResponse, error) {
	if selector == "" {
		return VictoriaMetricsResponse{}, errors.New("metric selector is required")
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	req.TimeMode = TimeModeRealtime
	if req.Step == 0 {
		req.Step = 5 * time.Second
	}
	if err := ValidateMetricsQuery(req, true); err != nil {
		return VictoriaMetricsResponse{}, err
	}
	return s.Client.QueryRange(ctx, RangeQuery{
		Query: selector,
		Start: now.Add(-5 * time.Minute),
		End:   now,
		Step:  req.Step,
	})
}

func (s MetricsService) QueryRange(ctx context.Context, req MetricsQueryRequest, selector string, isAdmin bool) (VictoriaMetricsResponse, error) {
	if selector == "" {
		return VictoriaMetricsResponse{}, errors.New("metric selector is required")
	}
	if err := ValidateMetricsQuery(req, isAdmin); err != nil {
		return VictoriaMetricsResponse{}, err
	}
	return s.Client.QueryRange(ctx, RangeQuery{
		Query: selector,
		Start: req.Start,
		End:   req.End,
		Step:  req.Step,
	})
}

func (s MetricsService) QueryFixedWindow(ctx context.Context, req MetricsQueryRequest, selector string, window FixedTimeWindow, isAdmin bool) (VictoriaMetricsResponse, error) {
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	duration, defaultStep, err := fixedWindowDuration(window)
	if err != nil {
		return VictoriaMetricsResponse{}, err
	}
	if req.Step == 0 {
		req.Step = defaultStep
	}
	req.TimeMode = TimeModeFixed
	req.Start = now.Add(-duration)
	req.End = now
	return s.QueryRange(ctx, req, selector, isAdmin)
}

func fixedWindowDuration(window FixedTimeWindow) (time.Duration, time.Duration, error) {
	switch window {
	case FixedWindowFiveMinutes:
		return 5 * time.Minute, 5 * time.Second, nil
	case FixedWindowTenMinutes:
		return 10 * time.Minute, 10 * time.Second, nil
	case FixedWindowFifteenMinutes:
		return 15 * time.Minute, 10 * time.Second, nil
	case FixedWindowThirtyMinutes:
		return 30 * time.Minute, 30 * time.Second, nil
	case FixedWindowHour:
		return time.Hour, time.Minute, nil
	case FixedWindowDay:
		return 24 * time.Hour, 5 * time.Minute, nil
	case FixedWindowWeek:
		return 7 * 24 * time.Hour, 30 * time.Minute, nil
	case FixedWindowMonth:
		return 30 * 24 * time.Hour, time.Hour, nil
	default:
		return 0, 0, errors.New("unsupported fixed time window")
	}
}
