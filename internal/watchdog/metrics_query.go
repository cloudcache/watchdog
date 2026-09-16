package watchdog

import (
	"errors"
	"time"

	"github.com/cloudcache/watchdog/internal/metricdomain"
)

type TimeMode string

const (
	TimeModeRealtime TimeMode = "realtime"
	TimeModeFixed    TimeMode = "fixed"
	TimeModeCustom   TimeMode = "custom"
)

type MetricValueMode = metricdomain.ValueMode

const (
	MetricValueCorrected = metricdomain.ValueCorrected
	MetricValueRaw       = metricdomain.ValueRaw
	MetricValueBoth      = metricdomain.ValueBoth
)

var AllowedQuerySteps = metricdomain.AllowedQuerySteps

type MetricsQueryRequest struct {
	TenantID       ID
	TargetID       ID
	PortID         ID
	DeviceID       ID
	Metric         string
	PortIfIndex    uint64
	Start          time.Time
	End            time.Time
	Step           time.Duration
	TimeMode       TimeMode
	ValueMode      MetricValueMode
	TrafficView    TrafficViewMode
	CollectionStep time.Duration
	Window         FixedTimeWindow
	MaxDataPoints  int
	Func           string // "" | "rate"
	RateWindow     string // prometheus range for rate(), e.g. "5m"
}

func ValidateMetricsQuery(req MetricsQueryRequest, isAdmin bool) error {
	if req.ValueMode == "" {
		req.ValueMode = MetricValueCorrected
	}
	if req.Metric != "" && !IsKnownMetric(req.Metric) {
		return errors.New("unsupported metric")
	}
	if (req.ValueMode == MetricValueRaw || req.ValueMode == MetricValueBoth) && !isAdmin {
		return errors.New("raw metric value mode requires admin permission")
	}
	switch req.TimeMode {
	case TimeModeRealtime:
		if req.Step == 0 {
			req.Step = 5 * time.Second
		}
		if req.Step > time.Minute {
			return errors.New("realtime query step must be one minute or less")
		}
	case TimeModeFixed, TimeModeCustom:
		if req.Step == 0 && req.End.After(req.Start) {
			req.Step = AutoQueryStep(req.End.Sub(req.Start), req.MaxDataPoints)
		}
		if !IsAllowedQueryStep(req.Step) {
			return errors.New("unsupported query step")
		}
		if req.CollectionStep > 0 && req.Step < req.CollectionStep {
			return errors.New("query step cannot be smaller than collection step")
		}
		if !req.End.After(req.Start) {
			return errors.New("query end must be after start")
		}
	default:
		return errors.New("unsupported time mode")
	}
	return nil
}

// VictoriaMetrics rejects range queries returning more points per series than
// -search.maxPointsPerTimeseries (30000 by default). Keep a margin so
// month-scale windows stay queryable at auto-chosen steps.
const maxVMPointsPerSeries = 25000

// minStepForRange returns the smallest allowed query step that keeps
// window/step at or under maxPoints.
func minStepForRange(window time.Duration, maxPoints int) time.Duration {
	if window <= 0 || maxPoints <= 0 {
		return AllowedQuerySteps[0]
	}
	for _, step := range AllowedQuerySteps {
		if int(window/step) <= maxPoints {
			return step
		}
	}
	return AllowedQuerySteps[len(AllowedQuerySteps)-1]
}

func IsAllowedQueryStep(step time.Duration) bool {
	return metricdomain.IsAllowedQueryStep(step)
}

func AutoQueryStep(window time.Duration, maxDataPoints int) time.Duration {
	return metricdomain.AutoQueryStep(window, maxDataPoints)
}
