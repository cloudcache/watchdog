package watchdog

import (
	"testing"
	"time"
)

func TestValidateMetricsQuerySupportsFixedSteps(t *testing.T) {
	for _, step := range []time.Duration{
		time.Minute,
		5 * time.Minute,
		10 * time.Minute,
		15 * time.Minute,
		30 * time.Minute,
		time.Hour,
	} {
		err := ValidateMetricsQuery(MetricsQueryRequest{
			TimeMode:       TimeModeFixed,
			Start:          time.Unix(0, 0),
			End:            time.Unix(3600, 0),
			Step:           step,
			CollectionStep: time.Minute,
		}, false)
		if err != nil {
			t.Fatalf("step %s: %v", step, err)
		}
	}
}

func TestValidateMetricsQueryRejectsUnsupportedCustomStep(t *testing.T) {
	err := ValidateMetricsQuery(MetricsQueryRequest{
		TimeMode: TimeModeCustom,
		Start:    time.Unix(0, 0),
		End:      time.Unix(3600, 0),
		Step:     7 * time.Minute,
	}, false)
	if err == nil {
		t.Fatal("expected unsupported custom step error")
	}
}

func TestValidateMetricsQueryRejectsStepBelowCollectionStep(t *testing.T) {
	err := ValidateMetricsQuery(MetricsQueryRequest{
		TimeMode:       TimeModeFixed,
		Start:          time.Unix(0, 0),
		End:            time.Unix(3600, 0),
		Step:           time.Minute,
		CollectionStep: 5 * time.Minute,
	}, false)
	if err == nil {
		t.Fatal("expected collection step error")
	}
}

func TestValidateMetricsQueryRawAndBothRequireAdmin(t *testing.T) {
	for _, mode := range []MetricValueMode{MetricValueRaw, MetricValueBoth} {
		err := ValidateMetricsQuery(MetricsQueryRequest{
			TimeMode:  TimeModeFixed,
			Start:     time.Unix(0, 0),
			End:       time.Unix(3600, 0),
			Step:      time.Minute,
			ValueMode: mode,
		}, false)
		if err == nil {
			t.Fatalf("expected admin error for %s", mode)
		}
		err = ValidateMetricsQuery(MetricsQueryRequest{
			TimeMode:  TimeModeFixed,
			Start:     time.Unix(0, 0),
			End:       time.Unix(3600, 0),
			Step:      time.Minute,
			ValueMode: mode,
		}, true)
		if err != nil {
			t.Fatalf("admin mode %s: %v", mode, err)
		}
	}
}

func TestValidateMetricsQueryRealtimeAllowsSmallSteps(t *testing.T) {
	err := ValidateMetricsQuery(MetricsQueryRequest{
		TimeMode: TimeModeRealtime,
		Step:     5 * time.Second,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	err = ValidateMetricsQuery(MetricsQueryRequest{
		TimeMode: TimeModeRealtime,
		Step:     2 * time.Minute,
	}, false)
	if err == nil {
		t.Fatal("expected realtime step error")
	}
}

func TestAutoQueryStepRoundsUpLikeDashboardInterval(t *testing.T) {
	if got := AutoQueryStep(time.Hour, 600); got != 10*time.Second {
		t.Fatalf("1h step = %s", got)
	}
	if got := AutoQueryStep(24*time.Hour, 600); got != 5*time.Minute {
		t.Fatalf("24h step = %s", got)
	}
	if got := AutoQueryStep(30*24*time.Hour, 1200); got != time.Hour {
		t.Fatalf("30d step = %s", got)
	}
}

func TestFixedWindowDurationSupportsShortDashboardWindows(t *testing.T) {
	tests := map[FixedTimeWindow]time.Duration{
		FixedWindowFiveMinutes:    5 * time.Minute,
		FixedWindowTenMinutes:     10 * time.Minute,
		FixedWindowFifteenMinutes: 15 * time.Minute,
		FixedWindowThirtyMinutes:  30 * time.Minute,
	}
	for window, want := range tests {
		got, step, err := fixedWindowDuration(window)
		if err != nil {
			t.Fatalf("%s: %v", window, err)
		}
		if got != want {
			t.Fatalf("%s duration = %s, want %s", window, got, want)
		}
		if step <= 0 || !IsAllowedQueryStep(step) {
			t.Fatalf("%s step = %s, want allowed positive step", window, step)
		}
	}
}

func TestMinStepForRangeKeepsMonthQueryable(t *testing.T) {
	if step := minStepForRange(31*24*time.Hour, maxVMPointsPerSeries); step != 2*time.Minute {
		t.Fatalf("31d step = %s, want 2m", step)
	}
	if step := minStepForRange(10*24*time.Hour, maxVMPointsPerSeries); step != time.Minute {
		t.Fatalf("10d step = %s, want 1m", step)
	}
	if step := minStepForRange(365*24*time.Hour, maxVMPointsPerSeries); int((365*24*time.Hour)/step) > maxVMPointsPerSeries {
		t.Fatalf("1y step %s still exceeds the point limit", step)
	}
}
