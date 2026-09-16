package metricdomain

import "time"

var AllowedQuerySteps = []time.Duration{
	time.Second,
	5 * time.Second,
	10 * time.Second,
	15 * time.Second,
	30 * time.Second,
	time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	time.Hour,
	2 * time.Hour,
	6 * time.Hour,
	12 * time.Hour,
	24 * time.Hour,
}

func IsAllowedQueryStep(step time.Duration) bool {
	for _, allowed := range AllowedQuerySteps {
		if step == allowed {
			return true
		}
	}
	return false
}

func AutoQueryStep(window time.Duration, maxDataPoints int) time.Duration {
	if maxDataPoints <= 0 {
		maxDataPoints = 1200
	}
	if window <= 0 {
		return time.Minute
	}
	raw := window / time.Duration(maxDataPoints)
	if raw <= 0 {
		raw = time.Second
	}
	for _, step := range AllowedQuerySteps {
		if raw <= step {
			return step
		}
	}
	return AllowedQuerySteps[len(AllowedQuerySteps)-1]
}
