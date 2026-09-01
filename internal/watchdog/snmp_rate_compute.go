package watchdog

import "math"

type CounterRateOptions struct {
	CounterBits int
	ToBits      bool
}

func ComputeCounterRate(samples []VMValue, opts CounterRateOptions) []VMValue {
	if len(samples) < 2 {
		return nil
	}
	bits := opts.CounterBits
	if bits != 32 {
		bits = 64
	}
	modulus := math.Pow(2, float64(bits))
	rate := make([]VMValue, 0, len(samples)-1)
	lastIdx := 0
	for i := 1; i < len(samples); i++ {
		prev := samples[lastIdx]
		curr := samples[i]
		dt := curr.Time.Sub(prev.Time).Seconds()
		if dt <= 0 {
			lastIdx = i
			continue
		}
		delta := curr.Value - prev.Value
		if delta < 0 {
			if bits == 64 {
				lastIdx = i
				continue
			}
			delta += modulus
		}
		v := delta / dt
		if opts.ToBits {
			v *= 8
		}
		rate = append(rate, VMValue{Time: curr.Time, Value: v})
		// A zero delta renders as a real 0 bps point, but the baseline stays
		// put: when the poller and the query grid beat against each other, a
		// counter repeats for one step and the whole increase lands on the
		// next one. Dividing that increase by the accumulated interval keeps
		// the rate at 1x instead of a 0 / 2x sawtooth (which peak-hold
		// bucketing would then read as double the true traffic). A genuinely
		// idle port still yields consecutive zero deltas and a flat 0 line.
		if delta != 0 {
			lastIdx = i
		}
	}
	// Trailing zero deltas at the live edge are usually VictoriaMetrics
	// staleness fill ahead of the poller's next write, not measured idleness;
	// rendering them drags the newest point down to a false 0. Trim up to the
	// last few zero points — a genuinely idle stretch is longer than that and
	// still renders as a flat 0 line.
	trimmed := 0
	for len(rate) > 0 && trimmed < 3 && rate[len(rate)-1].Value == 0 {
		rate = rate[:len(rate)-1]
		trimmed++
	}
	return rate
}
