package server

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

func hourlyMarkers(from time.Time, hours int, generation uint64) []flowch.RollupMarker {
	markers := make([]flowch.RollupMarker, 0, hours)
	for hour := range hours {
		markers = append(markers, flowch.RollupMarker{Bucket: from.Add(time.Duration(hour) * time.Hour), Generation: generation})
	}
	return markers
}

func TestFlowCoverageBoundaryReadsAggregatesAcrossOldGaps(t *testing.T) {
	const floor = uint64(1790070054)
	const hot = floor + 1000
	day := func(d, h int) time.Time { return time.Date(2026, 9, d, h, 0, 0, 0, time.UTC) }

	// Production on 2026-09-29: 1h covered 09-22..09-25 07:00, missing through
	// 09-27 00:00 (raw deleted), covered again until the unsealed current hour.
	production := append(hourlyMarkers(day(22, 7), 72, hot), hourlyMarkers(day(27, 1), 53, hot)...)

	cases := []struct {
		name     string
		markers  []flowch.RollupMarker
		from, to time.Time
		want     time.Time
		readable int
		expected int
	}{
		{"fully covered", hourlyMarkers(day(28, 0), 24, hot), day(28, 0), day(29, 0), day(29, 0), 24, 24},
		{"unsealed tail stays raw", hourlyMarkers(day(28, 0), 23, hot), day(28, 0), day(29, 0), day(28, 23), 23, 24},
		{"7-day report across the permanent gap", production, day(22, 7), day(29, 7), day(29, 6), 125, 168},
		{"range starting before retained data", hourlyMarkers(day(27, 1), 29, hot), day(20, 0), day(28, 7), day(28, 6), 29, 199},
		{"recent gap is still filled exactly from raw",
			append(hourlyMarkers(day(28, 0), 20, hot), hourlyMarkers(day(28, 21), 2, hot)...), day(28, 0), day(28, 23), day(28, 20), 22, 23},
		{"no coverage at all", nil, day(28, 0), day(29, 0), day(28, 0), 0, 24},
		{"below-floor markers are gaps; lifecycle generations always count",
			append(hourlyMarkers(day(28, 0), 12, floor-1), hourlyMarkers(day(28, 12), 12, flowch.LifecycleGenerationFloor+1)...),
			day(28, 0), day(29, 0), day(29, 0), 12, 24},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			coverage := summarizeFlowCoverage(tc.markers, tc.from, tc.to, time.Hour, floor)
			if got := coverage.boundary(tc.to); !got.Equal(tc.want) || coverage.readable != tc.readable || coverage.expected != tc.expected {
				t.Fatalf("boundary=%s readable=%d/%d, want %s %d/%d", got, coverage.readable, coverage.expected, tc.want, tc.readable, tc.expected)
			}
		})
	}
}
