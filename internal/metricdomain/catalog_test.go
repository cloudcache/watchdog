package metricdomain

import (
	"testing"
	"time"
)

func TestCatalogContainsSNMPTrafficModes(t *testing.T) {
	for _, name := range []string{SNMPIfInBps, SNMPIfOutBps} {
		if !IsKnown(name) {
			t.Fatalf("metric %q is missing", name)
		}
		var modes []ValueMode
		for _, definition := range Catalog {
			if definition.Name == name {
				modes = definition.ValueModes
				break
			}
		}
		if len(modes) != 3 || modes[0] != ValueCorrected || modes[1] != ValueRaw || modes[2] != ValueBoth {
			t.Fatalf("metric %q modes = %v", name, modes)
		}
	}
}

func TestAutoQueryStepRoundsUp(t *testing.T) {
	tests := []struct {
		window time.Duration
		points int
		want   time.Duration
	}{
		{time.Hour, 600, 10 * time.Second},
		{24 * time.Hour, 600, 5 * time.Minute},
		{30 * 24 * time.Hour, 1200, time.Hour},
	}
	for _, test := range tests {
		if got := AutoQueryStep(test.window, test.points); got != test.want {
			t.Fatalf("AutoQueryStep(%s, %d) = %s, want %s", test.window, test.points, got, test.want)
		}
	}
}
