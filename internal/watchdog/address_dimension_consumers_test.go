package watchdog

import "testing"

func TestAddressDimensionObservedQueryabilityIsConservative(t *testing.T) {
	for _, test := range []struct {
		name     string
		observed uint64
		ready    uint64
		want     string
	}{
		{name: "no observed workers", want: AddressDimensionQueryabilityUnknown},
		{name: "all observed ready", observed: 2, ready: 2, want: AddressDimensionQueryabilityReady},
		{name: "some observed ready", observed: 3, ready: 1, want: AddressDimensionQueryabilityPartial},
		{name: "none observed ready", observed: 2, want: AddressDimensionQueryabilityUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := addressDimensionObservedQueryability(test.observed, test.ready); got != test.want {
				t.Fatalf("queryability = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAddressDimensionConsumerFiltersAreClosedEnums(t *testing.T) {
	for _, valid := range []string{"", AddressDimensionConsumerReady, AddressDimensionConsumerDownloaded, AddressDimensionConsumerFailed, AddressDimensionConsumerUnreported} {
		if !validAddressDimensionConsumerStateFilter(valid) {
			t.Fatalf("valid state %q rejected", valid)
		}
	}
	if validAddressDimensionConsumerStateFilter("installed") {
		t.Fatal("storage ACK state leaked into API readiness filter")
	}
	for _, valid := range []string{"", AddressDimensionDriftCurrent, AddressDimensionDriftBehind, AddressDimensionDriftAhead, AddressDimensionDriftUninstalled} {
		if !validAddressDimensionDriftFilter(valid) {
			t.Fatalf("valid drift %q rejected", valid)
		}
	}
	if validAddressDimensionDriftFilter("stale") {
		t.Fatal("unknown drift filter accepted")
	}
}
