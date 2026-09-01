package watchdog

import "time"

type TrafficViewMode string

const (
	TrafficViewRaw      TrafficViewMode = "raw"
	TrafficViewCustomer TrafficViewMode = "customer"
	TrafficViewSupplier TrafficViewMode = "supplier"
)

func normalizeTrafficView(value TrafficViewMode) TrafficViewMode {
	switch value {
	case TrafficViewRaw, TrafficViewCustomer, TrafficViewSupplier:
		return value
	default:
		return ""
	}
}

func trafficViewQueryStep(view TrafficViewMode) time.Duration {
	switch normalizeTrafficView(view) {
	case TrafficViewCustomer, TrafficViewRaw:
		return time.Minute
	case TrafficViewSupplier:
		return 5 * time.Minute
	default:
		return 0
	}
}

func trafficViewBucket(view TrafficViewMode) time.Duration {
	if normalizeTrafficView(view) == TrafficViewCustomer {
		return 5 * time.Minute
	}
	return 0
}
