export type TrafficViewMode = "raw" | "customer" | "supplier"

export const trafficViewModes: TrafficViewMode[] = ["customer", "supplier", "raw"]

export function trafficViewLabel(mode: TrafficViewMode) {
	switch (mode) {
		case "raw":
			return "Raw"
		case "customer":
			return "Customer"
		case "supplier":
			return "Supplier"
	}
}

export function trafficViewValueMode(mode: TrafficViewMode, canUseRaw = true) {
	switch (mode) {
		case "raw":
			return canUseRaw ? "raw" : "corrected"
		case "supplier":
		case "customer":
			return "corrected"
	}
}

export function trafficViewAggregate(mode: TrafficViewMode) {
	switch (mode) {
		case "raw":
		case "customer":
		case "supplier":
			return "sum"
	}
}

export function trafficViewQueryStep(mode: TrafficViewMode) {
	return mode === "supplier" ? "300" : "60"
}

export function trafficViewRateBase(mode: TrafficViewMode) {
	return mode === "supplier" ? 1024 : 1000
}

export function trafficViewExportAggregation(mode: TrafficViewMode) {
	switch (mode) {
		case "raw":
			return "avg_5m"
		case "customer":
		case "supplier":
			return "p95_5m"
	}
}

export function trafficViewExportStep(mode: TrafficViewMode) {
	return mode === "supplier" ? "300000000000" : "60000000000"
}

export function trafficViewFromValue(valueMode?: string, aggregation?: string): TrafficViewMode {
	const value = valueMode?.toLowerCase()
	if (value === "raw") {
		return aggregation === "avg_5m" ? "raw" : "supplier"
	}
	if (value === "both") {
		return "supplier"
	}
	return "customer"
}
