const rateUnits = ["bps", "Kbps", "Mbps", "Gbps", "Tbps", "Pbps"]
const binaryRateUnits = ["bps", "Kibps", "Mibps", "Gibps", "Tibps", "Pibps"]

export function formatBitsPerSecond(value?: number | null, base = 1000) {
	if (typeof value !== "number" || !Number.isFinite(value)) {
		return "—"
	}
	const units = base === 1024 ? binaryRateUnits : rateUnits
	const sign = value < 0 ? "-" : ""
	let current = Math.abs(value)
	let unitIndex = 0
	while (current >= base && unitIndex < units.length - 1) {
		current /= base
		unitIndex += 1
	}
	if (unitIndex === 0) {
		return `${sign}${current.toFixed(0)} ${units[unitIndex]}`
	}
	return `${sign}${current.toFixed(2)} ${units[unitIndex]}`
}

export function formatMetricValue(value?: number | null, unit?: string) {
	if (unit === "bps") {
		return formatBitsPerSecond(value)
	}
	if (typeof value !== "number" || !Number.isFinite(value)) {
		return "—"
	}
	if (unit === "percent") {
		return `${value.toFixed(1)}%`
	}
	return Number.isInteger(value) ? String(value) : value.toFixed(2)
}
