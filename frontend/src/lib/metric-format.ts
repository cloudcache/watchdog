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

export function formatGigabitsPerSecond(value?: number | null, base = 1000) {
	if (typeof value !== "number" || !Number.isFinite(value)) {
		return "—"
	}
	const unit = base === 1024 ? "Gibps" : "Gbps"
	return `${(value / base ** 3).toFixed(2)} ${unit}`
}

export function formatMetricValue(value?: number | null, unit?: string) {
	if (unit === "bps" || unit === "bits_per_second") {
		return formatBitsPerSecond(value)
	}
	if (typeof value !== "number" || !Number.isFinite(value)) {
		return "—"
	}
	if (unit === "packets_per_second") {
		return formatSI(value, ["pps", "Kpps", "Mpps", "Gpps", "Tpps"])
	}
	if (unit === "bytes") {
		return formatSI(value, ["B", "KB", "MB", "GB", "TB", "PB"])
	}
	if (unit === "packets") {
		return formatSI(value, ["packets", "K packets", "M packets", "G packets", "T packets"])
	}
	if (unit === "records") {
		return formatSI(value, ["records", "K records", "M records", "G records", "T records"])
	}
	if (unit === "percent") {
		return `${value.toFixed(1)}%`
	}
	return Number.isInteger(value) ? String(value) : value.toFixed(2)
}

function formatSI(value: number, units: string[]) {
	const sign = value < 0 ? "-" : ""
	let current = Math.abs(value)
	let unitIndex = 0
	while (current >= 1000 && unitIndex < units.length - 1) {
		current /= 1000
		unitIndex += 1
	}
	const digits = unitIndex === 0 ? 0 : current >= 100 ? 0 : current >= 10 ? 1 : 2
	return `${sign}${current.toFixed(digits)} ${units[unitIndex]}`
}
