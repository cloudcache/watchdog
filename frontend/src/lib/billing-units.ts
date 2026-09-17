export const BPS_PER_MBPS = 1_000_000
export const BYTES_PER_GB = 1_000_000_000

export function formatBillingUnit(value: number | undefined, divisor: number): string {
	if (value === undefined) return ""
	return String(value / divisor)
}

export function parseBillingUnit(value: string, multiplier: number): number | undefined {
	const normalized = value.trim()
	if (!/^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/.test(normalized)) return undefined
	const result = Number(normalized) * multiplier
	if (!Number.isFinite(result) || result < 0 || !Number.isSafeInteger(result)) return undefined
	return result
}

export function reconciliationUnit(algorithm: "95th" | "daily_95th" | "average" | "total") {
	return algorithm === "total" ? { label: "GB", multiplier: BYTES_PER_GB } : { label: "Mbps", multiplier: BPS_PER_MBPS }
}
