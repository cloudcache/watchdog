export type VPNRuleMatchInput = {
	remote_ports?: number[]
	protocols?: number[]
	remote_asns?: number[]
	remote_prefix_ids?: string[]
	remote_countries?: string[]
	transport_hints?: string[]
	min_duration_ms?: number
	min_total_bytes?: number
	min_flow_records?: number
	min_active_buckets?: number
	min_symmetry_ratio?: number
	min_dominance_ratio?: number
}

export function parsePositiveIntegers(value: string, maximum: number) {
	if (!value.trim()) return undefined
	const items = [...new Set(value.split(",").map((item) => Number(item.trim())))].sort((a, b) => a - b)
	if (items.some((item) => !Number.isInteger(item) || item < 1 || item > maximum)) {
		throw new Error(`Values must be integers between 1 and ${maximum}`)
	}
	return items
}

export function parseIdentifiers(value: string, transform: (value: string) => string = (item) => item) {
	if (!value.trim()) return undefined
	const items = [
		...new Set(
			value
				.split(",")
				.map((item) => transform(item.trim()))
				.filter(Boolean)
		),
	].sort()
	return items.length ? items : undefined
}

export function optionalPositive(value: string) {
	if (!value.trim()) return undefined
	const parsed = Number(value)
	if (!Number.isFinite(parsed) || parsed <= 0) throw new Error("Value must be positive")
	return parsed
}

export function optionalRatio(value: string) {
	if (!value.trim()) return undefined
	const parsed = Number(value)
	if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) throw new Error("Ratio must be between 0 and 1")
	return parsed
}
