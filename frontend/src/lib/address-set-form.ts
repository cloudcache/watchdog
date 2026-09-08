export function parseAddressEntries(value: string): string[] {
	return [
		...new Set(
			value
				.split(/[\n,]/)
				.map((item) => item.trim())
				.filter(Boolean)
		),
	]
}

export function parsePrefixLabels(value: string): Record<string, string> {
	const parsed = parseLabelAssignments(value, false)
	return Object.fromEntries(Object.entries(parsed).map(([key, values]) => [key, values[0]]))
}

export function parseSetLabelSelector(value: string): Record<string, string[]> {
	return parseLabelAssignments(value, true)
}

export function parseUnsignedIntegerEntries(value: string, minimum: number, maximum: number, label: string): number[] {
	return parseAddressEntries(value).map((raw) => {
		const parsed = Number(raw)
		if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
			throw new Error(`${label} must be an integer between ${minimum} and ${maximum}: ${raw}`)
		}
		return parsed
	})
}

export function formatSetLabelSelector(value: unknown): string {
	if (!isRecord(value)) return ""
	return Object.entries(value)
		.flatMap(([key, rawValues]) => {
			const values = toStringList(rawValues)
			return values.length ? [`${key}=${values.join("|")}`] : []
		})
		.join(",")
}

export function formatAddressSetSelector(value: unknown): string {
	if (!isRecord(value)) return "—"
	const parts: string[] = []
	const labels = formatSetLabelSelector(value.labels)
	if (labels) parts.push(labels)
	appendSelectorPart(parts, "geo", value.geo_node_ids)
	appendSelectorPart(parts, "operator", value.operator_ids)
	appendSelectorPart(parts, "asn", value.asns)
	const families = toScalarList(value.families)
	if (families.length) parts.push(`IPv${families.join("|IPv")}`)
	return parts.join(", ") || "—"
}

function appendSelectorPart(parts: string[], label: string, value: unknown) {
	const values = toScalarList(value)
	if (values.length) parts.push(`${label}:${values.join("|")}`)
}

function toStringList(value: unknown): string[] {
	if (typeof value === "string") return value ? [value] : []
	if (!Array.isArray(value)) return []
	return value.filter((item): item is string => typeof item === "string" && item.length > 0)
}

function toScalarList(value: unknown): Array<string | number> {
	if (!Array.isArray(value)) return []
	return value.filter(
		(item): item is string | number =>
			(typeof item === "string" && item.length > 0) || (typeof item === "number" && Number.isFinite(item))
	)
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value)
}

function parseLabelAssignments(value: string, allowMany: boolean): Record<string, string[]> {
	const labels: Record<string, string[]> = {}
	for (const rawPair of value.split(",")) {
		const pair = rawPair.trim()
		if (!pair) continue
		const separator = pair.indexOf("=")
		if (separator <= 0 || separator === pair.length - 1) {
			throw new Error(`Invalid label assignment: ${pair}`)
		}
		const key = pair.slice(0, separator).trim()
		if (!key || labels[key]) {
			throw new Error(`Duplicate or empty label key: ${key || pair}`)
		}
		const values = [
			...new Set(
				pair
					.slice(separator + 1)
					.split(allowMany ? "|" : "\u0000")
					.map((item) => item.trim())
			),
		]
		if (values.some((item) => !item)) {
			throw new Error(`Empty value for label: ${key}`)
		}
		labels[key] = values
	}
	return labels
}
