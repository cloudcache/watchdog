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
