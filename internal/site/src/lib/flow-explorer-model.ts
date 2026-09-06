export type FlowPoint = {
	bucket: string
	dimension_value: string
	other: boolean
	value: number
	dimension_snapshot_id: string
	geo_version: string
	classification_version: number
	received_records: number
	unknown_sampling_records: number
	quality_records: number
	generated_at: string
}

export type FlowJointPoint = {
	bucket: string
	dimension_values: string[]
	other: boolean
	value: number
	dimension_snapshot_id: string
	geo_version: string
	classification_version: number
	received_records: number
	unknown_sampling_records: number
	quality_records: number
	observed_at: string
}

export type FlowPlan = {
	requested_from: string
	requested_to: string
	effective_from: string
	effective_to: string
	source: "1m" | "1h" | "flow_records"
	source_seconds?: number
	step_seconds: number
	target_points: number
	max_range_seconds?: number
}

export type FlowSeries = {
	name: string
	label: string
	path: string[]
	values: { time: number; value: number }[]
	minimum: number
	maximum: number
	last: number
	average: number
	p95: number
	total: number
	receivedRecords: number
	unknownSamplingRecords: number
	qualityRecords: number
	sankeyValue: number
}

export type FlowFilters = {
	directions?: string[]
	categories?: string[]
	businesses?: string[]
	target_ids?: string[]
	device_ids?: string[]
	exporter_ids?: string[]
	dimension_values?: string[]
}

export const FLOW_TIME_PRESETS = [
	{ value: "5m", label: "Last 5 minutes", durationMs: 5 * 60_000 },
	{ value: "15m", label: "Last 15 minutes", durationMs: 15 * 60_000 },
	{ value: "30m", label: "Last 30 minutes", durationMs: 30 * 60_000 },
	{ value: "1h", label: "Last hour", durationMs: 60 * 60_000 },
	{ value: "3h", label: "Last 3 hours", durationMs: 3 * 60 * 60_000 },
	{ value: "6h", label: "Last 6 hours", durationMs: 6 * 60 * 60_000 },
	{ value: "12h", label: "Last 12 hours", durationMs: 12 * 60 * 60_000 },
	{ value: "24h", label: "Last 24 hours", durationMs: 24 * 60 * 60_000 },
	{ value: "2d", label: "Last 2 days", durationMs: 2 * 24 * 60 * 60_000 },
	{ value: "7d", label: "Last 7 days", durationMs: 7 * 24 * 60 * 60_000 },
	{ value: "30d", label: "Last 30 days", durationMs: 30 * 24 * 60 * 60_000 },
	{ value: "90d", label: "Last 3 months", durationMs: 90 * 24 * 60 * 60_000 },
	{ value: "180d", label: "Last 6 months", durationMs: 180 * 24 * 60 * 60_000 },
	{ value: "1y", label: "Last year", durationMs: 365 * 24 * 60 * 60_000 },
	{ value: "custom", label: "Custom" },
] as const

export function resolveFlowTimeRange(
	preset: string,
	customStart: string,
	customEnd: string,
	now = new Date()
): { start: string; end: string } {
	const selected = FLOW_TIME_PRESETS.find((item) => item.value === preset)
	if (!selected) throw new Error("Unknown time range")
	if (selected.value === "custom") {
		const start = floorMinute(new Date(customStart))
		const end = floorMinute(new Date(customEnd))
		if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || end <= start) {
			throw new Error("Start and end must define a valid time range")
		}
		return { start: start.toISOString(), end: end.toISOString() }
	}
	const end = floorMinute(now)
	return { start: new Date(end.getTime() - selected.durationMs).toISOString(), end: end.toISOString() }
}

export function parseFlowFilter(expression: string): FlowFilters {
	const result: FlowFilters = {}
	if (!expression.trim()) return result
	const fieldMap: Record<string, keyof FlowFilters> = {
		direction: "directions",
		category: "categories",
		business: "businesses",
		target: "target_ids",
		device: "device_ids",
		exporter: "exporter_ids",
		dimension: "dimension_values",
	}
	for (const clause of splitOutside(expression, "AND")) {
		const match = clause.match(/^([a-z_]+)\s*(=|IN)\s*(.+)$/i)
		if (!match) throw new Error(`Invalid filter clause: ${clause}`)
		const key = fieldMap[match[1].toLowerCase()]
		if (!key) throw new Error(`Unsupported filter field: ${match[1]}`)
		const operation = match[2].toUpperCase()
		let source = match[3].trim()
		if (operation === "IN") {
			if (!source.startsWith("(") || !source.endsWith(")")) throw new Error(`IN requires parentheses: ${clause}`)
			source = source.slice(1, -1)
		}
		const values = operation === "IN" ? splitOutside(source, ",") : [source]
		const normalized = values.map(unquote).filter(Boolean)
		if (normalized.length === 0) throw new Error(`Filter has no values: ${clause}`)
		result[key] = unique([...(result[key] ?? []), ...normalized])
	}
	return result
}

export function mergeFlowFilters(base: FlowFilters, extra: FlowFilters): FlowFilters {
	const result: FlowFilters = {}
	for (const key of Object.keys({ ...base, ...extra }) as (keyof FlowFilters)[]) {
		const values = unique([...(base[key] ?? []), ...(extra[key] ?? [])])
		if (values.length > 0) result[key] = values
	}
	return result
}

export function buildFlowSeries(points: FlowPoint[], plan: FlowPlan | undefined, unit: string): FlowSeries[] {
	return buildSeries(
		points.map((point) => ({ ...point, path: [point.other ? "Other" : point.dimension_value] })),
		plan,
		unit
	)
}

export function buildFlowJointSeries(points: FlowJointPoint[], plan: FlowPlan | undefined, unit: string): FlowSeries[] {
	return buildSeries(
		points.map((point) => ({ ...point, path: point.dimension_values.map((value) => (point.other ? "Other" : value)) })),
		plan,
		unit
	)
}

type SeriesPoint = Pick<
	FlowPoint,
	| "bucket"
	| "other"
	| "value"
	| "dimension_snapshot_id"
	| "geo_version"
	| "classification_version"
	| "received_records"
	| "unknown_sampling_records"
	| "quality_records"
> & { path: string[] }

function buildSeries(points: SeriesPoint[], plan: FlowPlan | undefined, unit: string): FlowSeries[] {
	const grouped = new Map<string, { path: string[]; version: string; rows: SeriesPoint[] }>()
	for (const point of points) {
		const version = `${point.dimension_snapshot_id}/${point.geo_version}/${point.classification_version}`
		const key = JSON.stringify([point.path, version])
		const current = grouped.get(key) ?? { path: point.path, version, rows: [] }
		current.rows.push(point)
		grouped.set(key, current)
	}
	return [...grouped.values()].map(({ path, version, rows }) => {
		const sorted = rows.slice().sort((a, b) => Date.parse(a.bucket) - Date.parse(b.bucket))
		const plottedValues = fillSeriesValues(sorted, plan)
		const values = plottedValues.map((point) => point.value)
		const total = sorted.reduce((sum, row) => {
			if (unit !== "bits_per_second" && unit !== "packets_per_second") return sum + row.value
			const start = Date.parse(row.bucket)
			const effectiveEnd = Date.parse(plan?.effective_to ?? row.bucket)
			const seconds = Math.max(0, Math.min(plan?.step_seconds ?? 0, (effectiveEnd - start) / 1000))
			return sum + (row.value * seconds) / (unit === "bits_per_second" ? 8 : 1)
		}, 0)
		const parsedRangeSeconds = (Date.parse(plan?.effective_to ?? "") - Date.parse(plan?.effective_from ?? "")) / 1000
		const rangeSeconds =
			Number.isFinite(parsedRangeSeconds) && parsedRangeSeconds > 0 ? parsedRangeSeconds : Math.max(1, values.length)
		const average =
			unit === "bits_per_second"
				? (total * 8) / rangeSeconds
				: unit === "packets_per_second"
					? total / rangeSeconds
					: values.reduce((sum, value) => sum + value, 0) / Math.max(1, values.length)
		const label = path.join(" → ")
		return {
			name: `${label} [${version}]`,
			label,
			path,
			values: plottedValues,
			minimum: Math.min(...values),
			maximum: Math.max(...values),
			last: values[values.length - 1] ?? 0,
			average,
			p95: percentile(values, 0.95),
			total,
			receivedRecords: sorted.reduce((sum, row) => sum + row.received_records, 0),
			unknownSamplingRecords: sorted.reduce((sum, row) => sum + row.unknown_sampling_records, 0),
			qualityRecords: sorted.reduce((sum, row) => sum + row.quality_records, 0),
			sankeyValue: unit === "bits_per_second" || unit === "packets_per_second" ? average : total,
		}
	})
}

function fillSeriesValues(rows: SeriesPoint[], plan: FlowPlan | undefined) {
	const existing = new Map(rows.map((row) => [Date.parse(row.bucket), row.value]))
	const from = Date.parse(plan?.effective_from ?? "")
	const to = Date.parse(plan?.effective_to ?? "")
	const step = (plan?.step_seconds ?? 0) * 1000
	if (!Number.isFinite(from) || !Number.isFinite(to) || step <= 0 || to <= from) {
		return rows.map((row) => ({ time: Date.parse(row.bucket), value: row.value }))
	}
	const result: { time: number; value: number }[] = []
	for (let time = from; time < to; time += step) result.push({ time, value: existing.get(time) ?? 0 })
	return result
}

function floorMinute(value: Date) {
	return new Date(Math.floor(value.getTime() / 60_000) * 60_000)
}

function percentile(values: number[], ratio: number) {
	if (values.length === 0) return 0
	const sorted = values.slice().sort((a, b) => a - b)
	return sorted[Math.max(0, Math.ceil(sorted.length * ratio) - 1)]
}

function unique(values: string[]) {
	return [...new Set(values)]
}

function unquote(value: string) {
	const trimmed = value.trim()
	if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
		return trimmed.slice(1, -1).replace(/\\([\\'"])/g, "$1")
	}
	return trimmed
}

function splitOutside(value: string, separator: string) {
	const result: string[] = []
	let quote = ""
	let depth = 0
	let start = 0
	for (let index = 0; index < value.length; index += 1) {
		const current = value[index]
		if (quote) {
			if (current === "\\") index += 1
			else if (current === quote) quote = ""
			continue
		}
		if (current === "'" || current === '"') {
			quote = current
			continue
		}
		if (current === "(") depth += 1
		else if (current === ")") depth -= 1
		if (depth < 0) throw new Error("Unbalanced filter parentheses")
		const candidate = value.slice(index, index + separator.length)
		const wordBoundary =
			separator !== "AND" || (/\s/.test(value[index - 1] ?? " ") && /\s/.test(value[index + separator.length] ?? " "))
		if (depth === 0 && candidate.toUpperCase() === separator && wordBoundary) {
			result.push(value.slice(start, index).trim())
			start = index + separator.length
			index += separator.length - 1
		}
	}
	if (quote || depth !== 0) throw new Error("Unterminated quote or parentheses in filter")
	result.push(value.slice(start).trim())
	return result.filter(Boolean)
}
