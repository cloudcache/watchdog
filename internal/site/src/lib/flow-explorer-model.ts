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

export type FlowFilterOperator = "eq" | "ne" | "in" | "not_in" | "gt" | "gte" | "lt" | "lte"

export type FlowFilterExpression = {
	op: "and" | "or" | "not" | "predicate"
	args?: FlowFilterExpression[]
	field?: string
	operator?: FlowFilterOperator
	values?: string[]
}

export type FlowQuickFilterInput = {
	countryCode?: string
	provinceCode?: string
	cityCode?: string
	operatorASNs?: number[]
}

export type FlowTrafficSurface = "overview" | "dimensions" | "source" | "destination" | "overseas"
export type FlowSurface = FlowTrafficSurface | "vpn"
export type FlowSurfaceQueryMode = "direction" | "src_ip" | "dst_ip" | "advanced" | "vpn"

export type FlowSurfacePreset = {
	path: string
	queryMode: FlowSurfaceQueryMode
	dimension: string
	filter: string
	advancedOpen: boolean
}

// These presets define navigation and query defaults only. They deliberately
// do not duplicate the QueryGateway request schema owned by the Flow provider.
export const FLOW_SURFACE_PRESETS: Record<FlowSurface, FlowSurfacePreset> = {
	overview: { path: "/flow", queryMode: "direction", dimension: "category", filter: "", advancedOpen: false },
	dimensions: {
		path: "/flow/dimensions",
		queryMode: "advanced",
		dimension: "category",
		filter: "",
		advancedOpen: true,
	},
	source: { path: "/flow/source", queryMode: "src_ip", dimension: "src_ip", filter: "", advancedOpen: false },
	destination: {
		path: "/flow/destination",
		queryMode: "dst_ip",
		dimension: "dst_ip",
		filter: "",
		advancedOpen: false,
	},
	overseas: {
		path: "/flow/overseas",
		queryMode: "advanced",
		dimension: "geo.country",
		filter: "category = overseas",
		advancedOpen: true,
	},
	vpn: { path: "/flow/vpn", queryMode: "vpn", dimension: "", filter: "", advancedOpen: false },
}

export function flowSurfacePreset(surface: FlowSurface): FlowSurfacePreset {
	return FLOW_SURFACE_PRESETS[surface]
}

const FLOW_FILTER_FIELDS = new Set([
	"direction",
	"category",
	"business",
	"target",
	"device",
	"exporter",
	"src_ip",
	"dst_ip",
	"local_ip",
	"remote_ip",
	"asn",
	"isp",
	"geo.continent",
	"geo.region",
	"geo.country",
	"geo.province",
	"geo.city",
	"local_prefix",
	"remote_prefix",
	"local_port",
	"remote_port",
	"protocol",
	"observation_interface",
])

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

export function resolveOverseasRange(
	startValue: string,
	endValue: string
): { start: string; end: string; bucket: "1m" | "1h" } {
	const start = new Date(startValue)
	const end = new Date(endValue)
	if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || end <= start) {
		throw new Error("Start and end must define a valid overseas range")
	}
	const bucket: "1m" | "1h" = end.getTime() - start.getTime() <= 7 * 24 * 60 * 60_000 ? "1m" : "1h"
	const bucketMilliseconds = bucket === "1m" ? 60_000 : 3_600_000
	const alignedStart = new Date(Math.floor(start.getTime() / bucketMilliseconds) * bucketMilliseconds)
	const alignedEnd = new Date(Math.floor(end.getTime() / bucketMilliseconds) * bucketMilliseconds)
	if (alignedEnd <= alignedStart) throw new Error("Overseas range contains no closed bucket")
	return { start: alignedStart.toISOString(), end: alignedEnd.toISOString(), bucket }
}

export function parseFlowFilter(expression: string): FlowFilterExpression | undefined {
	if (!expression.trim()) return undefined
	return new FlowFilterParser(tokenizeFlowFilter(expression)).parse()
}

export function mergeFlowFilters(base: FlowFilters, extra: FlowFilters): FlowFilters {
	const result: FlowFilters = {}
	for (const key of Object.keys({ ...base, ...extra }) as (keyof FlowFilters)[]) {
		const values = unique([...(base[key] ?? []), ...(extra[key] ?? [])])
		if (values.length > 0) result[key] = values
	}
	return result
}

export function buildFlowQuickFilter(input: FlowQuickFilterInput): FlowFilterExpression | undefined {
	const predicates: FlowFilterExpression[] = []
	for (const [field, value] of [
		["geo.country", input.countryCode],
		["geo.province", input.provinceCode],
		["geo.city", input.cityCode],
	] as const) {
		if (value) predicates.push({ op: "predicate", field, operator: "eq", values: [value] })
	}
	const asns = [...new Set((input.operatorASNs ?? []).filter((asn) => Number.isInteger(asn) && asn > 0))].sort(
		(a, b) => a - b
	)
	if (asns.length > 0) {
		predicates.push({ op: "predicate", field: "asn", operator: "in", values: asns.map(String) })
	}
	if (predicates.length === 0) return undefined
	return predicates.length === 1 ? predicates[0] : { op: "and", args: predicates }
}

export function buildFlowCSV(series: FlowSeries[], unit: string): string {
	const rows: (string | number)[][] = [
		[
			"series",
			"bucket",
			"value",
			"unit",
			"minimum",
			"maximum",
			"last",
			"average",
			"p95",
			"total",
			"received_records",
			"unknown_sampling_records",
			"quality_records",
		],
	]
	for (const item of series) {
		for (const point of item.values) {
			rows.push([
				item.label,
				new Date(point.time).toISOString(),
				point.value,
				unit,
				item.minimum,
				item.maximum,
				item.last,
				item.average,
				item.p95,
				item.total,
				item.receivedRecords,
				item.unknownSamplingRecords,
				item.qualityRecords,
			])
		}
	}
	return rows.map((row) => row.map(csvCell).join(",")).join("\n")
}

export function buildFlowSeries(
	points: FlowPoint[] | null | undefined,
	plan: FlowPlan | undefined,
	unit: string
): FlowSeries[] {
	return buildSeries(
		(points ?? []).map((point) => ({ ...point, path: [point.other ? "Other" : point.dimension_value] })),
		plan,
		unit
	)
}

export function buildFlowJointSeries(
	points: FlowJointPoint[] | null | undefined,
	plan: FlowPlan | undefined,
	unit: string
): FlowSeries[] {
	return buildSeries(
		(points ?? []).map((point) => ({
			...point,
			path: point.dimension_values.map((value) => (point.other ? "Other" : value)),
		})),
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

function csvCell(value: string | number) {
	let text = String(value)
	if (/^[=+\-@]/.test(text)) text = `'${text}`
	return `"${text.replaceAll('"', '""')}"`
}

type FlowFilterToken = { kind: "word" | "string" | "operator" | "left" | "right" | "comma"; value: string }

function tokenizeFlowFilter(input: string): FlowFilterToken[] {
	const tokens: FlowFilterToken[] = []
	for (let index = 0; index < input.length; ) {
		const current = input[index]
		if (/\s/.test(current)) {
			index += 1
			continue
		}
		if (current === "(" || current === ")" || current === ",") {
			tokens.push({ kind: current === "(" ? "left" : current === ")" ? "right" : "comma", value: current })
			index += 1
			continue
		}
		if (current === "=" || current === "!" || current === ">" || current === "<") {
			const pair = input.slice(index, index + 2)
			const value = pair === "!=" || pair === ">=" || pair === "<=" ? pair : current
			if (value === "!") throw new Error(`Invalid filter operator at position ${index + 1}`)
			tokens.push({ kind: "operator", value })
			index += value.length
			continue
		}
		if (current === "'" || current === '"') {
			const quote = current
			let value = ""
			index += 1
			let closed = false
			while (index < input.length) {
				const character = input[index]
				if (character === "\\") {
					index += 1
					if (index >= input.length) break
					value += input[index]
					index += 1
				} else if (character === quote) {
					index += 1
					closed = true
					break
				} else {
					value += character
					index += 1
				}
			}
			if (!closed) throw new Error("Unterminated quoted filter value")
			tokens.push({ kind: "string", value })
			continue
		}
		const start = index
		while (index < input.length && !/[\s(),!<>=]/.test(input[index])) index += 1
		if (start === index) throw new Error(`Invalid filter token at position ${index + 1}`)
		tokens.push({ kind: "word", value: input.slice(start, index) })
	}
	return tokens
}

class FlowFilterParser {
	private index = 0
	private readonly tokens: FlowFilterToken[]

	constructor(tokens: FlowFilterToken[]) {
		this.tokens = tokens
	}

	parse(): FlowFilterExpression {
		const expression = this.parseOr()
		if (this.peek()) throw new Error(`Unexpected filter token: ${this.peek()?.value}`)
		return expression
	}

	private parseOr(): FlowFilterExpression {
		const args = [this.parseAnd()]
		while (this.matchWord("OR")) args.push(this.parseAnd())
		return args.length === 1 ? args[0] : { op: "or", args }
	}

	private parseAnd(): FlowFilterExpression {
		const args = [this.parseUnary()]
		while (this.matchWord("AND")) args.push(this.parseUnary())
		return args.length === 1 ? args[0] : { op: "and", args }
	}

	private parseUnary(): FlowFilterExpression {
		if (this.matchWord("NOT")) return { op: "not", args: [this.parseUnary()] }
		if (this.match("left")) {
			const expression = this.parseOr()
			this.require("right", "Expected ')' in filter")
			return expression
		}
		return this.parsePredicate()
	}

	private parsePredicate(): FlowFilterExpression {
		const fieldToken = this.require("word", "Expected filter field")
		const field = fieldToken.value.toLowerCase()
		if (!FLOW_FILTER_FIELDS.has(field)) throw new Error(`Unsupported filter field: ${fieldToken.value}`)
		let operator: FlowFilterOperator
		const symbol = this.match("operator")
		if (symbol) {
			operator = ({ "=": "eq", "!=": "ne", ">": "gt", ">=": "gte", "<": "lt", "<=": "lte" } as const)[
				symbol.value as "=" | "!=" | ">" | ">=" | "<" | "<="
			]
		} else if (this.matchWord("IN")) {
			operator = "in"
		} else if (this.matchWord("NOT")) {
			if (!this.matchWord("IN")) throw new Error(`Expected IN after NOT for ${field}`)
			operator = "not_in"
		} else {
			throw new Error(`Expected an operator after ${field}`)
		}
		const values: string[] = []
		if (operator === "in" || operator === "not_in") {
			this.require("left", `${operator.toUpperCase()} requires parentheses`)
			values.push(this.parseValue())
			while (this.match("comma")) values.push(this.parseValue())
			this.require("right", "Expected ')' after filter values")
		} else {
			values.push(this.parseValue())
		}
		return { op: "predicate", field, operator, values }
	}

	private parseValue() {
		const token = this.peek()
		if (!token || (token.kind !== "word" && token.kind !== "string")) throw new Error("Expected filter value")
		this.index += 1
		if (!token.value) throw new Error("Filter value cannot be empty")
		return token.value
	}

	private require(kind: FlowFilterToken["kind"], message: string) {
		const token = this.match(kind)
		if (!token) throw new Error(message)
		return token
	}

	private match(kind: FlowFilterToken["kind"]) {
		const token = this.peek()
		if (!token || token.kind !== kind) return undefined
		this.index += 1
		return token
	}

	private matchWord(value: string) {
		const token = this.peek()
		if (!token || token.kind !== "word" || token.value.toUpperCase() !== value) return false
		this.index += 1
		return true
	}

	private peek() {
		return this.tokens[this.index]
	}
}
