import type { FlowDimensionLabel, FlowJointPoint, FlowPoint } from "@/lib/flow-explorer-model"

export type FlowTablePage = {
	items: Array<{
		name: string
		label: string
		path: string[]
		last: number
		average: number
		p95: number
		maximum: number
		minimum: number
		total: number
		received_records: number
		unknown_sampling_ratio: number
		quality_record_ratio: number
		inbound?: FlowEndpointDirectionSummary
		outbound?: FlowEndpointDirectionSummary
		categories?: Record<string, FlowEndpointCategorySummary>
		residual?: FlowEndpointCategorySummary
		businesses?: string[]
	}>
	total: number
	limit: number
	offset: number
	filter_options: Record<string, Array<{ value: string; label?: string; count: number }>>
}

export type FlowEndpointDirectionSummary = {
	last: number
	average: number
	p95: number
	maximum: number
	total: number
}

export type FlowEndpointCategorySummary = {
	inbound: number
	outbound: number
	inbound_share?: number
	outbound_share?: number
}

export type FlowReportKind = "overview" | "dimensions" | "endpoints" | "overseas" | "vpn"
export type FlowReportSurface = "overview" | "dimensions" | "source" | "destination" | "overseas" | "vpn"
export type FlowReportDisplayMode = "value" | "share" | "difference"

export type FlowReportPanelData = {
	points?: (FlowPoint | FlowJointPoint)[] | null
	metric?: { name: string; unit: string }
	dimension?: { kind: string; additive: boolean }
	dimensions?: { kind: string; additive: boolean }[]
	table?: FlowTablePage
	matrix_table?: FlowBusinessMatrixPage
	dimension_labels?: Record<string, FlowDimensionLabel>
	[key: string]: unknown
}

export type FlowDirectionalValue = {
	inbound: number
	outbound: number
	inbound_share?: number
	outbound_share?: number
}

export type FlowBusinessMatrixPage = {
	items: Array<{
		business: string
		total: FlowDirectionalValue
		categories: Record<string, FlowDirectionalValue>
		residual: FlowDirectionalValue
	}>
	total: number
	limit: number
	offset: number
	filter_options: Record<string, Array<{ value: string; label?: string; count: number }>>
}

export type FlowDistributionPage = {
	items: Array<{ value: string; count: number; bytes: number }>
	total: number
	limit: number
	offset: number
	filter_options: Record<string, Array<{ value: string; label?: string; count: number }>>
}

export type FlowReportPanel = {
	id: string
	status: "ready" | "unavailable"
	reason?: string
	data?: FlowReportPanelData
	meta: {
		unit?: string
		timezone?: string
		step_seconds?: number
		as_of?: string
		versions?: Record<string, string>
		completeness?: {
			complete_ratio: number
			late_ratio: number
			unknown_ratio: number
			partial: boolean
			warnings?: string[]
		}
	}
}

export type FlowReportResponse = {
	data: {
		schema_version: 1
		kind: FlowReportKind
		side?: "source" | "destination"
		display_mode: FlowReportDisplayMode
		range: {
			requested_from: string
			requested_to: string
			effective_from: string
			effective_to: string
			timezone: string
		}
		plan: { source: string; source_seconds: number; display_seconds: number }
		watermark: { latest_complete_bucket?: string; generated_at: string }
		versions?: Record<string, string>
		completeness: {
			complete_ratio: number
			late_ratio: number
			unknown_ratio: number
			partial: boolean
			warnings?: string[]
		}
		panels: FlowReportPanel[]
		warnings?: string[]
	}
	meta: {
		request_id: string
		as_of: string
		unit?: string
		timezone?: string
		step_seconds?: number
		versions?: Record<string, string>
		complete_ratio: number
		unknown_ratio: number
		partial: boolean
		warnings?: string[]
	}
}

export type FlowReportSeries = {
	name: string
	values: Array<{ time: number; value: number }>
}

export const FLOW_REPORT_CATEGORIES = [
	"on_net_local_city",
	"on_net_cross_city",
	"on_net_cross_province",
	"off_net_in_province",
	"off_net_cross_province",
	"overseas",
] as const

export const FLOW_REPORT_RESIDUALS = ["unknown", "internal", "transit", "ambiguous"] as const

export const FLOW_REPORT_CATEGORY_LABELS: Record<string, string> = {
	on_net_local_city: "On-net · local city",
	on_net_cross_city: "On-net · cross-city",
	on_net_cross_province: "On-net · other province",
	off_net_in_province: "Off-net · same province",
	off_net_cross_province: "Off-net · other province",
	overseas: "Overseas",
	unknown: "Unknown",
	internal: "Internal",
	transit: "Transit",
	ambiguous: "Ambiguous",
}

export const FLOW_REPORT_TIME_PRESETS = [
	{ value: "5m", label: "Last 5 minutes" },
	{ value: "15m", label: "Last 15 minutes" },
	{ value: "30m", label: "Last 30 minutes" },
	{ value: "1h", label: "Last hour" },
	{ value: "3h", label: "Last 3 hours" },
	{ value: "6h", label: "Last 6 hours" },
	{ value: "12h", label: "Last 12 hours" },
	{ value: "24h", label: "Last 24 hours" },
	{ value: "2d", label: "Last 2 days" },
	{ value: "today", label: "Today" },
	{ value: "this_week", label: "This week" },
	{ value: "7d", label: "Last 7 days" },
	{ value: "30d", label: "Last 30 days" },
	{ value: "3mo", label: "Last 3 months" },
	{ value: "6mo", label: "Last 6 months" },
	{ value: "1y", label: "Last year" },
	{ value: "this_month", label: "This month" },
	{ value: "previous_month", label: "Previous month" },
	{ value: "custom", label: "Custom" },
] as const

export function flowReportKind(surface: FlowReportSurface): FlowReportKind {
	if (surface === "source" || surface === "destination") return "endpoints"
	return surface
}

export function resolveFlowReportRange(
	preset: string,
	customStart: string,
	customEnd: string,
	timezone: string,
	now = new Date()
): { start: string; end: string } {
	if (preset === "custom") {
		const start = floorMinute(new Date(customStart))
		const end = floorMinute(new Date(customEnd))
		if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || end <= start) {
			throw new Error("Start and end must define a valid time range")
		}
		return { start: start.toISOString(), end: end.toISOString() }
	}
	const end = floorMinute(now)
	const durations: Record<string, number> = {
		"5m": 5 * 60_000,
		"15m": 15 * 60_000,
		"30m": 30 * 60_000,
		"1h": 60 * 60_000,
		"3h": 3 * 60 * 60_000,
		"6h": 6 * 60 * 60_000,
		"12h": 12 * 60 * 60_000,
		"24h": 24 * 60 * 60_000,
		"2d": 2 * 24 * 60 * 60_000,
		"7d": 7 * 24 * 60 * 60_000,
		"30d": 30 * 24 * 60 * 60_000,
		"3mo": 90 * 24 * 60 * 60_000,
		"6mo": 180 * 24 * 60 * 60_000,
		"1y": 365 * 24 * 60 * 60_000,
	}
	if (durations[preset]) {
		return { start: new Date(end.getTime() - durations[preset]).toISOString(), end: end.toISOString() }
	}
	if (preset === "today") {
		return { start: zonedBoundary(end, timezone, 0).toISOString(), end: end.toISOString() }
	}
	if (preset === "this_week") {
		const parts = zonedParts(end, timezone)
		const localNoon = new Date(Date.UTC(parts.year, parts.month - 1, parts.day, 12))
		const isoDay = localNoon.getUTCDay() || 7
		return { start: zonedBoundary(end, timezone, 1 - isoDay).toISOString(), end: end.toISOString() }
	}
	if (preset === "this_month") {
		const parts = zonedParts(end, timezone)
		return { start: zonedDate(timezone, parts.year, parts.month, 1).toISOString(), end: end.toISOString() }
	}
	if (preset === "previous_month") {
		const parts = zonedParts(end, timezone)
		const monthStart = zonedDate(timezone, parts.year, parts.month, 1)
		const previousEnd = monthStart
		const previous = new Date(Date.UTC(parts.year, parts.month - 2, 1))
		const previousStart = zonedDate(timezone, previous.getUTCFullYear(), previous.getUTCMonth() + 1, 1)
		return { start: previousStart.toISOString(), end: previousEnd.toISOString() }
	}
	throw new Error("Unknown time range")
}

export function reportPanel(response: FlowReportResponse | null, id: string): FlowReportPanel | undefined {
	return response?.data.panels.find((panel) => panel.id === id)
}

export function panelPoints(panel?: FlowReportPanel): FlowPoint[] {
	if (!panel?.data?.points) return []
	return panel.data.points.filter((point): point is FlowPoint => "dimension_value" in point)
}

export function panelJointPoints(panel?: FlowReportPanel): FlowJointPoint[] {
	if (!panel?.data?.points) return []
	return panel.data.points.filter((point): point is FlowJointPoint => "dimension_values" in point)
}

export function buildReportSeries(panel?: FlowReportPanel): FlowReportSeries[] {
	const grouped = new Map<string, Array<{ time: number; value: number }>>()
	for (const point of panelPoints(panel)) {
		const values = grouped.get(point.dimension_value) ?? []
		values.push({ time: new Date(point.bucket).getTime(), value: point.value })
		grouped.set(point.dimension_value, values)
	}
	return [...grouped.entries()]
		.map(([name, values]) => ({ name, values: values.sort((a, b) => a.time - b.time) }))
		.sort((a, b) => seriesTotal(b) - seriesTotal(a) || a.name.localeCompare(b.name))
}

export function reportSeriesStats(series?: FlowReportSeries) {
	const values = (series?.values ?? []).map((point) => point.value).sort((a, b) => a - b)
	if (values.length === 0) return { current: 0, average: 0, p95: 0, maximum: 0, total: 0 }
	const source = series?.values ?? []
	const current = source[source.length - 1]?.value ?? 0
	const total = values.reduce((sum, value) => sum + value, 0)
	return {
		current,
		average: total / values.length,
		p95: percentile(values, 0.95),
		maximum: values[values.length - 1],
		total,
	}
}

export function transformReportSeries(
	inbound: FlowReportSeries[],
	outbound: FlowReportSeries[],
	mode: FlowReportDisplayMode
): { inbound: FlowReportSeries[]; outbound: FlowReportSeries[]; difference: FlowReportSeries[] } {
	if (mode === "value") return { inbound, outbound, difference: [] }
	if (mode === "share") {
		return { inbound: shareSeries(inbound), outbound: shareSeries(outbound), difference: [] }
	}
	const names = new Set([...inbound.map((series) => series.name), ...outbound.map((series) => series.name)])
	const inMap = seriesPointMap(inbound)
	const outMap = seriesPointMap(outbound)
	const difference: FlowReportSeries[] = []
	for (const name of names) {
		const times = new Set([...(inMap.get(name)?.keys() ?? []), ...(outMap.get(name)?.keys() ?? [])])
		difference.push({
			name,
			values: [...times]
				.sort((a, b) => a - b)
				.map((time) => ({ time, value: (inMap.get(name)?.get(time) ?? 0) - (outMap.get(name)?.get(time) ?? 0) })),
		})
	}
	return { inbound: [], outbound: [], difference: difference.sort((a, b) => seriesTotal(b) - seriesTotal(a)) }
}

export function reportCategoryTotals(panel?: FlowReportPanel): Map<string, number> {
	const totals = new Map<string, number>()
	for (const point of panelPoints(panel)) {
		totals.set(point.dimension_value, (totals.get(point.dimension_value) ?? 0) + point.value)
	}
	return totals
}

export function reportDirectionTotals(panel?: FlowReportPanel): { inbound: number; outbound: number } {
	const totals = reportCategoryTotals(panel)
	return { inbound: totals.get("Inbound") ?? 0, outbound: totals.get("Outbound") ?? 0 }
}

function shareSeries(series: FlowReportSeries[]): FlowReportSeries[] {
	const totals = new Map<number, number>()
	for (const item of series) {
		for (const point of item.values) totals.set(point.time, (totals.get(point.time) ?? 0) + point.value)
	}
	return series.map((item) => ({
		...item,
		values: item.values.flatMap((point) => {
			const total = totals.get(point.time) ?? 0
			return total > 0 ? [{ ...point, value: point.value / total }] : []
		}),
	}))
}

function seriesPointMap(series: FlowReportSeries[]) {
	return new Map(series.map((item) => [item.name, new Map(item.values.map((point) => [point.time, point.value]))]))
}

function seriesTotal(series: FlowReportSeries) {
	return series.values.reduce((sum, point) => sum + point.value, 0)
}

function percentile(values: number[], ratio: number) {
	if (values.length === 0) return 0
	const rank = Math.max(0, Math.ceil(values.length * ratio) - 1)
	return values[Math.min(values.length - 1, rank)]
}

function floorMinute(value: Date) {
	return new Date(Math.floor(value.getTime() / 60_000) * 60_000)
}

function zonedBoundary(now: Date, timezone: string, dayDelta: number) {
	const parts = zonedParts(now, timezone)
	const date = new Date(Date.UTC(parts.year, parts.month - 1, parts.day + dayDelta))
	return zonedDate(timezone, date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate())
}

function zonedParts(value: Date, timezone: string) {
	const parts = new Intl.DateTimeFormat("en-CA", {
		timeZone: timezone,
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hourCycle: "h23",
	}).formatToParts(value)
	const get = (type: string) => Number(parts.find((part) => part.type === type)?.value)
	return {
		year: get("year"),
		month: get("month"),
		day: get("day"),
		hour: get("hour"),
		minute: get("minute"),
		second: get("second"),
	}
}

function zonedDate(timezone: string, year: number, month: number, day: number) {
	let candidate = new Date(Date.UTC(year, month - 1, day))
	for (let index = 0; index < 3; index++) {
		const parts = zonedParts(candidate, timezone)
		const represented = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second)
		const wanted = Date.UTC(year, month - 1, day)
		candidate = new Date(candidate.getTime() + wanted - represented)
	}
	return candidate
}
