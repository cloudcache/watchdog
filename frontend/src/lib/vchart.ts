import VChart, { type ILineChartSpec } from "@visactor/vchart"
import { formatMetricValue } from "@/lib/metric-format"

export type LineSeries = {
	name: string
	unit?: string
	values: { time: number; value: number | null }[]
}

export type LineChartOptions = {
	series: LineSeries[]
	yFormatter?: (value: number | null) => string
	colors?: string[]
	absoluteValues?: boolean
}

export const TRAFFIC_DIRECTION_COLORS = ["#2563eb", "#16a34a"]
const colors = [...TRAFFIC_DIRECTION_COLORS, "#f59e0b", "#dc2626", "#7c3aed", "#0891b2"]

export function createLineChart(dom: HTMLElement, options: LineChartOptions): VChart {
	const spec = createLineChartSpec(options)
	const chart = new VChart(spec, { dom })
	chart.renderSync()
	return chart
}

export function updateLineChart(chart: VChart | null, dom: HTMLElement, options: LineChartOptions): VChart {
	const spec = createLineChartSpec(options)
	if (!chart) {
		return createLineChart(dom, options)
	}
	const updater = chart as VChart & { updateSpec?: (spec: unknown) => void; renderSync?: () => void }
	if (typeof updater.updateSpec === "function") {
		updater.updateSpec(spec)
		updater.renderSync?.()
		return chart
	}
	disposeChart(chart)
	return createLineChart(dom, options)
}

function createLineChartSpec(options: LineChartOptions): ILineChartSpec {
	const values = options.series.flatMap((series) =>
		insertGapBreaks(series.values).map((point) => ({
			time: point.time,
			value: point.value,
			valueLabel: formatPointValue(
				point.value === null || !options.absoluteValues ? point.value : Math.abs(point.value),
				series.unit,
				options.yFormatter
			),
			type: series.name,
		}))
	)
	const domain = timeDomain(values.map((point) => point.time))
	for (const value of values) {
		;(value as typeof value & { timeLabel: string }).timeLabel = formatChartTime(value.time, domain, true)
	}
	return {
		type: "line",
		background: "transparent",
		color: options.colors ?? colors,
		data: [{ id: "data", values }],
		xField: "time",
		yField: "value",
		seriesField: "type",
		line: { style: { lineWidth: 2 } },
		point: { visible: false },
		legends: { visible: true, orient: "top", position: "start" },
		axes: [
			{
				orient: "bottom",
				type: "time",
				min: domain.min,
				max: domain.max,
				nice: false,
				zero: false,
				layers: timeAxisLayers(domain),
			},
			{
				orient: "left",
				label: {
					formatMethod: (value: string | string[]) => {
						const label = Array.isArray(value) ? value[0] : value
						const numericValue = Number(label)
						const displayValue = Number.isFinite(numericValue)
							? options.absoluteValues
								? Math.abs(numericValue)
								: numericValue
							: null
						return options.yFormatter?.(displayValue) ?? label
					},
				},
				grid: { visible: true, style: { lineDash: [3, 3] } },
			},
		],
		tooltip: {
			visible: true,
			dimension: {
				title: { value: (datum?: { timeLabel?: string }) => datum?.timeLabel ?? "" },
				content: [{ key: { field: "type" }, value: { field: "valueLabel" } }],
			},
			mark: {
				title: { value: (datum?: { timeLabel?: string }) => datum?.timeLabel ?? "" },
				content: [{ key: { field: "type" }, value: { field: "valueLabel" } }],
			},
		},
	}
}

function timeAxisLayers(domain: { min: number; max: number }) {
	const range = Math.max(0, domain.max - domain.min)
	if (range <= 2 * 60 * 60 * 1000) {
		return [{ timeFormat: "%H:%M:%S", timeFormatMode: "local" as const }]
	}
	if (range <= 24 * 60 * 60 * 1000) {
		return [{ timeFormat: "%H:%M", timeFormatMode: "local" as const }]
	}
	if (range <= 14 * 24 * 60 * 60 * 1000) {
		return [{ timeFormat: "%m/%d %H:%M", timeFormatMode: "local" as const }]
	}
	if (range <= 180 * 24 * 60 * 60 * 1000) {
		return [{ timeFormat: "%m/%d", timeFormatMode: "local" as const }]
	}
	return [{ timeFormat: "%Y/%m/%d", timeFormatMode: "local" as const }]
}

function insertGapBreaks(values: { time: number; value: number | null }[]) {
	const sorted = values
		.filter((point) => Number.isFinite(point.time))
		.slice()
		.sort((a, b) => a.time - b.time)
	if (sorted.length < 2) {
		return sorted
	}
	const step = nominalStep(sorted.map((point) => point.time))
	if (step <= 0) {
		return sorted
	}
	const maxGap = step * 3
	const result: { time: number; value: number | null }[] = [sorted[0]]
	for (let index = 1; index < sorted.length; index += 1) {
		const previous = sorted[index - 1]
		const current = sorted[index]
		if (current.time - previous.time > maxGap) {
			result.push({ time: previous.time + step, value: null })
			result.push({ time: current.time - step, value: null })
		}
		result.push(current)
	}
	return result
}

function nominalStep(times: number[]) {
	const sorted = times.slice().sort((a, b) => a - b)
	const gaps = sorted
		.slice(1)
		.map((time, index) => time - sorted[index])
		.filter((gap) => gap > 0 && Number.isFinite(gap))
		.sort((a, b) => a - b)
	if (gaps.length === 0) {
		return 0
	}
	return gaps[Math.floor(gaps.length / 2)]
}

function formatPointValue(value: number | null, unit?: string, fallback?: (value: number | null) => string) {
	if (unit) {
		return formatMetricValue(value, unit)
	}
	return fallback?.(value) ?? (value === null ? "—" : String(value))
}

function timeDomain(times: number[]) {
	const valid = times.filter((time) => Number.isFinite(time))
	if (valid.length === 0) {
		const now = Date.now()
		return { min: now - 60_000, max: now }
	}
	let min = Math.min(...valid)
	let max = Math.max(...valid)
	if (min === max) {
		min -= 30_000
		max += 30_000
	}
	return { min, max }
}

export function disposeChart(chart: VChart | null) {
	try {
		chart?.release()
	} catch {}
}

function formatChartTime(value: unknown, domain?: { min: number; max: number }, tooltip = false) {
	const date = chartDate(value)
	if (!date) {
		return ""
	}
	const range = domain ? Math.max(0, domain.max - domain.min) : 0
	if (tooltip) {
		return formatDate(date, { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" })
	}
	if (range <= 2 * 60 * 60 * 1000) {
		return formatDate(date, { hour: "2-digit", minute: "2-digit", second: "2-digit" })
	}
	if (range <= 24 * 60 * 60 * 1000) {
		return formatDate(date, { hour: "2-digit", minute: "2-digit" })
	}
	if (range <= 14 * 24 * 60 * 60 * 1000) {
		return formatDate(date, { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })
	}
	if (range <= 180 * 24 * 60 * 60 * 1000) {
		return formatDate(date, { month: "2-digit", day: "2-digit" })
	}
	return formatDate(date, { year: "numeric", month: "2-digit", day: "2-digit" })
}

function formatDate(date: Date, options: Intl.DateTimeFormatOptions) {
	return new Intl.DateTimeFormat(undefined, { ...options, hour12: false }).format(date)
}

function chartDate(value: unknown) {
	if (value && typeof value === "object" && "value" in value) {
		return chartDate((value as { value: unknown }).value)
	}
	if (value instanceof Date && !Number.isNaN(value.getTime())) {
		return value
	}
	if (typeof value === "number") {
		const timestamp = value < 10_000_000_000 ? value * 1000 : value
		const date = new Date(timestamp)
		return Number.isNaN(date.getTime()) ? null : date
	}
	if (typeof value === "string") {
		const parsed = Number(value)
		if (Number.isFinite(parsed)) {
			return chartDate(parsed)
		}
		const date = new Date(value)
		return Number.isNaN(date.getTime()) ? null : date
	}
	return null
}
