import { timeTicks } from "d3-time"
import { api } from "@/lib/api"
import { chartTimeData } from "@/lib/utils"
import type { ChartData, ChartTimeRange, ChartTimes, ContainerStatsRecord, SystemStatsRecord } from "@/types"

type ChartTimeData = {
	time: number
	data: {
		ticks: number[]
		domain: number[]
	}
	chartTime: ChartTimes
	rangeKey: string
}

type VMRangeResponse = {
	data?: {
		result?: {
			metric?: Record<string, string>
			values?: [number, string][]
		}[]
	}
}

const targetMetrics = {
	cpu: "watchdog_system_cpu_percent",
	memory: "watchdog_system_memory_percent",
	disk: "watchdog_system_disk_percent",
	netIn: "watchdog_system_net_in_bps",
	netOut: "watchdog_system_net_out_bps",
} as const

const containerMetrics = {
	cpu: "watchdog_container_cpu_percent",
	memory: "watchdog_container_memory_bytes",
	netTx: "watchdog_container_net_tx_bps",
	netRx: "watchdog_container_net_rx_bps",
} as const

const vmChartMaxDataPoints = "600"

export const cache = new Map<
	string,
	ChartTimeData | SystemStatsRecord[] | ContainerStatsRecord[] | ChartData["containerData"]
>()

// create ticks and domain for charts
export function getTimeData(chartTime: ChartTimes, lastCreated: number, customRange?: ChartTimeRange) {
	const range = resolveChartRange(chartTime, customRange)
	const rangeKey = `${range.start.toISOString()}_${range.end.toISOString()}`
	const cached = cache.get("td") as ChartTimeData | undefined
	if (cached && cached.chartTime === chartTime && cached.rangeKey === rangeKey) {
		if (!lastCreated || cached.time >= lastCreated) {
			return cached.data
		}
	}

	// const buffer = chartTime === "1m" ? 400 : 20_000
	const now = new Date(Date.now())
	const ticks = timeTicks(range.start, range.end, chartTimeData[chartTime].ticks ?? 12).map((date) => date.getTime())
	const data = {
		ticks,
		domain: [range.start.getTime(), range.end.getTime()],
	}
	cache.set("td", { time: now.getTime(), data, chartTime, rangeKey })
	return data
}

/** Append new records onto prev with gap detection. Converts string `created` values to ms timestamps in place.
 * Pass `maxLen` to cap the result length in one copy instead of slicing again after the call. */
export function appendData<T extends { created: string | number | null }>(
	prev: T[],
	newRecords: T[],
	expectedInterval: number,
	maxLen?: number
): T[] {
	if (!newRecords.length) return prev
	// Pre-trim prev so the single slice() below is the only copy we make
	const trimmed = maxLen && prev.length >= maxLen ? prev.slice(-(maxLen - newRecords.length)) : prev
	const result = trimmed.slice()
	let prevTime = (trimmed.at(-1)?.created as number) ?? 0
	for (const record of newRecords) {
		if (record.created !== null) {
			if (typeof record.created === "string") {
				record.created = new Date(record.created).getTime()
			}
			if (prevTime && (record.created as number) - prevTime > expectedInterval * 1.5) {
				result.push({ created: null, ...("stats" in record ? { stats: null } : {}) } as T)
			}
			prevTime = record.created as number
		}
		result.push(record)
	}
	return result
}

export async function getSystemStatsFromVM(
	systemId: string,
	chartTime: ChartTimes,
	customRange?: ChartTimeRange
): Promise<SystemStatsRecord[]> {
	const range = resolveChartRange(chartTime, customRange)
	const params = new URLSearchParams({
		target_id: systemId,
		time_mode: "custom",
		start: range.start.toISOString(),
		end: range.end.toISOString(),
		max_data_points: vmChartMaxDataPoints,
	})
	const [cpu, memory, disk, netIn, netOut] = await Promise.all([
		getVMMetric(targetMetrics.cpu, params),
		getVMMetric(targetMetrics.memory, params),
		getVMMetric(targetMetrics.disk, params),
		getVMMetric(targetMetrics.netIn, params),
		getVMMetric(targetMetrics.netOut, params),
	])
	return makeSystemStatsFromVM({ cpu, memory, disk, netIn, netOut })
}

export async function getRealtimeSystemStatsFromVM(systemId: string): Promise<SystemStatsRecord[]> {
	const params = new URLSearchParams({
		target_id: systemId,
		max_data_points: "60",
	})
	const [cpu, memory, disk, netIn, netOut] = await Promise.all([
		getVMMetric(targetMetrics.cpu, params, "/api/v1/metrics/realtime"),
		getVMMetric(targetMetrics.memory, params, "/api/v1/metrics/realtime"),
		getVMMetric(targetMetrics.disk, params, "/api/v1/metrics/realtime"),
		getVMMetric(targetMetrics.netIn, params, "/api/v1/metrics/realtime"),
		getVMMetric(targetMetrics.netOut, params, "/api/v1/metrics/realtime"),
	])
	return makeSystemStatsFromVM({ cpu, memory, disk, netIn, netOut })
}

export async function getContainerDataFromVM(
	systemId: string,
	chartTime: ChartTimes,
	customRange?: ChartTimeRange
): Promise<ChartData["containerData"]> {
	const range = resolveChartRange(chartTime, customRange)
	const params = new URLSearchParams({
		target_id: systemId,
		time_mode: "custom",
		start: range.start.toISOString(),
		end: range.end.toISOString(),
		max_data_points: vmChartMaxDataPoints,
	})
	const [cpu, memory, netTx, netRx] = await Promise.all([
		getVMMetricResponse(containerMetrics.cpu, params),
		getVMMetricResponse(containerMetrics.memory, params),
		getVMMetricResponse(containerMetrics.netTx, params),
		getVMMetricResponse(containerMetrics.netRx, params),
	])
	return makeContainerDataFromVM({ cpu, memory, netTx, netRx })
}

function resolveChartRange(chartTime: ChartTimes, customRange?: ChartTimeRange) {
	if (chartTime === "custom") {
		const start = customRange?.start ? new Date(customRange.start) : null
		const end = customRange?.end ? new Date(customRange.end) : null
		if (start && end && !Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime()) && end > start) {
			return { start, end }
		}
	}
	const end = new Date()
	return { start: chartTimeData[chartTime].getOffset(end), end }
}

export async function getRealtimeContainerDataFromVM(systemId: string): Promise<ChartData["containerData"]> {
	const params = new URLSearchParams({
		target_id: systemId,
		max_data_points: "60",
	})
	const [cpu, memory, netTx, netRx] = await Promise.all([
		getVMMetricResponse(containerMetrics.cpu, params, "/api/v1/metrics/realtime"),
		getVMMetricResponse(containerMetrics.memory, params, "/api/v1/metrics/realtime"),
		getVMMetricResponse(containerMetrics.netTx, params, "/api/v1/metrics/realtime"),
		getVMMetricResponse(containerMetrics.netRx, params, "/api/v1/metrics/realtime"),
	])
	return makeContainerDataFromVM({ cpu, memory, netTx, netRx })
}

async function getVMMetric(metric: string, baseParams: URLSearchParams, endpoint = "/api/v1/metrics/query") {
	const response = await getVMMetricResponse(metric, baseParams, endpoint)
	return response.data?.result?.[0]?.values ?? []
}

async function getVMMetricResponse(metric: string, baseParams: URLSearchParams, endpoint = "/api/v1/metrics/query") {
	const params = new URLSearchParams(baseParams)
	params.set("metric", metric)
	return await api.send<VMRangeResponse>(`${endpoint}?${params.toString()}`, {})
}

function makeSystemStatsFromVM(series: {
	cpu: [number, string][]
	memory: [number, string][]
	disk: [number, string][]
	netIn: [number, string][]
	netOut: [number, string][]
}): SystemStatsRecord[] {
	const byTime = new Map<number, Partial<Record<keyof typeof series, number>>>()
	for (const [name, values] of Object.entries(series) as [keyof typeof series, [number, string][]][]) {
		for (const [timestamp, value] of values) {
			const time = Math.round(timestamp * 1000)
			const point = byTime.get(time) ?? {}
			point[name] = Number(value) || 0
			byTime.set(time, point)
		}
	}
	return [...byTime.entries()]
		.sort(([a], [b]) => a - b)
		.map(([created, point]) => {
			const memoryPercent = point.memory ?? 0
			const diskPercent = point.disk ?? 0
			const sentBytes = (point.netOut ?? 0) / 8
			const receivedBytes = (point.netIn ?? 0) / 8
			return {
				system: "",
				created,
				stats: {
					cpu: point.cpu ?? 0,
					mp: memoryPercent,
					m: 100,
					mu: memoryPercent,
					mb: 0,
					s: 0,
					su: 0,
					d: 100,
					du: diskPercent,
					dp: diskPercent,
					dr: 0,
					dw: 0,
					ns: sentBytes / 1024 / 1024,
					nr: receivedBytes / 1024 / 1024,
					b: [sentBytes, receivedBytes],
				},
			} as SystemStatsRecord
		})
}

function makeContainerDataFromVM(series: {
	cpu: VMRangeResponse
	memory: VMRangeResponse
	netTx: VMRangeResponse
	netRx: VMRangeResponse
}): ChartData["containerData"] {
	const byTime = new Map<number, ChartData["containerData"][0]>()
	mergeContainerSeries(byTime, series.cpu, "c", (value) => value)
	mergeContainerSeries(byTime, series.memory, "m", (value) => value)
	mergeContainerNetworkSeries(byTime, series.netTx, 0)
	mergeContainerNetworkSeries(byTime, series.netRx, 1)
	return [...byTime.entries()].sort(([a], [b]) => a - b).map(([, point]) => point)
}

function mergeContainerSeries(
	byTime: Map<number, ChartData["containerData"][0]>,
	response: VMRangeResponse,
	field: "c" | "m",
	convert: (value: number) => number
) {
	for (const item of response.data?.result ?? []) {
		const name = containerName(item.metric)
		for (const [timestamp, rawValue] of item.values ?? []) {
			const time = Math.round(timestamp * 1000)
			const point = byTime.get(time) ?? ({ created: time } as ChartData["containerData"][0])
			const container = ensureContainer(point, name)
			container[field] = convert(Number(rawValue) || 0)
			byTime.set(time, point)
		}
	}
}

function mergeContainerNetworkSeries(
	byTime: Map<number, ChartData["containerData"][0]>,
	response: VMRangeResponse,
	index: 0 | 1
) {
	for (const item of response.data?.result ?? []) {
		const name = containerName(item.metric)
		for (const [timestamp, rawValue] of item.values ?? []) {
			const time = Math.round(timestamp * 1000)
			const point = byTime.get(time) ?? ({ created: time } as ChartData["containerData"][0])
			const container = ensureContainer(point, name)
			const current = container.b ?? [0, 0]
			current[index] = (Number(rawValue) || 0) / 8
			container.b = current
			byTime.set(time, point)
		}
	}
}

function ensureContainer(point: ChartData["containerData"][0], name: string) {
	const values = point as Record<string, ContainerStatsRecord["stats"][0] | number | null>
	if (!values[name] || typeof values[name] !== "object") {
		values[name] = { n: name, c: 0, m: 0 }
	}
	return values[name] as ContainerStatsRecord["stats"][0]
}

function containerName(metric: Record<string, string> | undefined) {
	return metric?.container_name || metric?.container || metric?.name || "container"
}

export function makeContainerData(containers: ContainerStatsRecord[]): ChartData["containerData"] {
	const result = [] as ChartData["containerData"]
	for (const { created, stats } of containers) {
		if (!created) {
			result.push({ created: null } as ChartData["containerData"][0])
			continue
		}
		result.push(makeContainerPoint(new Date(created).getTime(), stats))
	}
	return result
}

/** Transform a single realtime container stats message into a ChartDataContainer point. */
export function makeContainerPoint(
	created: number,
	stats: ContainerStatsRecord["stats"]
): ChartData["containerData"][0] {
	const point: ChartData["containerData"][0] = { created } as ChartData["containerData"][0]
	for (const container of stats) {
		;(point as Record<string, unknown>)[container.n] = container
	}
	return point
}

export function dockerOrPodman(str: string, isPodman: boolean): string {
	if (isPodman) {
		return str.replace("docker", "podman").replace("Docker", "Podman")
	}
	return str
}
