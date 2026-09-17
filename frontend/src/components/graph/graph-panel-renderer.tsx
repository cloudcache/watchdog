import { Trans, useLingui } from "@lingui/react/macro"
import { createContext, memo, useCallback, useContext, useEffect, useRef, useState } from "react"
import { Link } from "@/components/router"
import { api } from "@/lib/api"
import { formatBitsPerSecond } from "@/lib/metric-format"
import { trafficViewQueryStep, trafficViewRateBase, type TrafficViewMode } from "@/lib/traffic-view"
import { type createLineChart, disposeChart, updateLineChart } from "@/lib/vchart"

// ---------------------------------------------------------------------------
// Declarative dashboard model (matches backend GraphDashboard).
// ---------------------------------------------------------------------------

export type GraphQuery = {
	metric: string
	scope: string // device | port | ports-aggregate
	aggregation?: string
	label?: string
	port_id?: string
	port_ids?: string[]
	transform?: { negative?: boolean; rate?: boolean }
}

export type GraphPanel = {
	id: string
	title: string
	type: string
	unit?: string
	stack?: string // "" | "normal" | "signed"
	queries: GraphQuery[]
	links?: { href: string; label: string }[]
	query_options: { max_data_points: number; min_interval: string }
}

export type GraphDashboard = {
	id: string
	title: string
	panels: GraphPanel[]
	refresh?: number // auto-refresh interval, seconds
}

// ---------------------------------------------------------------------------
// Device context provider. Pages wrap dashboard rendering so individual
// GraphPanelRenderer instances don't need device props threaded through.
// ---------------------------------------------------------------------------

export type GraphContextValue = {
	deviceId: string
	targetId: string
	portIds: string[]
	valueMode: string
	trafficView?: TrafficViewMode
}

const GraphContext = createContext<GraphContextValue | null>(null)

export function GraphContextProvider({ value, children }: { value: GraphContextValue; children: React.ReactNode }) {
	return <GraphContext.Provider value={value}>{children}</GraphContext.Provider>
}

// ---------------------------------------------------------------------------
// Unit formatting.
// ---------------------------------------------------------------------------

export function formatGraphValue(
	value: number | null | undefined,
	unit?: string,
	trafficView?: TrafficViewMode
): string {
	if (typeof value !== "number" || !Number.isFinite(value)) {
		return "—"
	}
	switch (unit) {
		case "bps":
			// Signed (mirrored) panels plot Out below the axis as a purely
			// visual flip; throughput has no negative semantics, so axis and
			// tooltip always label the magnitude (6 / 3 / 0 / 3 / 6).
			return formatBitsPerSecond(Math.abs(value), trafficView ? trafficViewRateBase(trafficView) : 1000)
		case "percent":
			return `${value.toFixed(1)}%`
		case "dbm":
			return `${value.toFixed(2)} dBm`
		case "count":
		case "pps":
			return formatCount(value)
		default:
			return Number.isInteger(value) ? String(value) : value.toFixed(2)
	}
}

function formatCount(value: number) {
	const abs = Math.abs(value)
	if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`
	if (abs >= 1_000) return `${(value / 1_000).toFixed(2)}K`
	return Number.isInteger(value) ? String(value) : value.toFixed(2)
}

// ---------------------------------------------------------------------------
// Renderer.
// ---------------------------------------------------------------------------

type VMRangeResponse = {
	data?: { result?: { values?: [number, string][] }[] }
}

type SeriesPoint = { time: number; value: number }
type Series = { name: string; values: SeriesPoint[] }
type PanelState = "loading" | "ok" | "no_data" | "error"

export type GraphPanelRendererProps = {
	panel: GraphPanel
	range: string
	refreshInterval?: number
}

/** GraphPanelRenderer executes a panel's queries and renders the chart. Pages
 * consume dashboard schemas via this component instead of issuing their own
 * scattered metric queries. Device context comes from GraphContextProvider. */
export const GraphPanelRenderer = memo(({ panel, range, refreshInterval }: GraphPanelRendererProps) => {
	const { t } = useLingui()
	const ctx = useContext(GraphContext)
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createLineChart> | null>(null)
	const [series, setSeries] = useState<Series[]>([])
	const [state, setState] = useState<PanelState>("loading")
	const [error, setError] = useState("")
	const requestSequence = useRef(0)
	const activeRequest = useRef<AbortController | null>(null)

	const load = useCallback(async () => {
		if (!ctx) {
			setState("error")
			setError("GraphPanelRenderer requires GraphContextProvider")
			return
		}
		const sequence = ++requestSequence.current
		activeRequest.current?.abort()
		const controller = new AbortController()
		activeRequest.current = controller
		setState("loading")
		setError("")
		try {
			const results = await Promise.all(
				panel.queries.map((query) =>
					execPanelQuery(query, ctx, range, panel.query_options.max_data_points, controller.signal)
				)
			)
			if (sequence !== requestSequence.current) return
			setSeries(results)
			setState(results.every((item) => item.values.length === 0) ? "no_data" : "ok")
		} catch (err) {
			if (controller.signal.aborted || sequence !== requestSequence.current) return
			setError(err instanceof Error ? err.message : t`Failed to load panel`)
			setState("error")
		}
	}, [ctx, panel, range, t])

	useEffect(() => {
		load().catch(() => undefined)
		return () => activeRequest.current?.abort()
	}, [load])

	useEffect(() => {
		if (!refreshInterval || refreshInterval <= 0) return
		const timer = globalThis.setInterval(() => {
			load().catch(() => undefined)
		}, refreshInterval * 1000)
		return () => globalThis.clearInterval(timer)
	}, [refreshInterval, load])

	useEffect(() => {
		const el = chartRef.current
		if (!el || state !== "ok" || series.length === 0) {
			disposeChart(chartInstance.current)
			chartInstance.current = null
			return
		}
		let cancelled = false
		let retryTimer: ReturnType<typeof setTimeout> | undefined
		const formatter = (value?: number | null) => formatGraphValue(value, panel.unit, ctx?.trafficView)
		const draw = () => {
			if (cancelled || !chartRef.current) return
			const rect = chartRef.current.getBoundingClientRect()
			if (rect.width < 2 || rect.height < 2) {
				if (retryTimer === undefined) {
					retryTimer = globalThis.setTimeout(() => {
						retryTimer = undefined
						draw()
					}, 50)
				}
				return
			}
			if (retryTimer !== undefined) {
				globalThis.clearTimeout(retryTimer)
				retryTimer = undefined
			}
			chartInstance.current = updateLineChart(chartInstance.current, chartRef.current, {
				series: series.map((entry) => ({ name: entry.name, values: entry.values })),
				yFormatter: formatter,
			})
		}
		const observer = new ResizeObserver(draw)
		observer.observe(el)
		const frame = globalThis.requestAnimationFrame(draw)
		retryTimer = globalThis.setTimeout(() => {
			retryTimer = undefined
			draw()
		}, 0)
		return () => {
			cancelled = true
			globalThis.cancelAnimationFrame(frame)
			if (retryTimer !== undefined) {
				globalThis.clearTimeout(retryTimer)
			}
			observer.disconnect()
			disposeChart(chartInstance.current)
			chartInstance.current = null
		}
	}, [series, state, panel.unit, ctx?.trafficView])

	return (
		<div className="grid gap-3 rounded-md border border-border p-4">
			<div className="flex items-center justify-between gap-2">
				<h3 className="text-sm font-medium">{panel.title}</h3>
				{panel.stack === "signed" ? (
					<span className="text-xs text-muted-foreground">
						<Trans>signed (In ↑ / Out ↓)</Trans>
					</span>
				) : null}
			</div>
			{state === "error" ? (
				<div className="text-sm text-destructive">{error}</div>
			) : state === "loading" ? (
				<div className="text-sm text-muted-foreground">
					<Trans>Loading...</Trans>
				</div>
			) : state === "no_data" ? (
				<div className="flex h-[200px] items-center justify-center rounded-md bg-muted/20 text-sm text-muted-foreground">
					<Trans>No data in this range.</Trans>
				</div>
			) : (
				<div ref={chartRef} className="h-[200px] w-full" />
			)}
			{panel.links && panel.links.length > 0 ? <GraphPanelLinks links={panel.links} /> : null}
		</div>
	)
})

function GraphPanelLinks({ links }: { links: NonNullable<GraphPanel["links"]> }) {
	return (
		<div className="max-h-28 overflow-auto border-t border-border pt-3 text-sm leading-6">
			{links.map((link, index) => (
				<span key={`${link.href}:${link.label}`}>
					<Link href={link.href} className="text-blue-700 hover:underline dark:text-blue-300">
						{link.label}
					</Link>
					{index < links.length - 1 ? <span className="text-muted-foreground">, </span> : null}
				</span>
			))}
		</div>
	)
}

async function execPanelQuery(
	query: GraphQuery,
	ctx: GraphContextValue,
	range: string,
	maxDataPoints: number,
	signal?: AbortSignal
): Promise<Series> {
	const params = new URLSearchParams({
		time_mode: "fixed",
		window: range,
		max_data_points: String(maxDataPoints),
		value_mode: ctx.valueMode,
	})
	if (ctx.trafficView) {
		params.set("traffic_view", ctx.trafficView)
		params.set("step", trafficViewQueryStep(ctx.trafficView))
	}
	if (query.transform?.rate) {
		params.set("func", "rate")
	}
	let url: string
	if (query.scope === "ports-aggregate") {
		params.set("metric", query.metric)
		params.set("aggregate", query.aggregation || "sum")
		// The dashboard may pin an explicit port membership (e.g. physical
		// ports only for device totals); fall back to the context port set.
		const portIds = query.port_ids?.length ? query.port_ids : ctx.portIds
		params.set("port_ids", portIds.join(","))
		url = `/api/v1/metrics/aggregate?${params.toString()}`
	} else {
		params.set("metric", query.metric)
		if (query.scope === "device") {
			params.set("target_id", ctx.targetId)
			params.set("device_id", ctx.deviceId)
		} else if (query.scope === "port") {
			params.set("port_id", query.port_id || "")
		}
		url = `/api/v1/metrics/query?${params.toString()}`
	}
	const data = await api.send<VMRangeResponse>(url, { signal })
	let values = vmValues(data)
	if (query.transform?.negative) {
		values = values.map((point) => ({ time: point.time, value: -point.value }))
	}
	return { name: query.label || query.metric, values }
}

function vmValues(response: VMRangeResponse): SeriesPoint[] {
	const out: SeriesPoint[] = []
	for (const result of response.data?.result ?? []) {
		for (const [time, value] of result.values ?? []) {
			out.push({ time: Math.round(time * 1000), value: Number(value) })
		}
	}
	return out
		.filter((point) => Number.isFinite(point.time) && Number.isFinite(point.value))
		.sort((a, b) => a.time - b.time)
}
