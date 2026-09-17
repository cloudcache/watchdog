import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, BarChart3Icon, PencilIcon, RefreshCwIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { isAdmin, api } from "@/lib/api"
import { formatMetricValue } from "@/lib/metric-format"
import { cn } from "@/lib/utils"
import { createLineChart, disposeChart } from "@/lib/vchart"

type AggregateGraph = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Metric?: string
	metric?: string
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	Unit?: string
	unit?: string
	Description?: string
	description?: string
}

type AggregateGraphPort = {
	PortID?: string
	port_id?: string
}

type NetworkPort = {
	ID?: string
	id?: string
	IfName?: string
	if_name?: string
	IfAlias?: string
	if_alias?: string
	IfDescr?: string
	if_descr?: string
	OperStatus?: string | number
	oper_status?: string | number
}

type PortDevice = {
	ID?: string
	id?: string
	SysName?: string
	sys_name?: string
	Model?: string
	model?: string
}

type PortMember = {
	device?: PortDevice | null
	port: NetworkPort
}

type VMRangeResponse = {
	data?: {
		result?: {
			values?: [number, string][]
		}[]
	}
}

type SeriesPoint = { time: number; value: number }

type ChartSeries = { name: string; values: SeriesPoint[] }

type AggregateGraphSummary = {
	samples?: number
	p95?: number
	peak?: number
	average?: number
	total_bytes?: number
}

type AggregateGraphDetailProps = {
	id: string
}

export default memo(({ id }: AggregateGraphDetailProps) => {
	const { t } = useLingui()
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createLineChart> | null>(null)
	const [graph, setGraph] = useState<AggregateGraph | null>(null)
	const [portLinks, setPortLinks] = useState<AggregateGraphPort[]>([])
	const [members, setMembers] = useState<PortMember[]>([])
	const [windowValue, setWindowValue] = useState("24h")
	const [valueMode, setValueMode] = useState("")
	const [splitSideType, setSplitSideType] = useState(false)
	const [chartSeries, setChartSeries] = useState<ChartSeries[]>([])
	const [summary, setSummary] = useState<AggregateGraphSummary | null>(null)
	const [loading, setLoading] = useState(true)
	const [chartLoading, setChartLoading] = useState(false)
	const [error, setError] = useState("")
	const [chartError, setChartError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [graphData, linkData, itemData] = await Promise.all([
				api.send<AggregateGraph>(`/api/v1/aggregate-graphs/${id}`, {}),
				api.send<{ items?: AggregateGraphPort[] }>(`/api/v1/aggregate-graphs/${id}/ports`, {}),
				api
					.send<{ items?: { Metric?: string; metric?: string }[] }>(`/api/v1/aggregate-graphs/${id}/items`, {})
					.catch(() => ({ items: [] })),
			])
			// Older graphs were saved without a unit; derive it from the item
			// metrics so the axis and tooltips are not raw numbers.
			if (!(graphData.Unit ?? graphData.unit)) {
				const metrics = (itemData.items ?? []).map((item) => item.Metric ?? item.metric ?? "")
				if (metrics.length > 0 && metrics.every((metric) => metric.endsWith("_bps"))) {
					graphData.Unit = "bps"
				}
			}
			setGraph(graphData)
			const links = linkData.items ?? []
			setPortLinks(links)
			const portIDs = links.map((link) => link.PortID ?? link.port_id ?? "").filter(Boolean)
			const resolved = await Promise.all(
				portIDs.map((portID) =>
					api
						// the endpoint wraps the row as { device, port }
						.send<{ device?: PortDevice; port?: NetworkPort } & NetworkPort>(`/api/v1/ports/${portID}`, {})
						.then((response): PortMember => ({ device: response.device, port: response.port ?? response }))
						.catch(() => null as PortMember | null)
				)
			)
			setMembers(resolved.filter((member): member is PortMember => Boolean(member)))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load aggregate graph`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	const refreshChart = useCallback(async () => {
		setChartLoading(true)
		setChartError("")
		try {
			const params = new URLSearchParams({
				time_mode: "fixed",
				window: windowValue,
				max_data_points: "600",
			})
			if (valueMode) {
				params.set("value_mode", valueMode)
			}
			if (splitSideType) {
				params.set("split_side_type", "true")
			}
			const data = await api.send<VMRangeResponse>(`/api/v1/aggregate-graphs/${id}/series?${params.toString()}`, {})
			setChartSeries(
				vmSeries(data).map((series) => ({
					...series,
					name: localizedSeriesName(series.name, t),
				}))
			)
		} catch (err) {
			setChartError(err instanceof Error ? err.message : t`Failed to load aggregate series`)
		} finally {
			setChartLoading(false)
		}
	}, [id, windowValue, valueMode, splitSideType, t])

	const refreshSummary = useCallback(async () => {
		try {
			const params = new URLSearchParams({ start: rangeStartForWindow(windowValue), end: new Date().toISOString() })
			const data = await api.send<AggregateGraphSummary>(
				`/api/v1/aggregate-graphs/${id}/summary?${params.toString()}`,
				{}
			)
			setSummary(data)
		} catch {
			setSummary(null)
		}
	}, [id, windowValue])

	useEffect(() => {
		document.title = `${graph?.Name ?? graph?.name ?? t`Graph`} / Watchdog`
		refresh()
	}, [refresh, t, graph?.Name, graph?.name])

	useEffect(() => {
		if (!graph) {
			return
		}
		refreshChart()
		refreshSummary()
	}, [graph, refreshChart, refreshSummary])

	useEffect(() => {
		if (!graph) {
			return
		}
		const timer = globalThis.setInterval(refreshChart, 30_000)
		return () => globalThis.clearInterval(timer)
	}, [graph, refreshChart])

	useEffect(() => {
		if (!chartRef.current || chartSeries.length === 0) {
			disposeChart(chartInstance.current)
			chartInstance.current = null
			return
		}
		const unit = graph?.Unit ?? graph?.unit ?? ""
		disposeChart(chartInstance.current)
		chartInstance.current = createLineChart(chartRef.current, {
			series: chartSeries.map((entry) => ({ name: entry.name, unit, values: entry.values })),
			yFormatter: (value) => formatMetricValue(value, unit),
		})
		return () => disposeChart(chartInstance.current)
	}, [graph, chartSeries, t])

	const unit = graph?.Unit ?? graph?.unit ?? ""

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "aggregate_graphs")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to saved graphs`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<BarChart3Icon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">{graph?.Name ?? graph?.name ?? "—"}</h1>
				</div>
				<div className="flex items-center gap-2">
					<Link
						href={getPagePath($router, "aggregate_graph_edit", { id })}
						className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
					>
						<PencilIcon className="me-2 h-4 w-4" />
						<Trans>Edit</Trans>
					</Link>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
					<Button
						variant="destructive"
						size="sm"
						onClick={async () => {
							if (!globalThis.confirm(t`Delete this aggregate graph?`)) return
							try {
								await api.send(`/api/v1/aggregate-graphs/${id}`, { method: "DELETE" })
								navigate(getPagePath($router, "aggregate_graphs"))
							} catch (err) {
								setError(err instanceof Error ? err.message : t`Failed to delete`)
							}
						}}
					>
						<Trash2Icon className="me-2 h-4 w-4" />
						<Trans>Delete</Trans>
					</Button>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			{graph ? (
				<div className="grid gap-3 rounded-md border border-border p-4">
					<div className="flex flex-wrap items-center gap-2 text-sm">
						<Badge variant="outline">{graph.Aggregation ?? graph.aggregation ?? "sum"}</Badge>
						<span className="font-mono text-xs text-muted-foreground">{graph.Metric ?? graph.metric}</span>
						{unit ? <span className="text-xs text-muted-foreground">· {unit}</span> : null}
					</div>
					{(graph.Description ?? graph.description) ? (
						<p className="text-sm text-muted-foreground">{graph.Description ?? graph.description}</p>
					) : null}
				</div>
			) : null}

			<div className="grid gap-3 rounded-md border border-border p-4">
				<div className="flex flex-wrap items-center justify-between gap-3">
					<h2 className="text-base font-medium">
						<Trans>Series</Trans>
					</h2>
					<div className="flex flex-wrap items-center gap-2">
						<Select
							value={valueMode || (graph?.ValueMode ?? graph?.value_mode ?? "corrected")}
							onValueChange={setValueMode}
						>
							<SelectTrigger className="w-32">
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="corrected">
									<Trans>Default</Trans>
								</SelectItem>
								{isAdmin() && (
									<SelectItem value="raw">
										<Trans>Raw</Trans>
									</SelectItem>
								)}
								{isAdmin() && (
									<SelectItem value="both">
										<Trans>All values</Trans>
									</SelectItem>
								)}
							</SelectContent>
						</Select>
						<label
							htmlFor="aggregate-graph-split-side"
							className="flex items-center gap-1.5 text-xs text-muted-foreground"
						>
							<Checkbox
								id="aggregate-graph-split-side"
								checked={splitSideType}
								onCheckedChange={(value) => setSplitSideType(value === true)}
							/>
							<Trans>Split side</Trans>
						</label>
						<Select value={windowValue} onValueChange={setWindowValue}>
							<SelectTrigger className="w-28">
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="5m">5m</SelectItem>
								<SelectItem value="10m">10m</SelectItem>
								<SelectItem value="15m">15m</SelectItem>
								<SelectItem value="30m">30m</SelectItem>
								<SelectItem value="1h">1h</SelectItem>
								<SelectItem value="24h">24h</SelectItem>
								<SelectItem value="7d">7d</SelectItem>
								<SelectItem value="30d">30d</SelectItem>
							</SelectContent>
						</Select>
					</div>
				</div>
				{chartError ? (
					<div className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-sm text-destructive">
						{chartError}
					</div>
				) : chartLoading && chartSeries.length === 0 ? (
					<div className="text-sm text-muted-foreground">
						<Trans>Loading chart data...</Trans>
					</div>
				) : chartSeries.length === 0 ? (
					<div className="flex h-[320px] w-full items-center justify-center rounded-md bg-muted/20 text-sm text-muted-foreground">
						<Trans>No samples in this range.</Trans>
					</div>
				) : (
					<div ref={chartRef} className="h-[360px] w-full" />
				)}
			</div>

			{summary?.samples ? (
				<div className="grid gap-3 rounded-md border border-border p-4">
					<div className="flex items-center justify-between gap-3">
						<h2 className="text-base font-medium">
							<Trans>Summary</Trans>
						</h2>
						<span className="text-xs text-muted-foreground">
							<Trans>from stored snapshots</Trans> · {summary.samples} <Trans>samples</Trans>
						</span>
					</div>
					<div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
						<SummaryTile label="P95" value={formatMetricValue(summary.p95, unit)} />
						<SummaryTile label={t`Peak`} value={formatMetricValue(summary.peak, unit)} />
						<SummaryTile label={t`Average`} value={formatMetricValue(summary.average, unit)} />
						<SummaryTile label={t`Total`} value={formatBytes(summary.total_bytes)} />
					</div>
				</div>
			) : null}

			<div className="grid gap-3 rounded-md border border-border p-4">
				<h2 className="text-base font-medium">
					<Trans>Ports</Trans> <span className="text-sm font-normal text-muted-foreground">({portLinks.length})</span>
				</h2>
				<div className="overflow-hidden rounded-md bg-card">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>
									<Trans>Device</Trans>
								</TableHead>
								<TableHead>
									<Trans>Port</Trans>
								</TableHead>
								<TableHead>
									<Trans>Alias</Trans>
								</TableHead>
								<TableHead>
									<Trans>Oper</Trans>
								</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{members.length === 0 ? (
								<TableRow>
									<TableCell colSpan={4} className="text-muted-foreground">
										<Trans>No ports found.</Trans>
									</TableCell>
								</TableRow>
							) : (
								members.map(({ device, port }) => {
									const deviceID = device?.ID ?? device?.id
									const deviceName = device?.SysName ?? device?.sys_name ?? device?.Model ?? device?.model ?? deviceID
									return (
										<TableRow key={port.ID ?? port.id}>
											<TableCell>
												{deviceID ? (
													<Link
														href={getPagePath($router, "network_device", { id: deviceID })}
														className="text-primary hover:underline"
													>
														{deviceName}
													</Link>
												) : (
													(deviceName ?? "—")
												)}
											</TableCell>
											<TableCell className="font-medium">
												{port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? "—"}
											</TableCell>
											<TableCell className="max-w-[24rem] truncate">{port.IfAlias || port.if_alias || "—"}</TableCell>
											<TableCell>{formatOperStatus(port.OperStatus ?? port.oper_status)}</TableCell>
										</TableRow>
									)
								})
							)}
						</TableBody>
					</Table>
				</div>
			</div>
		</div>
	)
})

type VMResultItem = {
	metric?: Record<string, string>
	values?: [number, string][]
}

function vmSeries(response: VMRangeResponse): ChartSeries[] {
	const results = (response.data?.result ?? []) as VMResultItem[]
	return results
		.map((item) => {
			const label = item.metric?.label || item.metric?.direction || item.metric?.value_mode || "series"
			const values = (item.values ?? [])
				.map(([time, value]) => ({ time: Math.round(time * 1000), value: Number(value) }))
				.filter((point) => Number.isFinite(point.time) && Number.isFinite(point.value))
			return { name: label, values }
		})
		.filter((entry) => entry.values.length > 0)
}

function localizedSeriesName(name: string, t: (message: TemplateStringsArray) => string) {
	const normalized = name.trim().toLowerCase()
	if (normalized === "in" || normalized === "inbound" || normalized.includes(" inbound ")) {
		return t`Inbound`
	}
	if (normalized === "out" || normalized === "outbound" || normalized.includes(" outbound ")) {
		return t`Outbound`
	}
	if (normalized === "raw") {
		return t`Raw`
	}
	if (normalized === "corrected") {
		return t`Series`
	}
	return name.replace(/\s*\(corrected\)\s*$/i, "")
}

function SummaryTile({ label, value }: { label: React.ReactNode; value: string }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="mt-1 text-lg font-semibold tabular-nums">{value}</div>
		</div>
	)
}

// SNMP ifOperStatus: 1=up 2=down 3=testing 4=unknown 5=dormant 6=notPresent 7=lowerLayerDown
function formatOperStatus(value?: string | number) {
	const raw = String(value ?? "").toLowerCase()
	if (raw === "") {
		return "—"
	}
	const name =
		{
			"1": "up",
			"2": "down",
			"3": "testing",
			"4": "unknown",
			"5": "dormant",
			"6": "notPresent",
			"7": "lowerLayerDown",
		}[raw] ?? raw
	return (
		<Badge variant={name === "up" ? "default" : "secondary"} className="font-normal">
			{name}
		</Badge>
	)
}

function formatBytes(value?: number) {
	if (!value || value <= 0) {
		return "—"
	}
	if (value >= 1_000_000_000_000) {
		return `${(value / 1_000_000_000_000).toFixed(2)} TB`
	}
	if (value >= 1_000_000_000) {
		return `${(value / 1_000_000_000).toFixed(2)} GB`
	}
	if (value >= 1_000_000) {
		return `${(value / 1_000_000).toFixed(2)} MB`
	}
	return `${value} B`
}

function rangeStartForWindow(windowValue: string) {
	const millis =
		windowValue === "5m"
			? 5 * 60_000
			: windowValue === "10m"
				? 10 * 60_000
				: windowValue === "15m"
					? 15 * 60_000
					: windowValue === "30m"
						? 30 * 60_000
						: windowValue === "1h"
							? 60 * 60_000
							: windowValue === "24h"
								? 24 * 60 * 60_000
								: windowValue === "7d"
									? 7 * 24 * 60 * 60_000
									: windowValue === "30d"
										? 30 * 24 * 60 * 60_000
										: 24 * 60 * 60_000
	return new Date(Date.now() - millis).toISOString()
}
