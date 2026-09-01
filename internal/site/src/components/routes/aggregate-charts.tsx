import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { BarChart3Icon, DownloadIcon, RefreshCwIcon, SaveIcon } from "lucide-react"
import type React from "react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { TrafficViewSwitcher } from "@/components/traffic-view-switcher"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { isAdmin, pb } from "@/lib/api"
import { formatBitsPerSecond, formatMetricValue } from "@/lib/metric-format"
import {
	trafficViewAggregate,
	trafficViewLabel,
	trafficViewQueryStep,
	trafficViewRateBase,
	trafficViewValueMode,
	type TrafficViewMode,
} from "@/lib/traffic-view"
import { type createLineChart, disposeChart, updateLineChart } from "@/lib/vchart"

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Type?: string
	target_type?: string
	Host?: string
	host?: string
}

type NetworkDevice = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	Name?: string
	name?: string
	SysName?: string
	sys_name?: string
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
	OperStatus?: string
	oper_status?: string
}

type MetricDefinition = {
	Name?: string
	name?: string
	Family?: string
	family?: string
	Scope?: string
	scope?: string
	Unit?: string
	unit?: string
	Description?: string
	description?: string
}

type VMRangeResponse = {
	data?: {
		result?: {
			metric?: Record<string, string>
			values?: [number, string][]
		}[]
	}
}

type QuerySeries = {
	name: string
	unit: string
	values: { time: number; value: number }[]
}

type AggregateMethod = "sum" | "avg" | "max" | "min" | "count"

const defaultMetrics = ["watchdog_snmp_if_in_bps", "watchdog_snmp_if_out_bps"]

export default memo(() => {
	const { t } = useLingui()
	const [targets, setTargets] = useState<TargetRecord[]>([])
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [portsByDevice, setPortsByDevice] = useState<Record<string, NetworkPort[]>>({})
	const [metrics, setMetrics] = useState<MetricDefinition[]>([])
	const [trafficView, setTrafficView] = useState<TrafficViewMode>("customer")
	const [selectedTargets, setSelectedTargets] = useState<string[]>([])
	const [selectedMetrics, setSelectedMetrics] = useState<string[]>(defaultMetrics)
	const [portMode, setPortMode] = useState("all")
	const [selectedPorts, setSelectedPorts] = useState<string[]>([])
	const [aggregate, setAggregate] = useState<AggregateMethod>("sum")
	const [timeMode, setTimeMode] = useState("fixed")
	const [window, setWindow] = useState("1h")
	const [start, setStart] = useState(defaultDateTime(-24))
	const [end, setEnd] = useState(defaultDateTime(0))
	const [step, setStep] = useState("auto")
	const [valueMode, setValueMode] = useState("corrected")
	const [series, setSeries] = useState<QuerySeries[]>([])
	const [loading, setLoading] = useState(true)
	const [chartLoading, setChartLoading] = useState(false)
	const [savingGraph, setSavingGraph] = useState(false)
	const [error, setError] = useState("")

	const applyTrafficView = useCallback((mode: TrafficViewMode) => {
		setTrafficView(mode)
		setStep(trafficViewQueryStep(mode))
		setValueMode(trafficViewValueMode(mode, isAdmin()))
		setAggregate(trafficViewAggregate(mode) as AggregateMethod)
		setSelectedMetrics(defaultMetrics)
	}, [])

	const selectedMetricDefs = useMemo(
		() =>
			selectedMetrics
				.map((name) => metrics.find((metric) => metricName(metric) === name))
				.filter(Boolean) as MetricDefinition[],
		[metrics, selectedMetrics]
	)
	const hasPortMetrics = selectedMetricDefs.some((metric) => metricScope(metric) === "port")
	const visiblePorts = useMemo(
		() =>
			devices
				.filter((device) => selectedTargets.includes(deviceTargetID(device)))
				.flatMap((device) =>
					(portsByDevice[deviceID(device)] ?? []).map((port) => ({
						...port,
						deviceName: device.SysName ?? device.sys_name ?? device.Name ?? device.name ?? deviceID(device),
					}))
				),
		[devices, portsByDevice, selectedTargets]
	)

	const refreshCatalog = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [targetData, deviceData, metricData] = await Promise.all([
				pb.send<{ items?: TargetRecord[] }>("/api/v1/targets", {}),
				pb.send<{ items?: NetworkDevice[] }>("/api/v1/network/devices", {}),
				pb.send<{ items?: MetricDefinition[] }>("/api/v1/metrics/catalog", {}),
			])
			const nextTargets = targetData.items ?? []
			const nextMetrics = metricData.items ?? []
			setTargets(nextTargets)
			setDevices(deviceData.items ?? [])
			setMetrics(nextMetrics)
			setSelectedTargets((current) => {
				const valid = current.filter((id) => nextTargets.some((target) => targetID(target) === id))
				if (valid.length > 0) {
					return valid
				}
				const firstTarget = nextTargets.find((target) => targetType(target) === "network") ?? nextTargets[0]
				const first = firstTarget ? targetID(firstTarget) : ""
				return first ? [first] : []
			})
			setSelectedMetrics((current) =>
				(current.length ? current : defaultMetrics).filter((name) =>
					nextMetrics.some((metric) => metricName(metric) === name)
				)
			)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load aggregate chart inputs`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Aggregate Charts`} / Beszel`
		refreshCatalog()
	}, [refreshCatalog, t])

	const ensurePorts = useCallback(async () => {
		const missingDeviceIDs = devices
			.filter((device) => selectedTargets.includes(deviceTargetID(device)))
			.map(deviceID)
			.filter((id) => id && !portsByDevice[id])
		if (missingDeviceIDs.length === 0) {
			return portsByDevice
		}
		const loaded = await Promise.all(
			missingDeviceIDs.map(async (id) => {
				const data = await pb.send<{ items?: NetworkPort[] }>(`/api/v1/network/devices/${id}/ports`, {})
				return [id, data.items ?? []] as const
			})
		)
		const next = { ...portsByDevice }
		for (const [id, ports] of loaded) {
			next[id] = ports
		}
		setPortsByDevice(next)
		return next
	}, [devices, portsByDevice, selectedTargets])

	useEffect(() => {
		if (!hasPortMetrics || selectedTargets.length === 0) {
			return
		}
		ensurePorts().catch(() => {})
	}, [ensurePorts, hasPortMetrics, selectedTargets.length])

	useEffect(() => {
		const visibleIDs = new Set(visiblePorts.map(portID))
		setSelectedPorts((current) => current.filter((id) => visibleIDs.has(id)))
	}, [visiblePorts])

	const buildTimeParams = useCallback(() => {
		const params = new URLSearchParams({
			time_mode: timeMode,
			max_data_points: "300",
		})
		if (timeMode === "fixed") {
			params.set("window", window)
		} else if (timeMode === "custom") {
			params.set("start", toISOString(start))
			params.set("end", toISOString(end))
		}
		if (step !== "auto") {
			params.set("step", step)
		}
		return params
	}, [end, start, step, timeMode, window])

	const refreshChart = useCallback(async () => {
		if (selectedTargets.length === 0 || selectedMetrics.length === 0) {
			setError(t`Select at least one target and one metric`)
			return
		}
		if (timeMode === "custom" && (!start || !end)) {
			setError(t`Select a custom start and end time`)
			return
		}
		setChartLoading(true)
		setError("")
		try {
			const latestPortsByDevice = hasPortMetrics ? await ensurePorts() : portsByDevice
			const allVisiblePortIDs = devices
				.filter((device) => selectedTargets.includes(deviceTargetID(device)))
				.flatMap((device) => latestPortsByDevice[deviceID(device)] ?? [])
				.map(portID)
				.filter(Boolean)
			const metricsToQuery =
				selectedMetricDefs.length > 0 ? selectedMetricDefs : selectedMetrics.map(fallbackMetricDefinition)
			const nextSeries: QuerySeries[] = []
			for (const metric of metricsToQuery) {
				const name = metricName(metric)
				if (!name) {
					continue
				}
				const scope = metricScope(metric)
				const unit = metricUnit(metric)
				const queryPortIDs = scope === "port" ? (portMode === "selected" ? selectedPorts : allVisiblePortIDs) : []
				if (scope === "port" && queryPortIDs.length === 0) {
					throw new Error(t`Select at least one port for port metrics`)
				}
				const metricSeries = await queryAggregateMetric({
					metric: name,
					unit,
					aggregate,
					trafficView,
					valueMode,
					targetIDs: selectedTargets,
					portIDs: queryPortIDs,
					timeParams: buildTimeParams(),
				})
				for (const item of metricSeries) {
					nextSeries.push({
						name: `${labelForMetric(metric)} ${aggregate}${item.mode ? ` (${item.mode})` : ""}`,
						unit,
						values: item.values,
					})
				}
			}
			setSeries(nextSeries)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load aggregate chart`)
		} finally {
			setChartLoading(false)
		}
	}, [
		aggregate,
		buildTimeParams,
		devices,
		ensurePorts,
		hasPortMetrics,
		portMode,
		portsByDevice,
		selectedPorts,
		selectedMetricDefs,
		selectedMetrics.length,
		selectedTargets,
		t,
		trafficView,
		timeMode,
		valueMode,
		start,
		end,
	])

	const saveAsGraph = useCallback(async () => {
		if (selectedMetrics.length === 0 || selectedTargets.length === 0) {
			return
		}
		setSavingGraph(true)
		setError("")
		try {
			const latestPortsByDevice = hasPortMetrics ? await ensurePorts() : portsByDevice
			// Honor the port selection mode: "selected" saves only the chosen
			// ports; "all" saves every discovered port under the chosen targets.
			const allVisiblePortIDs = devices
				.filter((device) => selectedTargets.includes(deviceTargetID(device)))
				.flatMap((device) => latestPortsByDevice[deviceID(device)] ?? [])
				.map(portID)
				.filter(Boolean)
			const portIDs = hasPortMetrics && portMode === "selected" ? selectedPorts : allVisiblePortIDs
			const firstTarget = targets.find((target) => targetID(target) === selectedTargets[0])
			const defaultName = `${firstTarget?.Name ?? firstTarget?.name ?? "Aggregate"} aggregate`
			const name = globalThis.prompt(t`Graph name`, defaultName)
			if (!name) {
				return
			}
			const graphID = createAggregateGraphID()
			await pb.send(`/api/v1/aggregate-graphs`, {
				method: "POST",
				body: {
					id: graphID,
					Name: name.trim(),
					Aggregation: aggregate,
					ValueMode: valueMode,
					Unit: "",
					Description: `Created from ${trafficViewLabel(trafficView)} view`,
				},
			})
			const items = selectedMetrics.map((metric) => {
				const direction = metric.includes("_if_in_") ? "in" : metric.includes("_if_out_") ? "out" : "other"
				return {
					metric,
					direction,
					label: direction === "in" ? "In" : direction === "out" ? "Out" : metric,
					total: direction === "in" || direction === "out",
				}
			})
			await pb.send(`/api/v1/aggregate-graphs/${graphID}/items`, { method: "PUT", body: { items } })
			if (portIDs.length > 0) {
				await pb.send(`/api/v1/aggregate-graphs/${graphID}/ports`, {
					method: "PUT",
					body: { ports: portIDs.map((id) => ({ PortID: id })) },
				})
			}
			navigate(getPagePath($router, "aggregate_graph", { id: graphID }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save aggregate graph`)
		} finally {
			setSavingGraph(false)
		}
	}, [
		aggregate,
		devices,
		ensurePorts,
		hasPortMetrics,
		portMode,
		portsByDevice,
		selectedMetrics,
		selectedPorts,
		selectedTargets,
		targets,
		trafficView,
		valueMode,
		t,
	])

	useEffect(() => {
		if (loading) {
			return
		}
		refreshChart()
	}, [loading, refreshChart])

	useEffect(() => {
		if (timeMode !== "realtime") {
			return
		}
		const timer = globalThis.setInterval(refreshChart, 10_000)
		return () => globalThis.clearInterval(timer)
	}, [refreshChart, timeMode])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<BarChart3Icon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Aggregate Charts</Trans>
					</h1>
				</div>
				<Button variant="outline" size="sm" onClick={refreshCatalog} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>
			<div className="flex flex-wrap items-center justify-between gap-3">
				<TrafficViewSwitcher value={trafficView} onChange={applyTrafficView} allowRaw={isAdmin()} />
				<div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
					<span>{trafficViewLabel(trafficView)}</span>
					<span>{valueMode}</span>
					<span>{aggregate}</span>
				</div>
			</div>

			<div className="grid gap-4 lg:grid-cols-[320px_1fr]">
				<div className="grid gap-4">
					<Panel title={t`Targets`}>
						<div className="grid max-h-[280px] gap-2 overflow-auto pe-1">
							{targets.map((target) => {
								const id = targetID(target)
								return (
									<div key={id} className="flex items-start gap-2 rounded-md px-2 py-1.5 hover:bg-muted/60">
										<Checkbox
											checked={selectedTargets.includes(id)}
											onCheckedChange={() => toggleSelected(setSelectedTargets, id)}
										/>
										<span className="min-w-0">
											<span className="block truncate text-sm font-medium">{target.Name ?? target.name ?? id}</span>
											<span className="block truncate text-xs text-muted-foreground">
												{target.Type ?? target.target_type ?? "target"} · {target.Host ?? target.host ?? id}
											</span>
										</span>
									</div>
								)
							})}
						</div>
					</Panel>

					<Panel title={t`Metrics`}>
						<div className="grid max-h-[320px] gap-2 overflow-auto pe-1">
							{metrics.map((metric) => {
								const name = metricName(metric)
								return (
									<div key={name} className="flex items-start gap-2 rounded-md px-2 py-1.5 hover:bg-muted/60">
										<Checkbox
											checked={selectedMetrics.includes(name)}
											onCheckedChange={() => toggleSelected(setSelectedMetrics, name)}
										/>
										<span className="min-w-0">
											<span className="block truncate text-sm font-medium">{labelForMetric(metric)}</span>
											<span className="block truncate text-xs text-muted-foreground">
												{metricFamily(metric)} · {metricScope(metric)} · {metricUnit(metric) || "value"}
											</span>
										</span>
									</div>
								)
							})}
						</div>
					</Panel>
				</div>

				<div className="grid gap-4">
					<div className="grid gap-3 rounded-md border border-border p-4">
						<div className="grid gap-3 md:grid-cols-4">
							<Select value={aggregate} onValueChange={(value: AggregateMethod) => setAggregate(value)}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="sum">sum</SelectItem>
									<SelectItem value="avg">avg</SelectItem>
									<SelectItem value="max">max</SelectItem>
									<SelectItem value="min">min</SelectItem>
									<SelectItem value="count">count</SelectItem>
								</SelectContent>
							</Select>
							<Select value={valueMode} onValueChange={setValueMode}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="corrected">corrected</SelectItem>
									{isAdmin() && <SelectItem value="raw">raw</SelectItem>}
									{isAdmin() && <SelectItem value="both">both</SelectItem>}
								</SelectContent>
							</Select>
							<Select value={timeMode} onValueChange={setTimeMode}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="realtime">realtime</SelectItem>
									<SelectItem value="fixed">fixed</SelectItem>
									<SelectItem value="custom">custom</SelectItem>
								</SelectContent>
							</Select>
							{timeMode === "fixed" ? (
								<Select value={window} onValueChange={setWindow}>
									<SelectTrigger>
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
							) : null}
							<Select value={step} onValueChange={setStep}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="auto">auto</SelectItem>
									<SelectItem value="60">1m</SelectItem>
									<SelectItem value="300">5m</SelectItem>
									<SelectItem value="600">10m</SelectItem>
									<SelectItem value="900">15m</SelectItem>
									<SelectItem value="1800">30m</SelectItem>
									<SelectItem value="3600">1h</SelectItem>
								</SelectContent>
							</Select>
						</div>
						{timeMode === "custom" ? (
							<div className="grid gap-3 md:grid-cols-2">
								<Input type="datetime-local" value={start} onChange={(event) => setStart(event.target.value)} />
								<Input type="datetime-local" value={end} onChange={(event) => setEnd(event.target.value)} />
							</div>
						) : null}
						{hasPortMetrics ? (
							<div className="grid gap-3">
								<Select value={portMode} onValueChange={setPortMode}>
									<SelectTrigger className="md:w-56">
										<SelectValue />
									</SelectTrigger>
									<SelectContent>
										<SelectItem value="all">
											<Trans>All discovered ports</Trans>
										</SelectItem>
										<SelectItem value="selected">
											<Trans>Selected ports</Trans>
										</SelectItem>
									</SelectContent>
								</Select>
								{portMode === "selected" ? (
									<div className="grid max-h-[220px] gap-2 overflow-auto rounded-md border border-border p-2">
										{visiblePorts.length === 0 ? (
											<div className="px-2 py-1 text-sm text-muted-foreground">
												<Trans>No discovered ports for selected targets.</Trans>
											</div>
										) : (
											visiblePorts.map((port) => {
												const id = portID(port)
												return (
													<div key={id} className="flex items-start gap-2 rounded-md px-2 py-1.5 hover:bg-muted/60">
														<Checkbox
															checked={selectedPorts.includes(id)}
															onCheckedChange={() => toggleSelected(setSelectedPorts, id)}
														/>
														<span className="min-w-0">
															<span className="block truncate text-sm font-medium">{portLabel(port)}</span>
															<span className="block truncate text-xs text-muted-foreground">
																{port.deviceName} · {port.OperStatus ?? port.oper_status ?? "unknown"}
															</span>
														</span>
													</div>
												)
											})
										)}
									</div>
								) : (
									<div className="text-sm text-muted-foreground">
										<Trans>Using every discovered port under the selected targets.</Trans>
									</div>
								)}
							</div>
						) : null}
						<div className="flex items-center justify-between gap-3">
							<div className="text-sm text-muted-foreground">
								{selectedTargets.length} <Trans>targets</Trans> · {selectedMetrics.length} <Trans>metrics</Trans>
							</div>
							<div className="flex items-center gap-2">
								<Button
									variant="outline"
									onClick={() => downloadAggregateCSV(series, trafficView)}
									disabled={series.length === 0}
								>
									<DownloadIcon className="me-2 h-4 w-4" />
									<Trans>Export CSV</Trans>
								</Button>
								<Button
									variant="outline"
									onClick={saveAsGraph}
									disabled={savingGraph || selectedMetrics.length === 0 || selectedTargets.length === 0}
								>
									<SaveIcon className="me-2 h-4 w-4" />
									<Trans>Save as graph</Trans>
								</Button>
								<Button onClick={refreshChart} disabled={chartLoading || loading}>
									<BarChart3Icon className="me-2 h-4 w-4" />
									<Trans>Refresh</Trans>
								</Button>
							</div>
						</div>
					</div>

					<div className="rounded-md border border-border p-4">
						{error ? <div className="pb-3 text-sm text-destructive">{error}</div> : null}
						{chartLoading ? (
							<div className="pb-3 text-sm text-muted-foreground">
								<Trans>Loading...</Trans>
							</div>
						) : null}
						<AggregateLineChart series={series} trafficView={trafficView} />
						{series.length > 0 ? (
							<div className="pt-2 text-xs text-muted-foreground">
								{series.length} <Trans>series</Trans> · {series.reduce((count, item) => count + item.values.length, 0)}{" "}
								<Trans>points</Trans>
							</div>
						) : null}
						{!chartLoading && !error && series.length === 0 ? (
							<div className="text-sm text-muted-foreground">
								<Trans>No aggregate samples are available for the current selection.</Trans>
							</div>
						) : null}
					</div>
				</div>
			</div>
		</div>
	)
})

function AggregateLineChart({ series, trafficView }: { series: QuerySeries[]; trafficView: TrafficViewMode }) {
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createLineChart> | null>(null)
	const pointCount = series.reduce((count, item) => count + item.values.length, 0)

	useEffect(() => {
		if (!chartRef.current || pointCount === 0) {
			disposeChart(chartInstance.current)
			chartInstance.current = null
			return
		}
		chartInstance.current = updateLineChart(chartInstance.current, chartRef.current, {
			series,
			yFormatter: (value) => formatValue(value ?? 0, series[0]?.unit, trafficView),
		})
		return () => {
			disposeChart(chartInstance.current)
			chartInstance.current = null
		}
	}, [pointCount, series, trafficView])

	return (
		<div className="relative min-h-[440px]">
			<div ref={chartRef} className="h-[440px] w-full" />
			{series.length > 0 && pointCount === 0 ? (
				<div className="absolute inset-0 grid place-items-center text-sm text-muted-foreground">
					<Trans>No metric samples found for this range.</Trans>
				</div>
			) : null}
		</div>
	)
}

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="mb-2 text-sm font-medium">{title}</div>
			{children}
		</div>
	)
}

async function queryAggregateMetric({
	metric,
	aggregate,
	trafficView,
	valueMode,
	targetIDs,
	portIDs,
	timeParams,
}: {
	metric: string
	unit: string
	aggregate: AggregateMethod
	trafficView: TrafficViewMode
	valueMode: string
	targetIDs: string[]
	portIDs: string[]
	timeParams: URLSearchParams
}) {
	const params = new URLSearchParams(timeParams)
	params.set("metric", metric)
	params.set("aggregate", aggregate)
	params.set("traffic_view", trafficView)
	if (valueMode) {
		params.set("value_mode", valueMode)
	}
	if (portIDs.length > 0) {
		params.set("port_ids", portIDs.join(","))
	} else {
		params.set("target_ids", targetIDs.join(","))
	}
	const data = await pb.send<VMRangeResponse>(`/api/v1/metrics/aggregate?${params.toString()}`, {})
	return vmSeries(data)
}

function downloadAggregateCSV(series: QuerySeries[], trafficView: TrafficViewMode) {
	if (series.length === 0) {
		return
	}
	const times = Array.from(new Set(series.flatMap((item) => item.values.map((point) => point.time)))).sort(
		(left, right) => left - right
	)
	const valueBySeries = series.map((item) => new Map(item.values.map((point) => [point.time, point.value])))
	const rows = [
		["timestamp", ...series.map((item) => item.name)],
		...times.map((time) => [
			new Date(time).toISOString(),
			...valueBySeries.map((values) => {
				const value = values.get(time)
				return value === undefined ? "" : String(value)
			}),
		]),
	]
	const csv = rows.map((row) => row.map(escapeCSV).join(",")).join("\n")
	const blob = new Blob([csv], { type: "text/csv;charset=utf-8" })
	const url = URL.createObjectURL(blob)
	const link = document.createElement("a")
	link.href = url
	link.download = `watchdog-${trafficView}-aggregate-${new Date().toISOString().replaceAll(":", "")}.csv`
	document.body.append(link)
	link.click()
	link.remove()
	URL.revokeObjectURL(url)
}

function escapeCSV(value: string) {
	if (/[",\n]/.test(value)) {
		return `"${value.replaceAll('"', '""')}"`
	}
	return value
}

function createAggregateGraphID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `aggr_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

// One entry per returned series, so a value_mode=both response renders raw
// and corrected as separate lines instead of silently dropping one.
function vmSeries(response: VMRangeResponse) {
	return (response.data?.result ?? []).map((entry) => ({
		mode: entry.metric?.value_mode,
		values: (entry.values ?? [])
			.map(([time, value]) => ({
				time: Math.round(time * 1000),
				value: Number(value),
			}))
			.filter((point) => Number.isFinite(point.time) && Number.isFinite(point.value)),
	}))
}

function toggleSelected(setter: React.Dispatch<React.SetStateAction<string[]>>, id: string) {
	setter((current) => (current.includes(id) ? current.filter((item) => item !== id) : [...current, id]))
}

function targetID(target: TargetRecord) {
	return target.ID ?? target.id ?? ""
}

function targetType(target: TargetRecord) {
	return target.Type ?? target.target_type ?? ""
}

function deviceID(device: NetworkDevice) {
	return device.ID ?? device.id ?? ""
}

function deviceTargetID(device: NetworkDevice) {
	return device.TargetID ?? device.target_id ?? ""
}

function portID(port: NetworkPort) {
	return port.ID ?? port.id ?? ""
}

function portLabel(port: NetworkPort) {
	const name = port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? portID(port)
	const alias = port.IfAlias ?? port.if_alias
	return alias ? `${name} · ${alias}` : name
}

function metricName(metric: MetricDefinition) {
	return metric.Name ?? metric.name ?? ""
}

function metricFamily(metric: MetricDefinition) {
	return metric.Family ?? metric.family ?? ""
}

function metricScope(metric: MetricDefinition) {
	return metric.Scope ?? metric.scope ?? ""
}

function metricUnit(metric: MetricDefinition) {
	return metric.Unit ?? metric.unit ?? ""
}

function labelForMetric(metric: MetricDefinition) {
	return metric.Description ?? metric.description ?? metricName(metric)
}

function fallbackMetricDefinition(name: string): MetricDefinition {
	const scope = name.includes("_if_") || name.includes("_optical_") ? "port" : "target"
	const unit = name.endsWith("_bps") ? "bps" : name.includes("percent") ? "percent" : ""
	return { Name: name, Description: name, Scope: scope, Unit: unit }
}

function formatValue(value: number, unit: string | undefined, trafficView: TrafficViewMode) {
	if (unit === "bps") {
		return formatBitsPerSecond(value, trafficViewRateBase(trafficView))
	}
	return formatMetricValue(value, unit)
}

function defaultDateTime(offsetHours: number) {
	const date = new Date(Date.now() + offsetHours * 60 * 60 * 1000)
	return toDateTimeLocal(date)
}

function toDateTimeLocal(date: Date) {
	const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
	return local.toISOString().slice(0, 16)
}

function toISOString(value: string) {
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toISOString()
}
