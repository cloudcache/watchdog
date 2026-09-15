import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import {
	BarChart3Icon,
	ChevronDownIcon,
	ChevronRightIcon,
	DownloadIcon,
	RefreshCwIcon,
	SaveIcon,
	SearchIcon,
	SettingsIcon,
} from "lucide-react"
import type React from "react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { TrafficViewSwitcher } from "@/components/traffic-view-switcher"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { isAdmin, api } from "@/lib/api"
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
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [portsByDevice, setPortsByDevice] = useState<Record<string, NetworkPort[]>>({})
	const [metrics, setMetrics] = useState<MetricDefinition[]>([])
	const [trafficView, setTrafficView] = useState<TrafficViewMode>("customer")
	const [selectedMetrics, setSelectedMetrics] = useState<string[]>(defaultMetrics)
	const [selectedPorts, setSelectedPorts] = useState<string[]>([])
	const [expandedDevices, setExpandedDevices] = useState<Record<string, boolean>>({})
	const [portSearch, setPortSearch] = useState("")
	const [graphName, setGraphName] = useState("")
	const [advancedOpen, setAdvancedOpen] = useState(false)
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
	const hasPortMetrics =
		selectedMetricDefs.length > 0
			? selectedMetricDefs.some((metric) => metricScope(metric) === "port")
			: selectedMetrics.some((name) => fallbackMetricDefinition(name).Scope === "port")
	const allPortIDs = useMemo(
		() =>
			devices
				.flatMap((device) => portsByDevice[deviceID(device)] ?? [])
				.map(portID)
				.filter(Boolean),
		[devices, portsByDevice]
	)
	const allTargetIDs = useMemo(() => Array.from(new Set(devices.map(deviceTargetID).filter(Boolean))), [devices])

	const refreshCatalog = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [deviceData, metricData] = await Promise.all([
				api.send<{ items?: NetworkDevice[] }>("/api/v1/devices", { query: { kind: "network" } }),
				api.send<{ items?: MetricDefinition[] }>("/api/v1/metrics/catalog", {}),
			])
			const nextDevices = deviceData.items ?? []
			setDevices(nextDevices)
			setMetrics(metricData.items ?? [])
			const loadedPorts = await Promise.all(
				nextDevices.map(async (device) => {
					const id = deviceID(device)
					const data = await api
						.send<{ items?: NetworkPort[] }>(`/api/v1/devices/${id}/ports`, {})
						.catch(() => ({ items: [] as NetworkPort[] }))
					return [id, data.items ?? []] as const
				})
			)
			setPortsByDevice(Object.fromEntries(loadedPorts))
			// A single-device install starts with its port list open.
			if (nextDevices.length === 1) {
				setExpandedDevices({ [deviceID(nextDevices[0])]: true })
			}
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load aggregate chart inputs`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Aggregate Charts`} / Watchdog`
		refreshCatalog()
	}, [refreshCatalog, t])

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
		if (selectedMetrics.length === 0) {
			setError(t`Select at least one metric`)
			return
		}
		if (timeMode === "custom" && (!start || !end)) {
			setError(t`Select a custom start and end time`)
			return
		}
		setChartLoading(true)
		setError("")
		try {
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
				// No explicit selection means "aggregate everything we discovered".
				const queryPortIDs = scope === "port" ? (selectedPorts.length > 0 ? selectedPorts : allPortIDs) : []
				if (scope === "port" && queryPortIDs.length === 0) {
					throw new Error(t`No ports discovered yet`)
				}
				const metricSeries = await queryAggregateMetric({
					metric: name,
					unit,
					aggregate,
					trafficView,
					valueMode,
					targetIDs: allTargetIDs,
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
		allPortIDs,
		allTargetIDs,
		buildTimeParams,
		selectedPorts,
		selectedMetricDefs,
		selectedMetrics,
		t,
		trafficView,
		timeMode,
		valueMode,
		start,
		end,
	])

	const saveAsGraph = useCallback(async () => {
		if (selectedMetrics.length === 0) {
			return
		}
		setSavingGraph(true)
		setError("")
		try {
			// An empty selection saves every discovered port, matching the chart.
			const portIDs = hasPortMetrics ? (selectedPorts.length > 0 ? selectedPorts : allPortIDs) : []
			const firstDevice = devices[0]
			const fallbackName = `${firstDevice ? deviceLabel(firstDevice) : "Aggregate"} aggregate`
			const name = graphName.trim() || fallbackName
			const graphID = createAggregateGraphID()
			await api.send(`/api/v1/aggregate-graphs`, {
				method: "POST",
				body: {
					id: graphID,
					Name: name,
					Aggregation: aggregate,
					ValueMode: valueMode,
					Unit: selectedMetrics.every((metric) => metric.endsWith("_bps")) ? "bps" : "",
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
			await api.send(`/api/v1/aggregate-graphs/${graphID}/items`, { method: "PUT", body: { items } })
			if (portIDs.length > 0) {
				await api.send(`/api/v1/aggregate-graphs/${graphID}/ports`, {
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
		allPortIDs,
		devices,
		graphName,
		hasPortMetrics,
		selectedMetrics,
		selectedPorts,
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

			<div className="flex flex-wrap items-center gap-3">
				<TrafficViewSwitcher value={trafficView} onChange={applyTrafficView} allowRaw={isAdmin()} />
				<div className="flex items-center gap-2">
					<span className="text-sm text-muted-foreground">
						<Trans>Time range</Trans>
					</span>
					<Select
						value={timeMode === "fixed" ? window : timeMode}
						onValueChange={(value) => {
							if (value === "realtime" || value === "custom") {
								setTimeMode(value)
							} else {
								setTimeMode("fixed")
								setWindow(value)
							}
						}}
					>
						<SelectTrigger className="w-36">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="realtime">
								<Trans>Realtime</Trans>
							</SelectItem>
							<SelectItem value="15m">15m</SelectItem>
							<SelectItem value="1h">1h</SelectItem>
							<SelectItem value="6h">6h</SelectItem>
							<SelectItem value="24h">24h</SelectItem>
							<SelectItem value="7d">7d</SelectItem>
							<SelectItem value="30d">30d</SelectItem>
							<SelectItem value="custom">
								<Trans>Custom</Trans>
							</SelectItem>
						</SelectContent>
					</Select>
				</div>
				{timeMode === "custom" ? (
					<div className="flex items-center gap-2">
						<Input
							type="datetime-local"
							className="w-52"
							value={start}
							onChange={(event) => setStart(event.target.value)}
						/>
						<span className="text-muted-foreground">→</span>
						<Input
							type="datetime-local"
							className="w-52"
							value={end}
							onChange={(event) => setEnd(event.target.value)}
						/>
					</div>
				) : null}
				<Button variant="ghost" size="sm" onClick={() => setAdvancedOpen((open) => !open)}>
					<SettingsIcon className="me-1.5 h-4 w-4" />
					<Trans>Advanced</Trans>
					{advancedOpen ? (
						<ChevronDownIcon className="ms-1 h-3.5 w-3.5" />
					) : (
						<ChevronRightIcon className="ms-1 h-3.5 w-3.5" />
					)}
				</Button>
				<span className="ms-auto text-xs text-muted-foreground">
					{trafficViewLabel(trafficView)} · {aggregate} · {valueMode}
				</span>
			</div>

			{advancedOpen ? (
				<div className="grid gap-4 rounded-md border border-border p-4">
					<div className="grid gap-3 sm:grid-cols-3">
						<LabeledControl label={t`Aggregation`}>
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
						</LabeledControl>
						<LabeledControl label={t`Step`}>
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
						</LabeledControl>
						<LabeledControl label={t`Value mode`}>
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
						</LabeledControl>
					</div>
					<div>
						<div className="mb-2 text-sm font-medium">
							<Trans>Metrics</Trans>
							<span className="ms-2 text-xs font-normal text-muted-foreground">
								<Trans>Traffic in/out is preselected; pick others only when needed.</Trans>
							</span>
						</div>
						<div className="grid max-h-[240px] gap-1 overflow-auto pe-1 sm:grid-cols-2">
							{metrics.map((metric) => {
								const name = metricName(metric)
								return (
									<button
										type="button"
										key={name}
										className="flex cursor-pointer items-start gap-2 rounded-md px-2 py-1.5 text-start hover:bg-muted/60"
										onClick={() => toggleSelected(setSelectedMetrics, name)}
									>
										<Checkbox className="pointer-events-none" checked={selectedMetrics.includes(name)} />
										<span className="min-w-0">
											<span className="block truncate text-sm">{labelForMetric(metric)}</span>
											<span className="block truncate text-xs text-muted-foreground">
												{metricFamily(metric)} · {metricScope(metric)} · {metricUnit(metric) || "value"}
											</span>
										</span>
									</button>
								)
							})}
						</div>
					</div>
				</div>
			) : null}

			<div className="grid gap-4 lg:grid-cols-[340px_1fr]">
				<PortPicker
					devices={devices}
					portsByDevice={portsByDevice}
					selectedPorts={selectedPorts}
					setSelectedPorts={setSelectedPorts}
					expandedDevices={expandedDevices}
					setExpandedDevices={setExpandedDevices}
					search={portSearch}
					setSearch={setPortSearch}
					disabled={!hasPortMetrics}
				/>

				<div className="grid content-start gap-4">
					<div className="flex flex-wrap items-center gap-2 rounded-md border border-border p-3">
						<Input
							className="w-64"
							placeholder={t`Graph name`}
							value={graphName}
							onChange={(event) => setGraphName(event.target.value)}
						/>
						<Button variant="outline" onClick={saveAsGraph} disabled={savingGraph || selectedMetrics.length === 0}>
							<SaveIcon className="me-2 h-4 w-4" />
							<Trans>Save as graph</Trans>
						</Button>
						<Button
							variant="outline"
							onClick={() => downloadAggregateCSV(series, trafficView)}
							disabled={series.length === 0}
						>
							<DownloadIcon className="me-2 h-4 w-4" />
							<Trans>Export CSV</Trans>
						</Button>
						<Button onClick={refreshChart} disabled={chartLoading || loading}>
							<BarChart3Icon className="me-2 h-4 w-4" />
							<Trans>Refresh</Trans>
						</Button>
						<span className="ms-auto text-sm text-muted-foreground">
							{selectedPorts.length > 0 ? (
								<Trans>{selectedPorts.length} ports selected</Trans>
							) : (
								<Trans>All discovered ports</Trans>
							)}
						</span>
					</div>

					<div className="rounded-md border border-border p-4">
						{error ? <div className="api-3 text-sm text-destructive">{error}</div> : null}
						{chartLoading ? (
							<div className="api-3 text-sm text-muted-foreground">
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

function PortPicker({
	devices,
	portsByDevice,
	selectedPorts,
	setSelectedPorts,
	expandedDevices,
	setExpandedDevices,
	search,
	setSearch,
	disabled,
}: {
	devices: NetworkDevice[]
	portsByDevice: Record<string, NetworkPort[]>
	selectedPorts: string[]
	setSelectedPorts: React.Dispatch<React.SetStateAction<string[]>>
	expandedDevices: Record<string, boolean>
	setExpandedDevices: React.Dispatch<React.SetStateAction<Record<string, boolean>>>
	search: string
	setSearch: (value: string) => void
	disabled: boolean
}) {
	const { t } = useLingui()
	const query = search.trim().toLowerCase()
	const selected = new Set(selectedPorts)

	const groups = devices
		.map((device) => {
			const id = deviceID(device)
			const label = deviceLabel(device)
			const ports = portsByDevice[id] ?? []
			const matching = query
				? label.toLowerCase().includes(query)
					? ports
					: ports.filter((port) => portSearchText(port).includes(query))
				: ports
			return { device, id, label, ports, matching }
		})
		.filter((group) => group.matching.length > 0 || !query)

	const toggleDevice = (portIDs: string[], allSelected: boolean) => {
		setSelectedPorts((current) => {
			const next = new Set(current)
			for (const id of portIDs) {
				if (allSelected) {
					next.delete(id)
				} else {
					next.add(id)
				}
			}
			return Array.from(next)
		})
	}

	return (
		<div className="grid content-start gap-3 rounded-md border border-border p-3">
			<div className="flex items-center justify-between">
				<div className="text-sm font-medium">
					<Trans>Ports</Trans>
				</div>
				{selectedPorts.length > 0 ? (
					<button
						type="button"
						className="text-xs text-muted-foreground underline-offset-2 hover:underline"
						onClick={() => setSelectedPorts([])}
					>
						<Trans>Clear selection</Trans>
					</button>
				) : null}
			</div>
			{disabled ? (
				<div className="text-sm text-muted-foreground">
					<Trans>Selected metrics do not aggregate per port.</Trans>
				</div>
			) : (
				<>
					<div className="relative">
						<SearchIcon className="pointer-events-none absolute start-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
						<Input
							className="ps-8"
							placeholder={t`Search ports...`}
							value={search}
							onChange={(event) => setSearch(event.target.value)}
						/>
					</div>
					<div className="text-xs text-muted-foreground">
						{selectedPorts.length > 0 ? (
							<Trans>{selectedPorts.length} ports selected</Trans>
						) : (
							<Trans>Nothing selected: all ports are aggregated.</Trans>
						)}
					</div>
					<div className="grid max-h-[520px] gap-1 overflow-auto pe-1">
						{groups.length === 0 ? (
							<div className="px-2 py-1 text-sm text-muted-foreground">
								<Trans>No ports match the search.</Trans>
							</div>
						) : null}
						{groups.map((group) => {
							const visiblePorts = group.matching
							const groupIDs = visiblePorts.map(portID).filter(Boolean)
							const selectedCount = groupIDs.filter((id) => selected.has(id)).length
							const allSelected = groupIDs.length > 0 && selectedCount === groupIDs.length
							const expanded = query ? true : (expandedDevices[group.id] ?? false)
							return (
								<div key={group.id}>
									<div className="flex items-center gap-1.5 rounded-md px-1 py-1.5 hover:bg-muted/60">
										<Checkbox
											checked={allSelected ? true : selectedCount > 0 ? "indeterminate" : false}
											onCheckedChange={() => toggleDevice(groupIDs, allSelected)}
										/>
										<button
											type="button"
											className="flex min-w-0 flex-1 items-center gap-1 text-start"
											onClick={() =>
												setExpandedDevices((current) => ({ ...current, [group.id]: !(current[group.id] ?? false) }))
											}
										>
											{expanded ? (
												<ChevronDownIcon className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
											) : (
												<ChevronRightIcon className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
											)}
											<span className="truncate text-sm font-medium">{group.label}</span>
											<span className="ms-auto shrink-0 text-xs text-muted-foreground">
												{selectedCount > 0 ? `${selectedCount}/` : ""}
												{groupIDs.length}
											</span>
										</button>
									</div>
									{expanded
										? visiblePorts.map((port) => {
												const id = portID(port)
												const oper = port.OperStatus ?? port.oper_status ?? ""
												return (
													<button
														type="button"
														key={id}
														className="ms-5 flex w-[calc(100%-1.25rem)] cursor-pointer items-center gap-2 rounded-md px-2 py-1 text-start hover:bg-muted/60"
														onClick={() => toggleSelected(setSelectedPorts, id)}
													>
														<Checkbox className="pointer-events-none" checked={selected.has(id)} />
														<span
															className={`h-1.5 w-1.5 shrink-0 rounded-full ${oper === "up" ? "bg-green-500" : "bg-muted-foreground/40"}`}
														/>
														<span className="min-w-0">
															<span className="block truncate text-sm">{portName(port)}</span>
															{portAlias(port) ? (
																<span className="block truncate text-xs text-muted-foreground">{portAlias(port)}</span>
															) : null}
														</span>
													</button>
												)
											})
										: null}
								</div>
							)
						})}
					</div>
				</>
			)}
		</div>
	)
}

function LabeledControl({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<span className="text-xs text-muted-foreground">{label}</span>
			{children}
		</div>
	)
}

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
	const data = await api.send<VMRangeResponse>(`/api/v1/metrics/aggregate?${params.toString()}`, {})
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

function deviceID(device: NetworkDevice) {
	return device.ID ?? device.id ?? ""
}

function deviceTargetID(device: NetworkDevice) {
	return device.TargetID ?? device.target_id ?? ""
}

function deviceLabel(device: NetworkDevice) {
	return device.SysName ?? device.sys_name ?? device.Name ?? device.name ?? deviceID(device)
}

function portID(port: NetworkPort) {
	return port.ID ?? port.id ?? ""
}

function portName(port: NetworkPort) {
	return port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? portID(port)
}

function portAlias(port: NetworkPort) {
	return port.IfAlias ?? port.if_alias ?? ""
}

function portSearchText(port: NetworkPort) {
	return `${portName(port)} ${portAlias(port)} ${port.IfDescr ?? port.if_descr ?? ""}`.toLowerCase()
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
