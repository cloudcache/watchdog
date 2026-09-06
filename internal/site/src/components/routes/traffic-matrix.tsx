import { Trans, useLingui } from "@lingui/react/macro"
import { BarChart3Icon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { createFlowExplorerChart, type FlowGraphType } from "@/lib/flow-explorer-chart"
import {
	buildFlowJointSeries,
	buildFlowSeries,
	FLOW_TIME_PRESETS,
	mergeFlowFilters,
	parseFlowFilter,
	resolveFlowTimeRange,
	type FlowFilters,
	type FlowJointPoint,
	type FlowPlan,
	type FlowPoint,
	type FlowSeries,
} from "@/lib/flow-explorer-model"
import { pb } from "@/lib/api"
import { disposeChart } from "@/lib/vchart"

type FlowAggregateResult = {
	points: (FlowPoint | FlowJointPoint)[]
	metric: { name: string; unit: string }
	dimension?: { kind: string; additive: boolean }
	dimensions?: { kind: string; additive: boolean }[]
	plan?: FlowPlan
	rollup_completeness?: { expected_buckets: number; covered_buckets: number; ratio: number; complete: boolean }
	mixed_versions: boolean
	version_count: number
}

type FlowQueryResponse = {
	data: FlowAggregateResult
	meta: {
		unit?: string
		timezone?: string
		step_seconds?: number
		completeness?: { complete_ratio: number; partial: boolean; unknown_ratio: number; warnings?: string[] }
	}
}

type AddressSetItem = { id: string; name: string }
type AddressSetList = { items: AddressSetItem[] }
type DeviceItem = { ID?: string; id?: string; SysName?: string; sys_name?: string; Name?: string; name?: string }
type DeviceList = { items: DeviceItem[] }

type GraphMode = FlowGraphType | "table"

const GRAPH_OPTIONS: { value: GraphMode; label: string }[] = [
	{ value: "lines", label: "Lines" },
	{ value: "stacked", label: "Stacked area" },
	{ value: "heatmap", label: "Heatmap" },
	{ value: "table", label: "Table only" },
]

const SANKEY_OPTION: { value: GraphMode; label: string } = { value: "sankey", label: "Sankey" }

const METRIC_OPTIONS = [
	{ value: "estimated_bps", label: "Estimated L3 bit rate" },
	{ value: "raw_bps", label: "Sampled L3 bit rate" },
	{ value: "estimated_pps", label: "Estimated packet rate" },
	{ value: "raw_pps", label: "Sampled packet rate" },
	{ value: "estimated_bytes", label: "Estimated bytes" },
	{ value: "raw_bytes", label: "Sampled bytes" },
	{ value: "estimated_packets", label: "Estimated packets" },
	{ value: "raw_packets", label: "Sampled packets" },
	{ value: "received_records", label: "Received Flow records" },
]

const DIMENSION_OPTIONS = [
	{ value: "total", label: "Total" },
	{ value: "category", label: "Traffic category" },
	{ value: "business", label: "Business" },
	{ value: "geo.continent", label: "Continent" },
	{ value: "geo.region", label: "Region" },
	{ value: "geo.country", label: "Country" },
	{ value: "geo.province", label: "Province" },
	{ value: "geo.city", label: "City" },
	{ value: "isp", label: "ISP" },
	{ value: "asn", label: "ASN" },
	{ value: "local_prefix", label: "Local prefix" },
	{ value: "remote_prefix", label: "Remote prefix" },
	{ value: "address_set", label: "Address set" },
	{ value: "src_ip", label: "Source IP" },
	{ value: "dst_ip", label: "Destination IP" },
	{ value: "remote_port", label: "Remote port" },
	{ value: "protocol", label: "IP protocol" },
	{ value: "observation_interface", label: "Observation interface" },
]

const POINT_OPTIONS = [100, 200, 300, 500, 1000]

function queryState(name: string, fallback: string) {
	if (typeof window === "undefined") return fallback
	return new URLSearchParams(window.location.search).get(name) || fallback
}

export default memo(function TrafficMatrix() {
	const { t } = useLingui()
	const [timeRange, setTimeRange] = useState(() => queryState("range", "1h"))
	const [customStart, setCustomStart] = useState(() => queryState("start", ""))
	const [customEnd, setCustomEnd] = useState(() => queryState("end", ""))
	const [metric, setMetric] = useState(() => queryState("metric", "estimated_bps"))
	const [dimension, setDimension] = useState(() => queryState("dimension", "category"))
	const [dimension2, setDimension2] = useState(() => queryState("dimension2", "none"))
	const [dimension3, setDimension3] = useState(() => queryState("dimension3", "none"))
	const [dimension4, setDimension4] = useState(() => queryState("dimension4", "none"))
	const [graphMode, setGraphMode] = useState<GraphMode>(() => queryState("graph", "lines") as GraphMode)
	const [targetPoints, setTargetPoints] = useState(() => Number(queryState("points", "300")))
	const [topN, setTopN] = useState(() => Number(queryState("top", "20")))
	const [includeOther, setIncludeOther] = useState(() => queryState("other", "1") !== "0")
	const [filterExpression, setFilterExpression] = useState(() => queryState("filter", ""))
	const [addressSets, setAddressSets] = useState<AddressSetItem[]>([])
	const [devices, setDevices] = useState<DeviceItem[]>([])
	const [selectedSet, setSelectedSet] = useState(() => queryState("set", "all"))
	const [selectedDevice, setSelectedDevice] = useState(() => queryState("device", "all"))
	const [series, setSeries] = useState<FlowSeries[]>([])
	const [response, setResponse] = useState<FlowQueryResponse | null>(null)
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createFlowExplorerChart> | null>(null)
	const initialQuery = useRef(false)

	useEffect(() => {
		Promise.all([
			pb.send<AddressSetList>("/api/v1/address-sets", {}),
			pb.send<DeviceList>("/api/v1/network/devices", {}),
		])
			.then(([sets, devs]) => {
				setAddressSets(sets.items ?? [])
				setDevices(devs.items ?? [])
			})
			.catch(() => {})
	}, [])

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const { start, end } = resolveFlowTimeRange(timeRange, customStart, customEnd)
			const selectedDimension = selectedSet === "all" ? dimension : "address_set"
			const selectedDimensions = [selectedDimension]
			const jointAllowed = selectedSet === "all" && selectedDimension !== "total" && selectedDimension !== "address_set"
			if (jointAllowed && dimension2 !== "none") selectedDimensions.push(dimension2)
			if (jointAllowed && dimension2 !== "none" && dimension3 !== "none") selectedDimensions.push(dimension3)
			if (jointAllowed && dimension2 !== "none" && dimension3 !== "none" && dimension4 !== "none")
				selectedDimensions.push(dimension4)
			const joint = selectedDimensions.length > 1
			const selectedTopN = selectedDimension === "total" ? 1 : Math.max(1, Math.min(100, topN))
			const expressionFilters = parseFlowFilter(filterExpression)
			const selectionFilters: FlowFilters = {}
			if (selectedDevice !== "all") selectionFilters.device_ids = [selectedDevice]
			if (selectedSet !== "all") selectionFilters.dimension_values = [selectedSet]
			const filters = mergeFlowFilters(expressionFilters, selectionFilters)
			const grouping = joint ? { dimensions: selectedDimensions } : { dimension: selectedDimension }
			const query = await pb.send<FlowQueryResponse>("/api/v1/query", {
				method: "POST",
				body: {
					dataset: "flow.traffic",
					from: start,
					to: end,
					// Zero means auto: the Flow provider chooses graph interval and
					// physical 1m/1h source independently from the selected range.
					step_seconds: 0,
					limit: 250_000,
					value_layer: "customer",
					parameters: {
						metric,
						...grouping,
						filters,
						top_n: selectedTopN,
						include_other: selectedDimension !== "total" && selectedDimension !== "address_set" && includeOther,
						target_points: targetPoints,
						timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
					},
				},
			})
			setResponse(query)
			const queryUnit = query.data?.metric?.unit ?? query.meta?.unit ?? ""
			setSeries(
				query.data?.dimensions
					? buildFlowJointSeries((query.data.points ?? []) as FlowJointPoint[], query.data.plan, queryUnit)
					: buildFlowSeries((query.data?.points ?? []) as FlowPoint[], query.data?.plan, queryUnit)
			)
			persistQueryState({
				range: timeRange,
				start: timeRange === "custom" ? customStart : "",
				end: timeRange === "custom" ? customEnd : "",
				metric,
				dimension,
				dimension2,
				dimension3,
				dimension4,
				graph: graphMode,
				points: String(targetPoints),
				top: String(selectedTopN),
				other: includeOther ? "1" : "0",
				filter: filterExpression,
				set: selectedSet,
				device: selectedDevice,
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load`)
		} finally {
			setLoading(false)
		}
	}, [
		timeRange,
		customStart,
		customEnd,
		metric,
		dimension,
		dimension2,
		dimension3,
		dimension4,
		graphMode,
		targetPoints,
		topN,
		includeOther,
		filterExpression,
		selectedSet,
		selectedDevice,
		t,
	])

	useEffect(() => {
		if (initialQuery.current) return
		initialQuery.current = true
		refresh().catch(() => {})
	}, [refresh])

	const jointSelected =
		selectedSet === "all" && dimension !== "total" && dimension !== "address_set" && dimension2 !== "none"
	useEffect(() => {
		if (!jointSelected && graphMode === "sankey") setGraphMode("lines")
	}, [graphMode, jointSelected])
	useEffect(() => {
		if (dimension2 === dimension) {
			setDimension2("none")
			setDimension3("none")
			setDimension4("none")
		} else if (dimension3 === dimension || dimension3 === dimension2) {
			setDimension3("none")
			setDimension4("none")
		} else if (dimension4 === dimension || dimension4 === dimension2 || dimension4 === dimension3) {
			setDimension4("none")
		}
	}, [dimension, dimension2, dimension3, dimension4])

	const unit = response?.data?.metric?.unit ?? response?.meta?.unit ?? ""
	const mixedVersions = response?.data?.mixed_versions ?? false
	const formatValue = useCallback((value: number) => formatFlowValue(value, unit), [unit])

	useEffect(() => {
		disposeChart(chartInstance.current)
		chartInstance.current = null
		if (chartRef.current && graphMode !== "table" && series.length > 0) {
			chartInstance.current = createFlowExplorerChart(
				chartRef.current,
				graphMode,
				series.map((item) => ({ ...item, name: mixedVersions ? item.name : item.label })),
				formatValue
			)
		}
		return () => {
			disposeChart(chartInstance.current)
			chartInstance.current = null
		}
	}, [formatValue, graphMode, mixedVersions, series])

	const records = useMemo(
		() =>
			series
				.slice()
				.sort((a, b) => b.maximum - a.maximum)
				.map((item) => ({
					dimension: mixedVersions ? item.name : item.label,
					last: formatFlowValue(item.last, unit),
					average: formatFlowValue(item.average, unit),
					p95: formatFlowValue(item.p95, unit),
					maximum: formatFlowValue(item.maximum, unit),
					minimum: formatFlowValue(item.minimum, unit),
					total: formatFlowTotal(item.total, unit),
					records: item.receivedRecords.toLocaleString(),
					unknown: ratio(item.unknownSamplingRecords, item.receivedRecords),
					quality: ratio(item.qualityRecords, item.receivedRecords),
					searchText: `${item.name} ${item.last} ${item.average} ${item.p95}`.toLowerCase(),
				})),
		[mixedVersions, series, unit]
	)

	const columns = useMemo(
		() => [
			{ field: "dimension", title: t`Dimension value`, width: 280, style: denseCellStyle() },
			{ field: "last", title: t`Last`, width: 130, style: denseCellStyle() },
			{ field: "average", title: t`Average`, width: 130, style: denseCellStyle() },
			{ field: "p95", title: "95th", width: 130, style: denseCellStyle() },
			{ field: "maximum", title: t`Maximum`, width: 130, style: denseCellStyle() },
			{ field: "minimum", title: t`Minimum`, width: 130, style: denseCellStyle() },
			{ field: "total", title: t`Total`, width: 150, style: denseCellStyle() },
			{ field: "records", title: t`Records`, width: 120, style: denseCellStyle() },
			{ field: "unknown", title: t`Unknown sampling`, width: 150, style: denseCellStyle() },
			{ field: "quality", title: t`Quality flags`, width: 130, style: denseCellStyle() },
		],
		[t]
	)

	const plan = response?.data?.plan
	const completeness = response?.meta?.completeness
	const secondaryOptions = [
		{ value: "none", label: t`None` },
		...DIMENSION_OPTIONS.filter(
			(item) => item.value !== "total" && item.value !== "address_set" && item.value !== dimension
		),
	]
	const tertiaryOptions = [
		{ value: "none", label: t`None` },
		...DIMENSION_OPTIONS.filter(
			(item) =>
				item.value !== "total" && item.value !== "address_set" && item.value !== dimension && item.value !== dimension2
		),
	]
	const quaternaryOptions = [
		{ value: "none", label: t`None` },
		...DIMENSION_OPTIONS.filter(
			(item) =>
				item.value !== "total" &&
				item.value !== "address_set" &&
				item.value !== dimension &&
				item.value !== dimension2 &&
				item.value !== dimension3
		),
	]
	const graphOptions = jointSelected ? [...GRAPH_OPTIONS, SANKEY_OPTION] : GRAPH_OPTIONS

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<BarChart3Icon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Flow Explorer</Trans>
					</h1>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="grid gap-4 rounded-md border border-border bg-card p-4">
				<div className="flex flex-wrap items-end gap-3">
					<OptionSelect label={t`Metric`} value={metric} onChange={setMetric} options={METRIC_OPTIONS} width="w-56" />
					<OptionSelect
						label={t`Group by`}
						value={dimension}
						onChange={setDimension}
						options={DIMENSION_OPTIONS}
						width="w-52"
						disabled={selectedSet !== "all"}
					/>
					<OptionSelect
						label={t`Then by`}
						value={dimension2}
						onChange={(value) => {
							setDimension2(value)
							if (value === "none") {
								setDimension3("none")
								setDimension4("none")
							}
						}}
						options={secondaryOptions}
						width="w-52"
						disabled={selectedSet !== "all" || dimension === "total" || dimension === "address_set"}
					/>
					<OptionSelect
						label={t`Then by`}
						value={dimension3}
						onChange={(value) => {
							setDimension3(value)
							if (value === "none") setDimension4("none")
						}}
						options={tertiaryOptions}
						width="w-52"
						disabled={!jointSelected}
					/>
					<OptionSelect
						label={t`Then by`}
						value={dimension4}
						onChange={setDimension4}
						options={quaternaryOptions}
						width="w-52"
						disabled={!jointSelected || dimension3 === "none"}
					/>
					<OptionSelect
						label={t`Graph type`}
						value={graphMode}
						onChange={(value) => setGraphMode(value as GraphMode)}
						options={graphOptions}
						width="w-44"
					/>
					<OptionSelect
						label={t`Time range`}
						value={timeRange}
						onChange={setTimeRange}
						options={FLOW_TIME_PRESETS}
						width="w-48"
					/>
					<OptionSelect
						label={t`Target points`}
						value={String(targetPoints)}
						onChange={(value) => setTargetPoints(Number(value))}
						options={POINT_OPTIONS.map((value) => ({ value: String(value), label: String(value) }))}
						width="w-32"
					/>
					<div className="grid gap-1.5">
						<Label className="text-xs">
							<Trans>Top N</Trans>
						</Label>
						<Input
							type="number"
							min={1}
							max={100}
							value={topN}
							onChange={(event) => setTopN(Number(event.target.value))}
							disabled={dimension === "total"}
							className="w-24"
						/>
					</div>
					<label htmlFor="flow-include-other" className="flex h-9 items-center gap-2 text-sm">
						<Checkbox
							id="flow-include-other"
							checked={includeOther}
							disabled={dimension === "total" || dimension === "address_set"}
							onCheckedChange={(value) => setIncludeOther(value === true)}
						/>
						<Trans>Include Other</Trans>
					</label>
				</div>

				{timeRange === "custom" && (
					<div className="flex flex-wrap gap-3">
						<DateInput label={t`Start`} value={customStart} onChange={setCustomStart} />
						<DateInput label={t`End`} value={customEnd} onChange={setCustomEnd} />
					</div>
				)}

				<div className="flex flex-wrap items-end gap-3">
					<OptionSelect
						label={t`Device`}
						value={selectedDevice}
						onChange={setSelectedDevice}
						width="w-56"
						options={[
							{ value: "all", label: t`All devices` },
							...devices.map((item) => ({
								value: item.ID ?? item.id ?? "",
								label: item.SysName ?? item.sys_name ?? item.Name ?? item.name ?? item.ID ?? item.id ?? "",
							})),
						]}
					/>
					<OptionSelect
						label={t`Address set`}
						value={selectedSet}
						onChange={setSelectedSet}
						width="w-56"
						options={[
							{ value: "all", label: t`All sets` },
							...addressSets.map((item) => ({ value: item.id, label: item.name })),
						]}
					/>
					<div className="grid min-w-80 grow gap-1.5">
						<Label className="text-xs">
							<Trans>Filter</Trans>
						</Label>
						<Input
							value={filterExpression}
							onChange={(event) => setFilterExpression(event.target.value)}
							onKeyDown={(event) => {
								if ((event.ctrlKey || event.metaKey) && event.key === "Enter") refresh().catch(() => {})
							}}
							placeholder='direction IN (in, out) AND category=overseas AND business="cdn"'
						/>
					</div>
					<Button onClick={refresh} disabled={loading}>
						<Trans>Apply</Trans>
					</Button>
				</div>
				<p className="text-xs text-muted-foreground">
					<Trans>
						Filter fields: direction, category, business, target, device, exporter, dimension. Operators: = and IN
						(...). Press Ctrl/Cmd+Enter to apply.
					</Trans>
				</p>
				{jointSelected && (
					<p className="text-xs text-muted-foreground">
						<Trans>
							Multi-dimension and Sankey queries read true tuples from the same Flow facts and are synchronously limited
							to 24 hours. Longer ranges require a published asynchronous joint index.
						</Trans>
					</p>
				)}
			</div>

			{error && (
				<div className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive">
					{error}
				</div>
			)}

			<div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
				{plan && (
					<>
						<Badge variant="outline">
							{plan.source_seconds ? formatDuration(plan.source_seconds) : plan.source} source
						</Badge>
						<Badge variant="outline">{formatDuration(plan.step_seconds)} display</Badge>
						<span>{formatRange(plan.effective_from, plan.effective_to)}</span>
					</>
				)}
				{completeness && (
					<Badge variant={completeness.partial ? "warning" : "success"}>
						{(completeness.complete_ratio * 100).toFixed(1)}% complete
					</Badge>
				)}
				{response?.data?.mixed_versions && (
					<Badge variant="warning">
						<Trans>Mixed classification versions</Trans>
					</Badge>
				)}
				{completeness?.warnings?.map((warning) => (
					<span key={warning}>{warning}</span>
				))}
			</div>

			{graphMode !== "table" && (
				<div className="rounded-md border border-border bg-card p-4">
					{series.length === 0 && !loading ? (
						<div className="flex h-80 items-center justify-center text-sm text-muted-foreground">
							<Trans>No Flow points found</Trans>
						</div>
					) : null}
					<div ref={chartRef} className={series.length > 0 ? "h-[420px]" : "h-0"} />
				</div>
			)}

			<div className="overflow-hidden rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No Flow rows found`}
					searchPlaceholder={t`Search Flow results...`}
					height={420}
				/>
			</div>
		</div>
	)
})

function OptionSelect({
	label,
	value,
	onChange,
	options,
	width,
	disabled = false,
}: {
	label: string
	value: string
	onChange: (value: string) => void
	options: readonly { value: string; label: string }[]
	width: string
	disabled?: boolean
}) {
	return (
		<div className="grid gap-1.5">
			<Label className="text-xs">{label}</Label>
			<Select value={value} onValueChange={onChange} disabled={disabled}>
				<SelectTrigger className={width}>
					<SelectValue />
				</SelectTrigger>
				<SelectContent>
					{options
						.filter((item) => item.value)
						.map((item) => (
							<SelectItem key={item.value} value={item.value}>
								{item.label}
							</SelectItem>
						))}
				</SelectContent>
			</Select>
		</div>
	)
}

function DateInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
	return (
		<div className="grid gap-1.5">
			<Label className="text-xs">{label}</Label>
			<Input type="datetime-local" value={value} onChange={(event) => onChange(event.target.value)} className="w-52" />
		</div>
	)
}

function persistQueryState(values: Record<string, string>) {
	if (typeof window === "undefined") return
	const parameters = new URLSearchParams(window.location.search)
	for (const [key, value] of Object.entries(values)) {
		if (value) parameters.set(key, value)
		else parameters.delete(key)
	}
	window.history.replaceState({}, "", `${window.location.pathname}?${parameters.toString()}`)
}

function formatDuration(seconds: number) {
	if (seconds % 86_400 === 0) return `${seconds / 86_400}d`
	if (seconds % 3_600 === 0) return `${seconds / 3_600}h`
	return `${seconds / 60}m`
}

function formatRange(start: string, end: string) {
	return `${new Date(start).toLocaleString()} – ${new Date(end).toLocaleString()}`
}

function formatFlowValue(value: number, unit: string) {
	if (!Number.isFinite(value)) return "—"
	if (unit === "bits_per_second") return formatMagnitude(value, ["bps", "Kbps", "Mbps", "Gbps", "Tbps"], 1000)
	if (unit === "packets_per_second") return formatMagnitude(value, ["pps", "Kpps", "Mpps", "Gpps"], 1000)
	if (unit === "bytes") return formatMagnitude(value, ["B", "KiB", "MiB", "GiB", "TiB"], 1024)
	return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function formatFlowTotal(value: number, unit: string) {
	if (unit === "bits_per_second" || unit === "bytes")
		return formatMagnitude(value, ["B", "KiB", "MiB", "GiB", "TiB", "PiB"], 1024)
	return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function formatMagnitude(value: number, units: string[], base: number) {
	let current = Math.abs(value)
	let index = 0
	while (current >= base && index < units.length - 1) {
		current /= base
		index += 1
	}
	return `${value < 0 ? "-" : ""}${index === 0 ? current.toFixed(0) : current.toFixed(2)} ${units[index]}`
}

function ratio(value: number, total: number) {
	return total > 0 ? `${((value / total) * 100).toFixed(2)}%` : "—"
}

function denseCellStyle() {
	return { fontSize: 12, padding: [8, 10, 8, 10] as [number, number, number, number] }
}
