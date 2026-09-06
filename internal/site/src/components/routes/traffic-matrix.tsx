import { Trans, useLingui } from "@lingui/react/macro"
import { BarChart3Icon, DownloadIcon, RefreshCwIcon, SlidersHorizontalIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { FlowRecordTable } from "@/components/flow/flow-record-table"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { createFlowExplorerChart, type FlowGraphType } from "@/lib/flow-explorer-chart"
import {
	buildFlowJointSeries,
	buildFlowCSV,
	buildFlowQuickFilter,
	buildFlowSeries,
	FLOW_TIME_PRESETS,
	flowSurfacePreset,
	mergeFlowFilters,
	parseFlowFilter,
	resolveFlowTimeRange,
	type FlowFilters,
	type FlowFilterExpression,
	type FlowJointPoint,
	type FlowPlan,
	type FlowPoint,
	type FlowSeries,
	type FlowTrafficSurface,
} from "@/lib/flow-explorer-model"
import { pb } from "@/lib/api"
import { disposeChart } from "@/lib/vchart"

type FlowAggregateResult = {
	points: (FlowPoint | FlowJointPoint)[] | null
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
type GeoNode = {
	id: string
	kind: string
	code: string
	parent_id?: string
	name: string
	short_name?: string
	sort_order: number
	enabled: boolean
}
type NetworkOperator = {
	id: string
	code: string
	name: string
	short_name?: string
	asns: number[]
	sort_order: number
	enabled: boolean
}
type ListResponse<T> = { items?: T[] }

type GraphMode = FlowGraphType | "table"
type QuickAnalysisMode = "direction" | "protocol" | "src_ip" | "dst_ip" | "local_prefix" | "remote_prefix"
type QueryMode = QuickAnalysisMode | "advanced"

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

const QUICK_ANALYSIS_OPTIONS: { value: QuickAnalysisMode; label: string }[] = [
	{ value: "direction", label: "Traffic direction" },
	{ value: "protocol", label: "Protocol" },
	{ value: "src_ip", label: "Top source IP" },
	{ value: "dst_ip", label: "Top destination IP" },
	{ value: "local_prefix", label: "Top local prefix" },
	{ value: "remote_prefix", label: "Top remote prefix" },
]

const QUICK_DIMENSIONS: Record<Exclude<QuickAnalysisMode, "direction">, string> = {
	protocol: "protocol",
	src_ip: "src_ip",
	dst_ip: "dst_ip",
	local_prefix: "local_prefix",
	remote_prefix: "remote_prefix",
}

function queryState(name: string, fallback: string) {
	if (typeof window === "undefined") return fallback
	return new URLSearchParams(window.location.search).get(name) || fallback
}

function isQuickAnalysisMode(value: string): value is QuickAnalysisMode {
	return QUICK_ANALYSIS_OPTIONS.some((option) => option.value === value)
}

export default memo(function TrafficMatrix({ surface = "overview" }: { surface?: FlowTrafficSurface }) {
	const { t } = useLingui()
	const surfacePreset = flowSurfacePreset(surface)
	const presetMode = surfacePreset.queryMode as QueryMode
	const initialMode = queryState("analysis", presetMode)
	const [quickAnalysis, setQuickAnalysis] = useState<QuickAnalysisMode>(() =>
		isQuickAnalysisMode(initialMode) ? initialMode : isQuickAnalysisMode(presetMode) ? presetMode : "direction"
	)
	const [queryMode, setQueryMode] = useState<QueryMode>(() =>
		initialMode === "advanced" || isQuickAnalysisMode(initialMode) ? initialMode : presetMode
	)
	const [timeRange, setTimeRange] = useState(() => queryState("range", "1h"))
	const [customStart, setCustomStart] = useState(() => queryState("start", ""))
	const [customEnd, setCustomEnd] = useState(() => queryState("end", ""))
	const [metric, setMetric] = useState(() => queryState("metric", "estimated_bps"))
	const [dimension, setDimension] = useState(() => queryState("dimension", surfacePreset.dimension))
	const [dimension2, setDimension2] = useState(() => queryState("dimension2", "none"))
	const [dimension3, setDimension3] = useState(() => queryState("dimension3", "none"))
	const [dimension4, setDimension4] = useState(() => queryState("dimension4", "none"))
	const [graphMode, setGraphMode] = useState<GraphMode>(() => queryState("graph", "lines") as GraphMode)
	const [targetPoints, setTargetPoints] = useState(() => Number(queryState("points", "300")))
	const [topN, setTopN] = useState(() => Number(queryState("top", "20")))
	const [includeOther, setIncludeOther] = useState(() => queryState("other", "1") !== "0")
	const [filterExpression, setFilterExpression] = useState(() => queryState("filter", surfacePreset.filter))
	const [addressSets, setAddressSets] = useState<AddressSetItem[]>([])
	const [devices, setDevices] = useState<DeviceItem[]>([])
	const [countries, setCountries] = useState<GeoNode[]>([])
	const [provinces, setProvinces] = useState<GeoNode[]>([])
	const [cities, setCities] = useState<GeoNode[]>([])
	const [operators, setOperators] = useState<NetworkOperator[]>([])
	const [selectedCountry, setSelectedCountry] = useState(() => queryState("country", "all"))
	const [selectedProvince, setSelectedProvince] = useState(() => queryState("province", "all"))
	const [selectedCity, setSelectedCity] = useState(() => queryState("city", "all"))
	const [selectedOperator, setSelectedOperator] = useState(() => queryState("operator", "all"))
	const [selectedSet, setSelectedSet] = useState(() => queryState("set", "all"))
	const [selectedDevice, setSelectedDevice] = useState(() => queryState("device", "all"))
	const [series, setSeries] = useState<FlowSeries[]>([])
	const [selectedDetailIP, setSelectedDetailIP] = useState("")
	const [response, setResponse] = useState<FlowQueryResponse | null>(null)
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState("")
	const [referenceError, setReferenceError] = useState("")
	const [referencesLoaded, setReferencesLoaded] = useState(false)
	const [provincesLoaded, setProvincesLoaded] = useState(false)
	const [citiesLoaded, setCitiesLoaded] = useState(false)
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

	useEffect(() => {
		Promise.all([
			pb.send<ListResponse<GeoNode>>("/api/v1/geo/dictionary", {
				query: { kind: "country", enabled: true, limit: 500 },
			}),
			pb.send<ListResponse<NetworkOperator>>("/api/v1/network/operators", {
				query: { enabled: true, limit: 500 },
			}),
		])
			.then(([geo, networkOperators]) => {
				setCountries(sortReferences(geo.items ?? []))
				setOperators(sortReferences(networkOperators.items ?? []))
				setReferenceError("")
			})
			.catch((err) => setReferenceError(err instanceof Error ? err.message : t`Failed to load filters`))
			.finally(() => setReferencesLoaded(true))
	}, [t])

	useEffect(() => {
		setProvincesLoaded(false)
		if (selectedCountry === "all") {
			setProvinces([])
			setProvincesLoaded(true)
			return
		}
		pb.send<ListResponse<GeoNode>>("/api/v1/geo/dictionary", {
			query: { kind: "province", parent_id: selectedCountry, enabled: true, limit: 500 },
		})
			.then((result) => setProvinces(sortReferences(result.items ?? [])))
			.catch((err) => setReferenceError(err instanceof Error ? err.message : t`Failed to load provinces`))
			.finally(() => setProvincesLoaded(true))
	}, [selectedCountry, t])

	useEffect(() => {
		setCitiesLoaded(false)
		if (selectedProvince === "all") {
			setCities([])
			setCitiesLoaded(true)
			return
		}
		pb.send<ListResponse<GeoNode>>("/api/v1/geo/dictionary", {
			query: { kind: "city", parent_id: selectedProvince, enabled: true, limit: 500 },
		})
			.then((result) => setCities(sortReferences(result.items ?? [])))
			.catch((err) => setReferenceError(err instanceof Error ? err.message : t`Failed to load cities`))
			.finally(() => setCitiesLoaded(true))
	}, [selectedProvince, t])

	const refresh = useCallback(
		async (modeOverride?: QueryMode) => {
			setLoading(true)
			setError("")
			try {
				const activeMode = modeOverride ?? queryMode
				const { start, end } = resolveFlowTimeRange(timeRange, customStart, customEnd)
				let selectedDimension: string
				let selectedDimensions: string[]
				let selectedTopN: number
				let canonicalFilter: FlowFilterExpression | undefined
				let filters: FlowFilters = {}

				if (activeMode === "advanced") {
					selectedDimension = selectedSet === "all" ? dimension : "address_set"
					selectedDimensions = [selectedDimension]
					const jointAllowed =
						selectedSet === "all" && selectedDimension !== "total" && selectedDimension !== "address_set"
					if (jointAllowed && dimension2 !== "none") selectedDimensions.push(dimension2)
					if (jointAllowed && dimension2 !== "none" && dimension3 !== "none") selectedDimensions.push(dimension3)
					if (jointAllowed && dimension2 !== "none" && dimension3 !== "none" && dimension4 !== "none")
						selectedDimensions.push(dimension4)
					selectedTopN = selectedDimension === "total" ? 1 : Math.max(1, Math.min(100, topN))
					canonicalFilter = parseFlowFilter(filterExpression)
					const selectionFilters: FlowFilters = {}
					if (selectedDevice !== "all") selectionFilters.device_ids = [selectedDevice]
					if (selectedSet !== "all") selectionFilters.dimension_values = [selectedSet]
					filters = mergeFlowFilters({}, selectionFilters)
				} else {
					const country = selectedReference(countries, selectedCountry, "country")
					const province = selectedReference(provinces, selectedProvince, "province")
					const city = selectedReference(cities, selectedCity, "city")
					const networkOperator = selectedReference(operators, selectedOperator, "operator")
					if (networkOperator && networkOperator.asns.length === 0) {
						throw new Error(`${networkOperator.name} has no ASN mapping`)
					}
					canonicalFilter = buildFlowQuickFilter({
						countryCode: country?.code,
						provinceCode: province?.code,
						cityCode: city?.code,
						operatorASNs: networkOperator?.asns,
					})
					selectedDimension = activeMode === "direction" ? "total" : QUICK_DIMENSIONS[activeMode]
					selectedDimensions = [selectedDimension]
					selectedTopN = activeMode === "direction" ? 1 : Math.max(1, Math.min(100, topN))
				}

				if (canonicalFilter) {
					const validated = await pb.send<{ valid: boolean; filter: FlowFilterExpression }>(
						"/api/v1/flow/filters/validate",
						{ method: "POST", body: { filter: canonicalFilter } }
					)
					canonicalFilter = validated.filter
				}
				const grouping =
					selectedDimensions.length > 1 ? { dimensions: selectedDimensions } : { dimension: selectedDimension }
				const sendQuery = (queryFilters: FlowFilters) =>
					pb.send<FlowQueryResponse>("/api/v1/flow/query", {
						method: "POST",
						body: {
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
								filters: queryFilters,
								filter: canonicalFilter,
								top_n: selectedTopN,
								include_other: selectedDimension !== "total" && selectedDimension !== "address_set" && includeOther,
								target_points: targetPoints,
								timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
							},
						},
					})
				let query: FlowQueryResponse
				if (activeMode === "direction") {
					const [inbound, outbound] = await Promise.all([
						sendQuery(mergeFlowFilters(filters, { directions: ["in"] })),
						sendQuery(mergeFlowFilters(filters, { directions: ["out"] })),
					])
					query = combineDirectionResponses(inbound, outbound)
				} else {
					query = await sendQuery(filters)
					if (activeMode === "protocol") query = labelProtocolPoints(query)
				}
				setResponse(query)
				setQueryMode(activeMode)
				const queryUnit = query.data?.metric?.unit ?? query.meta?.unit ?? ""
				setSeries(
					query.data?.dimensions
						? buildFlowJointSeries((query.data.points ?? []) as FlowJointPoint[], query.data.plan, queryUnit)
						: buildFlowSeries((query.data?.points ?? []) as FlowPoint[], query.data?.plan, queryUnit)
				)
				persistQueryState({
					analysis: activeMode,
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
					country: selectedCountry,
					province: selectedProvince,
					city: selectedCity,
					operator: selectedOperator,
					set: selectedSet,
					device: selectedDevice,
				})
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to load`)
			} finally {
				setLoading(false)
			}
		},
		[
			queryMode,
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
			countries,
			provinces,
			cities,
			operators,
			selectedCountry,
			selectedProvince,
			selectedCity,
			selectedOperator,
			selectedSet,
			selectedDevice,
			t,
		]
	)

	useEffect(() => {
		if (initialQuery.current) return
		if (
			queryMode !== "advanced" &&
			((selectedCountry !== "all" && !referencesLoaded) ||
				(selectedOperator !== "all" && !referencesLoaded) ||
				(selectedProvince !== "all" && !provincesLoaded) ||
				(selectedCity !== "all" && !citiesLoaded))
		)
			return
		initialQuery.current = true
		refresh().catch(() => {})
	}, [
		citiesLoaded,
		provincesLoaded,
		queryMode,
		referencesLoaded,
		refresh,
		selectedCity,
		selectedCountry,
		selectedOperator,
		selectedProvince,
	])

	const jointSelected =
		selectedSet === "all" && dimension !== "total" && dimension !== "address_set" && dimension2 !== "none"
	useEffect(() => {
		if ((queryMode !== "advanced" || !jointSelected) && graphMode === "sankey") setGraphMode("lines")
	}, [graphMode, jointSelected, queryMode])
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
	const exportCurrent = useCallback(() => {
		if (series.length === 0) return
		const blob = new Blob(["\ufeff", buildFlowCSV(series, unit)], { type: "text/csv;charset=utf-8" })
		const url = URL.createObjectURL(blob)
		const link = document.createElement("a")
		link.href = url
		link.download = `watchdog-flow-${new Date().toISOString().replaceAll(":", "-")}.csv`
		document.body.append(link)
		link.click()
		link.remove()
		window.setTimeout(() => URL.revokeObjectURL(url), 0)
	}, [series, unit])

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
					ip: item.path[0] ?? "",
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
	const countryOptions = referenceOptions(countries, t`All countries`)
	const provinceOptions = referenceOptions(provinces, t`All provinces`)
	const cityOptions = referenceOptions(cities, t`All cities`)
	const operatorOptions = referenceOptions(operators, t`All operators`)
	const detailRange = useMemo(() => {
		try {
			return resolveFlowTimeRange(timeRange, customStart, customEnd)
		} catch {
			return { start: "", end: "" }
		}
	}, [customEnd, customStart, timeRange])
	const pageCopy = {
		overview: {
			title: t`Flow Overview`,
			description: t`Traffic direction, category, completeness and current trends.`,
		},
		dimensions: {
			title: t`Multi-dimensional Flow Analysis`,
			description: t`Explore one to four dimensions with typed filters and true tuple aggregation.`,
		},
		source: {
			title: t`Source IP Analysis`,
			description: t`Find top source addresses, trends and related dimensions.`,
		},
		destination: {
			title: t`Destination IP Analysis`,
			description: t`Find top destination addresses, trends and related dimensions.`,
		},
		overseas: {
			title: t`Overseas Traffic`,
			description: t`Analyze overseas traffic by direction, location, operator and protocol.`,
		},
	}[surface]

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<BarChart3Icon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<div>
						<h1 className="text-xl font-semibold tracking-normal">{pageCopy.title}</h1>
						<p className="text-sm text-muted-foreground">{pageCopy.description}</p>
					</div>
				</div>
				<Button variant="outline" size="sm" onClick={() => refresh()} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="grid gap-4 rounded-md border border-border bg-card p-4">
				<div>
					<h2 className="font-medium">
						<Trans>Quick traffic analysis</Trans>
					</h2>
					<p className="mt-1 text-sm text-muted-foreground">
						<Trans>
							Select a location, operator and time range, then view traffic direction, protocol or Top addresses.
						</Trans>
					</p>
				</div>
				<div className="flex flex-wrap items-end gap-3">
					<OptionSelect
						label={t`Country`}
						value={selectedCountry}
						onChange={(value) => {
							setSelectedCountry(value)
							setSelectedProvince("all")
							setSelectedCity("all")
						}}
						options={countryOptions}
						width="w-48"
					/>
					<OptionSelect
						label={t`Province`}
						value={selectedProvince}
						onChange={(value) => {
							setSelectedProvince(value)
							setSelectedCity("all")
						}}
						options={provinceOptions}
						width="w-48"
						disabled={selectedCountry === "all"}
					/>
					<OptionSelect
						label={t`City`}
						value={selectedCity}
						onChange={setSelectedCity}
						options={cityOptions}
						width="w-48"
						disabled={selectedProvince === "all"}
					/>
					<OptionSelect
						label={t`Operator`}
						value={selectedOperator}
						onChange={setSelectedOperator}
						options={operatorOptions}
						width="w-48"
					/>
					<OptionSelect
						label={t`Time range`}
						value={timeRange}
						onChange={setTimeRange}
						options={FLOW_TIME_PRESETS}
						width="w-48"
					/>
				</div>
				{timeRange === "custom" && (
					<div className="flex flex-wrap gap-3">
						<DateInput label={t`Start`} value={customStart} onChange={setCustomStart} />
						<DateInput label={t`End`} value={customEnd} onChange={setCustomEnd} />
					</div>
				)}
				<div className="flex flex-wrap items-end gap-3">
					<OptionSelect
						label={t`Analysis`}
						value={quickAnalysis}
						onChange={(value) => setQuickAnalysis(value as QuickAnalysisMode)}
						options={QUICK_ANALYSIS_OPTIONS}
						width="w-52"
					/>
					<OptionSelect label={t`Metric`} value={metric} onChange={setMetric} options={METRIC_OPTIONS} width="w-56" />
					<OptionSelect
						label={t`Graph type`}
						value={graphMode === "sankey" ? "lines" : graphMode}
						onChange={(value) => setGraphMode(value as GraphMode)}
						options={GRAPH_OPTIONS}
						width="w-44"
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
							disabled={quickAnalysis === "direction"}
							className="w-24"
						/>
					</div>
					<Button onClick={() => refresh(quickAnalysis)} disabled={loading}>
						<BarChart3Icon className="me-2 h-4 w-4" />
						<Trans>Analyze</Trans>
					</Button>
					<Button variant="outline" onClick={exportCurrent} disabled={loading || series.length === 0}>
						<DownloadIcon className="me-2 h-4 w-4" />
						<Trans>Export current CSV</Trans>
					</Button>
				</div>
				<p className="text-xs text-muted-foreground">
					<Trans>
						Location and operator filters combined with protocol or address dimensions use the bounded cross-dimension
						path. Ranges over 24 hours require a published asynchronous index.
					</Trans>
				</p>
				{referenceError && <p className="text-xs text-destructive">{referenceError}</p>}
			</div>

			<details className="rounded-md border border-border bg-card" defaultOpen={surfacePreset.advancedOpen}>
				<summary className="flex cursor-pointer list-none items-center gap-2 p-4 font-medium">
					<SlidersHorizontalIcon className="h-4 w-4 text-muted-foreground" />
					<Trans>Advanced Flow Explorer</Trans>
				</summary>
				<div className="grid gap-4 border-t border-border p-4">
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
									if ((event.ctrlKey || event.metaKey) && event.key === "Enter") refresh("advanced").catch(() => {})
								}}
								placeholder="(geo.country=CN OR asn IN (4134, 4837)) AND remote_ip IN (203.0.113.0/24)"
							/>
						</div>
						<Button onClick={() => refresh("advanced")} disabled={loading}>
							<Trans>Apply</Trans>
						</Button>
					</div>
					<p className="text-xs text-muted-foreground">
						<Trans>
							Filter fields include IP/CIDR, ASN, ISP, Geo, ports, protocol, direction and resources. Operators: =, !=,
							IN, NOT IN, &gt;, &gt;=, &lt;, &lt;= with AND/OR/NOT and parentheses. Press Ctrl/Cmd+Enter to apply.
						</Trans>
					</p>
					{filterExpression.trim() && (
						<p className="text-xs text-muted-foreground">
							<Trans>
								Filters on fields not present in the 1m/1h rollups use the bounded base-fact path and are limited to 24
								hours until a matching asynchronous index is published.
							</Trans>
						</p>
					)}
					{jointSelected && (
						<p className="text-xs text-muted-foreground">
							<Trans>
								Multi-dimension and Sankey queries read true tuples from the same Flow facts and are synchronously
								limited to 24 hours. Longer ranges require a published asynchronous joint index.
							</Trans>
						</p>
					)}
				</div>
			</details>

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
					onRowClick={
						surface === "source" || surface === "destination"
							? (record) => setSelectedDetailIP(String(record.ip ?? ""))
							: undefined
					}
				/>
			</div>

			{(surface === "source" || surface === "destination") && (
				<FlowRecordTable
					endpoint={surface}
					selectedIP={selectedDetailIP}
					onIPChange={setSelectedDetailIP}
					from={detailRange.start}
					to={detailRange.end}
				/>
			)}
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

function sortReferences<T extends { sort_order: number; name: string }>(items: T[]) {
	return items.slice().sort((left, right) => left.sort_order - right.sort_order || left.name.localeCompare(right.name))
}

function referenceOptions(items: Array<{ id: string; name: string; short_name?: string }>, allLabel: string) {
	return [
		{ value: "all", label: allLabel },
		...items.map((item) => ({
			value: item.id,
			label: item.short_name ? `${item.name} (${item.short_name})` : item.name,
		})),
	]
}

function selectedReference<T extends { id: string; name: string }>(
	items: T[],
	id: string,
	kind: string
): T | undefined {
	if (id === "all") return undefined
	const item = items.find((candidate) => candidate.id === id)
	if (!item) throw new Error(`Selected ${kind} is unavailable`)
	return item
}

function combineDirectionResponses(inbound: FlowQueryResponse, outbound: FlowQueryResponse): FlowQueryResponse {
	const inboundUnit = inbound.data.metric?.unit ?? inbound.meta.unit ?? ""
	const outboundUnit = outbound.data.metric?.unit ?? outbound.meta.unit ?? ""
	if (inboundUnit !== outboundUnit) throw new Error("Direction query returned inconsistent units")
	const relabel = (response: FlowQueryResponse, label: string) =>
		((response.data.points ?? []) as FlowPoint[]).map((point) => ({ ...point, dimension_value: label }))
	const left = inbound.meta.completeness
	const right = outbound.meta.completeness
	const leftRollup = inbound.data.rollup_completeness
	const rightRollup = outbound.data.rollup_completeness
	const warnings = [...new Set([...(left?.warnings ?? []), ...(right?.warnings ?? [])])]
	return {
		data: {
			...inbound.data,
			points: [...relabel(inbound, "Inbound"), ...relabel(outbound, "Outbound")],
			dimension: { kind: "direction", additive: true },
			dimensions: undefined,
			mixed_versions: inbound.data.mixed_versions || outbound.data.mixed_versions,
			version_count: Math.max(inbound.data.version_count, outbound.data.version_count),
			rollup_completeness:
				leftRollup && rightRollup
					? {
							expected_buckets: Math.max(leftRollup.expected_buckets, rightRollup.expected_buckets),
							covered_buckets: Math.min(leftRollup.covered_buckets, rightRollup.covered_buckets),
							ratio: Math.min(leftRollup.ratio, rightRollup.ratio),
							complete: leftRollup.complete && rightRollup.complete,
						}
					: (leftRollup ?? rightRollup),
		},
		meta: {
			...inbound.meta,
			completeness:
				left && right
					? {
							complete_ratio: Math.min(left.complete_ratio, right.complete_ratio),
							partial: left.partial || right.partial,
							unknown_ratio: Math.max(left.unknown_ratio, right.unknown_ratio),
							warnings,
						}
					: (left ?? right),
		},
	}
}

function labelProtocolPoints(response: FlowQueryResponse): FlowQueryResponse {
	const names: Record<string, string> = {
		"1": "ICMP (1)",
		"6": "TCP (6)",
		"17": "UDP (17)",
		"47": "GRE (47)",
		"50": "ESP (50)",
		"51": "AH (51)",
		"58": "ICMPv6 (58)",
		"132": "SCTP (132)",
	}
	return {
		...response,
		data: {
			...response.data,
			points: ((response.data.points ?? []) as FlowPoint[]).map((point) => ({
				...point,
				dimension_value: names[point.dimension_value] ?? point.dimension_value,
			})),
		},
	}
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
