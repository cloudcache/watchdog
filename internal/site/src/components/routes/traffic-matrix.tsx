import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { BarChart3Icon, BookmarkIcon, DownloadIcon, RefreshCwIcon, SlidersHorizontalIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { FlowRecordTable } from "@/components/flow/flow-record-table"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { toast } from "@/components/ui/use-toast"
import { createFlowExplorerChart, type FlowGraphType } from "@/lib/flow-explorer-chart"
import {
	buildFlowJointSeries,
	buildFlowQuickFilter,
	buildFlowSeries,
	FLOW_TIME_PRESETS,
	enrichFlowGeoPoints,
	enrichFlowJointGeoPoints,
	flowSurfacePreset,
	mergeFlowFilters,
	parseFlowFilter,
	resolveOverseasRange,
	resolveFlowTimeRange,
	type FlowFilters,
	type FlowFilterExpression,
	type FlowDimensionLabel,
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
	table?: FlowTablePage
	dimension_labels?: Record<string, FlowDimensionLabel>
}

type FlowTableRow = {
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
}

type FlowTableFilterOption = { value: string; count: number }
type FlowTablePage = {
	items: FlowTableRow[]
	total: number
	limit: number
	offset: number
	filter_options: Record<string, FlowTableFilterOption[]>
}

type FlowTableControl = {
	search: string
	page: number
	pageSize: number
	sortBy: string
	sortDirection: "asc" | "desc"
	filters: Record<string, string[]>
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

type FlowQueryRequest = {
	dataset: "flow.traffic"
	from: string
	to: string
	step_seconds: number
	limit: number
	value_layer: "customer"
	parameters: Record<string, unknown>
}

type OverseasPoint = {
	bucket: string
	kind: "kpi" | "geo"
	geo_scope: "overseas" | "unknown_geo" | ""
	direction: "in" | "out" | "combined"
	ip_family: "ipv4" | "ipv6" | "unknown" | "all"
	geo_value?: string
	other: boolean
	dimension_snapshot_id: string
	geo_version: string
	classification_version: number
	value: number
	observed_remote_ips: number
	observed_local_hosts: number
	received_records: number
	unknown_sampling_records: number
	quality_records: number
	generated_at: string
}

type OverseasQueryResponse = {
	data: {
		points: OverseasPoint[] | null
		metric: { name: string; unit: string }
		geo_level?: "country" | "region"
		top_n?: number
		include_other?: boolean
		rollup_completeness: { expected_buckets: number; covered_buckets: number; ratio: number; complete: boolean }
		mixed_versions: boolean
		version_count: number
	}
	meta: {
		source: "1m" | "1h"
		step_seconds: number
		geo_labels?: Record<string, { code: string; name: string; breadcrumb: string[] }>
	}
}

type AddressSetItem = { id: string; name: string }
type AddressSetList = { items: AddressSetItem[] }
type DeviceItem = { ID?: string; id?: string; SysName?: string; sys_name?: string; Name?: string; name?: string }
type DeviceList = { items: DeviceItem[] }
type GeoNode = {
	id: string
	kind: string
	parent_id?: string
	name: string
	path: Array<{ id: string; name: string; kind: string }>
	additive: boolean
}
type NetworkOperator = {
	id: string
	flow_isp_id: number
	code: string
	name: string
	short_name?: string
	asns: number[]
	sort_order: number
	enabled: boolean
}
type ListResponse<T> = { items?: T[] }
type FlowGeoCatalogResponse = ListResponse<GeoNode> & { version: string; total: number }

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

const INITIAL_FLOW_TABLE_CONTROL: FlowTableControl = {
	search: "",
	page: 0,
	pageSize: 25,
	sortBy: "maximum",
	sortDirection: "desc",
	filters: {},
}

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

function queryListState(name: string, legacyName?: string) {
	if (typeof window === "undefined") return []
	const parameters = new URLSearchParams(window.location.search)
	const value = parameters.get(name) || (legacyName ? parameters.get(legacyName) : "") || ""
	return [
		...new Set(
			value
				.split(",")
				.map((item) => item.trim())
				.filter((item) => item && item !== "all")
		),
	].sort()
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
	const [advancedOpen, setAdvancedOpen] = useState(surfacePreset.advancedOpen)
	const [addressSets, setAddressSets] = useState<AddressSetItem[]>([])
	const [devices, setDevices] = useState<DeviceItem[]>([])
	const [countries, setCountries] = useState<GeoNode[]>([])
	const [provinces, setProvinces] = useState<GeoNode[]>([])
	const [cities, setCities] = useState<GeoNode[]>([])
	const [geoVersion, setGeoVersion] = useState("")
	const [operators, setOperators] = useState<NetworkOperator[]>([])
	const [selectedCountry, setSelectedCountry] = useState(() => queryState("country", "all"))
	const [selectedProvince, setSelectedProvince] = useState(() => queryState("province", "all"))
	const [selectedCity, setSelectedCity] = useState(() => queryState("city", "all"))
	const [selectedOperator, setSelectedOperator] = useState(() => queryState("operator", "all"))
	const [includeAnySets, setIncludeAnySets] = useState(() => queryListState("set_any", "set"))
	const [includeAllSets, setIncludeAllSets] = useState(() => queryListState("set_all"))
	const [excludeAnySets, setExcludeAnySets] = useState(() => queryListState("set_exclude"))
	const [addressSetEndpoint, setAddressSetEndpoint] = useState(() => queryState("set_endpoint", "either"))
	const [selectedDevice, setSelectedDevice] = useState(() => queryState("device", "all"))
	const [series, setSeries] = useState<FlowSeries[]>([])
	const [selectedDetailIP, setSelectedDetailIP] = useState("")
	const [response, setResponse] = useState<FlowQueryResponse | null>(null)
	const [tableControl, setTableControl] = useState<FlowTableControl>(INITIAL_FLOW_TABLE_CONTROL)
	const [tableSearch, setTableSearch] = useState("")
	const [overseasResponse, setOverseasResponse] = useState<OverseasQueryResponse | null>(null)
	const [overseasError, setOverseasError] = useState("")
	const [overseasGeoLevel, setOverseasGeoLevel] = useState(() => queryState("geo_level", "country"))
	const [loading, setLoading] = useState(false)
	const [exporting, setExporting] = useState(false)
	const [error, setError] = useState("")
	const [referenceError, setReferenceError] = useState("")
	const [referencesLoaded, setReferencesLoaded] = useState(false)
	const [provincesLoaded, setProvincesLoaded] = useState(false)
	const [citiesLoaded, setCitiesLoaded] = useState(false)
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createFlowExplorerChart> | null>(null)
	const initialQuery = useRef(false)
	const querySequence = useRef(0)
	const lastQueryRequest = useRef<FlowQueryRequest | null>(null)

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
		Promise.allSettled([
			pb.send<FlowGeoCatalogResponse>("/api/v1/flow/geo/catalog", {
				query: { level: "country", limit: 500 },
			}),
			pb.send<ListResponse<NetworkOperator>>("/api/v1/network/operators", {
				query: { enabled: true, limit: 500 },
			}),
		])
			.then(([geo, networkOperators]) => {
				const errors: string[] = []
				if (geo.status === "fulfilled") {
					setCountries(sortReferences(geo.value.items ?? []))
					setGeoVersion(geo.value.version)
				} else {
					setCountries([])
					setGeoVersion("")
					errors.push(geo.reason instanceof Error ? geo.reason.message : t`Failed to load Geo catalog`)
				}
				if (networkOperators.status === "fulfilled") {
					setOperators(sortReferences(networkOperators.value.items ?? []))
				} else {
					setOperators([])
					errors.push(
						networkOperators.reason instanceof Error ? networkOperators.reason.message : t`Failed to load operators`
					)
				}
				setReferenceError(errors.join("; "))
			})
			.finally(() => setReferencesLoaded(true))
	}, [t])

	useEffect(() => {
		setProvincesLoaded(false)
		if (selectedCountry === "all") {
			setProvinces([])
			setProvincesLoaded(true)
			return
		}
		pb.send<FlowGeoCatalogResponse>("/api/v1/flow/geo/catalog", {
			query: { level: "province", parent: selectedCountry, version: geoVersion, limit: 500 },
		})
			.then((result) => setProvinces(sortReferences(result.items ?? [])))
			.catch((err) => setReferenceError(err instanceof Error ? err.message : t`Failed to load provinces`))
			.finally(() => setProvincesLoaded(true))
	}, [geoVersion, selectedCountry, t])

	useEffect(() => {
		setCitiesLoaded(false)
		if (selectedProvince === "all") {
			setCities([])
			setCitiesLoaded(true)
			return
		}
		pb.send<FlowGeoCatalogResponse>("/api/v1/flow/geo/catalog", {
			query: { level: "city", parent: selectedProvince, version: geoVersion, limit: 500 },
		})
			.then((result) => setCities(sortReferences(result.items ?? [])))
			.catch((err) => setReferenceError(err instanceof Error ? err.message : t`Failed to load cities`))
			.finally(() => setCitiesLoaded(true))
	}, [geoVersion, selectedProvince, t])

	const refresh = useCallback(
		async (modeOverride?: QueryMode, tableOverride?: FlowTableControl) => {
			const sequence = ++querySequence.current
			setLoading(true)
			setError("")
			setOverseasError("")
			try {
				const activeMode = modeOverride ?? queryMode
				const activeTable = tableOverride ?? tableControl
				const { start, end } = resolveFlowTimeRange(timeRange, customStart, customEnd)
				let selectedDimension: string
				let selectedDimensions: string[]
				let selectedTopN: number
				let canonicalFilter: FlowFilterExpression | undefined
				let filters: FlowFilters = {}
				let operatorID: string | undefined
				const addressSetSelectionActive = includeAnySets.length + includeAllSets.length > 0
				if (!addressSetSelectionActive && excludeAnySets.length > 0) {
					throw new Error("Address set exclusion requires an include-any or include-all selection")
				}

				if (activeMode === "advanced") {
					selectedDimension = addressSetSelectionActive ? "address_set" : dimension
					selectedDimensions = [selectedDimension]
					const jointAllowed =
						!addressSetSelectionActive && selectedDimension !== "total" && selectedDimension !== "address_set"
					if (jointAllowed && dimension2 !== "none") selectedDimensions.push(dimension2)
					if (jointAllowed && dimension2 !== "none" && dimension3 !== "none") selectedDimensions.push(dimension3)
					if (jointAllowed && dimension2 !== "none" && dimension3 !== "none" && dimension4 !== "none")
						selectedDimensions.push(dimension4)
					selectedTopN =
						selectedDimension === "total" || addressSetSelectionActive ? 1 : Math.max(1, Math.min(100, topN))
					canonicalFilter = parseFlowFilter(filterExpression)
					if (addressSetSelectionActive && canonicalFilter) {
						throw new Error("Address set combinations and typed filters are separate bounded query modes")
					}
					const selectionFilters: FlowFilters = {}
					if (selectedDevice !== "all") selectionFilters.device_ids = [selectedDevice]
					filters = mergeFlowFilters({}, selectionFilters)
				} else {
					const country = selectedReference(countries, selectedCountry, "country")
					const province = selectedReference(provinces, selectedProvince, "province")
					const city = selectedReference(cities, selectedCity, "city")
					const networkOperator = selectedReference(operators, selectedOperator, "operator")
					operatorID = networkOperator?.id
					canonicalFilter = buildFlowQuickFilter({
						countryCode: country?.id,
						provinceCode: province?.id,
						cityCode: city?.id,
					})
					if ((country || province || city) && geoVersion) filters.geo_versions = [geoVersion]
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
				let submittedQuery: FlowQueryRequest | null = null
				const sendQuery = (queryFilters: FlowFilters) => {
					submittedQuery = {
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
							operator_selection: operatorID ? { operator_id: operatorID } : undefined,
							...grouping,
							filters: queryFilters,
							filter: canonicalFilter,
							top_n: selectedTopN,
							include_other: selectedDimension !== "total" && selectedDimension !== "address_set" && includeOther,
							address_set_endpoint: addressSetSelectionActive ? addressSetEndpoint : undefined,
							address_set_filter: addressSetSelectionActive
								? { include_any: includeAnySets, include_all: includeAllSets, exclude_any: excludeAnySets }
								: undefined,
							target_points: targetPoints,
							timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
							direction_split: activeMode === "direction" || undefined,
							table: {
								search: activeTable.search || undefined,
								sort_by: activeTable.sortBy,
								sort_direction: activeTable.sortDirection,
								limit: activeTable.pageSize,
								offset: activeTable.page * activeTable.pageSize,
								filters: activeTable.filters,
							},
						},
					}
					return pb.send<FlowQueryResponse>("/api/v1/flow/query", {
						method: "POST",
						body: submittedQuery,
					})
				}
				let query = await sendQuery(filters)
				if (activeMode === "protocol") query = labelProtocolPoints(query)
				if (sequence !== querySequence.current) return
				lastQueryRequest.current = submittedQuery
				setResponse(query)
				setQueryMode(activeMode)
				const queryUnit = query.data?.metric?.unit ?? query.meta?.unit ?? ""
				setSeries(
					query.data?.dimensions
						? buildFlowJointSeries(
								enrichFlowJointGeoPoints(
									query.data.points as FlowJointPoint[] | null,
									query.data.dimensions,
									query.data.dimension_labels
								),
								query.data.plan,
								queryUnit
							)
						: buildFlowSeries(
								enrichFlowGeoPoints(query.data?.points as FlowPoint[] | null, query.data?.dimension_labels),
								query.data?.plan,
								queryUnit
							)
				)
				if (surface === "overseas") {
					try {
						const overseasRange = resolveOverseasRange(start, end)
						const overseas = await pb.send<OverseasQueryResponse>("/api/v1/flow/overseas/query", {
							method: "POST",
							body: {
								from: overseasRange.start,
								to: overseasRange.end,
								bucket: overseasRange.bucket,
								metric,
								geo_level: overseasGeoLevel,
								view: "customer",
								top_n: Math.max(1, Math.min(100, topN)),
								include_other: includeOther,
								filters: selectedDevice === "all" ? {} : { device_ids: [selectedDevice] },
							},
						})
						if (sequence !== querySequence.current) return
						setOverseasResponse(overseas)
					} catch (overseasRequestError) {
						setOverseasResponse(null)
						setOverseasError(
							overseasRequestError instanceof Error ? overseasRequestError.message : t`Failed to load overseas summary`
						)
					}
				}
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
					set: "",
					set_any: includeAnySets.join(","),
					set_all: includeAllSets.join(","),
					set_exclude: excludeAnySets.join(","),
					set_endpoint: addressSetEndpoint,
					device: selectedDevice,
					geo_level: overseasGeoLevel,
				})
			} catch (err) {
				if (sequence !== querySequence.current) return
				setError(err instanceof Error ? err.message : t`Failed to load`)
			} finally {
				if (sequence === querySequence.current) setLoading(false)
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
			geoVersion,
			operators,
			selectedCountry,
			selectedProvince,
			selectedCity,
			selectedOperator,
			includeAnySets,
			includeAllSets,
			excludeAnySets,
			addressSetEndpoint,
			selectedDevice,
			overseasGeoLevel,
			surface,
			tableControl,
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

	const addressSetSelectionActive = includeAnySets.length + includeAllSets.length > 0
	const jointSelected =
		!addressSetSelectionActive && dimension !== "total" && dimension !== "address_set" && dimension2 !== "none"
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
	const exportCurrent = useCallback(async () => {
		if (!lastQueryRequest.current) return
		setExporting(true)
		try {
			const task = await pb.send<{ ID?: string; id?: string }>("/api/v1/flow/exports", {
				method: "POST",
				body: { query: lastQueryRequest.current, format: "csv" },
			})
			const id = task.ID ?? task.id ?? ""
			toast({ title: t`Flow export queued` })
			navigate(id ? getPagePath($router, "export_detail", { id }) : getPagePath($router, "exports"))
		} catch (reason) {
			toast({
				title: reason instanceof Error ? reason.message : t`Failed to create Flow export`,
				variant: "destructive",
			})
		} finally {
			setExporting(false)
		}
	}, [t])

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

	const tablePage = response?.data.table
	const records = useMemo(
		() =>
			(tablePage?.items ?? []).map((item) => ({
				ip: item.path[0] ?? "",
				dimension: flowDimensionLabel(mixedVersions ? item.name : item.label, queryMode),
				last: formatFlowValue(item.last, unit),
				average: formatFlowValue(item.average, unit),
				p95: formatFlowValue(item.p95, unit),
				maximum: formatFlowValue(item.maximum, unit),
				minimum: formatFlowValue(item.minimum, unit),
				total: formatFlowTotal(item.total, unit),
				records: item.received_records.toLocaleString(),
				unknown: formatFlowRatio(item.unknown_sampling_ratio),
				quality: formatFlowRatio(item.quality_record_ratio),
			})),
		[mixedVersions, queryMode, tablePage?.items, unit]
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
	const runTableQuery = useCallback(
		(next: FlowTableControl) => {
			setTableControl(next)
			refresh(undefined, next).catch(() => {})
		},
		[refresh]
	)
	const serverFiltering = useMemo(
		() => ({
			options: Object.fromEntries(
				Object.entries(tablePage?.filter_options ?? {}).map(([field, options]) => [
					field,
					options.map((option) => ({
						value: option.value,
						label: flowTableFilterLabel(field, option.value, unit, queryMode),
						count: option.count,
					})),
				])
			),
			selected: tableControl.filters,
			onColumnFilterChange: (field: string, values: unknown[]) => {
				const filters = { ...tableControl.filters, [field]: values.map(String) }
				if (filters[field].length === 0) delete filters[field]
				runTableQuery({ ...tableControl, page: 0, filters })
			},
			onClearAll: () => runTableQuery({ ...tableControl, page: 0, filters: {} }),
		}),
		[queryMode, runTableQuery, tableControl, tablePage?.filter_options, unit]
	)
	const serverSorting = useMemo(
		() => ({
			field: tableControl.sortBy,
			direction: tableControl.sortDirection,
			fields: {
				dimension: "dimension",
				last: "last",
				average: "average",
				p95: "p95",
				maximum: "maximum",
				minimum: "minimum",
				total: "total",
				records: "records",
				unknown: "unknown",
				quality: "quality",
			},
			onSortChange: (field: string, direction: "asc" | "desc") =>
				runTableQuery({ ...tableControl, page: 0, sortBy: field, sortDirection: direction }),
		}),
		[runTableQuery, tableControl]
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

			{surface === "overseas" && (
				<OverseasSummary
					response={overseasResponse}
					error={overseasError}
					geoLevel={overseasGeoLevel}
					onGeoLevelChange={setOverseasGeoLevel}
					onRefresh={() => refresh()}
					loading={loading}
					isNarrowed={
						selectedCountry !== "all" ||
						selectedProvince !== "all" ||
						selectedCity !== "all" ||
						selectedOperator !== "all"
					}
				/>
			)}

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
					<Button variant="outline" onClick={exportCurrent} disabled={loading || exporting || series.length === 0}>
						<DownloadIcon className="me-2 h-4 w-4" />
						<Trans>Export full query</Trans>
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

			<details
				className="rounded-md border border-border bg-card"
				open={advancedOpen}
				onToggle={(event) => setAdvancedOpen(event.currentTarget.open)}
			>
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
							disabled={addressSetSelectionActive}
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
							disabled={addressSetSelectionActive || dimension === "total" || dimension === "address_set"}
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
								disabled={addressSetSelectionActive || dimension === "total"}
								className="w-24"
							/>
						</div>
						<label htmlFor="flow-include-other" className="flex h-9 items-center gap-2 text-sm">
							<Checkbox
								id="flow-include-other"
								checked={includeOther}
								disabled={addressSetSelectionActive || dimension === "total" || dimension === "address_set"}
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
						<AddressSetChecklist
							label={t`Include any`}
							items={addressSets}
							value={includeAnySets}
							onChange={setIncludeAnySets}
						/>
						<AddressSetChecklist
							label={t`Include all`}
							items={addressSets}
							value={includeAllSets}
							onChange={setIncludeAllSets}
						/>
						<AddressSetChecklist
							label={t`Exclude any`}
							items={addressSets}
							value={excludeAnySets}
							onChange={setExcludeAnySets}
						/>
						<OptionSelect
							label={t`Address side`}
							value={addressSetEndpoint}
							onChange={setAddressSetEndpoint}
							width="w-40"
							options={[
								{ value: "either", label: t`Either side` },
								{ value: "local", label: t`Local side` },
								{ value: "remote", label: t`Remote side` },
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
						<Button
							variant="outline"
							onClick={() => {
								const path = getPagePath($router, "flow_filters")
								navigate(
									filterExpression.trim() ? `${path}?filter=${encodeURIComponent(filterExpression.trim())}` : path
								)
							}}
						>
							<BookmarkIcon className="me-2 h-4 w-4" />
							<Trans>Saved Filters</Trans>
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
						<Badge variant="outline">{plan.source} source</Badge>
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
					searchValue={tableSearch}
					onSearchChange={setTableSearch}
					onSearchSubmit={(search) => runTableQuery({ ...tableControl, page: 0, search: search.trim() })}
					serverFiltering={serverFiltering}
					serverSorting={serverSorting}
					serverPagination={{
						page: tableControl.page,
						pageSize: tableControl.pageSize,
						totalCount: tablePage?.total ?? 0,
						onPageChange: (page) => runTableQuery({ ...tableControl, page }),
						onPageSizeChange: (pageSize) => runTableQuery({ ...tableControl, page: 0, pageSize }),
					}}
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

function OverseasSummary({
	response,
	error,
	geoLevel,
	onGeoLevelChange,
	onRefresh,
	loading,
	isNarrowed,
}: {
	response: OverseasQueryResponse | null
	error: string
	geoLevel: string
	onGeoLevelChange: (value: string) => void
	onRefresh: () => void
	loading: boolean
	isNarrowed: boolean
}) {
	const chartRef = useRef<HTMLDivElement>(null)
	const chartInstance = useRef<ReturnType<typeof createFlowExplorerChart> | null>(null)
	const points = response?.data.points ?? []
	const unit = response?.data.metric.unit ?? ""
	const directionSeries = useMemo(() => {
		const source = points.filter(
			(point) =>
				point.kind === "kpi" && point.ip_family === "all" && (point.direction === "in" || point.direction === "out")
		)
		const grouped = new Map<string, FlowPoint>()
		for (const point of source) {
			const key = `${point.bucket}:${point.direction}`
			const current = grouped.get(key)
			grouped.set(key, {
				bucket: point.bucket,
				dimension_value: point.direction === "in" ? "Inbound" : "Outbound",
				other: false,
				value: (current?.value ?? 0) + point.value,
				dimension_snapshot_id: current?.dimension_snapshot_id ?? point.dimension_snapshot_id,
				geo_version: current?.geo_version ?? point.geo_version,
				classification_version: current?.classification_version ?? point.classification_version,
				received_records: (current?.received_records ?? 0) + point.received_records,
				unknown_sampling_records: (current?.unknown_sampling_records ?? 0) + point.unknown_sampling_records,
				quality_records: (current?.quality_records ?? 0) + point.quality_records,
				generated_at: current?.generated_at ?? point.generated_at,
			})
		}
		const rows = [...grouped.values()]
		if (rows.length === 0) return []
		const stepSeconds = response?.meta.step_seconds ?? 60
		const start = rows.reduce((minimum, row) => Math.min(minimum, Date.parse(row.bucket)), Number.POSITIVE_INFINITY)
		const last = rows.reduce((maximum, row) => Math.max(maximum, Date.parse(row.bucket)), Number.NEGATIVE_INFINITY)
		const plan: FlowPlan = {
			requested_from: new Date(start).toISOString(),
			requested_to: new Date(last + stepSeconds * 1000).toISOString(),
			effective_from: new Date(start).toISOString(),
			effective_to: new Date(last + stepSeconds * 1000).toISOString(),
			source: response?.meta.source ?? "1m",
			source_seconds: stepSeconds,
			step_seconds: stepSeconds,
			target_points: rows.length,
		}
		return buildFlowSeries(rows, plan, unit)
	}, [points, response?.meta.source, response?.meta.step_seconds, unit])

	useEffect(() => {
		disposeChart(chartInstance.current)
		chartInstance.current = null
		if (chartRef.current && directionSeries.length > 0) {
			chartInstance.current = createFlowExplorerChart(chartRef.current, "lines", directionSeries, (value) =>
				formatFlowValue(value, unit)
			)
		}
		return () => {
			disposeChart(chartInstance.current)
			chartInstance.current = null
		}
	}, [directionSeries, unit])

	const latestBucket = points
		.filter((point) => point.kind === "kpi" && point.ip_family === "all")
		.reduce((latest, point) => (point.bucket > latest ? point.bucket : latest), "")
	const latestKPI = points.filter(
		(point) => point.kind === "kpi" && point.ip_family === "all" && point.bucket === latestBucket
	)
	const sumLatest = (selector: (point: OverseasPoint) => number) =>
		latestKPI.reduce((sum, point) => sum + selector(point), 0)
	const directionValue = (direction: "in" | "out") =>
		latestKPI.filter((point) => point.direction === direction).reduce((sum, point) => sum + point.value, 0)
	const combined = latestKPI.filter((point) => point.direction === "combined")
	const observedRemoteIPs = combined.reduce((sum, point) => sum + point.observed_remote_ips, 0)
	const observedLocalHosts = combined.reduce((sum, point) => sum + point.observed_local_hosts, 0)
	const receivedRecords = sumLatest((point) => (point.direction === "combined" ? point.received_records : 0))
	const unknownSamplingRecords = sumLatest((point) =>
		point.direction === "combined" ? point.unknown_sampling_records : 0
	)
	const latestGeoBucket = points
		.filter((point) => point.kind === "geo" && point.direction === "combined")
		.reduce((latest, point) => (point.bucket > latest ? point.bucket : latest), "")
	const geoRows = points
		.filter((point) => point.kind === "geo" && point.direction === "combined" && point.bucket === latestGeoBucket)
		.sort((left, right) => right.value - left.value)
	const geoLabel = (point: OverseasPoint) => {
		if (point.geo_scope === "unknown_geo") return "Unknown geography"
		if (point.other) return "Other"
		return response?.meta.geo_labels?.[`${point.geo_version}:${point.geo_value}`]?.name ?? point.geo_value ?? "Unknown"
	}

	return (
		<section className="grid gap-4 rounded-md border border-border bg-card p-4">
			<div className="flex flex-wrap items-end justify-between gap-3">
				<div>
					<h2 className="font-medium">
						<Trans>Overseas summary</Trans>
					</h2>
					<p className="mt-1 text-sm text-muted-foreground">
						<Trans>Inbound and outbound traffic from the published overseas rollup.</Trans>
					</p>
				</div>
				<div className="flex items-end gap-2">
					<OptionSelect
						label="Geo level"
						value={geoLevel}
						onChange={onGeoLevelChange}
						options={[
							{ value: "country", label: "Country" },
							{ value: "region", label: "Region" },
						]}
						width="w-40"
					/>
					<Button variant="outline" size="sm" onClick={onRefresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			{isNarrowed && (
				<p className="rounded-md border border-yellow-300 bg-yellow-50 p-2 text-xs text-yellow-900 dark:bg-yellow-950/30 dark:text-yellow-100">
					<Trans>
						This summary applies the selected device only. Country, province, city and operator filters apply to the
						Explorer below because the overseas rollup does not contain cross-dimension tuples.
					</Trans>
				</p>
			)}
			{error && (
				<p className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive">{error}</p>
			)}

			{response && (
				<>
					<div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
						<Badge variant={response.data.rollup_completeness.complete ? "success" : "warning"}>
							{(response.data.rollup_completeness.ratio * 100).toFixed(1)}% complete
						</Badge>
						<Badge variant="outline">{response.meta.source} source</Badge>
						{response.data.mixed_versions && <Badge variant="warning">Mixed classification versions</Badge>}
						{latestBucket && <span>{new Date(latestBucket).toLocaleString()}</span>}
					</div>
					<div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
						<FlowMetricCard label="Inbound" value={formatFlowValue(directionValue("in"), unit)} />
						<FlowMetricCard label="Outbound" value={formatFlowValue(directionValue("out"), unit)} />
						<FlowMetricCard label="Observed remote IPs" value={observedRemoteIPs.toLocaleString()} />
						<FlowMetricCard label="Observed local hosts" value={observedLocalHosts.toLocaleString()} />
					</div>
					<div className="flex flex-wrap gap-4 text-xs text-muted-foreground">
						<span>{receivedRecords.toLocaleString()} sampled records</span>
						<span>{ratio(unknownSamplingRecords, receivedRecords)} unknown sampling</span>
					</div>
					<div className="grid gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]">
						<div className="rounded-md border border-border p-3">
							<h3 className="mb-2 text-sm font-medium">Traffic trend</h3>
							{directionSeries.length === 0 && !loading ? (
								<div className="flex h-64 items-center justify-center text-sm text-muted-foreground">
									No overseas points found
								</div>
							) : null}
							<div ref={chartRef} className={directionSeries.length > 0 ? "h-72" : "h-0"} />
						</div>
						<div className="rounded-md border border-border p-3">
							<h3 className="mb-2 text-sm font-medium">Top {geoLevel === "country" ? "countries" : "regions"}</h3>
							{geoRows.length === 0 ? (
								<p className="py-8 text-center text-sm text-muted-foreground">No geography points found</p>
							) : (
								<div className="grid gap-2">
									{geoRows.map((point, index) => {
										const label = response.meta.geo_labels?.[`${point.geo_version}:${point.geo_value}`]
										return (
											<div
												key={`${point.geo_version}:${point.geo_value}:${point.other}`}
												className="flex items-center justify-between gap-3 text-sm"
												title={label?.breadcrumb.join(" / ")}
											>
												<span className="min-w-0 truncate text-muted-foreground">
													{index + 1}. {geoLabel(point)}
												</span>
												<span className="shrink-0 font-medium tabular-nums">{formatFlowValue(point.value, unit)}</span>
											</div>
										)
									})}
								</div>
							)}
						</div>
					</div>
				</>
			)}
			{!response && !error && !loading && (
				<p className="py-8 text-center text-sm text-muted-foreground">No overseas result loaded</p>
			)}
		</section>
	)
}

function FlowMetricCard({ label, value }: { label: string; value: string }) {
	return (
		<div className="rounded-md border border-border p-3">
			<p className="text-xs text-muted-foreground">{label}</p>
			<p className="mt-2 text-2xl font-semibold tabular-nums">{value}</p>
		</div>
	)
}

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

function AddressSetChecklist({
	label,
	items,
	value,
	onChange,
}: {
	label: string
	items: AddressSetItem[]
	value: string[]
	onChange: (value: string[]) => void
}) {
	return (
		<div className="grid w-48 gap-1.5">
			<Label className="text-xs">
				{label} ({value.length})
			</Label>
			<div className="h-28 overflow-y-auto rounded-md border border-input bg-background p-2">
				{items.length === 0 ? (
					<p className="text-xs text-muted-foreground">No address sets</p>
				) : (
					items.map((item, index) => (
						<div key={item.id} className="flex items-center gap-2 py-1 text-xs">
							<Checkbox
								id={`flow-address-set-${label}-${index}`}
								checked={value.includes(item.id)}
								onCheckedChange={(checked) =>
									onChange(
										checked === true
											? [...new Set([...value, item.id])].sort()
											: value.filter((candidate) => candidate !== item.id)
									)
								}
							/>
							<Label
								htmlFor={`flow-address-set-${label}-${index}`}
								className="cursor-pointer truncate text-xs"
								title={item.name}
							>
								{item.name}
							</Label>
						</div>
					))
				)}
			</div>
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

function sortReferences<T extends { sort_order?: number; name: string; id?: string }>(items: T[]) {
	return items
		.slice()
		.sort(
			(left, right) =>
				(left.sort_order ?? 0) - (right.sort_order ?? 0) ||
				left.name.localeCompare(right.name) ||
				(left.id ?? "").localeCompare(right.id ?? "")
		)
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

function labelProtocolPoints(response: FlowQueryResponse): FlowQueryResponse {
	return {
		...response,
		data: {
			...response.data,
			points: ((response.data.points ?? []) as FlowPoint[]).map((point) => ({
				...point,
				dimension_value: protocolLabel(point.dimension_value),
			})),
		},
	}
}

function protocolLabel(value: string) {
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
	return names[value] ?? value
}

function flowDimensionLabel(value: string, mode: QueryMode) {
	if (mode !== "protocol") return value
	const match = /^(\d+)(.*)$/.exec(value)
	return match ? `${protocolLabel(match[1])}${match[2]}` : value
}

function formatFlowRatio(value: number) {
	return `${(Math.max(0, value) * 100).toFixed(1)}%`
}

function flowTableFilterLabel(field: string, value: string, unit: string, mode: QueryMode) {
	if (field === "dimension") return flowDimensionLabel(value, mode)
	const numeric = Number(value)
	if (!Number.isFinite(numeric)) return value
	if (field === "records") return numeric.toLocaleString()
	if (field === "unknown" || field === "quality") return formatFlowRatio(numeric)
	if (field === "total") return formatFlowTotal(numeric, unit)
	return formatFlowValue(numeric, unit)
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
