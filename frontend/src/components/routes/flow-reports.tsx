import type { MessageDescriptor } from "@lingui/core"
import { msg, t } from "@lingui/core/macro"
import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DownloadIcon, RefreshCwIcon, SlidersHorizontalIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { FlowRecordTable } from "@/components/flow/flow-record-table"
import { Button, buttonVariants } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { toast } from "@/components/ui/use-toast"
import { canManageAddressLibrary, api } from "@/lib/api"
import type { FlowFilterExpression } from "@/lib/flow-explorer-model"
import {
	buildReportSeries,
	FLOW_REPORT_CATEGORIES,
	flowReportCategoryLabel,
	FLOW_REPORT_RESIDUALS,
	FLOW_REPORT_TIME_PRESETS,
	flowReportKind,
	panelJointPoints,
	panelPoints,
	reportCategoryTotals,
	reportPanel,
	reportSeriesStats,
	resolveFlowReportRange,
	transformReportSeries,
	type FlowReportDisplayMode,
	type FlowReportPanel,
	type FlowReportResponse,
	type FlowReportSeries,
	type FlowReportSurface,
	type FlowDistributionPage,
	type FlowTablePage,
} from "@/lib/flow-report-model"
import { createLineChart, disposeChart } from "@/lib/vchart"
import type { ColumnDefine, ServerFilterOption } from "@/lib/vtable"
import { cn } from "@/lib/utils"

type ReferenceItem = { id: string; name: string; path?: Array<{ id: string; name: string }> }
type OperatorItem = { id: string; name: string; short_name?: string }
type DeviceItem = { ID?: string; id?: string; SysName?: string; sys_name?: string; Name?: string; name?: string }
type ListResponse<T> = { items?: T[]; version?: string }
type AddressDimensionRuntimeStatus = { snapshot?: { id?: string } }
type FlowReportExportQuery = {
	dataset: "flow.traffic"
	from: string
	to: string
	step_seconds: number
	limit: number
	value_layer: "customer"
	parameters: Record<string, unknown>
}

type TableControl = {
	search: string
	page: number
	pageSize: number
	sortBy: string
	sortDirection: "asc" | "desc"
	filters: Record<string, string[]>
}

type VPNReportSummary = {
	finding_count: number
	suspected_hosts: number
	high_risk_hosts: number
	active_ports: number
	inbound_bytes: number
	outbound_bytes: number
	total_bytes: number
	minimum_complete_ratio: number
	port_distribution: Array<{ value: string; count: number; bytes: number }>
	type_distribution: Array<{ value: string; count: number; bytes: number }>
	port_table?: FlowDistributionPage
	type_table?: FlowDistributionPage
	trend: Array<{ bucket: string; inbound_bytes: number; outbound_bytes: number }>
	rule_set_versions: string[]
	source_generations: number[]
}

type VPNRulePublicationSummary = {
	snapshot_id: string
	version: number
	effective_from: string
	rule_count: number
	queryability: string
	observed_workers: number
	ready_workers: number
	failed_workers: number
	behind_workers: number
	uninstalled_workers: number
}

const INITIAL_TABLE: TableControl = {
	search: "",
	page: 0,
	pageSize: 25,
	sortBy: "maximum",
	sortDirection: "desc",
	filters: {},
}

const INITIAL_REPORT_TABLES: Record<string, TableControl> = {
	business_matrix: { ...INITIAL_TABLE, sortBy: "total" },
	port_distribution: { ...INITIAL_TABLE, sortBy: "bytes" },
	type_distribution: { ...INITIAL_TABLE, sortBy: "bytes" },
}

const METRICS = [
	["estimated_bps", msg`Estimated bit rate`],
	["estimated_bytes", msg`Estimated bytes`],
	["estimated_pps", msg`Estimated packet rate`],
	["estimated_packets", msg`Estimated packets`],
	["raw_bps", msg`Sampled bit rate`],
	["raw_bytes", msg`Sampled bytes`],
] as const

const GROUPINGS = [
	{ value: "category", label: msg`Default six classes`, dimension: "category", categories: [] },
	{
		value: "onnet_province",
		label: msg`On-net by province`,
		dimension: "geo.province",
		categories: ["on_net_local_city", "on_net_cross_city", "on_net_cross_province"],
	},
	{
		value: "offnet_operator",
		label: msg`Off-net by operator`,
		dimension: "isp",
		categories: ["off_net_in_province", "off_net_cross_province"],
	},
	{ value: "overseas", label: msg`Overseas by country`, dimension: "geo.country", categories: ["overseas"] },
	{ value: "vpn", label: msg`VPN report`, dimension: "category", categories: [] },
] as const

const WEEKDAYS = [
	[1, msg`Mon`],
	[2, msg`Tue`],
	[3, msg`Wed`],
	[4, msg`Thu`],
	[5, msg`Fri`],
	[6, msg`Sat`],
	[7, msg`Sun`],
] as const

const PAGE_COPY: Record<FlowReportSurface, { title: MessageDescriptor; description: MessageDescriptor }> = {
	overview: {
		title: msg`Flow Overview`,
		description: msg`Direction, six exclusive traffic classes and business matrix.`,
	},
	dimensions: {
		title: msg`Traffic by Dimension`,
		description: msg`Fast country, province, city, operator and business reporting.`,
	},
	source: {
		title: msg`Source IP Report`,
		description: msg`Top source addresses with direction, traffic class and drill-down.`,
	},
	destination: {
		title: msg`Destination IP Report`,
		description: msg`Top destination addresses with direction, traffic class and drill-down.`,
	},
	overseas: {
		title: msg`Overseas Traffic Report`,
		description: msg`Countries, regions, ASN, ports and protocols with observed-data caveats.`,
	},
	vpn: {
		title: msg`VPN Traffic Report`,
		description: msg`Traffic analysis separated from findings, evidence and probe disposition.`,
	},
}

function useFlowReportCategoryLabel() {
	const { i18n } = useLingui()
	return useCallback((key: string) => flowReportCategoryLabel(key, i18n), [i18n])
}

export default memo(function FlowReports({ surface }: { surface: FlowReportSurface }) {
	const { t, i18n } = useLingui()
	const defaultTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC"
	const [range, setRange] = useState(() => queryState("range", "1h"))
	const [customStart, setCustomStart] = useState(() => queryState("start", ""))
	const [customEnd, setCustomEnd] = useState(() => queryState("end", ""))
	const [timezone, setTimezone] = useState(() => queryState("timezone", defaultTimezone))
	const [metric, setMetric] = useState(() =>
		queryState("metric", surface === "vpn" ? "estimated_bytes" : "estimated_bps")
	)
	const [displayMode, setDisplayMode] = useState<FlowReportDisplayMode>(
		() => queryState("display", "value") as FlowReportDisplayMode
	)
	const [groupBy, setGroupBy] = useState(() => queryState("group", "category"))
	const [topN, setTopN] = useState(() => boundedNumber(queryState("top", "20"), 1, 100, 20))
	const [business, setBusiness] = useState(() => queryState("business", ""))
	const [country, setCountry] = useState(() => queryState("country", "all"))
	const [province, setProvince] = useState(() => queryState("province", "all"))
	const [city, setCity] = useState(() => queryState("city", "all"))
	const [operator, setOperator] = useState(() => queryState("operator", "all"))
	const [device, setDevice] = useState(() => queryState("device", "all"))
	const [peakStart, setPeakStart] = useState(() => queryState("peak_start", "20:00"))
	const [peakEnd, setPeakEnd] = useState(() => queryState("peak_end", "23:00"))
	const [peakEnabled, setPeakEnabled] = useState(() => queryState("peak", "") === "1")
	const [peakDays, setPeakDays] = useState<number[]>(() => parsePeakDays(queryState("peak_days", "1,2,3,4,5")))
	const [countries, setCountries] = useState<ReferenceItem[]>([])
	const [provinces, setProvinces] = useState<ReferenceItem[]>([])
	const [cities, setCities] = useState<ReferenceItem[]>([])
	const [operators, setOperators] = useState<OperatorItem[]>([])
	const [devices, setDevices] = useState<DeviceItem[]>([])
	const [geoVersion, setGeoVersion] = useState("")
	const [table, setTable] = useState<TableControl>(INITIAL_TABLE)
	const [reportTables, setReportTables] = useState<Record<string, TableControl>>(INITIAL_REPORT_TABLES)
	const [tableSearch, setTableSearch] = useState("")
	const [selectedDetailIP, setSelectedDetailIP] = useState(() => queryState("ip", ""))
	const [response, setResponse] = useState<FlowReportResponse | null>(null)
	const [loading, setLoading] = useState(false)
	const [exporting, setExporting] = useState(false)
	const [error, setError] = useState("")
	const [referenceWarning, setReferenceWarning] = useState("")
	const requestSequence = useRef(0)
	const abortRef = useRef<AbortController | null>(null)
	const lastExportQuery = useRef<FlowReportExportQuery | null>(null)

	useEffect(() => {
		document.title = `${i18n._(PAGE_COPY[surface].title)} / Watchdog`
	}, [surface, i18n])

	useEffect(() => {
		Promise.allSettled([
			api.send<ListResponse<ReferenceItem>>("/api/v1/geo/dictionary", {
				query: { kind: "country", enabled: true, limit: 500 },
			}),
			api.send<AddressDimensionRuntimeStatus>("/api/v1/dimensions/address/status"),
			api.send<ListResponse<OperatorItem>>("/api/v1/network/operators", { query: { enabled: true, limit: 500 } }),
			api.send<ListResponse<DeviceItem>>("/api/v1/devices", { query: { kind: "network" } }),
		]).then(([geo, addressRuntime, operatorResult, deviceResult]) => {
			const warnings: string[] = []
			if (geo.status === "fulfilled") {
				setCountries(sortByName(geo.value.items ?? []))
			} else warnings.push(t`Geography dictionary is unavailable`)
			if (addressRuntime.status === "fulfilled" && addressRuntime.value.snapshot?.id) {
				setGeoVersion(addressRuntime.value.snapshot.id)
			} else {
				setGeoVersion("")
				warnings.push(t`Address source data is imported but no Flow address snapshot is active`)
			}
			if (operatorResult.status === "fulfilled") setOperators(sortByName(operatorResult.value.items ?? []))
			else warnings.push(t`Operator catalog is unavailable`)
			if (deviceResult.status === "fulfilled") setDevices(deviceResult.value.items ?? [])
			else warnings.push(t`Device catalog is unavailable`)
			setReferenceWarning(warnings.join("; "))
		})
	}, [t])

	useEffect(() => {
		if (country === "all") {
			setProvinces([])
			return
		}
		api
			.send<ListResponse<ReferenceItem>>("/api/v1/geo/dictionary", {
				query: { kind: "province", parent_id: country, enabled: true, limit: 500 },
			})
			.then((result) => setProvinces(sortByName(result.items ?? [])))
			.catch(() => setReferenceWarning(t`Province catalog is unavailable`))
	}, [country, t])

	useEffect(() => {
		if (province === "all") {
			setCities([])
			return
		}
		api
			.send<ListResponse<ReferenceItem>>("/api/v1/geo/dictionary", {
				query: { kind: "city", parent_id: province, enabled: true, limit: 500 },
			})
			.then((result) => setCities(sortByName(result.items ?? [])))
			.catch(() => setReferenceWarning(t`City catalog is unavailable`))
	}, [province, t])

	const refresh = useCallback(
		async (nextTable?: TableControl, nextReportTables?: Record<string, TableControl>) => {
			const activeTable = nextTable ?? table
			const activeReportTables = nextReportTables ?? reportTables
			const sequence = ++requestSequence.current
			abortRef.current?.abort()
			const controller = new AbortController()
			abortRef.current = controller
			setLoading(true)
			setError("")
			lastExportQuery.current = null
			try {
				const selectedRange = resolveFlowReportRange(range, customStart, customEnd, timezone)
				const filter = geoFilter(country, province, city)
				const filters: Record<string, unknown> = {}
				const businesses = business
					.split(",")
					.map((value) => value.trim())
					.filter(Boolean)
				if (businesses.length) filters.businesses = businesses
				const grouping = GROUPINGS.find((item) => item.value === groupBy) ?? GROUPINGS[0]
				if (surface === "dimensions" && grouping.categories.length) filters.categories = grouping.categories
				if (device !== "all") filters.device_ids = [device]
				if (filter && geoVersion) filters.geo_versions = [geoVersion]
				const body: Record<string, unknown> = {
					from: selectedRange.start,
					to: selectedRange.end,
					step_seconds: 0,
					limit: 250_000,
					value_layer: "customer",
					metric: surface === "vpn" ? "estimated_bytes" : metric,
					filters,
					filter,
					top_n: topN,
					include_other: true,
					timezone,
					target_points: 300,
					operator_selection: operator === "all" ? undefined : { operator_id: operator },
					report: {
						schema_version: 1,
						kind: flowReportKind(surface),
						side: surface === "source" || surface === "destination" ? surface : undefined,
						group_by: surface === "dimensions" ? grouping.dimension : undefined,
						display_mode: displayMode,
						peak_windows: peakEnabled ? [{ days: peakDays, start_local: peakStart, end_local: peakEnd }] : undefined,
						tables:
							surface === "overview"
								? { business_matrix: tableRequest(activeReportTables.business_matrix) }
								: surface === "vpn"
									? {
											port_distribution: tableRequest(activeReportTables.port_distribution),
											type_distribution: tableRequest(activeReportTables.type_distribution),
										}
									: undefined,
					},
				}
				if (surface === "source" || surface === "destination") {
					body.table = {
						search: activeTable.search || undefined,
						sort_by: activeTable.sortBy,
						sort_direction: activeTable.sortDirection,
						limit: activeTable.pageSize,
						offset: activeTable.page * activeTable.pageSize,
						filters: activeTable.filters,
					}
				}
				const { from, to, step_seconds, limit, value_layer, ...parameters } = body
				const result = await api.send<FlowReportResponse>("/api/v1/flow/reports/query", {
					method: "POST",
					body,
					signal: controller.signal,
				})
				if (sequence !== requestSequence.current) return
				setResponse(result)
				lastExportQuery.current = {
					dataset: "flow.traffic",
					from: String(from),
					to: String(to),
					step_seconds: Number(step_seconds),
					limit: Number(limit),
					value_layer: value_layer as "customer",
					parameters,
				}
				writeURLState({
					range,
					start: customStart,
					end: customEnd,
					timezone,
					metric,
					display: displayMode,
					group: groupBy,
					top: topN,
					business,
					country,
					province,
					city,
					operator,
					device,
					peak: peakEnabled ? "1" : "",
					peak_days: peakDays.join(","),
					peak_start: peakStart,
					peak_end: peakEnd,
				})
			} catch (reason) {
				if (sequence !== requestSequence.current || controller.signal.aborted) return
				setResponse(null)
				setError(reason instanceof Error ? reason.message : t`Failed to load Flow report`)
			} finally {
				if (sequence === requestSequence.current) setLoading(false)
			}
		},
		[
			business,
			city,
			country,
			customEnd,
			customStart,
			device,
			displayMode,
			geoVersion,
			groupBy,
			metric,
			operator,
			peakDays,
			peakEnabled,
			peakEnd,
			peakStart,
			province,
			range,
			reportTables,
			surface,
			table,
			timezone,
			topN,
			t,
		]
	)

	useEffect(() => {
		refresh().catch(() => {})
		return () => abortRef.current?.abort()
		// The initial request is intentional; subsequent controls use Refresh so
		// editing a time or filter does not produce query storms.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [surface])

	const runTable = useCallback(
		(next: TableControl) => {
			setTable(next)
			refresh(next).catch(() => {})
		},
		[refresh]
	)
	const updateReportTable = useCallback(
		(id: string, next: TableControl, run: boolean) => {
			const tables = { ...reportTables, [id]: next }
			setReportTables(tables)
			if (run) refresh(undefined, tables).catch(() => {})
		},
		[refresh, reportTables]
	)

	const exportFullReport = useCallback(async () => {
		if (!lastExportQuery.current) return
		setExporting(true)
		try {
			const task = await api.send<{ ID?: string; id?: string }>("/api/v1/flow/exports", {
				method: "POST",
				body: { query: lastExportQuery.current, format: "csv" },
			})
			const id = task.ID ?? task.id ?? ""
			toast({ title: t`Flow report export queued` })
			navigate(id ? getPagePath($router, "export_detail", { id }) : getPagePath($router, "exports"))
		} catch (reason) {
			toast({
				title: reason instanceof Error ? reason.message : t`Failed to create Flow report export`,
				variant: "destructive",
			})
		} finally {
			setExporting(false)
		}
	}, [t])

	const copy = PAGE_COPY[surface]
	const detailRange = useMemo(() => {
		try {
			return resolveFlowReportRange(range, customStart, customEnd, timezone)
		} catch {
			return { start: "", end: "" }
		}
	}, [customEnd, customStart, range, timezone])
	return (
		<div className="my-4 grid gap-4">
			<div className="flex flex-wrap items-start justify-between gap-3">
				<div>
					<h1 className="text-2xl font-semibold">{i18n._(copy.title)}</h1>
					<p className="text-sm text-muted-foreground">{i18n._(copy.description)}</p>
				</div>
				<Link href={getPagePath($router, "traffic_matrix")} className={buttonVariants({ variant: "outline" })}>
					<SlidersHorizontalIcon className="mr-2 h-4 w-4" />
					<Trans>Advanced Explorer</Trans>
				</Link>
			</div>

			<Card>
				<CardContent className="grid gap-4 pt-6">
					<div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-6">
						<ReportSelect
							label={t`Time range`}
							value={range}
							onChange={setRange}
							options={FLOW_REPORT_TIME_PRESETS.map((item) => [item.value, item.label])}
						/>
						<ReportSelect
							label={t`Metric`}
							value={surface === "vpn" ? "estimated_bytes" : metric}
							onChange={setMetric}
							options={METRICS.map(([value, label]) => [value, i18n._(label)] as const)}
							disabled={surface === "vpn"}
						/>
						<ReportSelect
							label={t`Display`}
							value={displayMode}
							onChange={(value) => setDisplayMode(value as FlowReportDisplayMode)}
							options={[
								["value", t`Value`],
								["share", t`Share`],
								["difference", t`Inbound − outbound`],
							]}
						/>
						{surface === "dimensions" ? (
							<ReportSelect
								label={t`Quick grouping`}
								value={groupBy}
								onChange={(value) => (value === "vpn" ? navigate(getPagePath($router, "flow_vpn")) : setGroupBy(value))}
								options={GROUPINGS.map((item) => [item.value, i18n._(item.label)] as const)}
							/>
						) : null}
						<ReportSelect
							label={t`Country`}
							value={country}
							onChange={(value) => {
								setCountry(value)
								setProvince("all")
								setCity("all")
							}}
							options={[["all", t`All countries`], ...countries.map((item) => [item.id, item.name] as const)]}
						/>
						<ReportSelect
							label={t`Province`}
							value={province}
							onChange={(value) => {
								setProvince(value)
								setCity("all")
							}}
							options={[["all", t`All provinces`], ...provinces.map((item) => [item.id, item.name] as const)]}
							disabled={country === "all"}
						/>
						<ReportSelect
							label={t`City`}
							value={city}
							onChange={setCity}
							options={[["all", t`All cities`], ...cities.map((item) => [item.id, item.name] as const)]}
							disabled={province === "all"}
						/>
						<ReportSelect
							label={t`Operator`}
							value={operator}
							onChange={setOperator}
							options={[
								["all", t`All operators`],
								...operators.map((item) => [item.id, item.short_name || item.name] as const),
							]}
						/>
						<ReportSelect
							label={t`Device`}
							value={device}
							onChange={setDevice}
							options={[
								["all", t`All devices`],
								...devices.map((item) => [deviceID(item), deviceName(item)] as const).filter((item) => item[0]),
							]}
						/>
						<ReportInput
							label={t`Business`}
							value={business}
							onChange={setBusiness}
							placeholder={t`Comma-separated labels`}
						/>
						<ReportInput label={t`Timezone`} value={timezone} onChange={setTimezone} />
						<ReportInput
							label={t`Top N`}
							type="number"
							value={String(topN)}
							onChange={(value) => setTopN(boundedNumber(value, 1, 100, 20))}
						/>
						<ReportInput label={t`Peak start`} type="time" value={peakStart} onChange={setPeakStart} />
						<ReportInput label={t`Peak end`} type="time" value={peakEnd} onChange={setPeakEnd} />
					</div>
					<label htmlFor="flow-report-peak-enabled" className="flex items-center gap-2 text-sm">
						<Checkbox
							id="flow-report-peak-enabled"
							checked={peakEnabled}
							onCheckedChange={(checked) => setPeakEnabled(checked === true)}
						/>
						<Trans>Apply the local peak window to every report panel</Trans>
					</label>
					{peakEnabled ? <ReportDaySelector value={peakDays} onChange={setPeakDays} /> : null}
					{range === "custom" ? (
						<div className="grid gap-3 sm:grid-cols-2">
							<ReportInput label={t`Start`} type="datetime-local" value={customStart} onChange={setCustomStart} />
							<ReportInput label={t`End`} type="datetime-local" value={customEnd} onChange={setCustomEnd} />
						</div>
					) : null}
					<div className="flex flex-wrap items-center gap-2">
						<Button onClick={() => refresh()} disabled={loading}>
							<RefreshCwIcon className={cn("mr-2 h-4 w-4", loading && "animate-spin")} />
							<Trans>Refresh</Trans>
						</Button>
						<Button variant="outline" disabled={!lastExportQuery.current || exporting} onClick={exportFullReport}>
							<DownloadIcon className="mr-2 h-4 w-4" />
							{exporting ? <Trans>Queuing...</Trans> : <Trans>Export full report</Trans>}
						</Button>
						<Button variant="ghost" disabled={!response} onClick={() => response && downloadReport(response, surface)}>
							<DownloadIcon className="mr-2 h-4 w-4" />
							<Trans>Download visible data</Trans>
						</Button>
						{referenceWarning ? <span className="text-sm text-amber-700">{referenceWarning}</span> : null}
					</div>
				</CardContent>
			</Card>

			{error ? (
				<div className="rounded-md border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
					{error}
				</div>
			) : null}
			{response ? <ReportStatus response={response} /> : null}
			{response ? (
				<ReportBody
					surface={surface}
					response={response}
					table={table}
					tableSearch={tableSearch}
					setTableSearch={setTableSearch}
					runTable={runTable}
					reportTables={reportTables}
					updateReportTable={updateReportTable}
					loading={loading}
					onSelectIP={setSelectedDetailIP}
				/>
			) : !loading && !error ? (
				<div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
					<Trans>No report data.</Trans>
				</div>
			) : null}
			{(surface === "source" || surface === "destination") && detailRange.start && detailRange.end ? (
				<FlowRecordTable
					endpoint={surface}
					selectedIP={selectedDetailIP}
					onIPChange={setSelectedDetailIP}
					from={detailRange.start}
					to={detailRange.end}
				/>
			) : null}
		</div>
	)
})

function ReportBody({
	surface,
	response,
	table,
	tableSearch,
	setTableSearch,
	runTable,
	reportTables,
	updateReportTable,
	loading,
	onSelectIP,
}: {
	surface: FlowReportSurface
	response: FlowReportResponse
	table: TableControl
	tableSearch: string
	setTableSearch: (value: string) => void
	runTable: (next: TableControl) => void
	reportTables: Record<string, TableControl>
	updateReportTable: (id: string, next: TableControl, run: boolean) => void
	loading: boolean
	onSelectIP: (address: string) => void
}) {
	switch (surface) {
		case "overview":
			return (
				<OverviewReport
					response={response}
					table={reportTables.business_matrix}
					updateTable={(next, run) => updateReportTable("business_matrix", next, run)}
					loading={loading}
				/>
			)
		case "dimensions":
			return <DimensionReport response={response} />
		case "source":
		case "destination":
			return (
				<EndpointReport
					response={response}
					table={table}
					tableSearch={tableSearch}
					setTableSearch={setTableSearch}
					runTable={runTable}
					loading={loading}
					onSelectIP={onSelectIP}
				/>
			)
		case "overseas":
			return <OverseasReport response={response} />
		case "vpn":
			return <VPNReport response={response} tables={reportTables} updateTable={updateReportTable} loading={loading} />
	}
}

function OverviewReport({
	response,
	table,
	updateTable,
	loading,
}: {
	response: FlowReportResponse
	table: TableControl
	updateTable: (next: TableControl, run: boolean) => void
	loading: boolean
}) {
	const { t } = useLingui()
	const total = reportPanel(response, "total")
	const inbound = reportPanel(response, "category_in")
	const outbound = reportPanel(response, "category_out")
	const totalSeries = buildReportSeries(total)
	const inboundCurrent = reportSeriesStats(totalSeries.find((series) => series.name === "Inbound")).current
	const outboundCurrent = reportSeriesStats(totalSeries.find((series) => series.name === "Outbound")).current
	const inboundTotals = reportCategoryTotals(inbound)
	const outboundTotals = reportCategoryTotals(outbound)
	const totalInbound = [...inboundTotals.values()].reduce((sum, value) => sum + value, 0)
	const totalOutbound = [...outboundTotals.values()].reduce((sum, value) => sum + value, 0)
	const displayTotalSeries = totalSeries.map((series) => ({
		...series,
		name: series.name === "Inbound" ? t`Inbound` : series.name === "Outbound" ? t`Outbound` : series.name,
	}))
	return (
		<div className="grid gap-4">
			<Card>
				<CardHeader>
					<CardTitle>
						<Trans>Total traffic</Trans>
					</CardTitle>
					<CardDescription>
						<Trans>Inbound and outbound from the local-network perspective.</Trans>
					</CardDescription>
				</CardHeader>
				<CardContent>
					<div className="mb-3 flex flex-wrap gap-6 text-xl font-semibold">
						<span>↓ {formatMetric(inboundCurrent, total?.meta.unit)}</span>
						<span>↑ {formatMetric(outboundCurrent, total?.meta.unit)}</span>
					</div>
					<ReportChart series={displayTotalSeries} unit={total?.meta.unit} />
				</CardContent>
			</Card>
			<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
				{FLOW_REPORT_CATEGORIES.map((category) => (
					<CategoryCard
						key={category}
						category={category}
						inbound={inbound}
						outbound={outbound}
						inboundTotal={totalInbound}
						outboundTotal={totalOutbound}
					/>
				))}
			</div>
			<CategoryShareChart inbound={inboundTotals} outbound={outboundTotals} />
			<ResidualCard
				inbound={inboundTotals}
				outbound={outboundTotals}
				inboundTotal={totalInbound}
				outboundTotal={totalOutbound}
			/>
			<BusinessMatrix
				inbound={reportPanel(response, "business_category_in")}
				outbound={reportPanel(response, "business_category_out")}
				table={table}
				updateTable={updateTable}
				loading={loading}
			/>
		</div>
	)
}

function CategoryCard({
	category,
	inbound,
	outbound,
	inboundTotal,
	outboundTotal,
}: {
	category: string
	inbound?: FlowReportPanel
	outbound?: FlowReportPanel
	inboundTotal: number
	outboundTotal: number
}) {
	const { t } = useLingui()
	const categoryLabel = useFlowReportCategoryLabel()
	const inSeries = buildReportSeries(inbound).find((series) => series.name === category)
	const outSeries = buildReportSeries(outbound).find((series) => series.name === category)
	const inStats = reportSeriesStats(inSeries)
	const outStats = reportSeriesStats(outSeries)
	return (
		<Card>
			<CardHeader className="api-3">
				<CardTitle>{categoryLabel(category)}</CardTitle>
			</CardHeader>
			<CardContent>
				<div className="grid grid-cols-2 gap-2 text-sm">
					<div>
						<span className="text-muted-foreground">{t`Inbound`}</span>
						<div className="font-semibold">{formatMetric(inStats.current, inbound?.meta.unit)}</div>
						<div className="text-xs text-muted-foreground">{formatShare(inStats.total, inboundTotal)}</div>
					</div>
					<div>
						<span className="text-muted-foreground">{t`Outbound`}</span>
						<div className="font-semibold">{formatMetric(outStats.current, outbound?.meta.unit)}</div>
						<div className="text-xs text-muted-foreground">{formatShare(outStats.total, outboundTotal)}</div>
					</div>
				</div>
				<ReportChart
					compact
					series={
						[inSeries && { ...inSeries, name: t`Inbound` }, outSeries && { ...outSeries, name: t`Outbound` }].filter(
							Boolean
						) as FlowReportSeries[]
					}
					unit={inbound?.meta.unit}
				/>
			</CardContent>
		</Card>
	)
}

function CategoryShareChart({ inbound, outbound }: { inbound: Map<string, number>; outbound: Map<string, number> }) {
	const categoryLabel = useFlowReportCategoryLabel()
	const inboundTotal = [...inbound.values()].reduce((sum, value) => sum + value, 0)
	const outboundTotal = [...outbound.values()].reduce((sum, value) => sum + value, 0)
	const inboundClassified = FLOW_REPORT_CATEGORIES.reduce((sum, category) => sum + (inbound.get(category) ?? 0), 0)
	const outboundClassified = FLOW_REPORT_CATEGORIES.reduce((sum, category) => sum + (outbound.get(category) ?? 0), 0)
	return (
		<Card>
			<CardHeader>
				<CardTitle>
					<Trans>Six-class share</Trans>
				</CardTitle>
				<CardDescription>
					<Trans>Shares use the full directional total. Residual traffic remains outside the six classes.</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent className="grid gap-4">
				{FLOW_REPORT_CATEGORIES.map((category) => {
					const inShare = inboundTotal ? (inbound.get(category) ?? 0) / inboundTotal : null
					const outShare = outboundTotal ? (outbound.get(category) ?? 0) / outboundTotal : null
					return (
						<div key={category} className="grid gap-1">
							<div className="flex justify-between gap-3 text-sm">
								<span>{categoryLabel(category)}</span>
								<span>
									↓ {formatNullablePercent(inShare)} · ↑ {formatNullablePercent(outShare)}
								</span>
							</div>
							<div className="grid h-2 grid-cols-2 gap-1">
								<div className="overflow-hidden rounded bg-muted">
									<div className="h-full bg-blue-500" style={{ width: `${Math.min(100, (inShare ?? 0) * 100)}%` }} />
								</div>
								<div className="overflow-hidden rounded bg-muted">
									<div
										className="h-full bg-emerald-500"
										style={{ width: `${Math.min(100, (outShare ?? 0) * 100)}%` }}
									/>
								</div>
							</div>
						</div>
					)
				})}
				<div className="border-t pt-3 text-sm text-muted-foreground">
					<Trans>
						Classified coverage: ↓ {formatShare(inboundClassified, inboundTotal)} · ↑{" "}
						{formatShare(outboundClassified, outboundTotal)}
					</Trans>
				</div>
			</CardContent>
		</Card>
	)
}

function ResidualCard({
	inbound,
	outbound,
	inboundTotal,
	outboundTotal,
}: {
	inbound: Map<string, number>
	outbound: Map<string, number>
	inboundTotal: number
	outboundTotal: number
}) {
	const categoryLabel = useFlowReportCategoryLabel()
	const rows = FLOW_REPORT_RESIDUALS.map((category) => ({
		category,
		inbound: inbound.get(category) ?? 0,
		outbound: outbound.get(category) ?? 0,
	})).filter((row) => row.inbound || row.outbound)
	if (!rows.length) return null
	return (
		<Card>
			<CardHeader>
				<CardTitle>
					<Trans>Classification residuals</Trans>
				</CardTitle>
				<CardDescription>
					<Trans>Residual traffic is explicit and never redistributed into the six business classes.</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent className="grid gap-2">
				{rows.map((row) => (
					<div key={row.category} className="flex flex-wrap justify-between gap-3 border-b py-2 text-sm">
						<span>{categoryLabel(row.category)}</span>
						<span>
							↓ {formatShare(row.inbound, inboundTotal)} · ↑ {formatShare(row.outbound, outboundTotal)}
						</span>
					</div>
				))}
			</CardContent>
		</Card>
	)
}

function BusinessMatrix({
	inbound,
	outbound,
	table,
	updateTable,
	loading,
}: {
	inbound?: FlowReportPanel
	outbound?: FlowReportPanel
	table: TableControl
	updateTable: (next: TableControl, run: boolean) => void
	loading: boolean
}) {
	const { t } = useLingui()
	const categoryLabel = useFlowReportCategoryLabel()
	if (inbound?.status === "unavailable" || outbound?.status === "unavailable")
		return <UnavailablePanel title={t`Business × traffic class`} reason={inbound?.reason || outbound?.reason} />
	const page = inbound?.data?.matrix_table
	const rows = (page?.items ?? []).map((item) => ({
		business: item.business,
		total: formatDirectionalValue(item.total, totalUnit(inbound?.meta.unit)),
		...Object.fromEntries(
			FLOW_REPORT_CATEGORIES.map((category) => [
				category,
				formatDirectionalValue(item.categories[category], totalUnit(inbound?.meta.unit)),
			])
		),
		residual: formatDirectionalValue(item.residual, totalUnit(inbound?.meta.unit)),
	}))
	const columns: ColumnDefine[] = [
		{ field: "business", title: t`Business`, width: 220 },
		{ field: "total", title: t`Total · ↓ / ↑`, width: 245 },
		...FLOW_REPORT_CATEGORIES.map((category) => ({
			field: category,
			title: `${categoryLabel(category)} · ↓ / ↑`,
			width: 245,
		})),
		{ field: "residual", title: t`Residual · ↓ / ↑`, width: 225 },
	]
	const filtering = reportTableFiltering(page?.filter_options, table, updateTable)
	const sorting = {
		field: table.sortBy,
		direction: table.sortDirection,
		fields: Object.fromEntries(
			["business", "total", ...FLOW_REPORT_CATEGORIES, "residual"].map((field) => [field, field])
		),
		onSortChange: (field: string, sortDirection: "asc" | "desc") =>
			updateTable({ ...table, page: 0, sortBy: field, sortDirection }, true),
	}
	return (
		<Card>
			<CardHeader>
				<CardTitle>
					<Trans>Business × traffic class</Trans>
				</CardTitle>
				<CardDescription>
					<Trans>Each cell is inbound / outbound and its directional share for the same frozen report window.</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent>
				<PagedVTable
					records={rows}
					columns={columns}
					loading={loading}
					emptyText={t`No business traffic`}
					height={Math.min(440, 42 * (rows.length + 1))}
					searchValue={table.search}
					onSearchChange={(search) => updateTable({ ...table, search }, false)}
					onSearchSubmit={(search) => updateTable({ ...table, page: 0, search: search.trim() }, true)}
					serverPagination={{
						page: table.page,
						pageSize: table.pageSize,
						totalCount: page?.total ?? 0,
						onPageChange: (page) => updateTable({ ...table, page }, true),
						onPageSizeChange: (pageSize) => updateTable({ ...table, page: 0, pageSize }, true),
					}}
					serverFiltering={filtering}
					serverSorting={sorting}
				/>
			</CardContent>
		</Card>
	)
}

function DimensionReport({ response }: { response: FlowReportResponse }) {
	const { t } = useLingui()
	const inbound = buildReportSeries(reportPanel(response, "dimension_in"))
	const outbound = buildReportSeries(reportPanel(response, "dimension_out"))
	const transformed = transformReportSeries(inbound, outbound, response.data.display_mode)
	if (response.data.display_mode === "difference")
		return (
			<ChartCard
				title={t`Inbound − outbound`}
				panel={reportPanel(response, "dimension_in")}
				series={transformed.difference}
			/>
		)
	const unit = response.data.display_mode === "share" ? "ratio" : undefined
	return (
		<div className="grid gap-4">
			<ChartCard
				title={t`Inbound`}
				panel={reportPanel(response, "dimension_in")}
				series={transformed.inbound}
				unitOverride={unit}
			/>
			<ChartCard
				title={t`Outbound`}
				panel={reportPanel(response, "dimension_out")}
				series={transformed.outbound}
				unitOverride={unit}
			/>
		</div>
	)
}

function EndpointReport({
	response,
	table,
	tableSearch,
	setTableSearch,
	runTable,
	loading,
	onSelectIP,
}: {
	response: FlowReportResponse
	table: TableControl
	tableSearch: string
	setTableSearch: (value: string) => void
	runTable: (next: TableControl) => void
	loading: boolean
	onSelectIP: (address: string) => void
}) {
	return (
		<EndpointTable
			response={response}
			table={table}
			tableSearch={tableSearch}
			setTableSearch={setTableSearch}
			runTable={runTable}
			loading={loading}
			onSelectIP={onSelectIP}
		/>
	)
}

function EndpointTable({
	response,
	table,
	tableSearch,
	setTableSearch,
	runTable,
	loading,
	onSelectIP,
}: {
	response: FlowReportResponse
	table: TableControl
	tableSearch: string
	setTableSearch: (value: string) => void
	runTable: (next: TableControl) => void
	loading: boolean
	onSelectIP: (address: string) => void
}) {
	const { t } = useLingui()
	const categoryLabel = useFlowReportCategoryLabel()
	const panel = reportPanel(response, "endpoint")
	const page = panel?.data?.table
	const records = (page?.items ?? []).map((item) => ({
		address: item.path[0] || item.name,
		current: formatMetric(item.last, panel?.meta.unit),
		average: formatMetric(item.average, panel?.meta.unit),
		p95: formatMetric(item.p95, panel?.meta.unit),
		maximum: formatMetric(item.maximum, panel?.meta.unit),
		total: formatMetric(item.total, totalUnit(panel?.meta.unit)),
		inbound: formatEndpointDirection(item.inbound, panel?.meta.unit),
		outbound: formatEndpointDirection(item.outbound, panel?.meta.unit),
		business: item.businesses?.join(", ") || "—",
		...Object.fromEntries(
			FLOW_REPORT_CATEGORIES.map((name) => [
				name,
				formatEndpointCategory(item.categories?.[name], totalUnit(panel?.meta.unit)),
			])
		),
		residual: formatEndpointCategory(item.residual, totalUnit(panel?.meta.unit)),
		correction: canManageAddressLibrary() ? t`Correct attribution` : undefined,
		correctionHref: addressCorrectionHref(response, item.path[0] || item.name),
	}))
	const columns: ColumnDefine[] = [
		{ field: "address", filterField: "dimension", title: t`IP address`, width: 190 },
		{ field: "current", filterField: "last", title: t`Combined · current`, width: 150 },
		{ field: "average", filterField: "average", title: t`Combined · average`, width: 155 },
		{ field: "p95", filterField: "p95", title: t`Combined · 95th`, width: 145 },
		{ field: "maximum", filterField: "maximum", title: t`Combined · peak`, width: 145 },
		{ field: "total", filterField: "total", title: t`Combined · total`, width: 150 },
		{ field: "business", title: t`Business labels`, width: 210 },
		{ field: "inbound", title: t`Inbound current / total`, width: 190 },
		{ field: "outbound", title: t`Outbound current / total`, width: 195 },
		...FLOW_REPORT_CATEGORIES.map((name) => ({
			field: name,
			title: `${categoryLabel(name)} · ↓ / ↑`,
			width: 205,
		})),
		{ field: "residual", title: t`Residual · ↓ / ↑`, width: 190 },
		...(canManageAddressLibrary()
			? [
					{
						field: "correction",
						title: t`Correction`,
						width: 155,
						filter: false,
						style: { color: "#2563eb", cursor: "pointer" },
					},
				]
			: []),
	]
	const filtering = {
		options: Object.fromEntries(
			Object.entries(page?.filter_options ?? {}).map(([field, options]) => [
				field,
				options.map((option) => ({ value: option.value, label: option.label || option.value, count: option.count })),
			])
		) as Record<string, ServerFilterOption[]>,
		selected: table.filters,
		onColumnFilterChange: (field: string, values: unknown[]) => {
			const filters = { ...table.filters, [field]: values.map(String) }
			if (!filters[field].length) delete filters[field]
			runTable({ ...table, page: 0, filters })
		},
		onClearAll: () => runTable({ ...table, page: 0, filters: {} }),
	}
	const sorting = {
		field: table.sortBy,
		direction: table.sortDirection,
		fields: {
			address: "dimension",
			current: "last",
			average: "average",
			p95: "p95",
			maximum: "maximum",
			total: "total",
			business: "business",
			inbound: "inbound",
			outbound: "outbound",
			...Object.fromEntries([...FLOW_REPORT_CATEGORIES, "residual"].map((field) => [field, field])),
		},
		onSortChange: (field: string, sortDirection: "asc" | "desc") =>
			runTable({ ...table, page: 0, sortBy: field, sortDirection }),
	}
	return (
		<Card>
			<CardHeader>
				<CardTitle>
					<Trans>Top endpoints</Trans>
				</CardTitle>
				<CardDescription>
					<Trans>
						One stable page ranked by combined traffic, with exact inbound/outbound and traffic-class values for the
						same addresses and frozen report window. Select a row to load records below. Attribution correction opens an
						address-library draft; it never mutates a published snapshot.
					</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent>
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No endpoint traffic`}
					height={Math.min(520, 42 * (records.length + 1))}
					searchValue={tableSearch}
					onSearchChange={setTableSearch}
					onSearchSubmit={(search) => runTable({ ...table, page: 0, search: search.trim() })}
					serverPagination={{
						page: table.page,
						pageSize: table.pageSize,
						totalCount: page?.total ?? 0,
						onPageChange: (page) => runTable({ ...table, page }),
						onPageSizeChange: (pageSize) => runTable({ ...table, page: 0, pageSize }),
					}}
					serverFiltering={filtering}
					serverSorting={sorting}
					onCellClick={(record, field) => {
						if (field === "correction") window.location.assign(String(record.correctionHref || ""))
					}}
					onRowClick={(record) => {
						const address = String(record.address || "")
						if (address) onSelectIP(address)
					}}
				/>
			</CardContent>
		</Card>
	)
}

function addressCorrectionHref(response: FlowReportResponse, address: string) {
	const path = getPagePath($router, "address_library", { section: "prefixes" })
	const query = new URLSearchParams({
		q: address,
		draft: "1",
		ip: address,
		evidence: "flow_report",
		from: response.data.range.effective_from,
		to: response.data.range.effective_to,
	})
	for (const [key, value] of Object.entries(response.data.versions ?? {})) query.set(key, value)
	return `${path}?${query.toString()}`
}

function OverseasReport({ response }: { response: FlowReportResponse }) {
	const { t } = useLingui()
	const total = reportPanel(response, "total")
	return (
		<div className="grid gap-4">
			<div className="grid gap-4 xl:grid-cols-2">
				<OverseasObservedSummary panel={reportPanel(response, "observed")} />
				<OverseasVPNShare panel={reportPanel(response, "vpn_share")} />
			</div>
			<ChartCard title={t`Overseas inbound / outbound`} panel={total} series={buildReportSeries(total)} />
			{[
				["country", t`Country`],
				["region", t`Region`],
				["asn", t`ASN`],
				["remote_port", t`Remote port`],
				["protocol", t`Protocol`],
			].map(([id, title]) => (
				<div key={id} className="grid gap-4 xl:grid-cols-2">
					<ChartCard
						title={t`${title} · inbound`}
						panel={reportPanel(response, `${id}_in`)}
						series={buildReportSeries(reportPanel(response, `${id}_in`))}
					/>
					<ChartCard
						title={t`${title} · outbound`}
						panel={reportPanel(response, `${id}_out`)}
						series={buildReportSeries(reportPanel(response, `${id}_out`))}
					/>
				</div>
			))}
		</div>
	)
}

function OverseasObservedSummary({ panel }: { panel?: FlowReportPanel }) {
	const { t } = useLingui()
	if (!panel || panel.status === "unavailable")
		return <UnavailablePanel title={t`Observed overseas endpoints`} reason={panel?.reason} />
	const points = (panel.data?.points ?? []) as Array<Record<string, unknown>>
	const kpi = points.filter(
		(point) => point.kind === "kpi" && point.direction === "combined" && point.ip_family === "all"
	)
	const remoteIPs = Math.max(0, ...kpi.map((point) => Number(point.observed_remote_ips ?? 0)))
	const localHosts = Math.max(0, ...kpi.map((point) => Number(point.observed_local_hosts ?? 0)))
	return (
		<div className="grid gap-3 sm:grid-cols-2">
			<MetricCard
				title={t`Observed overseas IPs`}
				value={remoteIPs.toLocaleString()}
				note={t`Exact observed cardinality; not sampling-expanded`}
			/>
			<MetricCard
				title={t`Local hosts with overseas traffic`}
				value={localHosts.toLocaleString()}
				note={t`Exact observed cardinality; not sampling-expanded`}
			/>
		</div>
	)
}

function OverseasVPNShare({ panel }: { panel?: FlowReportPanel }) {
	const { t } = useLingui()
	if (!panel || panel.status === "unavailable")
		return <UnavailablePanel title={t`VPN share of overseas traffic`} reason={panel?.reason} />
	const sourcePoints: unknown[] = panel.data?.points ?? []
	const points = sourcePoints.filter(isOverseasVPNSharePoint)
	const combined = points.find((point) => point.direction === "combined")
	return (
		<Card>
			<CardHeader>
				<CardTitle>
					<Trans>VPN share of overseas traffic</Trans>
				</CardTitle>
				<CardDescription>
					<Trans>
						VPN findings and estimated overseas bytes use the same frozen window and immutable classification versions.
					</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent className="grid gap-3">
				<div className="text-3xl font-semibold">{combined?.ratio == null ? "—" : formatPercent(combined.ratio)}</div>
				<div className="text-sm text-muted-foreground">
					<Trans>
						{formatBytes(combined?.vpn_bytes ?? 0)} VPN / {formatBytes(combined?.total_bytes ?? 0)} overseas · unknown
						Geo excluded {formatBytes(combined?.unknown_geo_bytes ?? 0)}
					</Trans>
				</div>
				<Link href={getPagePath($router, "flow_vpn")} className={buttonVariants({ variant: "outline", size: "sm" })}>
					<Trans>Open VPN findings and evidence</Trans>
				</Link>
			</CardContent>
		</Card>
	)
}

type OverseasVPNSharePoint = {
	direction: string
	vpn_bytes: number
	total_bytes: number
	ratio?: number
	unknown_geo_bytes: number
}

function isOverseasVPNSharePoint(value: unknown): value is OverseasVPNSharePoint {
	if (!value || typeof value !== "object") return false
	const point = value as Record<string, unknown>
	return (
		typeof point.direction === "string" &&
		typeof point.vpn_bytes === "number" &&
		typeof point.total_bytes === "number" &&
		typeof point.unknown_geo_bytes === "number" &&
		(point.ratio === undefined || typeof point.ratio === "number")
	)
}

function VPNReport({
	response,
	tables,
	updateTable,
	loading,
}: {
	response: FlowReportResponse
	tables: Record<string, TableControl>
	updateTable: (id: string, next: TableControl, run: boolean) => void
	loading: boolean
}) {
	const { t } = useLingui()
	const panel = reportPanel(response, "vpn_findings")
	if (!panel || panel.status === "unavailable")
		return <UnavailablePanel title={t`VPN analysis`} reason={panel?.reason || t`VPN findings are unavailable`} />
	const summary = panel.data as unknown as VPNReportSummary
	const totalPanel = reportPanel(response, "total")
	const denominator = [...reportCategoryTotals(totalPanel).values()].reduce((sum, value) => sum + value, 0)
	const trend: FlowReportSeries[] = [
		{
			name: t`Inbound`,
			values: (summary.trend ?? []).map((point) => ({
				time: new Date(point.bucket).getTime(),
				value: point.inbound_bytes,
			})),
		},
		{
			name: t`Outbound`,
			values: (summary.trend ?? []).map((point) => ({
				time: new Date(point.bucket).getTime(),
				value: point.outbound_bytes,
			})),
		},
	]
	return (
		<div className="grid gap-4">
			<div className="flex justify-end">
				<a className={buttonVariants({ variant: "outline" })} href="#vpn-findings">
					<Trans>Review findings, evidence and probe timeline</Trans>
				</a>
			</div>
			<div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
				<MetricCard
					title={t`Suspected hosts`}
					value={summary.suspected_hosts.toLocaleString()}
					note={t`${summary.finding_count.toLocaleString()} findings`}
				/>
				<MetricCard
					title={t`VPN traffic`}
					value={formatMetric(summary.total_bytes, "bytes")}
					note={t`${formatShare(summary.total_bytes, denominator)} of total`}
				/>
				<MetricCard
					title={t`Active ports`}
					value={summary.active_ports.toLocaleString()}
					note={t`Observed remote ports`}
				/>
				<MetricCard
					title={t`High-risk hosts`}
					value={summary.high_risk_hosts.toLocaleString()}
					note={t`Completeness ${formatPercent(summary.minimum_complete_ratio)}`}
				/>
			</div>
			<ChartCard title={t`VPN traffic trend`} panel={panel} series={trend} />
			<div className="grid gap-4 xl:grid-cols-2">
				<DistributionTable
					title={t`VPN port distribution`}
					page={summary.port_table}
					table={tables.port_distribution}
					updateTable={(next, run) => updateTable("port_distribution", next, run)}
					loading={loading}
				/>
				<DistributionTable
					title={t`VPN type distribution`}
					page={summary.type_table}
					table={tables.type_distribution}
					updateTable={(next, run) => updateTable("type_distribution", next, run)}
					loading={loading}
				/>
			</div>
			<div className="grid gap-4 xl:grid-cols-2">
				<VPNRulePublication panel={reportPanel(response, "vpn_rule_publication")} />
				<UnavailablePanel
					title={t`Active probe timeline`}
					reason={reportPanel(response, "vpn_probe_timeline")?.reason}
				/>
			</div>
		</div>
	)
}

function VPNRulePublication({ panel }: { panel?: FlowReportPanel }) {
	const { t } = useLingui()
	if (!panel || panel.status === "unavailable")
		return <UnavailablePanel title={t`Rule-set publication`} reason={panel?.reason} />
	const publication = panel.data as unknown as VPNRulePublicationSummary
	return (
		<Card>
			<CardHeader>
				<CardTitle>
					<Trans>Rule-set publication</Trans>
				</CardTitle>
				<CardDescription>
					<Trans>
						Version {publication.version} · effective {new Date(publication.effective_from).toLocaleString()}
					</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent className="grid gap-2 text-sm">
				<div className="text-2xl font-semibold">
					<Trans>{publication.rule_count.toLocaleString()} rules</Trans>
				</div>
				<div className="text-muted-foreground">
					<Trans>
						{publication.ready_workers.toLocaleString()} of {publication.observed_workers.toLocaleString()} observed
						workers ready · {publication.queryability}
					</Trans>
				</div>
				{publication.failed_workers + publication.behind_workers + publication.uninstalled_workers > 0 ? (
					<div className="text-amber-700">
						<Trans>
							{publication.failed_workers.toLocaleString()} failed · {publication.behind_workers.toLocaleString()}{" "}
							behind · {publication.uninstalled_workers.toLocaleString()} uninstalled
						</Trans>
					</div>
				) : null}
			</CardContent>
		</Card>
	)
}

function ChartCard({
	title,
	panel,
	series,
	unitOverride,
}: {
	title: string
	panel?: FlowReportPanel
	series: FlowReportSeries[]
	unitOverride?: string
}) {
	const { t } = useLingui()
	if (panel?.status === "unavailable") return <UnavailablePanel title={title} reason={panel.reason} />
	const stats = reportSeriesStats(series[0])
	const unit = unitOverride ?? panel?.meta.unit
	return (
		<Card>
			<CardHeader>
				<CardTitle>{title}</CardTitle>
				<CardDescription>
					{series.length
						? t`${series.length} series · current ${formatMetric(stats.current, unit)} · 95th ${formatMetric(stats.p95, unit)} · peak ${formatMetric(stats.maximum, unit)} · average ${formatMetric(stats.average, unit)}`
						: t`No traffic in the selected window.`}
				</CardDescription>
			</CardHeader>
			<CardContent>
				<ReportChart series={series} unit={unit} />
			</CardContent>
		</Card>
	)
}

function ReportChart({
	series,
	unit,
	compact = false,
}: {
	series: FlowReportSeries[]
	unit?: string
	compact?: boolean
}) {
	const ref = useRef<HTMLDivElement>(null)
	const instance = useRef<ReturnType<typeof createLineChart> | null>(null)
	useEffect(() => {
		if (!ref.current || series.every((item) => item.values.length === 0)) return
		disposeChart(instance.current)
		instance.current = createLineChart(ref.current, {
			series: series.slice(0, compact ? 2 : 20).map((item) => ({ ...item, unit })),
			yFormatter: (value) => formatMetric(value ?? 0, unit),
		})
		return () => {
			disposeChart(instance.current)
			instance.current = null
		}
	}, [compact, series, unit])
	if (series.every((item) => item.values.length === 0))
		return (
			<div className={cn("grid place-items-center text-sm text-muted-foreground", compact ? "h-24" : "h-72")}>
				<Trans>No data</Trans>
			</div>
		)
	return <div ref={ref} className={compact ? "mt-3 h-24" : "h-72"} />
}

function DistributionTable({
	title,
	page,
	table,
	updateTable,
	loading,
}: {
	title: string
	page?: FlowDistributionPage
	table: TableControl
	updateTable: (next: TableControl, run: boolean) => void
	loading: boolean
}) {
	const { t } = useLingui()
	const records = (page?.items ?? []).map((item) => ({
		value: item.value,
		findings: item.count.toLocaleString(),
		traffic: formatMetric(item.bytes, "bytes"),
	}))
	const filtering = reportTableFiltering(page?.filter_options, table, updateTable)
	const sorting = {
		field: table.sortBy,
		direction: table.sortDirection,
		fields: { value: "value", findings: "count", traffic: "bytes" },
		onSortChange: (field: string, sortDirection: "asc" | "desc") =>
			updateTable({ ...table, page: 0, sortBy: field, sortDirection }, true),
	}
	return (
		<Card>
			<CardHeader>
				<CardTitle>{title}</CardTitle>
			</CardHeader>
			<CardContent>
				<PagedVTable
					records={records}
					columns={[
						{ field: "value", title: t`Value`, width: 220 },
						{ field: "findings", filterField: "count", title: t`Findings`, width: 120 },
						{ field: "traffic", filterField: "bytes", title: t`Traffic`, width: 150 },
					]}
					loading={loading}
					emptyText={t`No findings`}
					height={Math.min(360, 42 * (records.length + 1))}
					searchValue={table.search}
					onSearchChange={(search) => updateTable({ ...table, search }, false)}
					onSearchSubmit={(search) => updateTable({ ...table, page: 0, search: search.trim() }, true)}
					serverPagination={{
						page: table.page,
						pageSize: table.pageSize,
						totalCount: page?.total ?? 0,
						onPageChange: (page) => updateTable({ ...table, page }, true),
						onPageSizeChange: (pageSize) => updateTable({ ...table, page: 0, pageSize }, true),
					}}
					serverFiltering={filtering}
					serverSorting={sorting}
				/>
			</CardContent>
		</Card>
	)
}

function MetricCard({ title, value, note }: { title: string; value: string; note: string }) {
	return (
		<Card>
			<CardHeader className="api-2">
				<CardDescription>{title}</CardDescription>
				<CardTitle className="text-2xl">{value}</CardTitle>
			</CardHeader>
			<CardContent className="text-xs text-muted-foreground">{note}</CardContent>
		</Card>
	)
}

function UnavailablePanel({ title, reason }: { title: string; reason?: string }) {
	const { t } = useLingui()
	return (
		<Card>
			<CardHeader>
				<CardTitle>{title}</CardTitle>
				<CardDescription>
					<Trans>Unavailable</Trans>
				</CardDescription>
			</CardHeader>
			<CardContent className="text-sm text-amber-700">
				{reason || t`Required data is not available for this report window.`}
			</CardContent>
		</Card>
	)
}

function ReportStatus({ response }: { response: FlowReportResponse }) {
	const unavailable = response.data.panels.filter((panel) => panel.status === "unavailable")
	const warnings = [...(response.data.warnings ?? []), ...(response.meta.warnings ?? [])]
	if (!response.meta.partial && !unavailable.length && !warnings.length) return null
	return (
		<div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900">
			<div className="font-medium">
				<Trans>Report completeness: {formatPercent(response.meta.complete_ratio)}</Trans>
			</div>
			{unavailable.map((panel) => (
				<div key={panel.id}>
					{panel.id}: {panel.reason}
				</div>
			))}
			{warnings.map((warning) => (
				<div key={warning}>{warning}</div>
			))}
		</div>
	)
}

function ReportSelect({
	label,
	value,
	onChange,
	options,
	disabled = false,
}: {
	label: string
	value: string
	onChange: (value: string) => void
	options: ReadonlyArray<readonly [string, string]>
	disabled?: boolean
}) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			<Select value={value} onValueChange={onChange} disabled={disabled}>
				<SelectTrigger>
					<SelectValue />
				</SelectTrigger>
				<SelectContent>
					{options.map(([option, title]) => (
						<SelectItem key={option} value={option}>
							{title}
						</SelectItem>
					))}
				</SelectContent>
			</Select>
		</div>
	)
}

function ReportInput({
	label,
	value,
	onChange,
	type = "text",
	placeholder,
}: {
	label: string
	value: string
	onChange: (value: string) => void
	type?: string
	placeholder?: string
}) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			<Input type={type} value={value} placeholder={placeholder} onChange={(event) => onChange(event.target.value)} />
		</div>
	)
}

function ReportDaySelector({ value, onChange }: { value: number[]; onChange: (value: number[]) => void }) {
	const { i18n } = useLingui()
	return (
		<div className="flex flex-wrap items-center gap-3 rounded-md border p-3">
			<span className="text-sm font-medium">
				<Trans>Peak weekdays</Trans>
			</span>
			{WEEKDAYS.map(([day, label]) => (
				<label key={day} htmlFor={`flow-report-peak-day-${day}`} className="flex items-center gap-1.5 text-sm">
					<Checkbox
						id={`flow-report-peak-day-${day}`}
						checked={value.includes(day)}
						onCheckedChange={(checked) => {
							const next = checked === true ? [...value, day] : value.filter((item) => item !== day)
							if (next.length) onChange(next.sort((a, b) => a - b))
						}}
					/>
					{i18n._(label)}
				</label>
			))}
		</div>
	)
}

function geoFilter(country: string, province: string, city: string): FlowFilterExpression | undefined {
	const predicates: FlowFilterExpression[] = []
	if (country !== "all") predicates.push({ op: "predicate", field: "geo.country", operator: "eq", values: [country] })
	if (province !== "all")
		predicates.push({ op: "predicate", field: "geo.province", operator: "eq", values: [province] })
	if (city !== "all") predicates.push({ op: "predicate", field: "geo.city", operator: "eq", values: [city] })
	if (!predicates.length) return undefined
	if (predicates.length === 1) return predicates[0]
	return { op: "and", args: predicates }
}

function formatEndpointDirection(item: FlowTablePage["items"][number]["inbound"] | undefined, unit?: string) {
	if (!item) return "—"
	return `${formatMetric(item.last, unit)} / ${formatMetric(item.total, totalUnit(unit))}`
}

function formatEndpointCategory(item: FlowTablePage["items"][number]["residual"] | undefined, unit?: string) {
	if (!item) return "—"
	return `↓ ${formatMetric(item.inbound, unit)} (${formatNullablePercent(item.inbound_share ?? null)}) / ↑ ${formatMetric(item.outbound, unit)} (${formatNullablePercent(item.outbound_share ?? null)})`
}

function tableRequest(table: TableControl) {
	return {
		search: table.search || undefined,
		sort_by: table.sortBy,
		sort_direction: table.sortDirection,
		limit: table.pageSize,
		offset: table.page * table.pageSize,
		filters: table.filters,
	}
}

function reportTableFiltering(
	options: Record<string, Array<{ value: string; label?: string; count: number }>> | undefined,
	table: TableControl,
	updateTable: (next: TableControl, run: boolean) => void
) {
	return {
		options: Object.fromEntries(
			Object.entries(options ?? {}).map(([field, values]) => [
				field,
				values.map((option) => ({ value: option.value, label: option.label || option.value, count: option.count })),
			])
		) as Record<string, ServerFilterOption[]>,
		selected: table.filters,
		onColumnFilterChange: (field: string, values: unknown[]) => {
			const filters = { ...table.filters, [field]: values.map(String) }
			if (!filters[field].length) delete filters[field]
			updateTable({ ...table, page: 0, filters }, true)
		},
		onClearAll: () => updateTable({ ...table, page: 0, filters: {} }, true),
	}
}

function formatDirectionalValue(
	value: { inbound: number; outbound: number; inbound_share?: number; outbound_share?: number } | undefined,
	unit?: string
) {
	if (!value) return "—"
	return `↓ ${formatMetric(value.inbound, unit)} (${formatNullablePercent(value.inbound_share ?? null)}) / ↑ ${formatMetric(value.outbound, unit)} (${formatNullablePercent(value.outbound_share ?? null)})`
}

function formatMetric(value: number, unit?: string) {
	if (!Number.isFinite(value)) return "—"
	if (unit === "bits_per_second") return `${formatCompact(value)}bps`
	if (unit === "packets_per_second") return `${formatCompact(value)}pps`
	if (unit === "bytes") return formatBytes(value)
	if (unit === "packets") return t`${formatCompact(value)} packets`
	if (unit === "records") return t`${formatCompact(value)} records`
	if (unit === "ratio") return formatPercent(value)
	return formatCompact(value)
}

function totalUnit(unit?: string) {
	if (unit === "bits_per_second") return "bytes"
	return unit
}

function formatCompact(value: number) {
	return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 2 }).format(value)
}

function formatBytes(value: number) {
	const units = ["B", "KB", "MB", "GB", "TB", "PB"]
	let current = Math.abs(value)
	let index = 0
	while (current >= 1000 && index < units.length - 1) {
		current /= 1000
		index++
	}
	return `${value < 0 ? "−" : ""}${current.toFixed(current >= 100 ? 0 : current >= 10 ? 1 : 2)} ${units[index]}`
}

function formatPercent(value: number) {
	return `${(Math.max(0, value) * 100).toFixed(1)}%`
}

function formatShare(value: number, total: number) {
	return total > 0 ? formatPercent(value / total) : "—"
}

function formatNullablePercent(value: number | null) {
	return value == null ? "—" : formatPercent(value)
}

function queryState(name: string, fallback: string) {
	if (typeof window === "undefined") return fallback
	return new URLSearchParams(window.location.search).get(name) || fallback
}

function boundedNumber(value: string, minimum: number, maximum: number, fallback: number) {
	const parsed = Number(value)
	return Number.isFinite(parsed) ? Math.max(minimum, Math.min(maximum, Math.round(parsed))) : fallback
}

function parsePeakDays(value: string) {
	const days = [
		...new Set(
			value
				.split(",")
				.map(Number)
				.filter((day) => Number.isInteger(day) && day >= 1 && day <= 7)
		),
	].sort((a, b) => a - b)
	return days.length ? days : [1, 2, 3, 4, 5]
}

function writeURLState(state: Record<string, unknown>) {
	const parameters = new URLSearchParams()
	for (const [key, value] of Object.entries(state)) {
		if (value !== "" && value !== "all" && value != null) parameters.set(key, String(value))
	}
	window.history.replaceState(null, "", `${window.location.pathname}?${parameters}`)
}

function sortByName<T extends { name: string }>(items: T[]) {
	return items.slice().sort((a, b) => a.name.localeCompare(b.name))
}

function deviceID(device: DeviceItem) {
	return device.ID || device.id || ""
}
function deviceName(device: DeviceItem) {
	return device.SysName || device.sys_name || device.Name || device.name || deviceID(device)
}

function downloadReport(response: FlowReportResponse, surface: FlowReportSurface) {
	const rows: string[][] = [["panel", "bucket", "dimension", "value", "status", "reason"]]
	for (const panel of response.data.panels) {
		if (panel.status !== "ready") {
			rows.push([panel.id, "", "", "", panel.status, panel.reason ?? ""])
			continue
		}
		for (const point of panelPoints(panel))
			rows.push([panel.id, point.bucket, point.dimension_value, String(point.value), panel.status, ""])
		for (const point of panelJointPoints(panel))
			rows.push([panel.id, point.bucket, point.dimension_values.join(" / "), String(point.value), panel.status, ""])
	}
	const csv = rows.map((row) => row.map(csvCell).join(",")).join("\n")
	const link = document.createElement("a")
	link.href = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }))
	link.download = `flow-${surface}-${response.meta.request_id || "report"}.csv`
	link.click()
	URL.revokeObjectURL(link.href)
}

function csvCell(value: string) {
	return `"${value.replaceAll('"', '""')}"`
}
