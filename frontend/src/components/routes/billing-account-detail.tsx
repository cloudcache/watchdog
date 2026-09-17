import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import {
	ArrowLeftIcon,
	CalculatorIcon,
	CalendarPlusIcon,
	CheckCircle2Icon,
	DownloadIcon,
	PencilIcon,
	RefreshCwIcon,
	SaveIcon,
} from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { api, downloadWatchdogFile } from "@/lib/api"
import { newUUID } from "@/lib/random"
import { cn } from "@/lib/utils"
import type { ColumnDefine } from "@/lib/vtable"

type BillingAccount = {
	id: string
	name: string
	status: string
	measurement_type: string
	billing_method: string
	algorithm: string
	billing_day: number
	timezone: string
	direction: string
	default_layer: string
	price_currency: string
	unit_price: string
	minimum_percent: number
	contract_bandwidth_bps?: number
	traffic_allowance_bytes?: number
	row_version: number
}
type BillingPort = {
	account_id: string
	port_id: string
	device_id: string
	if_index: number
	if_name: string
	direction: string
}
type Period = {
	id: string
	account_id: string
	date_from: string
	date_to: string
	timezone: string
	status: string
	algorithm: string
	default_layer: string
	allowed?: number
	used?: number
	overuse?: number
	calculation_version: number
	row_version: number
	publication_ref: string
}
type Value = {
	id: string
	layer: string
	unit: string
	in_bytes: number
	out_bytes: number
	selected_bytes: number
	rate_95th_bps: number
	rate_daily_95th_bps: number
	rate_average_bps: number
	algorithm_value: number
	coverage: number
	expected_buckets: number
	observed_buckets: number
	missing_buckets: number
	reset_buckets: number
	gap_buckets: number
	unknown_sampling_records: number
	source_generation_min: number
	source_generation_max: number
}
type Issue = {
	id: string
	kind: string
	severity: string
	left_layer: string
	right_layer: string
	metric: string
	expected_value: number
	actual_value: number
	delta_value: number
	threshold_value: number
	status: string
	resolution_note: string
	row_version: number
}
type Adjustment = {
	id: string
	layer: string
	unit: string
	amount: number
	reason: string
	evidence_ref: string
	status: string
	reverses_adjustment_id?: string
	row_version: number
}
type Reconciliation = {
	id: string
	operation_ref: string
	calculation_version: number
	status: string
	threshold_abs: number
	threshold_percent: number
	issue_count: number
	created_at: string
}
type Job = { id: string; status: string; last_error_detail?: string }
type NetworkDevice = { id: string; name?: string; sys_name?: string }
type NetworkPort = { id: string; if_index: number; if_name?: string; if_alias?: string; oper_status?: string }
type Page<T> = { items: T[]; total: number; row_version?: number }
type SortDirection = "asc" | "desc"

export default memo(({ id }: { id: string }) => {
	const { t } = useLingui()
	const [account, setAccount] = useState<BillingAccount | null>(null)
	const [etag, setETag] = useState("")
	const [bindings, setBindings] = useState<Record<string, string>>({})
	const [availablePorts, setAvailablePorts] = useState<(NetworkPort & { device_id: string; device_name: string })[]>([])
	const [periods, setPeriods] = useState<Period[]>([])
	const [periodTotal, setPeriodTotal] = useState(0)
	const [periodPage, setPeriodPage] = useState(0)
	const [periodSize, setPeriodSize] = useState(25)
	const [periodSearch, setPeriodSearch] = useState("")
	const [periodQuery, setPeriodQuery] = useState("")
	const [periodStatus, setPeriodStatus] = useState("")
	const [periodSort, setPeriodSort] = useState("date_from:desc")
	const [selected, setSelected] = useState<Period | null>(null)
	const [selectedETag, setSelectedETag] = useState("")
	const [values, setValues] = useState<Value[]>([])
	const [loading, setLoading] = useState(true)
	const [busy, setBusy] = useState(false)
	const [message, setMessage] = useState("")
	const [reload, setReload] = useState(0)
	const [windowInput, setWindowInput] = useState(() => defaultWindow())
	const [external, setExternal] = useState({ value: "", coverage: "1", reference: "" })
	const [adjustment, setAdjustment] = useState({ layer: "customer", amount: "", reason: "", evidence_ref: "" })
	const request = useRef(0)
	const initializedWindow = useRef(false)

	const loadAccountAndPorts = useCallback(async () => {
		const [detail, bound, devices] = await Promise.all([
			api.send<{
				account: BillingAccount
				suggested_period: { date_from: string; date_to: string }
			}>(`/api/v1/billing/accounts/${id}`, {
				onResponse: (response) => setETag(response.headers.get("ETag") ?? ""),
			}),
			api.send<Page<BillingPort>>(`/api/v1/billing/accounts/${id}/ports`, {
				query: { limit: 500, offset: 0, sort: "device_id", order: "asc" },
			}),
			api.send<Page<NetworkDevice>>("/api/v1/devices", { query: { kind: "network", limit: 500, offset: 0 } }),
		])
		setAccount(detail.account)
		if (!initializedWindow.current) {
			setWindowInput({
				from: localDateTime(new Date(detail.suggested_period.date_from)),
				to: localDateTime(new Date(detail.suggested_period.date_to)),
			})
			initializedWindow.current = true
		}
		setBindings(Object.fromEntries((bound.items ?? []).map((item) => [item.port_id, item.direction])))
		const loaded = await Promise.all(
			(devices.items ?? []).map(async (device) => {
				const page = await api.send<Page<NetworkPort>>(`/api/v1/devices/${device.id}/ports`, {
					query: { limit: 500, offset: 0 },
				})
				return (page.items ?? []).map((port) => ({
					...port,
					device_id: device.id,
					device_name: device.sys_name || device.name || device.id,
				}))
			})
		)
		setAvailablePorts(loaded.flat())
	}, [id])

	const loadPeriods = useCallback(async () => {
		const [sort, order] = periodSort.split(":")
		const data = await api.send<Page<Period>>(`/api/v1/billing/accounts/${id}/periods`, {
			query: {
				limit: periodSize,
				offset: periodPage * periodSize,
				q: periodQuery || undefined,
				status: periodStatus || undefined,
				sort,
				order,
			},
		})
		setPeriods(data.items ?? [])
		setPeriodTotal(data.total ?? 0)
	}, [id, periodPage, periodQuery, periodSize, periodSort, periodStatus])

	const loadSelected = useCallback(async (periodID: string) => {
		const data = await api.send<{ period: Period; values: Value[] }>(`/api/v1/billing/periods/${periodID}`, {
			onResponse: (response) => setSelectedETag(response.headers.get("ETag") ?? ""),
		})
		setSelected(data.period)
		setValues(data.values ?? [])
	}, [])

	useEffect(() => {
		const timer = setTimeout(() => {
			setPeriodPage(0)
			setPeriodQuery(periodSearch.trim())
		}, 300)
		return () => clearTimeout(timer)
	}, [periodSearch])
	useEffect(() => {
		document.title = `${t`Billing Account`} / Watchdog`
		const current = ++request.current
		setLoading(true)
		setMessage("")
		Promise.all([loadAccountAndPorts(), loadPeriods()])
			.catch((err) => {
				if (current === request.current)
					setMessage(err instanceof Error ? err.message : t`Failed to load billing account`)
			})
			.finally(() => {
				if (current === request.current) setLoading(false)
			})
	}, [loadAccountAndPorts, loadPeriods, reload, t])

	const refreshAll = async () => {
		setReload((value) => value + 1)
		if (selected) await loadSelected(selected.id)
	}
	const savePorts = async () => {
		if (!etag) return
		setBusy(true)
		setMessage("")
		try {
			const data = await api.send<{ row_version: number }>(`/api/v1/billing/accounts/${id}/ports`, {
				method: "PUT",
				headers: { "If-Match": etag },
				body: { items: Object.entries(bindings).map(([port_id, direction]) => ({ port_id, direction })) },
			})
			setETag(`"${data.row_version}"`)
			setMessage(t`Saved`)
		} catch (err) {
			setMessage(errorText(err))
		} finally {
			setBusy(false)
		}
	}
	const createPeriod = async () => {
		setBusy(true)
		setMessage("")
		try {
			const period = await api.send<Period>(`/api/v1/billing/accounts/${id}/periods`, {
				method: "POST",
				body: { date_from: new Date(windowInput.from).toISOString(), date_to: new Date(windowInput.to).toISOString() },
			})
			await loadSelected(period.id)
			setPeriodPage(0)
			setReload((value) => value + 1)
		} catch (err) {
			setMessage(errorText(err))
		} finally {
			setBusy(false)
		}
	}
	const runOperation = async (kind: "calculate" | "reconcile") => {
		if (!selected || !selectedETag) return
		setBusy(true)
		setMessage("")
		try {
			const job = await api.send<Job>(`/api/v1/billing/periods/${selected.id}/${kind}`, {
				method: "POST",
				headers: { "If-Match": selectedETag, "Idempotency-Key": newUUID() },
				body: {},
			})
			await waitForJob(`/api/v1/billing/jobs/${job.id}`)
			await loadSelected(selected.id)
			setReload((value) => value + 1)
		} catch (err) {
			setMessage(errorText(err))
		} finally {
			setBusy(false)
		}
	}
	const transition = async (kind: "approve" | "close") => {
		if (!selected || !selectedETag) return
		setBusy(true)
		setMessage("")
		try {
			await api.send(`/api/v1/billing/periods/${selected.id}/${kind}`, {
				method: "POST",
				headers: { "If-Match": selectedETag },
				body: { calculation_version: selected.calculation_version },
			})
			await loadSelected(selected.id)
			setReload((value) => value + 1)
		} catch (err) {
			setMessage(errorText(err))
		} finally {
			setBusy(false)
		}
	}
	const importExternal = async () => {
		if (!selected || !selectedETag) return
		const number = Number(external.value)
		const requestedCoverage = Number(external.coverage)
		const expectedBuckets = Math.floor(
			(new Date(selected.date_to).getTime() - new Date(selected.date_from).getTime()) / 300_000
		)
		const observedBuckets = Math.round(expectedBuckets * requestedCoverage)
		if (
			!Number.isFinite(number) ||
			number < 0 ||
			!Number.isFinite(requestedCoverage) ||
			requestedCoverage < 0 ||
			requestedCoverage > 1 ||
			expectedBuckets < 1
		) {
			setMessage(t`External evidence is invalid`)
			return
		}
		const total = selected.algorithm === "total"
		setBusy(true)
		setMessage("")
		try {
			await api.send(`/api/v1/billing/periods/${selected.id}/external`, {
				method: "POST",
				headers: { "If-Match": selectedETag },
				body: {
					unit: total ? "bytes" : "bps",
					algorithm_value: number,
					selected_bytes: total ? number : 0,
					rate_95th_bps: selected.algorithm === "95th" ? number : 0,
					rate_daily_95th_bps: selected.algorithm === "daily_95th" ? number : 0,
					rate_average_bps: selected.algorithm === "average" ? number : 0,
					coverage: observedBuckets / expectedBuckets,
					expected_buckets: expectedBuckets,
					observed_buckets: observedBuckets,
					missing_buckets: expectedBuckets - observedBuckets,
					gap_buckets: 0,
					provenance: { reference: external.reference },
				},
			})
			await loadSelected(selected.id)
			setMessage(t`Saved`)
		} catch (err) {
			setMessage(errorText(err))
		} finally {
			setBusy(false)
		}
	}
	const createAdjustment = async () => {
		if (!selected) return
		setBusy(true)
		setMessage("")
		try {
			await api.send(`/api/v1/billing/periods/${selected.id}/adjustments`, {
				method: "POST",
				body: {
					...adjustment,
					unit: selected.algorithm === "total" ? "bytes" : "bps",
					amount: Number(adjustment.amount),
				},
			})
			setAdjustment({ layer: "customer", amount: "", reason: "", evidence_ref: "" })
			setReload((value) => value + 1)
		} catch (err) {
			setMessage(errorText(err))
		} finally {
			setBusy(false)
		}
	}
	const exportEvidence = async (format: "csv" | "parquet") => {
		if (!selected) return
		setBusy(true)
		setMessage("")
		try {
			const job = await api.send<Job>(`/api/v1/billing/periods/${selected.id}/exports`, {
				method: "POST",
				headers: { "Idempotency-Key": newUUID() },
				body: { format, calculation_version: selected.calculation_version },
			})
			await waitForJob(`/api/v1/billing/exports/${job.id}`)
			await downloadWatchdogFile(`/api/v1/billing/exports/${job.id}/download`)
		} catch (err) {
			setMessage(errorText(err))
		} finally {
			setBusy(false)
		}
	}

	const periodColumns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "date_from", title: t`From`, width: 180 },
			{ field: "date_to", title: t`To`, width: 180 },
			{ field: "status", title: t`Status`, width: 110, filterField: "status" },
			{ field: "algorithm", title: t`Algorithm`, width: 110 },
			{ field: "used_display", title: t`Used`, width: 140 },
			{ field: "overuse_display", title: t`Overuse`, width: 140 },
			{ field: "calculation_version", title: t`Version`, width: 90 },
		],
		[t]
	)
	const periodRecords = useMemo(
		() =>
			periods.map((item) => ({
				...item,
				date_from: formatTime(item.date_from),
				date_to: formatTime(item.date_to),
				used_display: formatValue(item.used, item.algorithm),
				overuse_display: formatValue(item.overuse, item.algorithm),
			})),
		[periods]
	)
	const [periodSortField, periodSortDirection] = periodSort.split(":") as [string, SortDirection]
	const periodFiltering = useMemo(
		() => ({
			options: { status: ["open", "calculated", "approved", "closed"].map((value) => ({ value, label: value })) },
			selected: { status: periodStatus ? [periodStatus] : [] },
			selection: { status: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) => {
				setPeriodPage(0)
				setPeriodStatus(String(values[0] ?? ""))
			},
			onClearAll: () => {
				setPeriodPage(0)
				setPeriodStatus("")
			},
		}),
		[periodStatus]
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "billing")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to billing`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<h1 className="truncate text-xl font-semibold">{account?.name ?? id}</h1>
					<AccountStatus value={account?.status} />
				</div>
				<div className="flex gap-2">
					<Link
						href={getPagePath($router, "billing_edit", { id })}
						className={buttonVariants({ variant: "outline", size: "sm" })}
					>
						<PencilIcon className="me-2 h-4 w-4" />
						<Trans>Edit</Trans>
					</Link>
					<Button variant="outline" size="sm" onClick={refreshAll}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>
			{message ? (
				<div className="rounded-md border border-border p-3 text-sm text-muted-foreground">{message}</div>
			) : null}
			<div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
				<Info
					label={t`Measurement type`}
					value={account?.measurement_type === "bandwidth" ? t`Bandwidth` : account ? t`Traffic` : undefined}
				/>
				<Info
					label={t`Billing method`}
					value={
						account?.billing_method === "package_port"
							? t`Port package`
							: account?.billing_method === "monthly_95th"
								? t`Monthly 95th`
								: account?.billing_method === "daily_95th"
									? t`Daily 95th`
									: account
										? t`Monthly average`
										: undefined
					}
				/>
				<Info
					label={t`Direction`}
					value={
						account?.direction === "in"
							? t`Inbound`
							: account?.direction === "out"
								? t`Outbound`
								: account
									? t`In + out`
									: undefined
					}
				/>
				<Info
					label={t`Value strategy`}
					value={
						account?.default_layer === "customer"
							? t`Customer`
							: account?.default_layer === "supplier"
								? t`Supplier`
								: account
									? t`Raw`
									: undefined
					}
				/>
				<Info label={t`Contract bandwidth`} value={formatBPS(account?.contract_bandwidth_bps)} />
				<Info
					label={account?.measurement_type === "traffic" ? t`Traffic allowance` : t`Billing minimum`}
					value={
						account?.measurement_type === "traffic"
							? formatBytes(account.traffic_allowance_bytes)
							: account?.billing_method === "package_port"
								? "—"
								: account
									? `${account.minimum_percent ?? 0}%`
									: undefined
					}
				/>
				<Info label={t`Unit price`} value={account ? `${account.price_currency} ${account.unit_price}` : undefined} />
				<Info label={t`Timezone`} value={account?.timezone} />
				<Info label={t`Billing Day`} value={account?.billing_day} />
			</div>
			<Tabs defaultValue="periods">
				<TabsList>
					<TabsTrigger value="periods">
						<Trans>Periods</Trans>
					</TabsTrigger>
					<TabsTrigger value="ports">
						<Trans>Ports</Trans>
					</TabsTrigger>
				</TabsList>
				<TabsContent value="ports" className="mt-4">
					<div className="grid gap-3 rounded-md border border-border p-4">
						<div className="flex justify-between">
							<h2 className="font-medium">
								<Trans>Billing Ports</Trans>
							</h2>
							<Button size="sm" onClick={savePorts} disabled={busy || loading}>
								<SaveIcon className="me-2 h-4 w-4" />
								<Trans>Save</Trans>
							</Button>
						</div>
						<div className="grid max-h-[500px] gap-2 overflow-auto md:grid-cols-2">
							{availablePorts.map((port) => {
								const checked = port.id in bindings
								return (
									<div key={port.id} className="grid grid-cols-[1fr_130px] gap-2 rounded-md border border-border p-3">
										<div className="flex min-w-0 gap-2">
											<Checkbox
												aria-label={t`Include billing port`}
												checked={checked}
												onCheckedChange={() =>
													setBindings((current) => {
														const next = { ...current }
														if (checked) delete next[port.id]
														else next[port.id] = account?.direction ?? "agg"
														return next
													})
												}
											/>
											<span className="truncate text-sm">
												{port.device_name} · {port.if_name || port.id}
												<span className="block text-xs text-muted-foreground">
													ifIndex {port.if_index} · {port.oper_status ?? "unknown"}
												</span>
											</span>
										</div>
										<Select
											disabled={!checked}
											value={bindings[port.id] ?? account?.direction ?? "agg"}
											onValueChange={(value) => setBindings((current) => ({ ...current, [port.id]: value }))}
										>
											<SelectTrigger>
												<SelectValue />
											</SelectTrigger>
											<SelectContent>
												<SelectItem value="in">
													<Trans>Inbound</Trans>
												</SelectItem>
												<SelectItem value="out">
													<Trans>Outbound</Trans>
												</SelectItem>
												<SelectItem value="agg">
													<Trans>In + out</Trans>
												</SelectItem>
											</SelectContent>
										</Select>
									</div>
								)
							})}
						</div>
					</div>
				</TabsContent>
				<TabsContent value="periods" className="mt-4">
					<div className="grid gap-4">
						<div className="grid gap-3 rounded-md border border-border p-4 lg:grid-cols-[1fr_1fr_auto]">
							<Field label={t`From`}>
								<Input
									type="datetime-local"
									value={windowInput.from}
									onChange={(event) => setWindowInput({ ...windowInput, from: event.target.value })}
								/>
							</Field>
							<Field label={t`To`}>
								<Input
									type="datetime-local"
									value={windowInput.to}
									onChange={(event) => setWindowInput({ ...windowInput, to: event.target.value })}
								/>
							</Field>
							<Button className="self-end" size="sm" onClick={createPeriod} disabled={busy}>
								<CalendarPlusIcon className="me-2 h-4 w-4" />
								<Trans>Create period</Trans>
							</Button>
						</div>
						<div className="overflow-hidden rounded-md border border-border bg-card p-3">
							<PagedVTable
								records={periodRecords}
								columns={periodColumns}
								loading={loading}
								emptyText={t`No billing periods found.`}
								searchPlaceholder={t`Search period ID...`}
								searchValue={periodSearch}
								onSearchChange={setPeriodSearch}
								serverFiltering={periodFiltering}
								serverSorting={{
									field: periodSortField,
									direction: periodSortDirection,
									fields: { date_from: "date_from", date_to: "date_to", status: "status", used_display: "used" },
									onSortChange: (field, direction) => {
										setPeriodPage(0)
										setPeriodSort(`${field}:${direction}`)
									},
								}}
								serverPagination={{
									page: periodPage,
									pageSize: periodSize,
									totalCount: periodTotal,
									onPageChange: setPeriodPage,
									onPageSizeChange: (value) => {
										setPeriodPage(0)
										setPeriodSize(value)
									},
								}}
								onRowClick={(record) => loadSelected(String(record.id)).catch((err) => setMessage(errorText(err)))}
							/>
						</div>
						{selected ? (
							<PeriodWorkspace
								period={selected}
								etag={selectedETag}
								values={values}
								busy={busy}
								external={external}
								setExternal={setExternal}
								adjustment={adjustment}
								setAdjustment={setAdjustment}
								onCalculate={() => runOperation("calculate")}
								onReconcile={() => runOperation("reconcile")}
								onApprove={() => transition("approve")}
								onClose={() => transition("close")}
								onImport={importExternal}
								onAdjustment={createAdjustment}
								onExport={exportEvidence}
								onRefresh={() => loadSelected(selected.id)}
								notify={setMessage}
								reload={reload}
							/>
						) : null}
					</div>
				</TabsContent>
			</Tabs>
		</div>
	)
})

function PeriodWorkspace({
	period,
	etag,
	values,
	busy,
	external,
	setExternal,
	adjustment,
	setAdjustment,
	onCalculate,
	onReconcile,
	onApprove,
	onClose,
	onImport,
	onAdjustment,
	onExport,
	onRefresh,
	notify,
	reload,
}: {
	period: Period
	etag: string
	values: Value[]
	busy: boolean
	external: { value: string; coverage: string; reference: string }
	setExternal: (value: { value: string; coverage: string; reference: string }) => void
	adjustment: { layer: string; amount: string; reason: string; evidence_ref: string }
	setAdjustment: (value: { layer: string; amount: string; reason: string; evidence_ref: string }) => void
	onCalculate: () => void
	onReconcile: () => void
	onApprove: () => void
	onClose: () => void
	onImport: () => void
	onAdjustment: () => void
	onExport: (format: "csv" | "parquet") => void
	onRefresh: () => Promise<void>
	notify: (message: string) => void
	reload: number
}) {
	const { t } = useLingui()
	const valueColumns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "layer", title: t`Layer`, width: 110 },
			{ field: "algorithm_value_display", title: t`Value`, width: 150 },
			{ field: "in_display", title: t`Inbound`, width: 140 },
			{ field: "out_display", title: t`Outbound`, width: 140 },
			{ field: "coverage_display", title: t`Coverage`, width: 110 },
			{ field: "buckets", title: t`Buckets`, width: 110 },
			{ field: "evidence_gaps", title: t`Missing / reset / gap`, width: 170 },
			{ field: "unknown_sampling_records", title: t`Unknown sampling`, width: 140 },
			{ field: "generations", title: t`Source generation`, width: 160 },
		],
		[t]
	)
	const valueRecords = values.map((item) => ({
		...item,
		algorithm_value_display: formatValue(item.algorithm_value, period.algorithm),
		in_display: formatBytes(item.in_bytes),
		out_display: formatBytes(item.out_bytes),
		coverage_display: `${(item.coverage * 100).toFixed(2)}%`,
		buckets: `${item.observed_buckets}/${item.expected_buckets}`,
		evidence_gaps: `${item.missing_buckets}/${item.reset_buckets}/${item.gap_buckets}`,
		generations: `${item.source_generation_min}-${item.source_generation_max}`,
	}))
	return (
		<div className="grid gap-4 rounded-md border border-border p-4">
			<div className="flex flex-wrap items-center justify-between gap-2">
				<div className="flex items-center gap-2">
					<h2 className="font-medium">{period.id}</h2>
					<Badge variant={period.status === "closed" ? "success" : "outline"}>{period.status}</Badge>
					<span className="text-xs text-muted-foreground">v{period.calculation_version}</span>
				</div>
				<div className="flex flex-wrap gap-2">
					<Button
						size="sm"
						variant="outline"
						onClick={onCalculate}
						disabled={busy || !etag || !["open", "calculated"].includes(period.status)}
					>
						<CalculatorIcon className="me-2 h-4 w-4" />
						<Trans>Calculate</Trans>
					</Button>
					<Button size="sm" variant="outline" onClick={onReconcile} disabled={busy || period.status !== "calculated"}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Reconcile</Trans>
					</Button>
					<Button size="sm" variant="outline" onClick={onApprove} disabled={busy || period.status !== "calculated"}>
						<CheckCircle2Icon className="me-2 h-4 w-4" />
						<Trans>Approve</Trans>
					</Button>
					<Button size="sm" onClick={onClose} disabled={busy || period.status !== "approved"}>
						<Trans>Close</Trans>
					</Button>
					<Button size="sm" variant="outline" onClick={() => onExport("csv")} disabled={!period.calculation_version}>
						<DownloadIcon className="me-2 h-4 w-4" />
						CSV
					</Button>
					<Button
						size="sm"
						variant="outline"
						onClick={() => onExport("parquet")}
						disabled={!period.calculation_version}
					>
						Parquet
					</Button>
				</div>
			</div>
			<div className="grid gap-3 md:grid-cols-4">
				<Info label={t`From`} value={formatTime(period.date_from)} />
				<Info label={t`To`} value={formatTime(period.date_to)} />
				<Info label={t`Used`} value={formatValue(period.used, period.algorithm)} />
				<Info label={t`Overuse`} value={formatValue(period.overuse, period.algorithm)} />
			</div>
			<div className="overflow-hidden rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={valueRecords}
					columns={valueColumns}
					emptyText={t`Calculate the period to create evidence.`}
					showSearch={false}
					showPagination={false}
					height={270}
				/>
			</div>
			<div className="grid gap-4 xl:grid-cols-2">
				<div className="grid gap-3 rounded-md border border-border p-3">
					<h3 className="font-medium">
						<Trans>External evidence</Trans>
					</h3>
					<Field label={t`Value`}>
						<Input
							type="number"
							min="0"
							value={external.value}
							onChange={(event) => setExternal({ ...external, value: event.target.value })}
						/>
					</Field>
					<Field label={t`Coverage`}>
						<Input
							type="number"
							min="0"
							max="1"
							step="0.01"
							value={external.coverage}
							onChange={(event) => setExternal({ ...external, coverage: event.target.value })}
						/>
					</Field>
					<Field label={t`Reference`}>
						<Input
							value={external.reference}
							onChange={(event) => setExternal({ ...external, reference: event.target.value })}
						/>
					</Field>
					<Button
						size="sm"
						variant="outline"
						onClick={onImport}
						disabled={busy || !external.value || ["approved", "closed"].includes(period.status)}
					>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Import</Trans>
					</Button>
				</div>
				<div className="grid gap-3 rounded-md border border-border p-3">
					<h3 className="font-medium">
						<Trans>Adjustment</Trans>
					</h3>
					<Field label={t`Layer`}>
						<Select value={adjustment.layer} onValueChange={(layer) => setAdjustment({ ...adjustment, layer })}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								{["raw", "supplier", "customer", "snmp", "external"].map((layer) => (
									<SelectItem key={layer} value={layer}>
										{layer}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Signed amount`}>
						<Input
							type="number"
							value={adjustment.amount}
							onChange={(event) => setAdjustment({ ...adjustment, amount: event.target.value })}
						/>
					</Field>
					<Field label={t`Reason`}>
						<Input
							value={adjustment.reason}
							onChange={(event) => setAdjustment({ ...adjustment, reason: event.target.value })}
						/>
					</Field>
					<Field label={t`Evidence reference`}>
						<Input
							value={adjustment.evidence_ref}
							onChange={(event) => setAdjustment({ ...adjustment, evidence_ref: event.target.value })}
						/>
					</Field>
					<Button
						size="sm"
						variant="outline"
						onClick={onAdjustment}
						disabled={busy || period.status !== "calculated" || !adjustment.amount || !adjustment.reason}
					>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Add adjustment</Trans>
					</Button>
				</div>
			</div>
			<EvidenceLists period={period} onPeriodRefresh={onRefresh} notify={notify} reload={reload} />
		</div>
	)
}

function EvidenceLists({
	period,
	onPeriodRefresh,
	notify,
	reload,
}: {
	period: Period
	onPeriodRefresh: () => Promise<void>
	notify: (message: string) => void
	reload: number
}) {
	const { t } = useLingui()
	const [issues, setIssues] = useState<Page<Issue>>({ items: [], total: 0 })
	const [adjustments, setAdjustments] = useState<Page<Adjustment>>({ items: [], total: 0 })
	const [runs, setRuns] = useState<Page<Reconciliation>>({ items: [], total: 0 })
	const [issuePage, setIssuePage] = useState(0),
		[adjustPage, setAdjustPage] = useState(0),
		[runPage, setRunPage] = useState(0)
	const [issueSize, setIssueSize] = useState(10),
		[adjustSize, setAdjustSize] = useState(10),
		[runSize, setRunSize] = useState(10)
	const [issueSearch, setIssueSearch] = useState(""),
		[adjustSearch, setAdjustSearch] = useState(""),
		[runSearch, setRunSearch] = useState("")
	const [issueQuery, setIssueQuery] = useState(""),
		[adjustQuery, setAdjustQuery] = useState(""),
		[runQuery, setRunQuery] = useState("")
	const [issueStatus, setIssueStatus] = useState(""),
		[adjustStatus, setAdjustStatus] = useState(""),
		[runStatus, setRunStatus] = useState("")
	const [issueSort, setIssueSort] = useState("created_at:desc"),
		[adjustSort, setAdjustSort] = useState("created_at:desc"),
		[runSort, setRunSort] = useState("created_at:desc")
	useEffect(() => {
		const timer = setTimeout(() => {
			setIssuePage(0)
			setAdjustPage(0)
			setRunPage(0)
			setIssueQuery(issueSearch.trim())
			setAdjustQuery(adjustSearch.trim())
			setRunQuery(runSearch.trim())
		}, 300)
		return () => clearTimeout(timer)
	}, [adjustSearch, issueSearch, runSearch])
	const load = useCallback(async () => {
		try {
			const [issueSortField, issueOrder] = issueSort.split(":"),
				[adjustSortField, adjustOrder] = adjustSort.split(":"),
				[runSortField, runOrder] = runSort.split(":")
			const [issueData, adjustmentData, runData] = await Promise.all([
				api.send<Page<Issue>>(`/api/v1/billing/periods/${period.id}/issues`, {
					query: {
						limit: issueSize,
						offset: issuePage * issueSize,
						q: issueQuery || undefined,
						status: issueStatus || undefined,
						sort: issueSortField,
						order: issueOrder,
					},
				}),
				api.send<Page<Adjustment>>(`/api/v1/billing/periods/${period.id}/adjustments`, {
					query: {
						limit: adjustSize,
						offset: adjustPage * adjustSize,
						q: adjustQuery || undefined,
						status: adjustStatus || undefined,
						sort: adjustSortField,
						order: adjustOrder,
					},
				}),
				api.send<Page<Reconciliation>>(`/api/v1/billing/periods/${period.id}/reconciliations`, {
					query: {
						limit: runSize,
						offset: runPage * runSize,
						q: runQuery || undefined,
						status: runStatus || undefined,
						sort: runSortField,
						order: runOrder,
					},
				}),
			])
			setIssues(issueData)
			setAdjustments(adjustmentData)
			setRuns(runData)
		} catch (err) {
			notify(errorText(err))
		}
	}, [
		adjustPage,
		adjustQuery,
		adjustSize,
		adjustSort,
		adjustStatus,
		issuePage,
		issueQuery,
		issueSize,
		issueSort,
		issueStatus,
		notify,
		period.id,
		runPage,
		runQuery,
		runSize,
		runSort,
		runStatus,
	])
	useEffect(() => {
		load()
	}, [load, period.calculation_version, reload])
	const issueAction = async (record: Record<string, unknown>, field: string) => {
		if (field !== "action" || record.status !== "open") return
		const note = window.prompt(t`Resolution note`, "reviewed")
		if (!note) return
		try {
			await api.send(`/api/v1/billing/issues/${record.id}`, {
				method: "PATCH",
				headers: { "If-Match": `"${record.row_version}"` },
				body: { status: "acknowledged", resolution_note: note },
			})
			await load()
		} catch (err) {
			notify(errorText(err))
		}
	}
	const adjustmentAction = async (record: Record<string, unknown>, field: string) => {
		if (field !== "action") return
		try {
			if (record.status === "pending")
				await api.send(`/api/v1/billing/adjustments/${record.id}/approve`, {
					method: "POST",
					headers: { "If-Match": `"${record.row_version}"` },
					body: {},
				})
			else {
				const reason = window.prompt(t`Reversal reason`)
				if (!reason) return
				await api.send(`/api/v1/billing/adjustments/${record.id}/reverse`, { method: "POST", body: { reason } })
			}
			await load()
			await onPeriodRefresh()
		} catch (err) {
			notify(errorText(err))
		}
	}
	const issueColumns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "severity", title: t`Severity`, width: 100 },
			{ field: "kind", title: t`Type`, width: 140 },
			{ field: "comparison", title: t`Comparison`, width: 180 },
			{ field: "delta_value", title: t`Delta`, width: 130 },
			{ field: "threshold_value", title: t`Threshold`, width: 130 },
			{ field: "status", title: t`Status`, width: 120, filterField: "status" },
			{ field: "action", title: t`Action`, width: 130 },
		],
		[t]
	)
	const adjustmentColumns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "layer", title: t`Layer`, width: 110 },
			{ field: "amount", title: t`Amount`, width: 130 },
			{ field: "unit", title: t`Unit`, width: 90 },
			{ field: "reason", title: t`Reason`, width: 220 },
			{ field: "evidence_ref", title: t`Evidence`, width: 170 },
			{ field: "status", title: t`Status`, width: 110, filterField: "status" },
			{ field: "action", title: t`Action`, width: 120 },
		],
		[t]
	)
	const runColumns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "calculation_version", title: t`Version`, width: 90 },
			{ field: "status", title: t`Status`, width: 110, filterField: "status" },
			{ field: "issue_count", title: t`Issues`, width: 90 },
			{ field: "threshold_abs", title: t`Absolute threshold`, width: 150 },
			{ field: "threshold_percent", title: t`Percent threshold`, width: 150 },
			{ field: "created_at", title: t`Created`, width: 180 },
		],
		[t]
	)
	const filter = (value: string, set: (value: string) => void, options: string[]) => ({
		options: { status: options.map((item) => ({ value: item, label: item })) },
		selected: { status: value ? [value] : [] },
		selection: { status: "single" as const },
		onColumnFilterChange: (_field: string, values: unknown[]) => set(String(values[0] ?? "")),
		onClearAll: () => set(""),
	})
	const [issueSortField, issueSortDirection] = issueSort.split(":") as [string, SortDirection]
	const [adjustSortField, adjustSortDirection] = adjustSort.split(":") as [string, SortDirection]
	const [runSortField, runSortDirection] = runSort.split(":") as [string, SortDirection]
	return (
		<Tabs defaultValue="issues">
			<TabsList>
				<TabsTrigger value="issues">
					<Trans>Issues</Trans> ({issues.total})
				</TabsTrigger>
				<TabsTrigger value="adjustments">
					<Trans>Adjustments</Trans> ({adjustments.total})
				</TabsTrigger>
				<TabsTrigger value="runs">
					<Trans>Reconciliations</Trans> ({runs.total})
				</TabsTrigger>
			</TabsList>
			<TabsContent value="issues" className="mt-3">
				<PagedVTable
					records={issues.items.map((item) => ({
						...item,
						comparison: `${item.left_layer}/${item.right_layer || "-"}`,
						action: item.status === "open" ? t`Acknowledge` : "—",
					}))}
					columns={issueColumns}
					emptyText={t`No reconciliation issues.`}
					searchValue={issueSearch}
					onSearchChange={setIssueSearch}
					searchPlaceholder={t`Search issues...`}
					serverFiltering={filter(
						issueStatus,
						(value) => {
							setIssuePage(0)
							setIssueStatus(value)
						},
						["open", "acknowledged", "resolved"]
					)}
					serverSorting={{
						field: issueSortField,
						direction: issueSortDirection,
						fields: { severity: "severity", kind: "kind", delta_value: "delta_value", status: "status" },
						onSortChange: (field, direction) => {
							setIssuePage(0)
							setIssueSort(`${field}:${direction}`)
						},
					}}
					serverPagination={{
						page: issuePage,
						pageSize: issueSize,
						totalCount: issues.total,
						onPageChange: setIssuePage,
						onPageSizeChange: (value) => {
							setIssuePage(0)
							setIssueSize(value)
						},
					}}
					onCellClick={issueAction}
				/>
			</TabsContent>
			<TabsContent value="adjustments" className="mt-3">
				<PagedVTable
					records={adjustments.items.map((item) => ({
						...item,
						action: item.status === "pending" ? t`Approve` : t`Reverse`,
					}))}
					columns={adjustmentColumns}
					emptyText={t`No adjustments.`}
					searchValue={adjustSearch}
					onSearchChange={setAdjustSearch}
					searchPlaceholder={t`Search adjustments...`}
					serverFiltering={filter(
						adjustStatus,
						(value) => {
							setAdjustPage(0)
							setAdjustStatus(value)
						},
						["pending", "approved"]
					)}
					serverSorting={{
						field: adjustSortField,
						direction: adjustSortDirection,
						fields: { layer: "layer", amount: "amount", status: "status" },
						onSortChange: (field, direction) => {
							setAdjustPage(0)
							setAdjustSort(`${field}:${direction}`)
						},
					}}
					serverPagination={{
						page: adjustPage,
						pageSize: adjustSize,
						totalCount: adjustments.total,
						onPageChange: setAdjustPage,
						onPageSizeChange: (value) => {
							setAdjustPage(0)
							setAdjustSize(value)
						},
					}}
					onCellClick={adjustmentAction}
				/>
			</TabsContent>
			<TabsContent value="runs" className="mt-3">
				<PagedVTable
					records={runs.items.map((item) => ({ ...item, created_at: formatTime(item.created_at) }))}
					columns={runColumns}
					emptyText={t`No reconciliation runs.`}
					searchValue={runSearch}
					onSearchChange={setRunSearch}
					searchPlaceholder={t`Search reconciliations...`}
					serverFiltering={filter(
						runStatus,
						(value) => {
							setRunPage(0)
							setRunStatus(value)
						},
						["ok", "issues"]
					)}
					serverSorting={{
						field: runSortField,
						direction: runSortDirection,
						fields: {
							calculation_version: "calculation_version",
							status: "status",
							issue_count: "issue_count",
							created_at: "created_at",
						},
						onSortChange: (field, direction) => {
							setRunPage(0)
							setRunSort(`${field}:${direction}`)
						},
					}}
					serverPagination={{
						page: runPage,
						pageSize: runSize,
						totalCount: runs.total,
						onPageChange: setRunPage,
						onPageSizeChange: (value) => {
							setRunPage(0)
							setRunSize(value)
						},
					}}
				/>
			</TabsContent>
		</Tabs>
	)
}

async function waitForJob(path: string) {
	const deadline = Date.now() + 120_000
	while (Date.now() < deadline) {
		const job = await api.send<Job>(path, {})
		if (job.status === "succeeded") return job
		if (job.status === "failed" || job.status === "canceled")
			throw new Error(job.last_error_detail || `Job ${job.status}`)
		await new Promise((resolve) => setTimeout(resolve, 500))
	}
	throw new Error("Billing operation timed out")
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}
function Info({ label, value }: { label: string; value?: React.ReactNode }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="mt-1 truncate text-sm">{value ?? "—"}</div>
		</div>
	)
}
function AccountStatus({ value }: { value?: string }) {
	return value ? <Badge variant={value === "active" ? "success" : "outline"}>{value}</Badge> : null
}
function errorText(error: unknown) {
	return error instanceof Error ? error.message : String(error)
}
function formatTime(value?: string) {
	return value ? new Date(value).toLocaleString() : "—"
}
function formatValue(value: number | undefined, algorithm: string) {
	return algorithm === "total" ? formatBytes(value) : formatBPS(value)
}
function formatBytes(value?: number) {
	if (value === undefined) return "—"
	if (value >= 1e12) return `${(value / 1e12).toFixed(2)} TB`
	if (value >= 1e9) return `${(value / 1e9).toFixed(2)} GB`
	if (value >= 1e6) return `${(value / 1e6).toFixed(2)} MB`
	return `${value} B`
}
function formatBPS(value?: number) {
	if (value === undefined) return "—"
	if (value >= 1e9) return `${(value / 1e9).toFixed(2)} Gbps`
	if (value >= 1e6) return `${(value / 1e6).toFixed(2)} Mbps`
	if (value >= 1e3) return `${(value / 1e3).toFixed(2)} Kbps`
	return `${value} bps`
}
function defaultWindow() {
	const to = new Date(Math.floor(Date.now() / 300_000) * 300_000)
	const from = new Date(to.getTime() - 30 * 24 * 60 * 60 * 1000)
	return { from: localDateTime(from), to: localDateTime(to) }
}
function localDateTime(value: Date) {
	const local = new Date(value.getTime() - value.getTimezoneOffset() * 60_000)
	return local.toISOString().slice(0, 16)
}
