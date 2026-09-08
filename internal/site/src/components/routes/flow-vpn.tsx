import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { DownloadIcon, ListChecksIcon, RefreshCwIcon, SaveIcon, ShieldCheckIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { toast } from "@/components/ui/use-toast"
import { pb } from "@/lib/api"
import type { ColumnDefine, ServerFilterOption } from "@/lib/vtable"
import { cn } from "@/lib/utils"

type VPNFinding = {
	id: string
	window_start: string
	window_end: string
	conversation_key: string
	local_ip: string
	remote_ip: string
	primary_protocol: number
	primary_local_port: number
	primary_remote_port: number
	local_to_remote_bytes: number
	remote_to_local_bytes: number
	flow_record_count: number
	active_bucket_count: number
	max_duration_ms: number
	remote_asn: number
	remote_country: string
	remote_prefix_id: string
	complete_ratio: number
	score: number
	risk_level: string
	verdict: string
	probe_recommended: boolean
	probe_block_reason: string
	decision_rule_id: string
	evidence_schema_version: number
	evidence: Record<string, unknown>
	rule_set_version: string
	dimension_snapshot_id: string
	geo_version: string
	classification_version: number
	source_generation: number
	generated_at: string
	disposition: string
	disposition_note: string
	disposition_by?: string
	disposition_at?: string
	probe_status: string
	probe_job_id?: string
	probe_result: Record<string, unknown>
	expires_at: string
	row_version: number
}

type FindingsResponse = { items?: VPNFinding[]; total?: number }
type FacetsResponse = { items?: Array<{ value: string; count: number }> }
type RangePreset = "1h" | "6h" | "24h" | "7d" | "30d"
type SortDirection = "asc" | "desc"

const filterFields = [
	"window_end",
	"local_ip",
	"remote_ip",
	"primary_protocol",
	"primary_local_port",
	"primary_remote_port",
	"local_to_remote_bytes",
	"remote_to_local_bytes",
	"remote_asn",
	"remote_country",
	"score",
	"risk_level",
	"verdict",
	"disposition",
	"probe_status",
] as const

const rangeDurations: Record<RangePreset, number> = {
	"1h": 60 * 60 * 1000,
	"6h": 6 * 60 * 60 * 1000,
	"24h": 24 * 60 * 60 * 1000,
	"7d": 7 * 24 * 60 * 60 * 1000,
	"30d": 30 * 24 * 60 * 60 * 1000,
}

export default memo(() => {
	const { t } = useLingui()
	const [findings, setFindings] = useState<VPNFinding[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [range, setRange] = useState<RangePreset>("24h")
	const [rangeAnchor, setRangeAnchor] = useState(() => Date.now())
	const [sortField, setSortField] = useState("window_end")
	const [sortDirection, setSortDirection] = useState<SortDirection>("desc")
	const [columnFilters, setColumnFilters] = useState<Record<string, unknown[]>>({})
	const [selected, setSelected] = useState<VPNFinding | null>(null)
	const [disposition, setDisposition] = useState("unreviewed")
	const [note, setNote] = useState("")
	const [saving, setSaving] = useState(false)
	const [exporting, setExporting] = useState(false)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	const selectedRange = useMemo(() => {
		const to = new Date(rangeAnchor)
		return { from: new Date(to.getTime() - rangeDurations[range]), to }
	}, [range, rangeAnchor])

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const buildQuery = useCallback(
		(includePage: boolean, excludedField = "") => {
			const params = new URLSearchParams({
				from: selectedRange.from.toISOString(),
				to: selectedRange.to.toISOString(),
			})
			if (query) params.set("q", query)
			for (const [field, values] of Object.entries(columnFilters)) {
				if (field === excludedField) continue
				for (const value of values) params.append(`filter.${field}`, String(value))
			}
			if (includePage) {
				params.set("limit", String(pageSize))
				params.set("offset", String(page * pageSize))
				params.set("sort_by", sortField)
				params.set("sort_direction", sortDirection)
			}
			return params
		},
		[columnFilters, page, pageSize, query, selectedRange, sortDirection, sortField]
	)

	const refresh = useCallback(async () => {
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<FindingsResponse>(`/api/v1/flow/vpn/findings?${buildQuery(true)}`, {})
			if (sequence !== requestSequence.current) return
			setFindings(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (reason) {
			if (sequence !== requestSequence.current) return
			setFindings([])
			setTotal(0)
			setError(reason instanceof Error ? reason.message : t`Failed to load VPN findings`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [buildQuery, t])

	useEffect(() => {
		document.title = `${t`VPN Risk`} / Watchdog`
		refresh()
	}, [refresh, t])

	const records = useMemo(
		() =>
			findings.map((finding) => ({
				window: formatDate(finding.window_end),
				localIP: finding.local_ip,
				remoteIP: finding.remote_ip,
				protocol: protocolLabel(finding.primary_protocol),
				localPort: finding.primary_local_port,
				remotePort: finding.primary_remote_port,
				outbound: formatBytes(finding.local_to_remote_bytes),
				inbound: formatBytes(finding.remote_to_local_bytes),
				remoteASN: finding.remote_asn || "—",
				country: finding.remote_country || t`Unknown`,
				score: finding.score,
				risk: finding.risk_level,
				verdict: finding.verdict,
				disposition: finding.disposition,
				probe: finding.probe_status,
				finding,
			})),
		[findings, t]
	)

	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "window", filterField: "window_end", title: t`Window end`, width: 175 },
			{ field: "localIP", filterField: "local_ip", title: t`Local IP`, width: 155 },
			{ field: "remoteIP", filterField: "remote_ip", title: t`Remote IP`, width: 155 },
			{ field: "protocol", filterField: "primary_protocol", title: t`Protocol`, width: 95 },
			{ field: "localPort", filterField: "primary_local_port", title: t`Local port`, width: 100 },
			{ field: "remotePort", filterField: "primary_remote_port", title: t`Remote port`, width: 110 },
			{ field: "outbound", filterField: "local_to_remote_bytes", title: t`Outbound`, width: 110 },
			{ field: "inbound", filterField: "remote_to_local_bytes", title: t`Inbound`, width: 110 },
			{ field: "remoteASN", filterField: "remote_asn", title: "ASN", width: 100 },
			{ field: "country", filterField: "remote_country", title: t`Country`, width: 95 },
			{ field: "score", title: t`Score`, width: 80 },
			{ field: "risk", filterField: "risk_level", title: t`Risk`, width: 100 },
			{ field: "verdict", title: t`Verdict`, width: 135 },
			{ field: "disposition", title: t`Disposition`, width: 135 },
			{ field: "probe", filterField: "probe_status", title: t`Probe`, width: 125 },
		],
		[t]
	)

	const serverFiltering = useMemo(
		() => ({
			options: Object.fromEntries(filterFields.map((field) => [field, [] as ServerFilterOption[]])),
			selected: columnFilters,
			loadOptions: async (field: string, facetSearch: string, signal: AbortSignal) => {
				const params = buildQuery(false, field)
				params.set("field", field)
				if (query) params.set("search", query)
				params.set("q", facetSearch)
				params.set("limit", "100")
				const data = await pb.send<FacetsResponse>(`/api/v1/flow/vpn/findings/facets?${params}`, { signal })
				return (data.items ?? []).map((item) => ({
					value: item.value,
					label: facetLabel(field, item.value, t),
					count: item.count,
				}))
			},
			onColumnFilterChange: (field: string, values: unknown[]) => {
				setPage(0)
				setColumnFilters((current) => {
					const next = { ...current }
					if (values.length > 0) next[field] = values
					else delete next[field]
					return next
				})
			},
			onClearAll: () => {
				setPage(0)
				setColumnFilters({})
			},
		}),
		[buildQuery, columnFilters, t]
	)

	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				window: "window_end",
				localIP: "local_ip",
				remoteIP: "remote_ip",
				protocol: "primary_protocol",
				localPort: "primary_local_port",
				remotePort: "primary_remote_port",
				outbound: "local_to_remote_bytes",
				inbound: "remote_to_local_bytes",
				remoteASN: "remote_asn",
				country: "remote_country",
				score: "score",
				risk: "risk_level",
				verdict: "verdict",
				disposition: "disposition",
				probe: "probe_status",
			},
			onSortChange: (field: string, direction: SortDirection) => {
				setPage(0)
				setSortField(field)
				setSortDirection(direction)
			},
		}),
		[sortDirection, sortField]
	)

	const chooseFinding = (finding: VPNFinding) => {
		setSelected(finding)
		setDisposition(finding.disposition)
		setNote(finding.disposition_note)
	}

	const saveDisposition = async () => {
		if (!selected) return
		setSaving(true)
		setError("")
		try {
			const updated = await pb.send<VPNFinding>(`/api/v1/flow/vpn/findings/${selected.id}/actions/disposition`, {
				method: "POST",
				headers: { "If-Match": `"${selected.row_version}"` },
				body: { disposition, note },
			})
			setSelected(updated)
			setFindings((current) => current.map((item) => (item.id === updated.id ? updated : item)))
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to update VPN finding`)
		} finally {
			setSaving(false)
		}
	}

	const exportCurrent = async () => {
		setExporting(true)
		setError("")
		try {
			const frozenFilters = Object.fromEntries(
				Object.entries(columnFilters).map(([field, values]) => [field, values.map(String)])
			)
			const task = await pb.send<{ ID?: string; id?: string }>("/api/v1/flow/vpn/findings/exports", {
				method: "POST",
				body: {
					from: selectedRange.from.toISOString(),
					to: selectedRange.to.toISOString(),
					search: query,
					column_filters: frozenFilters,
					sort_by: sortField,
					sort_direction: sortDirection,
					format: "csv",
				},
			})
			const id = task.ID ?? task.id ?? ""
			toast({ title: t`VPN finding export queued` })
			navigate(id ? getPagePath($router, "export_detail", { id }) : getPagePath($router, "exports"))
		} catch (reason) {
			setError(reason instanceof Error ? reason.message : t`Failed to export VPN findings`)
		} finally {
			setExporting(false)
		}
	}

	return (
		<div id="vpn-findings" className="grid scroll-mt-4 gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<ShieldCheckIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<div>
						<h1 className="text-xl font-semibold tracking-normal">
							<Trans>VPN Risk</Trans> ({total})
						</h1>
						<p className="text-sm text-muted-foreground">
							<Trans>Review passive evidence and human dispositions. Active probes are separately authorized.</Trans>
						</p>
					</div>
				</div>
				<div className="flex items-end gap-2">
					<Link className={cn(buttonVariants({ variant: "outline" }))} href={getPagePath($router, "flow_vpn_rules")}>
						<ListChecksIcon className="me-2 h-4 w-4" />
						<Trans>Rules</Trans>
					</Link>
					<div className="grid gap-1">
						<Label className="text-xs">
							<Trans>Time range</Trans>
						</Label>
						<Select
							value={range}
							onValueChange={(value: RangePreset) => {
								setPage(0)
								setRange(value)
								setRangeAnchor(Date.now())
							}}
						>
							<SelectTrigger className="w-28">
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								{Object.keys(rangeDurations).map((value) => (
									<SelectItem key={value} value={value}>
										{value}
									</SelectItem>
								))}
							</SelectContent>
						</Select>
					</div>
					<Button variant="outline" onClick={exportCurrent} disabled={loading || exporting || total === 0}>
						<DownloadIcon className="me-2 h-4 w-4" />
						<Trans>Export CSV</Trans>
					</Button>
					<Button
						variant="outline"
						onClick={() => {
							setRangeAnchor(Date.now())
						}}
						disabled={loading}
					>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			<div className="overflow-hidden rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No VPN findings found in this time range.`}
					searchPlaceholder={t`Search IP, prefix, rule set, or finding ID...`}
					searchValue={search}
					onSearchChange={setSearch}
					onSearchSubmit={(value) => {
						setPage(0)
						setQuery(value.trim())
					}}
					serverFiltering={serverFiltering}
					serverSorting={serverSorting}
					serverPagination={{
						page,
						pageSize,
						totalCount: total,
						onPageChange: setPage,
						onPageSizeChange: (value) => {
							setPage(0)
							setPageSize(value)
						},
					}}
					height={520}
					onRowClick={(record) => chooseFinding(record.finding as VPNFinding)}
				/>
			</div>

			{selected ? (
				<div className="grid gap-4 rounded-md border border-border bg-card p-4">
					<div className="flex flex-wrap items-center justify-between gap-2">
						<div>
							<h2 className="font-semibold">
								{selected.local_ip}:{selected.primary_local_port} → {selected.remote_ip}:{selected.primary_remote_port}
							</h2>
							<p className="text-xs text-muted-foreground">
								{selected.id} · {selected.rule_set_version} · generation {selected.source_generation}
							</p>
						</div>
						<Button size="sm" onClick={saveDisposition} disabled={saving}>
							<SaveIcon className="me-2 h-4 w-4" />
							<Trans>Save disposition</Trans>
						</Button>
					</div>
					<div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
						<Fact label={t`Score`} value={`${selected.score} / ${selected.risk_level}`} />
						<Fact label={t`Verdict`} value={selected.verdict} />
						<Fact label={t`Completeness`} value={`${(selected.complete_ratio * 100).toFixed(1)}%`} />
						<Fact
							label={t`Probe`}
							value={`${selected.probe_status}${selected.probe_block_reason ? ` · ${selected.probe_block_reason}` : ""}`}
						/>
					</div>
					<div className="grid gap-3 md:grid-cols-[220px_1fr]">
						<div className="grid gap-1.5">
							<Label>
								<Trans>Disposition</Trans>
							</Label>
							<Select value={disposition} onValueChange={setDisposition}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									{["unreviewed", "confirmed", "false_positive", "allowed", "suppressed"].map((value) => (
										<SelectItem key={value} value={value}>
											{value}
										</SelectItem>
									))}
								</SelectContent>
							</Select>
						</div>
						<div className="grid gap-1.5">
							<Label htmlFor="vpn-disposition-note">
								<Trans>Review note</Trans>
							</Label>
							<Input
								id="vpn-disposition-note"
								value={note}
								maxLength={2000}
								onChange={(event) => setNote(event.target.value)}
							/>
						</div>
					</div>
					<div className="grid gap-1.5">
						<Label>
							<Trans>Evidence</Trans>
						</Label>
						<pre className="max-h-72 overflow-auto rounded-md bg-muted p-3 text-xs">
							{JSON.stringify(selected.evidence, null, 2)}
						</pre>
					</div>
				</div>
			) : null}
		</div>
	)
})

function Fact({ label, value }: { label: string; value: string }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="mt-1 font-medium">{value || "—"}</div>
		</div>
	)
}

function protocolLabel(value: number) {
	if (value === 6) return "TCP (6)"
	if (value === 17) return "UDP (17)"
	return String(value)
}

function facetLabel(field: string, value: string, translate: (message: string) => string) {
	if (field === "primary_protocol") return protocolLabel(Number(value))
	if (field === "remote_country" && value === "_unknown") return translate("Unknown")
	if (field === "local_to_remote_bytes" || field === "remote_to_local_bytes") return formatBytes(Number(value))
	if (field === "window_end") return formatDate(value)
	return value || translate("Unknown")
}

function formatBytes(value: number) {
	if (!Number.isFinite(value) || value < 0) return "—"
	const units = ["B", "KB", "MB", "GB", "TB", "PB"]
	let unit = 0
	let amount = value
	while (amount >= 1000 && unit < units.length - 1) {
		amount /= 1000
		unit++
	}
	return `${amount >= 100 ? amount.toFixed(0) : amount >= 10 ? amount.toFixed(1) : amount.toFixed(2)} ${units[unit]}`
}

function formatDate(value: string) {
	if (!value) return "—"
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}
