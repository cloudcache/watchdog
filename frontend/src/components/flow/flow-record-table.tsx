import type { I18n } from "@lingui/core"
import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { DownloadIcon } from "lucide-react"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { toast } from "@/components/ui/use-toast"
import { api } from "@/lib/api"
import {
	buildFlowRecordRows,
	formatFlowBytes,
	type FlowRecordRow,
	updateFlowRecordCursors,
} from "@/lib/flow-record-model"
import { flowDimensionValueLabel } from "@/lib/flow-report-model"

type FlowRecordEndpoint = "source" | "destination"
type FlowRecordView = "customer" | "supplier" | "raw"
const FLOW_DETAIL_EXPORT_PAGE_SIZE = 500

type FlowRecordSearchResponse = {
	data: {
		rows: FlowRecordRow[]
		has_more: boolean
		next_cursor?: string
	}
}

type FlowRecordFacetResponse = {
	data: {
		field: string
		items: { value: string; count: number }[]
	}
}

const DETAIL_FIELDS: Record<FlowRecordView, string[]> = {
	customer: [
		"src_ip",
		"dst_ip",
		"src_port",
		"dst_port",
		"ip_protocol",
		"business_direction",
		"category",
		"remote_asn",
		"remote_country",
		"estimated_bytes",
		"sampling_rate",
		"quality_flags",
	],
	supplier: [
		"src_ip",
		"dst_ip",
		"src_port",
		"dst_port",
		"ip_protocol",
		"business_direction",
		"category",
		"remote_asn",
		"remote_country",
		"raw_bytes",
		"estimated_bytes",
		"sampling_rate",
		"quality_flags",
	],
	raw: [
		"src_ip",
		"dst_ip",
		"src_port",
		"dst_port",
		"ip_protocol",
		"source_asn",
		"destination_asn",
		"raw_bytes",
		"estimated_bytes",
		"sampling_rate",
		"quality_flags",
	],
}

const FILTER_FIELDS: Record<FlowRecordView, string[]> = {
	customer: ["event_time", ...DETAIL_FIELDS.customer],
	supplier: ["event_time", ...DETAIL_FIELDS.supplier],
	raw: ["event_time", ...DETAIL_FIELDS.raw],
}

export function FlowRecordTable({
	endpoint,
	selectedIP,
	onIPChange,
	from,
	to,
}: {
	endpoint: FlowRecordEndpoint
	selectedIP: string
	onIPChange: (ip: string) => void
	from: string
	to: string
}) {
	const { t, i18n } = useLingui()
	const [search, setSearch] = useState(selectedIP)
	const [view, setView] = useState<FlowRecordView>("customer")
	const [columnFilters, setColumnFilters] = useState<Record<string, string[]>>({})
	const [sortField, setSortField] = useState("event_time")
	const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc")
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [rows, setRows] = useState<FlowRecordRow[]>([])
	const [hasMore, setHasMore] = useState(false)
	const [loading, setLoading] = useState(false)
	const [exporting, setExporting] = useState(false)
	const [error, setError] = useState("")
	const cursors = useRef<string[]>([""])
	const activeRequest = useRef<AbortController | null>(null)

	useEffect(() => setSearch(selectedIP), [selectedIP])

	const loadPage = useCallback(
		async (nextPage: number) => {
			if (!selectedIP || !from || !to) {
				setRows([])
				setHasMore(false)
				return
			}
			const cursor = cursors.current[nextPage]
			if (nextPage > 0 && !cursor) return
			activeRequest.current?.abort()
			const controller = new AbortController()
			activeRequest.current = controller
			setLoading(true)
			setError("")
			try {
				const response = await api.send<FlowRecordSearchResponse>("/api/v1/flow/records/search", {
					method: "POST",
					signal: controller.signal,
					body: {
						ip: selectedIP,
						endpoint,
						from,
						to,
						view,
						fields: DETAIL_FIELDS[view],
						column_filters: Object.entries(columnFilters).map(([field, values]) => ({ field, values })),
						sort: { field: sortField, direction: sortDirection },
						limit: pageSize,
						cursor,
					},
				})
				if (activeRequest.current !== controller) return
				setRows(response.data.rows ?? [])
				setHasMore(Boolean(response.data.has_more))
				setPage(nextPage)
				cursors.current = updateFlowRecordCursors(cursors.current, nextPage, response.data.next_cursor)
			} catch (reason) {
				if (controller.signal.aborted) return
				setRows([])
				setHasMore(false)
				setError(reason instanceof Error ? reason.message : t`Failed to load Flow records`)
			} finally {
				if (activeRequest.current === controller) {
					activeRequest.current = null
					setLoading(false)
				}
			}
		},
		[columnFilters, endpoint, from, pageSize, selectedIP, sortDirection, sortField, t, to, view]
	)

	useEffect(() => {
		cursors.current = [""]
		loadPage(0).catch(() => {})
		return () => activeRequest.current?.abort()
	}, [loadPage])

	const records = useMemo(
		() =>
			buildFlowRecordRows(rows).map((row) => ({
				...row,
				direction: flowDimensionValueLabel("business_direction", row.direction, i18n),
				category: flowDimensionValueLabel("category", row.category, i18n),
			})),
		[i18n, i18n.locale, rows]
	)
	const columns = useMemo(() => {
		const common = [
			{ field: "event_time", title: t`Event time`, width: 180 },
			{ field: "src_ip", title: t`Source IP`, width: 170 },
			{ field: "src_port", title: t`Source port`, width: 100 },
			{ field: "dst_ip", title: t`Destination IP`, width: 170 },
			{ field: "dst_port", title: t`Destination port`, width: 110 },
			{ field: "protocol", filterField: "ip_protocol", title: t`Protocol`, width: 100 },
		]
		const tail =
			view === "raw"
				? [
						{ field: "source_asn", title: t`Source ASN`, width: 110 },
						{ field: "destination_asn", title: t`Destination ASN`, width: 120 },
						{ field: "raw_bytes", title: t`Raw bytes`, width: 120 },
					]
				: [
						{ field: "direction", filterField: "business_direction", title: t`Direction`, width: 110 },
						{ field: "category", title: t`Category`, width: 170 },
						{ field: "remote_asn", title: t`Remote ASN`, width: 110 },
						{ field: "country", filterField: "remote_country", title: t`Country`, width: 110 },
						...(view === "supplier" ? [{ field: "raw_bytes", title: t`Raw bytes`, width: 120 }] : []),
					]
		return [
			...common,
			...tail,
			{ field: "estimated_bytes", title: t`Estimated bytes`, width: 130 },
			{ field: "sampling_rate", title: t`Sampling rate`, width: 110 },
			{ field: "quality_flags", title: t`Quality flags`, width: 110 },
		]
	}, [t, view])
	const serverFiltering = useMemo(
		() => ({
			options: Object.fromEntries(
				FILTER_FIELDS[view].map((field) => [field, (columnFilters[field] ?? []).map((value) => ({ value }))])
			),
			selected: columnFilters,
			loadOptions: async (field: string, searchText: string, signal: AbortSignal) => {
				if (!selectedIP || !from || !to) return []
				const response = await api.send<FlowRecordFacetResponse>("/api/v1/flow/records/facets", {
					method: "POST",
					signal,
					body: {
						ip: selectedIP,
						endpoint,
						from,
						to,
						view,
						field,
						search: searchText || undefined,
						column_filters: Object.entries(columnFilters).map(([filterField, values]) => ({
							field: filterField,
							values,
						})),
						limit: 100,
					},
				})
				return (response.data.items ?? []).map((item) => ({
					value: item.value,
					label: flowFacetLabel(field, item.value, i18n),
					count: item.count,
				}))
			},
			onColumnFilterChange: (field: string, values: unknown[]) => {
				const normalized = values.map(String)
				setColumnFilters((current) => {
					const next = { ...current, [field]: normalized }
					if (normalized.length === 0) delete next[field]
					return next
				})
			},
			onClearAll: () => setColumnFilters({}),
		}),
		[columnFilters, endpoint, from, selectedIP, t, to, view]
	)
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				event_time: "event_time",
				src_ip: "src_ip",
				src_port: "src_port",
				dst_ip: "dst_ip",
				dst_port: "dst_port",
				protocol: "ip_protocol",
				direction: "business_direction",
				category: "category",
				source_asn: "source_asn",
				destination_asn: "destination_asn",
				remote_asn: "remote_asn",
				country: "remote_country",
				raw_bytes: "raw_bytes",
				estimated_bytes: "estimated_bytes",
				sampling_rate: "sampling_rate",
				quality_flags: "quality_flags",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => {
				cursors.current = [""]
				setPage(0)
				setSortField(field)
				setSortDirection(direction)
			},
		}),
		[sortDirection, sortField]
	)
	const submitSearch = (value: string) => {
		const normalized = value.trim()
		cursors.current = [""]
		if (normalized === selectedIP) {
			loadPage(0).catch(() => {})
			return
		}
		onIPChange(normalized)
	}
	const changeView = (next: FlowRecordView) => {
		cursors.current = [""]
		setColumnFilters({})
		setSortField("event_time")
		setSortDirection("desc")
		setPage(0)
		setView(next)
	}
	const exportDetails = async () => {
		if (!selectedIP || view === "customer") return
		setExporting(true)
		try {
			const task = await api.send<{ ID?: string; id?: string }>("/api/v1/flow/records/exports", {
				method: "POST",
				body: {
					query: {
						ip: selectedIP,
						endpoint,
						from,
						to,
						view,
						fields: DETAIL_FIELDS[view],
						column_filters: Object.entries(columnFilters).map(([field, values]) => ({ field, values })),
						sort: { field: sortField, direction: sortDirection },
						limit: FLOW_DETAIL_EXPORT_PAGE_SIZE,
					},
					format: "csv",
				},
			})
			const id = task.ID ?? task.id ?? ""
			toast({ title: t`Flow detail export queued` })
			navigate(id ? getPagePath($router, "export_detail", { id }) : getPagePath($router, "exports"))
		} catch (reason) {
			toast({
				title: reason instanceof Error ? reason.message : t`Failed to create Flow detail export`,
				variant: "destructive",
			})
		} finally {
			setExporting(false)
		}
	}

	return (
		<div className="grid gap-3 rounded-md border border-border bg-card p-3">
			<div className="flex flex-wrap items-start justify-between gap-3">
				<div>
					<h2 className="font-medium">
						<Trans>Flow record details</Trans>
					</h2>
					<p className="text-xs text-muted-foreground">
						<Trans>Select a Top IP row or enter an exact IPv4/IPv6 address and press Enter.</Trans>
					</p>
				</div>
				<div className="flex items-center gap-2">
					<Select value={view} onValueChange={(value) => changeView(value as FlowRecordView)}>
						<SelectTrigger className="w-36" aria-label={t`Value layer`}>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="customer">
								<Trans>Customer</Trans>
							</SelectItem>
							<SelectItem value="supplier">
								<Trans>Supplier</Trans>
							</SelectItem>
							<SelectItem value="raw">
								<Trans>Raw</Trans>
							</SelectItem>
						</SelectContent>
					</Select>
					{view !== "customer" && (
						<Button
							variant="outline"
							onClick={() => exportDetails().catch(() => {})}
							disabled={!selectedIP || exporting}
						>
							<DownloadIcon className="me-2 h-4 w-4" />
							{exporting ? <Trans>Queuing...</Trans> : <Trans>Export CSV</Trans>}
						</Button>
					)}
				</div>
			</div>
			{error && <div className="text-sm text-destructive">{error}</div>}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={selectedIP ? t`No matching Flow records` : t`Select or enter an IP address`}
				searchPlaceholder={endpoint === "source" ? t`Exact source IP...` : t`Exact destination IP...`}
				searchValue={search}
				onSearchChange={setSearch}
				onSearchSubmit={submitSearch}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				serverPagination={{
					page,
					pageSize,
					hasNextPage: hasMore,
					onPageChange: (next) => loadPage(next).catch(() => {}),
					onPageSizeChange: (size) => {
						setPageSize(size)
						setPage(0)
						cursors.current = [""]
					},
				}}
				height={420}
			/>
		</div>
	)
}

function flowFacetLabel(field: string, value: string, i18n: Pick<I18n, "_">) {
	if (field === "ip_protocol" || field === "business_direction" || field === "category") {
		return flowDimensionValueLabel(field, value, i18n)
	}
	if (field === "estimated_bytes" || field === "raw_bytes") return formatFlowBytes(value)
	if (field === "event_time") {
		const instant = new Date(value)
		if (!Number.isNaN(instant.valueOf())) return instant.toLocaleString()
	}
	return value || undefined
}
