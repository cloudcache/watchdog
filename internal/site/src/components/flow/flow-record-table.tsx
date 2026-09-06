import { Trans, useLingui } from "@lingui/react/macro"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { pb } from "@/lib/api"
import {
	buildFlowRecordRows,
	flowProtocolLabel,
	formatFlowBytes,
	type FlowRecordRow,
	updateFlowRecordCursors,
} from "@/lib/flow-record-model"

type FlowRecordEndpoint = "source" | "destination"

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

const DETAIL_FIELDS = [
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
]

const FILTER_FIELDS = [
	"event_time",
	"src_ip",
	"src_port",
	"dst_ip",
	"dst_port",
	"ip_protocol",
	"business_direction",
	"category",
	"remote_asn",
	"remote_country",
	"estimated_bytes",
	"sampling_rate",
	"quality_flags",
]

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
	const { t } = useLingui()
	const [search, setSearch] = useState(selectedIP)
	const [columnFilters, setColumnFilters] = useState<Record<string, string[]>>({})
	const [sortField, setSortField] = useState("event_time")
	const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc")
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [rows, setRows] = useState<FlowRecordRow[]>([])
	const [hasMore, setHasMore] = useState(false)
	const [loading, setLoading] = useState(false)
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
				const response = await pb.send<FlowRecordSearchResponse>("/api/v1/flow/records/search", {
					method: "POST",
					signal: controller.signal,
					body: {
						ip: selectedIP,
						endpoint,
						from,
						to,
						view: "customer",
						fields: DETAIL_FIELDS,
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
		[columnFilters, endpoint, from, pageSize, selectedIP, sortDirection, sortField, t, to]
	)

	useEffect(() => {
		cursors.current = [""]
		loadPage(0).catch(() => {})
		return () => activeRequest.current?.abort()
	}, [loadPage])

	const records = useMemo(() => buildFlowRecordRows(rows), [rows])
	const columns = useMemo(
		() => [
			{ field: "event_time", title: t`Event time`, width: 180 },
			{ field: "src_ip", title: t`Source IP`, width: 170 },
			{ field: "src_port", title: t`Source port`, width: 100 },
			{ field: "dst_ip", title: t`Destination IP`, width: 170 },
			{ field: "dst_port", title: t`Destination port`, width: 110 },
			{ field: "protocol", filterField: "ip_protocol", title: t`Protocol`, width: 100 },
			{ field: "direction", filterField: "business_direction", title: t`Direction`, width: 110 },
			{ field: "category", title: t`Category`, width: 170 },
			{ field: "remote_asn", title: t`Remote ASN`, width: 110 },
			{ field: "country", filterField: "remote_country", title: t`Country`, width: 110 },
			{ field: "estimated_bytes", title: t`Estimated bytes`, width: 130 },
			{ field: "sampling_rate", title: t`Sampling rate`, width: 110 },
			{ field: "quality_flags", title: t`Quality flags`, width: 110 },
		],
		[t]
	)
	const serverFiltering = useMemo(
		() => ({
			options: Object.fromEntries(
				FILTER_FIELDS.map((field) => [field, (columnFilters[field] ?? []).map((value) => ({ value }))])
			),
			selected: columnFilters,
			loadOptions: async (field: string, searchText: string, signal: AbortSignal) => {
				if (!selectedIP || !from || !to) return []
				const response = await pb.send<FlowRecordFacetResponse>("/api/v1/flow/records/facets", {
					method: "POST",
					signal,
					body: {
						ip: selectedIP,
						endpoint,
						from,
						to,
						view: "customer",
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
					label: flowFacetLabel(field, item.value),
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
		[columnFilters, endpoint, from, selectedIP, to]
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
				remote_asn: "remote_asn",
				country: "remote_country",
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

	return (
		<div className="grid gap-3 rounded-md border border-border bg-card p-3">
			<div>
				<h2 className="font-medium">
					<Trans>Flow record details</Trans>
				</h2>
				<p className="text-xs text-muted-foreground">
					<Trans>Select a Top IP row or enter an exact IPv4/IPv6 address and press Enter.</Trans>
				</p>
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

function flowFacetLabel(field: string, value: string) {
	if (field === "ip_protocol") return flowProtocolLabel(value)
	if (field === "estimated_bytes") return formatFlowBytes(value)
	if (field === "event_time") {
		const instant = new Date(value)
		if (!Number.isNaN(instant.valueOf())) return instant.toLocaleString()
	}
	return value || undefined
}
