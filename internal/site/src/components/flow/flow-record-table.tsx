import { Trans, useLingui } from "@lingui/react/macro"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { pb } from "@/lib/api"
import { buildFlowRecordRows, type FlowRecordRow, updateFlowRecordCursors } from "@/lib/flow-record-model"

type FlowRecordEndpoint = "source" | "destination"

type FlowRecordSearchResponse = {
	data: {
		rows: FlowRecordRow[]
		has_more: boolean
		next_cursor?: string
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

const CATEGORY_OPTIONS = [
	"on_net_local_city",
	"on_net_cross_city",
	"on_net_cross_province",
	"off_net_in_province",
	"off_net_cross_province",
	"overseas",
	"internal",
	"transit",
	"ambiguous",
	"unknown",
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
	const [directions, setDirections] = useState<string[]>([])
	const [categories, setCategories] = useState<string[]>([])
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
						filters: {
							directions: directions.length === 0 ? undefined : directions,
							categories: categories.length === 0 ? undefined : categories,
						},
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
		[categories, directions, endpoint, from, pageSize, selectedIP, sortDirection, sortField, t, to]
	)

	useEffect(() => {
		cursors.current = [""]
		loadPage(0).catch(() => {})
		return () => activeRequest.current?.abort()
	}, [loadPage])

	const records = useMemo(() => buildFlowRecordRows(rows), [rows])
	const columns = useMemo(
		() => [
			{ field: "event_time", title: t`Event time`, width: 180, filter: false },
			{ field: "src_ip", title: t`Source IP`, width: 170, filter: false },
			{ field: "src_port", title: t`Source port`, width: 100, filter: false },
			{ field: "dst_ip", title: t`Destination IP`, width: 170, filter: false },
			{ field: "dst_port", title: t`Destination port`, width: 110, filter: false },
			{ field: "protocol", title: t`Protocol`, width: 100, filter: false },
			{ field: "direction", title: t`Direction`, width: 110 },
			{ field: "category", title: t`Category`, width: 170 },
			{ field: "remote_asn", title: t`Remote ASN`, width: 110, filter: false },
			{ field: "country", title: t`Country`, width: 110, filter: false },
			{ field: "estimated_bytes", title: t`Estimated bytes`, width: 130, filter: false },
			{ field: "sampling_rate", title: t`Sampling rate`, width: 110, filter: false },
			{ field: "quality_flags", title: t`Quality flags`, width: 110, filter: false },
		],
		[t]
	)
	const serverFiltering = useMemo(
		() => ({
			options: {
				direction: ["in", "out", "internal", "transit", "ambiguous"].map((value) => ({ value })),
				category: CATEGORY_OPTIONS.map((value) => ({ value })),
			},
			selected: { direction: directions, category: categories },
			onColumnFilterChange: (field: string, values: unknown[]) => {
				const normalized = values.map(String)
				if (field === "direction") setDirections(normalized)
				if (field === "category") setCategories(normalized)
			},
			onClearAll: () => {
				setDirections([])
				setCategories([])
			},
		}),
		[categories, directions]
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
