import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { RefreshCwIcon, RouteIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import type { ColumnDefine } from "@/lib/vtable"

type BGPSessionRecord = {
	ID?: string
	DeviceID?: string
	DeviceSysName?: string
	PeerAddr?: string
	PeerAS?: number
	LocalAS?: number
	AFI?: string
	SAFI?: string
	State?: string
	AcceptedPrefixes?: number
	AdvertisedPrefixes?: number
	// Go time.Duration marshals as nanoseconds
	Uptime?: number
}

type BGPCounts = { total: number; established: number }
type BGPSessionsResponse = { items?: BGPSessionRecord[]; counts?: BGPCounts; total?: number }
const BGP_STATES = ["established", "idle", "active", "connect", "opensent", "openconfirm"]

export default memo(() => {
	const { t } = useLingui()
	const [sessions, setSessions] = useState<BGPSessionRecord[]>([])
	const [counts, setCounts] = useState<BGPCounts>({ total: 0, established: 0 })
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [stateFilter, setStateFilter] = useState("all")
	const [sort, setSort] = useState("device:asc")
	const [reloadKey, setReloadKey] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	// Debounce the search box so typing does not fire a request per keystroke.
	useEffect(() => {
		const handle = setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => clearTimeout(handle)
	}, [search])

	// Search, state filter, sort and paging all happen server-side.
	const buildQuery = useCallback(() => {
		const [sortField, order] = sort.split(":")
		return {
			q: debouncedSearch.trim() || undefined,
			state: stateFilter !== "all" ? stateFilter : undefined,
			sort: sortField,
			order,
			limit: pageSize,
			offset: page * pageSize || undefined,
		}
	}, [debouncedSearch, page, pageSize, sort, stateFilter])

	// Refetch from the top when the query changes or Refresh is clicked; a stale
	// response is dropped if a newer query started meanwhile.
	useEffect(() => {
		document.title = `${t`Core (BGP)`} / Watchdog`
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		api
			.send<BGPSessionsResponse>("/api/v1/bgp", { query: buildQuery() })
			.then((data) => {
				if (sequence !== requestSequence.current) return
				setSessions(data.items ?? [])
				if (data.counts) setCounts(data.counts)
				setTotal(data.total ?? 0)
			})
			.catch((err) => {
				if (sequence !== requestSequence.current) return
				setSessions([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load BGP sessions`)
			})
			.finally(() => {
				if (sequence === requestSequence.current) setLoading(false)
			})
	}, [buildQuery, reloadKey, t])

	const refresh = useCallback(() => setReloadKey((key) => key + 1), [])

	const records = useMemo(
		() =>
			sessions.map((session) => ({
				id: session.ID ?? "",
				deviceId: session.DeviceID ?? "",
				device: session.DeviceSysName || session.DeviceID || "—",
				peer: session.PeerAddr || "—",
				peerAs: session.PeerAS ? `AS${session.PeerAS}` : "—",
				family: [session.AFI, session.SAFI].filter(Boolean).join("/") || "—",
				state: session.State || "—",
				accepted: session.AcceptedPrefixes ?? 0,
				advertised: session.AdvertisedPrefixes ?? 0,
				uptime: formatUptime(session.Uptime),
			})),
		[sessions]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "device", title: t`Device`, width: 220 },
			{ field: "peer", title: t`Peer`, width: 200 },
			{ field: "peerAs", title: t`Peer AS`, width: 110 },
			{ field: "family", title: t`Address Family`, width: 140 },
			{ field: "state", title: t`State`, width: 130 },
			{ field: "accepted", title: t`Accepted`, width: 110 },
			{ field: "advertised", title: t`Advertised`, width: 110 },
			{ field: "uptime", title: t`Uptime`, width: 140 },
		],
		[t]
	)
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(
		() => ({
			options: { state: BGP_STATES.map((value) => ({ value })) },
			selected: { state: stateFilter === "all" ? [] : [stateFilter] },
			selection: { state: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetPage(() => setStateFilter(values.length > 0 ? String(values[0]) : "all")),
			onClearAll: () => resetPage(() => setStateFilter("all")),
		}),
		[stateFilter]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: { device: "device", peer: "peer", peerAs: "peer_as", state: "state" },
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<RouteIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Core (BGP)</Trans>
					</h1>
					<span className="text-sm text-muted-foreground">({total})</span>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="flex flex-wrap items-center gap-2">
				<Select value={stateFilter} onValueChange={(value) => resetPage(() => setStateFilter(value))}>
					<SelectTrigger className="w-36">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">{t`All states`}</SelectItem>
						<SelectItem value="established">{t`Established`}</SelectItem>
						<SelectItem value="idle">{t`Idle`}</SelectItem>
						<SelectItem value="active">{t`Active`}</SelectItem>
						<SelectItem value="connect">{t`Connect`}</SelectItem>
						<SelectItem value="opensent">OpenSent</SelectItem>
						<SelectItem value="openconfirm">OpenConfirm</SelectItem>
					</SelectContent>
				</Select>
				<Badge variant={counts.established > 0 ? "success" : "outline"}>
					{counts.established} <Trans>established</Trans>
				</Badge>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No BGP sessions found. Sessions appear after SNMP discovery on network devices.`}
					searchPlaceholder={t`Search by device, peer, AS...`}
					searchValue={search}
					onSearchChange={setSearch}
					height={520}
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
					onRowClick={(record) => {
						const deviceID = String(record.deviceId ?? "")
						if (deviceID) navigate(getPagePath($router, "network_device", { id: deviceID }))
					}}
				/>
			</div>
		</div>
	)
})

function formatUptime(nanoseconds?: number) {
	if (!nanoseconds || nanoseconds <= 0) {
		return "—"
	}
	let seconds = Math.floor(nanoseconds / 1e9)
	const days = Math.floor(seconds / 86400)
	seconds -= days * 86400
	const hours = Math.floor(seconds / 3600)
	seconds -= hours * 3600
	const minutes = Math.floor(seconds / 60)
	if (days > 0) {
		return `${days}d ${hours}h`
	}
	if (hours > 0) {
		return `${hours}h ${minutes}m`
	}
	return `${minutes}m`
}
