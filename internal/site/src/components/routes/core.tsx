import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowDownIcon, ArrowUpIcon, RefreshCwIcon, RouteIcon, SearchIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { createListTable, disposeTable, getRowRecord, type ListTable } from "@/lib/vtable"

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
type BGPSessionsResponse = { items?: BGPSessionRecord[]; counts?: BGPCounts }

const PAGE_SIZE = 100

export default memo(() => {
	const { t } = useLingui()
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [sessions, setSessions] = useState<BGPSessionRecord[]>([])
	const [counts, setCounts] = useState<BGPCounts>({ total: 0, established: 0 })
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [stateFilter, setStateFilter] = useState("all")
	const [sortField, setSortField] = useState("device")
	const [sortDesc, setSortDesc] = useState(false)
	const [reloadKey, setReloadKey] = useState(0)
	const [loading, setLoading] = useState(true)
	const [loadingMore, setLoadingMore] = useState(false)
	const [hasMore, setHasMore] = useState(false)
	const [error, setError] = useState("")

	// Debounce the search box so typing does not fire a request per keystroke.
	useEffect(() => {
		const handle = setTimeout(() => setDebouncedSearch(search), 300)
		return () => clearTimeout(handle)
	}, [search])

	// Search, state filter, sort and paging all happen server-side.
	const buildQuery = useCallback(
		(offset: number) => ({
			q: debouncedSearch.trim() || undefined,
			state: stateFilter !== "all" ? stateFilter : undefined,
			sort: sortField,
			order: sortDesc ? "desc" : "asc",
			limit: PAGE_SIZE,
			offset: offset || undefined,
		}),
		[debouncedSearch, stateFilter, sortField, sortDesc]
	)

	// Refetch from the top when the query changes or Refresh is clicked; a stale
	// response is dropped if a newer query started meanwhile.
	useEffect(() => {
		document.title = `${t`Core (BGP)`} / Watchdog`
		let cancelled = false
		setLoading(true)
		setError("")
		pb.send<BGPSessionsResponse>("/api/v1/network/bgp", { query: buildQuery(0) })
			.then((data) => {
				if (cancelled) return
				setSessions(data.items ?? [])
				if (data.counts) setCounts(data.counts)
				setHasMore((data.items ?? []).length === PAGE_SIZE)
			})
			.catch((err) => {
				if (!cancelled) setError(err instanceof Error ? err.message : t`Failed to load BGP sessions`)
			})
			.finally(() => {
				if (!cancelled) setLoading(false)
			})
		return () => {
			cancelled = true
		}
	}, [buildQuery, reloadKey, t])

	const loadMore = useCallback(async () => {
		if (loadingMore || !hasMore) return
		setLoadingMore(true)
		try {
			const data = await pb.send<BGPSessionsResponse>("/api/v1/network/bgp", { query: buildQuery(sessions.length) })
			const items = data.items ?? []
			setSessions((current) => [...current, ...items])
			setHasMore(items.length === PAGE_SIZE)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load BGP sessions`)
		} finally {
			setLoadingMore(false)
		}
	}, [buildQuery, hasMore, loadingMore, sessions.length, t])

	const refresh = useCallback(() => setReloadKey((key) => key + 1), [])

	useEffect(() => {
		if (!tableRef.current || loading || error) {
			return
		}
		const records = sessions.map((session) => ({
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
		}))
		disposeTable(tableInstance.current)
		tableInstance.current = createListTable(tableRef.current, {
			records,
			columns: [
				{ field: "device", title: t`Device`, width: 220 },
				{ field: "peer", title: t`Peer`, width: 200 },
				{ field: "peerAs", title: t`Peer AS`, width: 110 },
				{ field: "family", title: t`Address Family`, width: 140 },
				{ field: "state", title: t`State`, width: 130 },
				{ field: "accepted", title: t`Accepted`, width: 110 },
				{ field: "advertised", title: t`Advertised`, width: 110 },
				{ field: "uptime", title: t`Uptime`, width: 140 },
			],
		})
		tableInstance.current.on("click_cell", (args: { col: number; row: number }) => {
			const record = getRowRecord(tableInstance.current, args)
			if (record?.deviceId) {
				navigate(getPagePath($router, "network_device", { id: record.deviceId }))
			}
		})
		return () => disposeTable(tableInstance.current)
	}, [error, loading, t, sessions])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<RouteIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Core (BGP)</Trans>
					</h1>
					<span className="text-sm text-muted-foreground">({counts.total})</span>
				</div>
				<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="flex flex-wrap items-center gap-2">
				<div className="relative max-w-sm flex-1">
					<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						value={search}
						onChange={(e) => setSearch(e.target.value)}
						placeholder={t`Search by device, peer, AS...`}
						className="pl-9"
					/>
				</div>
				<Select value={stateFilter} onValueChange={setStateFilter}>
					<SelectTrigger className="w-36">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">{t`All states`}</SelectItem>
						<SelectItem value="established">{t`Established`}</SelectItem>
						<SelectItem value="idle">{t`Idle`}</SelectItem>
						<SelectItem value="active">{t`Active`}</SelectItem>
						<SelectItem value="connect">{t`Connect`}</SelectItem>
					</SelectContent>
				</Select>
				<Select value={sortField} onValueChange={setSortField}>
					<SelectTrigger className="w-32">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="device">{t`Device`}</SelectItem>
						<SelectItem value="peer">{t`Peer`}</SelectItem>
						<SelectItem value="peer_as">{t`Peer AS`}</SelectItem>
						<SelectItem value="state">{t`State`}</SelectItem>
					</SelectContent>
				</Select>
				<Button
					variant="outline"
					size="icon"
					className="size-9 shrink-0"
					onClick={() => setSortDesc((desc) => !desc)}
					title={sortDesc ? t`Descending` : t`Ascending`}
				>
					{sortDesc ? <ArrowDownIcon className="h-4 w-4" /> : <ArrowUpIcon className="h-4 w-4" />}
				</Button>
				<Badge variant={counts.established > 0 ? "success" : "outline"}>
					{counts.established} <Trans>established</Trans>
				</Badge>
			</div>

			<div className="rounded-md border border-border bg-card">
				{loading ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>Loading...</Trans>
					</div>
				) : null}
				{error ? <div className="p-3 text-sm text-destructive">{error}</div> : null}
				{!loading && !error && sessions.length === 0 ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>No BGP sessions found. Sessions appear after SNMP discovery on network devices.</Trans>
					</div>
				) : null}
				<div ref={tableRef} className="h-[520px] w-full" />
			</div>

			{hasMore ? (
				<div className="flex justify-center">
					<Button variant="outline" size="sm" onClick={loadMore} disabled={loadingMore}>
						{loadingMore ? <Trans>Loading...</Trans> : <Trans>Load more</Trans>}
					</Button>
				</div>
			) : null}
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
