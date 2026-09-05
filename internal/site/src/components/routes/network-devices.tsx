import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowDownIcon, ArrowUpIcon, NetworkIcon, PlusIcon, RadarIcon, RefreshCwIcon, SearchIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { vendorLogoFor } from "@/lib/vendor-logos"
import { createListTable, disposeTable, getRowRecord, type ListTable } from "@/lib/vtable"
type NetworkDevice = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	TargetID?: string
	target_id?: string
	Vendor?: string
	vendor?: string
	Model?: string
	model?: string
	Platform?: string
	platform?: string
	OSName?: string
	os_name?: string
	OSVersion?: string
	os_version?: string
	SysName?: string
	sys_name?: string
	SysDescr?: string
	sys_descr?: string
	SysObjectID?: string
	sys_object_id?: string
	SysLocation?: string
	sys_location?: string
	Uptime?: number
	uptime?: number
}

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Host?: string
	host?: string
	Status?: string
	status?: string
	Labels?: Record<string, string>
	labels?: Record<string, string>
}

type NetworkDeviceSummary = {
	Device?: NetworkDevice
	device?: NetworkDevice
	Target?: TargetRecord
	target?: TargetRecord
	Agent?: AgentRecord
	agent?: AgentRecord
	PortCount?: number
	port_count?: number
	UpPorts?: number
	up_ports?: number
	DownPorts?: number
	down_ports?: number
	BGPSessions?: number
	bgp_sessions?: number
	EstablishedBGP?: number
	established_bgp?: number
	LastSeen?: string
	last_seen?: string
}

type NetworkDeviceSummariesResponse = {
	items?: NetworkDeviceSummary[]
}

type DeviceCounts = { total: number; up: number; down: number; pending: number }
type DeviceSummariesResponse = NetworkDeviceSummariesResponse & { counts?: DeviceCounts }

const PAGE_SIZE = 100

type AgentRecord = {
	ID?: string
	id?: string
	Status?: string
	status?: string
	LastSeen?: string
	last_seen?: string
	LastRun?: string
	last_run?: string
	LastSuccess?: string
	last_success?: string
	LastError?: string
	last_error?: string
	RunCount?: number
	run_count?: number
	FailureCount?: number
	failure_count?: number
}

type DeviceTableRecord = {
	id: string
	deviceID: string
	targetID: string
	status: string
	agentStatus: string
	vendor: string
	vendorLogo: string
	searchText: string
	target: string
	host: string
	metrics: string
	platform: string
	os: string
	uptime: string
	location: string
	lastSeen: string
}

export default memo(() => {
	const { t } = useLingui()
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [records, setRecords] = useState<DeviceTableRecord[]>([])
	const [counts, setCounts] = useState<DeviceCounts>({ total: 0, up: 0, down: 0, pending: 0 })
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [statusFilter, setStatusFilter] = useState("all")
	const [sortField, setSortField] = useState("name")
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

	// Search, status filter, sort and paging all happen server-side; the table
	// shows exactly the returned page.
	const buildQuery = useCallback(
		(offset: number) => ({
			q: debouncedSearch.trim() || undefined,
			status: statusFilter !== "all" ? statusFilter : undefined,
			sort: sortField,
			order: sortDesc ? "desc" : "asc",
			limit: PAGE_SIZE,
			offset: offset || undefined,
		}),
		[debouncedSearch, statusFilter, sortField, sortDesc]
	)

	// Refetch from the top when the query changes or Refresh is clicked. The
	// cancelled flag drops a stale response if a newer query started meanwhile.
	useEffect(() => {
		document.title = `${t`Network Devices`} / Watchdog`
		let cancelled = false
		setLoading(true)
		setError("")
		pb.send<DeviceSummariesResponse>("/api/v1/network/devices/summary", { query: buildQuery(0) })
			.then((data) => {
				if (cancelled) return
				const items = (data.items ?? []).map(toTableRecord)
				setRecords(items)
				if (data.counts) setCounts(data.counts)
				setHasMore(items.length === PAGE_SIZE)
			})
			.catch((err) => {
				if (!cancelled) setError(err instanceof Error ? err.message : t`Failed to load network devices`)
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
			const data = await pb.send<DeviceSummariesResponse>("/api/v1/network/devices/summary", {
				query: buildQuery(records.length),
			})
			const items = (data.items ?? []).map(toTableRecord)
			setRecords((current) => [...current, ...items])
			setHasMore(items.length === PAGE_SIZE)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load network devices`)
		} finally {
			setLoadingMore(false)
		}
	}, [buildQuery, hasMore, loadingMore, records.length, t])

	const refresh = useCallback(() => setReloadKey((key) => key + 1), [])

	useEffect(() => {
		if (!tableRef.current || loading || error) {
			return
		}
		disposeTable(tableInstance.current)
		tableInstance.current = createListTable(tableRef.current, {
			records,
			rowHeight: 56,
			headerRowHeight: 38,
			widthMode: "adaptive",
			columns: [
				{
					field: "vendorLogo",
					filterField: "vendor",
					title: t`Vendor`,
					width: 80,
					cellType: "image",
					imageAutoSizing: false,
					keepAspectRatio: true,
					style: { margin: 14, textAlign: "center", textBaseline: "middle" },
				},
				{ field: "target", title: t`Device`, width: 280, style: denseCellStyle() },
				{ field: "metrics", title: t`Ports`, width: 120, style: denseCellStyle() },
				{ field: "os", title: t`OS`, width: 200, style: denseCellStyle() },
				{ field: "uptime", title: t`Uptime`, width: 120, style: denseCellStyle() },
				{ field: "lastSeen", title: t`Last Seen`, width: 130, style: denseCellStyle() },
				{ field: "location", title: t`Location`, width: 150, style: denseCellStyle() },
			],
		})
		tableInstance.current.on("click_cell", (args: { col: number; row: number }) => {
			const record = getRowRecord(tableInstance.current, args) as DeviceTableRecord | null
			if (!record?.id) return
			navigate(
				record.deviceID
					? getPagePath($router, "network_device", { id: record.deviceID })
					: getPagePath($router, "target_detail", { id: record.id })
			)
		})
		return () => disposeTable(tableInstance.current)
	}, [error, loading, records, t])

	const { total, up: upCount, down: downCount } = counts

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<NetworkIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Network Devices</Trans>
					</h1>
					<span className="text-sm text-muted-foreground">({total})</span>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "network_device_new"))}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "network_discover"))}>
						<RadarIcon className="me-2 h-4 w-4" />
						<Trans>Discover</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap items-center gap-2">
				<div className="relative max-w-sm flex-1">
					<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						value={search}
						onChange={(e) => setSearch(e.target.value)}
						placeholder={t`Search by name, host, vendor...`}
						className="pl-9"
					/>
				</div>
				<Select value={statusFilter} onValueChange={setStatusFilter}>
					<SelectTrigger className="w-32">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							{t`All`} ({total})
						</SelectItem>
						<SelectItem value="up">
							{t`Up`} ({upCount})
						</SelectItem>
						<SelectItem value="down">
							{t`Down`} ({downCount})
						</SelectItem>
						<SelectItem value="pending">{t`Pending`}</SelectItem>
					</SelectContent>
				</Select>
				<Select value={sortField} onValueChange={setSortField}>
					<SelectTrigger className="w-32">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="name">{t`Name`}</SelectItem>
						<SelectItem value="host">{t`Host`}</SelectItem>
						<SelectItem value="vendor">{t`Vendor`}</SelectItem>
						<SelectItem value="os">{t`OS`}</SelectItem>
						<SelectItem value="status">{t`Status`}</SelectItem>
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
				<div className="flex items-center gap-1.5">
					<Badge variant={upCount > 0 ? "success" : "outline"}>
						{upCount} <Trans>up</Trans>
					</Badge>
					<Badge variant={downCount > 0 ? "danger" : "outline"}>
						{downCount} <Trans>down</Trans>
					</Badge>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			<div className="overflow-hidden rounded-md bg-card">
				{loading ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>Loading...</Trans>
					</div>
				) : null}
				{!loading && !error && records.length === 0 ? (
					<div className="flex items-center justify-between gap-3 p-3">
						<div className="text-sm text-muted-foreground">
							{search || statusFilter !== "all" ? t`No devices match the filter.` : t`No network targets found.`}
						</div>
						{!search && statusFilter === "all" ? (
							<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "network_discover"))}>
								<RadarIcon className="me-2 h-4 w-4" />
								<Trans>Discover</Trans>
							</Button>
						) : null}
					</div>
				) : null}
				<div ref={tableRef} className="h-[620px] w-full" />
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

function toTableRecord(summary: NetworkDeviceSummary): DeviceTableRecord {
	const device = summary.Device ?? summary.device ?? {}
	const target = summary.Target ?? summary.target
	const agent = summary.Agent ?? summary.agent
	const id = device.ID ?? device.id ?? ""
	const targetID = device.TargetID ?? device.target_id ?? ""
	const vendor = firstText(device.Vendor, device.vendor)
	const host = firstText(target?.Host, target?.host, targetID)
	const targetName = firstText(target?.Name, target?.name, device.SysName, device.sys_name, id)
	const deviceName = firstText(device.SysName, device.sys_name, device.Model, device.model, id)
	const status = firstText(target?.Status, target?.status, "pending").toLowerCase()
	const agentStatus = firstText(agent?.Status, agent?.status, "pending").toLowerCase()
	const portCount = summary.PortCount ?? summary.port_count ?? 0
	const upPorts = summary.UpPorts ?? summary.up_ports ?? 0
	const downPorts = summary.DownPorts ?? summary.down_ports ?? 0
	const osName = firstText(device.OSName, device.os_name)
	const osVer = firstText(device.OSVersion, device.os_version)
	const targetLabels = target?.Labels ?? target?.labels ?? {}
	const location = firstText(
		device.SysLocation,
		device.sys_location,
		targetLabels.location,
		targetLabels.site,
		targetLabels.region
	)
	const uptimeNs = device.Uptime ?? device.uptime ?? 0
	const lastSuccess = firstText(
		summary.LastSeen,
		summary.last_seen,
		agent?.LastSuccess,
		agent?.last_success,
		agent?.LastSeen,
		agent?.last_seen
	)

	const statusDot = status === "up" ? "🟢" : status === "down" ? "🔴" : "🟡"
	const deviceDisplay = targetName !== deviceName ? `${targetName}\n${deviceName}` : targetName

	return {
		id: targetID || id,
		deviceID: id,
		targetID,
		status,
		agentStatus,
		vendor: vendor || "-",
		vendorLogo: vendorLogoFor(vendor),
		searchText: `${host} ${targetName} ${deviceName} ${vendor} ${location} ${osName}`.toLowerCase(),
		target: `${statusDot} ${host}\n${deviceDisplay}`,
		host,
		metrics: portCount ? `${upPorts}/${portCount} up\n${downPorts} down` : "-",
		platform: firstText(device.Platform, device.platform, device.Model, device.model) || "-",
		os: osName
			? osVer
				? `${osName}\n${osVer}`
				: osName
			: (firstText(device.SysDescr, device.sys_descr)?.slice(0, 40) ?? "-"),
		uptime: formatUptime(uptimeNs),
		location: location || "-",
		lastSeen: lastSuccess ? formatRelative(lastSuccess) : "-",
	}
}

function formatUptime(nanoseconds: number): string {
	if (!nanoseconds || nanoseconds <= 0) return "-"
	const seconds = Math.floor(nanoseconds / 1_000_000_000)
	const days = Math.floor(seconds / 86400)
	const hours = Math.floor((seconds % 86400) / 3600)
	if (days > 0) return `${days}d ${hours}h`
	const mins = Math.floor((seconds % 3600) / 60)
	if (hours > 0) return `${hours}h ${mins}m`
	return `${mins}m`
}

function formatRelative(isoTime: string): string {
	const date = new Date(isoTime)
	if (Number.isNaN(date.getTime())) return "-"
	const diff = Date.now() - date.getTime()
	if (diff < 60_000) return "<1m"
	if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
	if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
	return `${Math.floor(diff / 86_400_000)}d ago`
}

function denseCellStyle() {
	return {
		textAlign: "left" as const,
		textBaseline: "middle" as const,
		fontSize: 13,
		lineHeight: 20,
		autoWrapText: true,
		lineClamp: 3,
		padding: [5, 8, 5, 8],
	}
}

function firstText(...values: (string | undefined)[]) {
	return values.find((value) => value?.trim())?.trim() ?? ""
}
