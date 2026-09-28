import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { NetworkIcon, PlusIcon, RadarIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import { vendorLogoFor } from "@/lib/vendor-logos"
import type { ColumnDefine } from "@/lib/vtable"
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
	total?: number
}

type DeviceCounts = { total: number; up: number; down: number; pending: number }
type DeviceSummariesResponse = NetworkDeviceSummariesResponse & { counts?: DeviceCounts }

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
	const [records, setRecords] = useState<DeviceTableRecord[]>([])
	const [counts, setCounts] = useState<DeviceCounts>({ total: 0, up: 0, down: 0, pending: 0 })
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [statusFilter, setStatusFilter] = useState("all")
	const [sort, setSort] = useState("name:asc")
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

	// Search, status filter, sort and paging all happen server-side; the table
	// shows exactly the returned page.
	const buildQuery = useCallback(() => {
		const [sortField, sortDirection] = sort.split(":")
		return {
			q: debouncedSearch.trim() || undefined,
			status: statusFilter !== "all" ? statusFilter : undefined,
			sort: sortField,
			order: sortDirection,
			limit: pageSize,
			offset: page * pageSize || undefined,
		}
	}, [debouncedSearch, page, pageSize, sort, statusFilter])

	// Refetch the requested page when the query changes or Refresh is clicked.
	// A sequence guard prevents an older response from replacing newer state.
	useEffect(() => {
		document.title = `${t`Network Devices`} / Watchdog`
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		api
			.send<DeviceSummariesResponse>("/api/v1/devices/summary", { query: buildQuery() })
			.then((data) => {
				if (sequence !== requestSequence.current) return
				setRecords((data.items ?? []).map(toTableRecord))
				if (data.counts) setCounts(data.counts)
				setTotal(data.total ?? 0)
			})
			.catch((err) => {
				if (sequence !== requestSequence.current) return
				setRecords([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load network devices`)
			})
			.finally(() => {
				if (sequence === requestSequence.current) setLoading(false)
			})
	}, [buildQuery, reloadKey, t])

	const refresh = useCallback(() => setReloadKey((key) => key + 1), [])
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{
				field: "vendorLogo",
				title: t`Vendor`,
				width: 80,
				cellType: "image",
				imageAutoSizing: false,
				keepAspectRatio: true,
				style: { margin: 14, textAlign: "center", textBaseline: "middle" },
			},
			{ field: "target", filterField: "status", title: t`Device`, width: 280, style: denseCellStyle() },
			{ field: "metrics", title: t`Ports`, width: 120, style: denseCellStyle() },
			{ field: "os", title: t`OS`, width: 200, style: denseCellStyle() },
			{ field: "uptime", title: t`Uptime`, width: 120, style: denseCellStyle() },
			{ field: "lastSeen", title: t`Last Seen`, width: 130, style: denseCellStyle() },
			{ field: "location", title: t`Location`, width: 150, style: denseCellStyle() },
		],
		[t]
	)
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(
		() => ({
			options: {
				status: [
					{ value: "up", label: t`Up`, count: counts.up },
					{ value: "down", label: t`Down`, count: counts.down },
					{ value: "pending", label: t`Pending`, count: counts.pending },
				],
			},
			selected: { status: statusFilter === "all" ? [] : [statusFilter] },
			selection: { status: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetPage(() => setStatusFilter(values.length > 0 ? String(values[0]) : "all")),
			onClearAll: () => resetPage(() => setStatusFilter("all")),
		}),
		[counts, statusFilter, t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: { vendorLogo: "vendor", target: "name", os: "os" },
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	const { up: upCount, down: downCount } = counts

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
				<Select value={statusFilter} onValueChange={(value) => resetPage(() => setStatusFilter(value))}>
					<SelectTrigger className="w-32">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							{t`All`} ({counts.total})
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

			<div className="overflow-hidden rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={search || statusFilter !== "all" ? t`No devices match the filter.` : t`No network targets found.`}
					searchPlaceholder={t`Search by name, host, vendor...`}
					searchValue={search}
					onSearchChange={setSearch}
					height={620}
					rowHeight={56}
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
						const id = String(record.id ?? "")
						const deviceID = String(record.deviceID ?? "")
						if (!id) return
						navigate(
							deviceID
								? getPagePath($router, "network_device", { id: deviceID })
								: getPagePath($router, "target_detail", { id })
						)
					}}
				/>
			</div>
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
		searchText: `${host} ${targetName} ${deviceName} ${vendor} ${location} ${osName} ${osVer}`.toLowerCase(),
		target: `${statusDot} ${host}\n${deviceDisplay}`,
		host,
		metrics: portCount ? `${upPorts}/${portCount} up\n${downPorts} down` : "-",
		platform: firstText(device.Platform, device.platform, device.Model, device.model) || "-",
		os: osName
			? osVer
				? `${osName} ${osVer}`
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
