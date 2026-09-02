import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { NetworkIcon, PlusIcon, RadarIcon, RefreshCwIcon, SearchIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
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
}

type NetworkDeviceSummariesResponse = {
	items?: NetworkDeviceSummary[]
}

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
	const [allRecords, setAllRecords] = useState<DeviceTableRecord[]>([])
	const [search, setSearch] = useState("")
	const [statusFilter, setStatusFilter] = useState("all")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const records = useMemo(() => {
		const q = search.trim().toLowerCase()
		return allRecords.filter((r) => {
			if (statusFilter === "up" && r.status !== "up") return false
			if (statusFilter === "down" && r.status === "up") return false
			if (statusFilter === "pending" && r.status !== "pending") return false
			if (!q) return true
			return r.searchText.includes(q)
		})
	}, [allRecords, search, statusFilter])

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<NetworkDeviceSummariesResponse>("/api/v1/network/devices/summary", {})
			setAllRecords((data.items ?? []).map(toTableRecord))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load network devices`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Network Targets`} / WatchDog`
		refresh()
	}, [refresh, t])

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

	const upCount = allRecords.filter((r) => r.status === "up").length
	const downCount = allRecords.length - upCount

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<NetworkIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Network Targets</Trans>
					</h1>
					<span className="text-sm text-muted-foreground">({allRecords.length})</span>
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
							{t`All`} ({allRecords.length})
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
	const location = firstText(device.SysLocation, device.sys_location)
	const uptimeNs = device.Uptime ?? device.uptime ?? 0
	const lastSuccess = firstText(agent?.LastSuccess, agent?.last_success, agent?.LastSeen, agent?.last_seen)

	const statusDot = status === "up" ? "🟢" : status === "down" ? "🔴" : "🟡"
	const deviceDisplay = targetName !== deviceName ? `${targetName}\n${deviceName}` : targetName

	return {
		id: targetID || id,
		deviceID: id,
		targetID,
		status,
		agentStatus,
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
