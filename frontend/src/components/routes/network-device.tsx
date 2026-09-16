import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import {
	ArrowLeftIcon,
	BarChart3Icon,
	NetworkIcon,
	PencilIcon,
	RadarIcon,
	RefreshCwIcon,
	SlidersHorizontalIcon,
	Trash2Icon,
} from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import {
	AlertDialog,
	AlertDialogAction,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { isAdmin, api } from "@/lib/api"
import { formatBitsPerSecond } from "@/lib/metric-format"
import {
	trafficViewLabel,
	trafficViewQueryStep,
	trafficViewRateBase,
	trafficViewValueMode,
	type TrafficViewMode,
} from "@/lib/traffic-view"
import { cn } from "@/lib/utils"
import { vendorLogoFor } from "@/lib/vendor-logos"
import { GraphContextProvider, GraphPanelRenderer, type GraphDashboard } from "@/components/graph/graph-panel-renderer"
import { TrafficViewSwitcher } from "@/components/traffic-view-switcher"

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
	SNMPProfileID?: string
	snmp_profile_id?: string
	SNMPPort?: number
	snmp_port?: number
}

type NetworkPort = {
	ID?: string
	id?: string
	IfIndex?: number
	if_index?: number
	IfName?: string
	if_name?: string
	IfAlias?: string
	if_alias?: string
	IfDescr?: string
	if_descr?: string
	AdminStatus?: string
	admin_status?: string
	OperStatus?: string
	oper_status?: string
	SpeedBps?: number
	speed_bps?: number
	SideType?: string
	side_type?: string
	Addresses?: NetworkInterfaceAddress[]
	addresses?: NetworkInterfaceAddress[]
}

type NetworkInterfaceAddress = {
	Address?: string
	address?: string
	Family?: string
	family?: string
	PrefixLength?: number
	prefix_length?: number
	Origin?: string
	origin?: string
}

type NetworkPortsResponse = {
	items?: NetworkPort[]
	total?: number
	counts?: PortCounts
}

type PortCounts = { total: number; up: number; down: number }

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

type TargetsResponse = {
	items?: TargetRecord[]
}

type BGPSession = {
	ID?: string
	id?: string
	PeerAddr?: string
	peer_addr?: string
	PeerAS?: number
	peer_as?: number
	LocalAS?: number
	local_as?: number
	AFI?: string
	afi?: string
	SAFI?: string
	safi?: string
	State?: string
	state?: string
	AcceptedPrefixes?: number
	accepted_prefixes?: number
	DeniedPrefixes?: number
	denied_prefixes?: number
	AdvertisedPrefixes?: number
	advertised_prefixes?: number
}

type BGPSessionsResponse = {
	items?: BGPSession[]
	total?: number
	counts?: BGPCounts
}

type BGPCounts = { total: number; established: number }

type NetworkDeviceSensor = {
	ID?: string
	id?: string
	PortID?: string
	port_id?: string
	SensorIndex?: number
	sensor_index?: number
	Class?: string
	class?: string
	Name?: string
	name?: string
	OID?: string
	oid?: string
	Unit?: string
	unit?: string
	Value?: number
	value?: number
	Status?: string
	status?: string
}

type NetworkDeviceSensorsResponse = {
	items?: NetworkDeviceSensor[]
	total?: number
	counts?: SensorCounts
}

type SensorCounts = { total: number; problems: number }

type VMRangeResponse = {
	data?: {
		result?: {
			metric?: Record<string, string>
			values?: [number, string][]
		}[]
	}
}

type DeviceDetailProps = {
	id: string
}


type DeviceDeleteImpact = {
	resource_type: string
	behavior: string
	count: number
	detail?: string
	items?: { id: string; name: string }[]
}

function deviceImpactLabel(resourceType: string) {
	switch (resourceType) {
		case "network_port":
			return "Ports"
		case "network_device_sensor":
			return "Sensors"
		case "bgp_session":
			return "BGP sessions"
		case "device_vlan":
			return "VLANs"
		case "device_lag_group":
			return "LAG groups"
		case "device_physical_entity":
			return "Physical entities"
		case "network_interface_address":
			return "Interface addresses"
		case "snmp_collection_recipe":
			return "SNMP recipes"
		case "aggregate_graph":
			return "Aggregate graphs"
		case "metric_series":
			return "Metric series"
		default:
			return resourceType
	}
}

export default memo(({ id }: DeviceDetailProps) => {
	const { t } = useLingui()
	const [device, setDevice] = useState<NetworkDevice | null>(null)
	const [target, setTarget] = useState<TargetRecord | null>(null)
	const [ports, setPorts] = useState<NetworkPort[]>([])
	const [bgpCounts, setBGPCounts] = useState<BGPCounts>({ total: 0, established: 0 })
	const [sensorProblems, setSensorProblems] = useState<NetworkDeviceSensor[]>([])
	const [sensorCounts, setSensorCounts] = useState<SensorCounts>({ total: 0, problems: 0 })
	const [chartWindow, setChartWindow] = useState("24h")
	const [portTraffic, setPortTraffic] = useState<Record<string, { in?: number; out?: number }>>({})
	const [dashboard, setDashboard] = useState<GraphDashboard | null>(null)
	const [activeTab, setActiveTab] = useState("overview")
	const [trafficView, setTrafficView] = useState<TrafficViewMode>("customer")
	const [loading, setLoading] = useState(true)
	const [rediscovering, setRediscovering] = useState(false)
	const [error, setError] = useState("")
	const [deleteOpen, setDeleteOpen] = useState(false)
	const [deleteImpacts, setDeleteImpacts] = useState<DeviceDeleteImpact[]>([])

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [deviceData, portsData, bgpData, sensorData, targetsData, dashboardData] = await Promise.all([
				api.send<NetworkDevice>(`/api/v1/devices/${id}`, {}),
				api.send<NetworkPortsResponse>(`/api/v1/devices/${id}/ports`, {}),
				api.send<BGPSessionsResponse>(`/api/v1/devices/${id}/bgp?limit=1`, {}),
				api.send<NetworkDeviceSensorsResponse>(`/api/v1/devices/${id}/sensors?health=problem&limit=12`, {}).catch(
					() => ({ items: [], total: 0, counts: { total: 0, problems: 0 } })
				),
				api.send<TargetsResponse>("/api/v1/devices", {}),
				api.send<GraphDashboard>(`/api/v1/graph/devices/${id}/overview`, {}).catch(() => null),
			])
			setDevice(deviceData)
			const targetID = deviceData.TargetID ?? deviceData.target_id ?? ""
			setTarget((targetsData.items ?? []).find((item) => (item.ID ?? item.id) === targetID) ?? null)
			setPorts(portsData.items ?? [])
			setBGPCounts(bgpData.counts ?? { total: bgpData.total ?? 0, established: 0 })
			setSensorProblems(sensorData.items ?? [])
			setSensorCounts(sensorData.counts ?? { total: sensorData.total ?? 0, problems: sensorData.total ?? 0 })
			setDashboard(dashboardData)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load network device`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Network Device`} / Watchdog`
		refresh()
	}, [refresh, t])

	const targetID = device?.TargetID ?? device?.target_id ?? ""
	const deviceID = device?.ID ?? device?.id ?? id
	const portIDs = ports.map((port) => port.ID ?? port.id ?? "").filter(Boolean)
	const portIDsKey = portIDs.join(",")
	const supplierPortCount = ports.filter((port) => (port.SideType ?? port.side_type) === "provider").length
	const trafficViewDisplayLabel =
		trafficView === "customer" ? t`Customer` : trafficView === "supplier" ? t`Supplier` : t`Raw`
	const graphContext = useMemo(
		() => ({
			deviceId: deviceID,
			targetId: targetID,
			portIds: portIDs,
			trafficView,
			valueMode: trafficViewValueMode(trafficView, isAdmin()),
		}),
		// portIDs is derived on every render; the joined value is the stable
		// dependency for this immutable graph query context.
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[deviceID, targetID, portIDsKey, trafficView]
	)

	const refreshPortTraffic = useCallback(async () => {
		if (!targetID || !deviceID || portIDs.length === 0) {
			return
		}
		try {
			// The table must show the same numbers as the charts: follow the
			// active traffic view instead of always fetching raw values.
			const portParams = new URLSearchParams({
				target_id: targetID,
				device_id: deviceID,
				time_mode: "fixed",
				window: "10m",
				max_data_points: "120",
				step: trafficViewQueryStep(trafficView),
				value_mode: trafficViewValueMode(trafficView, isAdmin()),
				traffic_view: trafficView,
				// One series per interface, each corrected with its own policy.
				per_port: "1",
			})
			const [inPorts, outPorts] = await Promise.all([
				queryMetric(portParams, "watchdog_snmp_if_in_bps"),
				queryMetric(portParams, "watchdog_snmp_if_out_bps"),
			])
			setPortTraffic(mergePortTraffic(latestPortValues(inPorts), latestPortValues(outPorts)))
		} catch {
			// port traffic is best-effort; PortStatusOverview tolerates stale data
		}
	}, [targetID, deviceID, portIDsKey, trafficView])

	useEffect(() => {
		refreshPortTraffic()
	}, [refreshPortTraffic])

	useEffect(() => {
		const timer = globalThis.setInterval(refreshPortTraffic, 30_000)
		return () => globalThis.clearInterval(timer)
	}, [refreshPortTraffic])

	const saveAsGraph = async () => {
		const effectivePortIDs = portIDs
		if (effectivePortIDs.length === 0) {
			setError(t`Select at least one port`)
			return
		}
		const name = globalThis.prompt(t`Graph name`, `${title} traffic`)
		if (!name) {
			return
		}
		setError("")
		try {
			const graphID = createAggregateGraphID()
			await api.send(`/api/v1/aggregate-graphs`, {
				method: "POST",
				body: {
					id: graphID,
					Name: name.trim(),
					Aggregation: "sum",
					ValueMode: trafficViewValueMode(trafficView),
					Unit: "bps",
					Description: `Created from ${trafficViewLabel(trafficView)} view for device ${title} (${effectivePortIDs.length} ports)`,
				},
			})
			await api.send(`/api/v1/aggregate-graphs/${graphID}/items`, {
				method: "PUT",
				body: {
					items: [
						{ metric: "watchdog_snmp_if_in_bps", direction: "in", label: "In", total: true },
						{ metric: "watchdog_snmp_if_out_bps", direction: "out", label: "Out", total: true },
					],
				},
			})
			await api.send(`/api/v1/aggregate-graphs/${graphID}/ports`, {
				method: "PUT",
				body: { ports: effectivePortIDs.map((portID) => ({ PortID: portID })) },
			})
			navigate(getPagePath($router, "aggregate_graph", { id: graphID }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save aggregate graph`)
		}
	}

	const openDeletePreview = async () => {
		setError("")
		try {
			const preview = await api.send<{ impacts?: DeviceDeleteImpact[] }>(`/api/v1/devices/${id}/delete-preview`, {})
			setDeleteImpacts(preview.impacts ?? [])
		} catch {
			setDeleteImpacts([])
		}
		setDeleteOpen(true)
	}

	const deleteDevice = async () => {
		setDeleteOpen(false)
		setLoading(true)
		setError("")
		try {
			const response = await api.send<{ job_id?: string } | null>(`/api/v1/devices/${id}`, { method: "DELETE" })
			const jobID = response?.job_id
			if (jobID) {
				for (let attempt = 0; attempt < 120; attempt++) {
					const job = await api.send<{ status?: string; last_error_detail?: string }>(`/api/v1/operation-jobs/${jobID}`, {})
					if (job.status === "succeeded") {
						break
					}
					if (job.status === "failed" || job.status === "canceled") {
						throw new Error(job.last_error_detail || t`Failed to delete network device`)
					}
					await new Promise((resolve) => globalThis.setTimeout(resolve, 1000))
				}
			}
			navigate(getPagePath($router, "network"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete network device`)
			setLoading(false)
		}
	}

	const rediscover = async () => {
		setRediscovering(true)
		setError("")
		try {
			await api.send(`/api/v1/devices/${id}/snmp/discover`, { method: "POST" })
			await refresh()
			await refreshPortTraffic()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to rediscover network device`)
		} finally {
			setRediscovering(false)
		}
	}

	const targetName = target?.Name ?? target?.name ?? targetID
	const targetHost = target?.Host ?? target?.host ?? ""
	const targetStatus = target?.Status ?? target?.status ?? ""
	const targetLabelValues = target?.Labels ?? target?.labels ?? {}
	const resolvedLocation = firstText(
		device?.SysLocation,
		device?.sys_location,
		targetLabelValues.location,
		targetLabelValues.site,
		targetLabelValues.region
	)
	const title = firstValue(device?.SysName, device?.sys_name, device?.Name, device?.name, targetName, id)

	return (
		<div className="grid gap-4">
			{/* LibreNMS-style device header: status-colored left border, vendor logo,
			   hostname, OS, uptime, status badges, action buttons */}
			<div
				className={cn(
					"overflow-hidden rounded-md border border-border",
					targetStatus === "up" && "border-l-4 border-l-green-500",
					targetStatus === "down" && "border-l-4 border-l-red-500",
					targetStatus === "pending" && "border-l-4 border-l-yellow-500"
				)}
			>
				<div className="flex items-start justify-between gap-4 p-4">
					<div className="flex min-w-0 items-start gap-3">
						<Link
							href={getPagePath($router, "network")}
							className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
							aria-label={t`Back to network devices`}
						>
							<ArrowLeftIcon className="h-4 w-4" />
						</Link>
						{(() => {
							const logo = vendorLogoFor(firstText(device?.Vendor, device?.vendor))
							return logo ? (
								<img src={logo} alt="" className="h-12 w-16 shrink-0 object-contain" />
							) : (
								<NetworkIcon className="h-8 w-8 shrink-0 text-muted-foreground" strokeWidth={1.5} />
							)
						})()}
						<div className="min-w-0">
							<div className="flex items-center gap-2">
								<h1 className="truncate text-lg font-semibold">{title}</h1>
								<Badge variant={targetStatus === "up" ? "success" : targetStatus === "down" ? "danger" : "outline"}>
									{targetStatus || "pending"}
								</Badge>
							</div>
							<div className="mt-0.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs text-muted-foreground">
								{targetHost ? <span className="font-mono">{targetHost}</span> : null}
								{firstText(device?.OSName, device?.os_name) ? (
									<span>
										{firstText(device?.OSName, device?.os_name)}
										{firstText(device?.OSVersion, device?.os_version)
											? ` ${firstText(device?.OSVersion, device?.os_version)}`
											: ""}
									</span>
								) : null}
								{(device?.Uptime ?? device?.uptime) ? (
									<span>↑ {formatDeviceUptime(device?.Uptime ?? device?.uptime)}</span>
								) : null}
								{resolvedLocation ? <span>{resolvedLocation}</span> : null}
							</div>
						</div>
					</div>
					<div className="flex shrink-0 items-center gap-1.5">
						<Link
							href={getPagePath($router, "network_device_edit", { id })}
							className={cn(buttonVariants({ variant: "ghost", size: "sm" }))}
						>
							<PencilIcon className="h-4 w-4" />
						</Link>
						<Link
							href={getPagePath($router, "network_device_snmp", { id })}
							className={cn(buttonVariants({ variant: "ghost", size: "sm" }))}
						>
							<SlidersHorizontalIcon className="h-4 w-4" />
						</Link>
						<Button variant="ghost" size="sm" onClick={rediscover} disabled={loading || rediscovering}>
							<RadarIcon className="h-4 w-4" />
						</Button>
						<Button variant="ghost" size="sm" onClick={saveAsGraph} disabled={loading || portIDs.length === 0}>
							<BarChart3Icon className="h-4 w-4" />
						</Button>
						<Button variant="ghost" size="sm" onClick={openDeletePreview} disabled={loading}>
							<Trash2Icon className="h-4 w-4 text-destructive" />
						</Button>
						<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
							<RefreshCwIcon className="h-4 w-4" />
						</Button>
					</div>
				</div>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			<div className="flex flex-wrap items-center justify-between gap-3">
				<TrafficViewSwitcher value={trafficView} onChange={setTrafficView} allowRaw={isAdmin()} />
				<div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
					<span>{trafficViewDisplayLabel}</span>
					<span>{trafficViewValueMode(trafficView, isAdmin())}</span>
				</div>
			</div>

			<GraphContextProvider value={graphContext}>
				<Tabs value={activeTab} onValueChange={setActiveTab} className="grid gap-3">
					<TabsList className="w-full justify-start overflow-x-auto">
						<TabsTrigger value="overview">
							<Trans>Overview</Trans>
						</TabsTrigger>
						<TabsTrigger value="graphs">
							<Trans>Traffic</Trans>
						</TabsTrigger>
						<TabsTrigger value="ports">
							<Trans>Ports</Trans>
						</TabsTrigger>
						<TabsTrigger value="health">
							<Trans>Health</Trans>
						</TabsTrigger>
						<TabsTrigger value="vlans">
							<Trans>Switching</Trans>
						</TabsTrigger>
						<TabsTrigger value="bgp">BGP</TabsTrigger>
						<TabsTrigger value="inventory">
							<Trans>Inventory</Trans>
						</TabsTrigger>
						<TabsTrigger value="logs">
							<Trans>Events</Trans>
						</TabsTrigger>
						<TabsTrigger value="alerts">
							<Trans>Alerts</Trans>
						</TabsTrigger>
					</TabsList>

					<TabsContent value="overview" className="grid gap-3">
						<GraphRangeSelect value={chartWindow} onChange={setChartWindow} />
						{dashboard
							? dashboard.panels
									.filter((panel) => panel.id === "overall-traffic")
									.map((panel) => (
										<GraphPanelRenderer
											key={panel.id}
											panel={panel}
											range={chartWindow}
											refreshInterval={dashboard.refresh}
											knownEmpty={trafficView === "supplier" && supplierPortCount === 0}
											emptyMessage={
												trafficView === "supplier" && supplierPortCount === 0 ? (
													<Trans>No supplier ports are assigned to this device.</Trans>
												) : undefined
											}
										/>
									))
							: null}
						<PortStatusOverview
							ports={ports}
							selectedPortIDs={portIDs}
							portTraffic={portTraffic}
							trafficView={trafficView}
						/>
						<DeviceStatusSummary ports={ports} sensorCount={sensorCounts.total} bgpCounts={bgpCounts} />
						<DeviceSavedGraphs deviceId={id} />
					</TabsContent>

					<TabsContent value="graphs" className="grid gap-3 xl:grid-cols-2">
						{dashboard
							? dashboard.panels.map((panel) => (
									<GraphPanelRenderer
										key={panel.id}
										panel={panel}
										range={chartWindow}
										refreshInterval={dashboard.refresh}
									/>
								))
							: null}
					</TabsContent>

					<TabsContent value="health" className="grid gap-3">
						<DeviceHealthPanel
							ports={ports}
							sensorProblems={sensorProblems}
							sensorProblemCount={sensorCounts.problems}
							bgpCounts={bgpCounts}
						/>
						<DeviceSensorsTable deviceId={id} />
					</TabsContent>

					<TabsContent value="ports">
						<DevicePortsTable
							deviceId={id}
							portTraffic={portTraffic}
							trafficView={trafficView}
						/>
					</TabsContent>

					<TabsContent value="vlans" className="grid gap-3">
						<DeviceVLANsLAG deviceId={id} />
					</TabsContent>

					<TabsContent value="bgp" className="grid gap-3">
						<BGPSessionsTable deviceId={id} />
					</TabsContent>

					<TabsContent value="inventory" className="grid gap-3">
						<DeviceOverview
							device={device}
							targetID={targetID}
							targetName={targetName}
							targetHost={targetHost}
							targetStatus={targetStatus}
							targetLabels={formatTargetLabels(target)}
							resolvedLocation={resolvedLocation}
						/>
						<DeviceInventory deviceId={id} />
					</TabsContent>

					<TabsContent value="logs" className="grid gap-3">
						<DeviceEventLog deviceId={id} targetID={targetID} />
					</TabsContent>

					<TabsContent value="alerts" className="grid gap-3">
						<DeviceAlertLog deviceId={id} targetID={targetID} />
					</TabsContent>
				</Tabs>
			</GraphContextProvider>

			<AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
				<AlertDialogContent>
					<AlertDialogHeader>
						<AlertDialogTitle>
							<Trans>Delete this network device?</Trans>
						</AlertDialogTitle>
						<AlertDialogDescription>
							<Trans>This shows everything the deletion touches. Removed data cannot be recovered.</Trans>
						</AlertDialogDescription>
					</AlertDialogHeader>
					<div className="max-h-64 space-y-2 overflow-y-auto text-sm">
						{deleteImpacts.length === 0 ? (
							<div className="text-muted-foreground">
								<Trans>No dependent resources found.</Trans>
							</div>
						) : (
							deleteImpacts.map((impact) => (
								<div
									key={impact.resource_type}
									className="flex items-start justify-between gap-3 rounded-md border border-border p-2"
								>
									<div className="min-w-0">
										<div className="font-medium">{deviceImpactLabel(impact.resource_type)}</div>
										{impact.detail ? <div className="text-xs text-muted-foreground">{impact.detail}</div> : null}
									</div>
									<div className="flex shrink-0 items-center gap-2">
										{impact.count > 0 ? <span className="text-xs text-muted-foreground">×{impact.count}</span> : null}
										<Badge variant={impact.behavior === "deleted" ? "destructive" : "outline"} className="font-normal">
											{impact.behavior === "deleted" ? <Trans>deleted</Trans> : <Trans>detached</Trans>}
										</Badge>
									</div>
								</div>
							))
						)}
					</div>
					<AlertDialogFooter>
						<AlertDialogCancel>
							<Trans>Cancel</Trans>
						</AlertDialogCancel>
						<AlertDialogAction onClick={deleteDevice}>
							<Trans>Delete</Trans>
						</AlertDialogAction>
					</AlertDialogFooter>
				</AlertDialogContent>
			</AlertDialog>
		</div>
	)
})

function DeviceStatusSummary({
	ports,
	sensorCount,
	bgpCounts,
}: {
	ports: NetworkPort[]
	sensorCount: number
	bgpCounts: BGPCounts
}) {
	const upPorts = ports.filter((port) => {
		const s = (port.OperStatus ?? port.oper_status ?? "").toString().toLowerCase()
		return s === "up" || s === "1"
	}).length
	const downPorts = ports.filter((port) => {
		const s = (port.OperStatus ?? port.oper_status ?? "").toString().toLowerCase()
		return s === "down" || s === "2"
	}).length
	return (
		<div className="grid gap-3 md:grid-cols-4">
			<SummaryTile
				label={<Trans>Ports</Trans>}
				value={`${upPorts}/${ports.length}`}
				detail={<Trans>up / total</Trans>}
			/>
			<SummaryTile label={<Trans>Down Ports</Trans>} value={String(downPorts)} detail={<Trans>oper down</Trans>} />
			<SummaryTile label={<Trans>Sensors</Trans>} value={String(sensorCount)} detail={<Trans>discovered</Trans>} />
			<SummaryTile
				label={<Trans>BGP</Trans>}
				value={`${bgpCounts.established}/${bgpCounts.total}`}
				detail={<Trans>established / total</Trans>}
			/>
		</div>
	)
}

function SummaryTile({ label, value, detail }: { label: React.ReactNode; value: string; detail: React.ReactNode }) {
	return (
		<div className="rounded-md border border-border p-4">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="mt-2 text-2xl font-semibold tabular-nums">{value}</div>
			<div className="mt-1 text-xs text-muted-foreground">{detail}</div>
		</div>
	)
}

function GraphRangeSelect({ value, onChange }: { value: string; onChange: (value: string) => void }) {
	return (
		<div className="flex justify-end">
			<Select value={value} onValueChange={onChange}>
				<SelectTrigger className="w-28">
					<SelectValue />
				</SelectTrigger>
				<SelectContent>
					<SelectItem value="5m">5m</SelectItem>
					<SelectItem value="10m">10m</SelectItem>
					<SelectItem value="15m">15m</SelectItem>
					<SelectItem value="30m">30m</SelectItem>
					<SelectItem value="1h">1h</SelectItem>
					<SelectItem value="6h">6h</SelectItem>
					<SelectItem value="24h">24h</SelectItem>
					<SelectItem value="7d">7d</SelectItem>
					<SelectItem value="30d">30d</SelectItem>
				</SelectContent>
			</Select>
		</div>
	)
}

function DeviceHealthPanel({
	ports,
	sensorProblems,
	sensorProblemCount,
	bgpCounts,
}: {
	ports: NetworkPort[]
	sensorProblems: NetworkDeviceSensor[]
	sensorProblemCount: number
	bgpCounts: BGPCounts
}) {
	const downPorts = ports.filter((port) => {
		const s = (port.OperStatus ?? port.oper_status ?? "").toString().toLowerCase()
		return s === "down" || s === "2"
	})
	const disabledPorts = ports.filter((port) => {
		const s = (port.AdminStatus ?? port.admin_status ?? "").toString().toLowerCase()
		return s === "down" || s === "2"
	})
	const bgpProblems = Math.max(0, bgpCounts.total - bgpCounts.established)
	return (
		<div className="grid gap-3">
			<div className="grid gap-3 md:grid-cols-4">
				<SummaryTile label={<Trans>Oper Down</Trans>} value={String(downPorts.length)} detail={<Trans>ports</Trans>} />
				<SummaryTile
					label={<Trans>Admin Down</Trans>}
					value={String(disabledPorts.length)}
					detail={<Trans>ports</Trans>}
				/>
				<SummaryTile
					label={<Trans>Sensor Alerts</Trans>}
					value={String(sensorProblemCount)}
					detail={<Trans>not ok</Trans>}
				/>
				<SummaryTile
					label={<Trans>BGP Alerts</Trans>}
					value={String(bgpProblems)}
					detail={<Trans>not established</Trans>}
				/>
			</div>
			<div className="grid gap-3 lg:grid-cols-2">
				<CompactIssueList
					title={<Trans>Down Ports</Trans>}
					items={downPorts.map(portLabel).slice(0, 12)}
					empty={<Trans>No down ports.</Trans>}
				/>
				<CompactIssueList
					title={<Trans>Sensor Alerts</Trans>}
					items={sensorProblems
						.map(
							(sensor) => `${sensor.Name ?? sensor.name ?? "sensor"}: ${sensor.Status ?? sensor.status ?? "unknown"}`
						)
						.slice(0, 12)}
					empty={<Trans>No sensor alerts.</Trans>}
				/>
			</div>
		</div>
	)
}

function CompactIssueList({
	title,
	items,
	empty,
}: {
	title: React.ReactNode
	items: string[]
	empty: React.ReactNode
}) {
	return (
		<div className="rounded-md border border-border">
			<div className="border-b border-border bg-muted/30 px-4 py-2 text-sm font-medium">{title}</div>
			<div className="divide-y divide-border/50 text-sm">
				{items.length === 0 ? (
					<div className="px-4 py-3 text-muted-foreground">{empty}</div>
				) : (
					items.map((item) => (
						<div key={item} className="px-4 py-2">
							{item}
						</div>
					))
				)}
			</div>
		</div>
	)
}

function PortStatusOverview({
	ports,
	selectedPortIDs,
	portTraffic,
	trafficView,
}: {
	ports: NetworkPort[]
	selectedPortIDs: string[]
	portTraffic: Record<string, { in?: number; out?: number }>
	trafficView: TrafficViewMode
}) {
	if (ports.length === 0) {
		return null
	}
	const selected = new Set(selectedPortIDs)
	const up = ports.filter((port) => portState(port) === "up").length
	const down = ports.filter((port) => portState(port) === "down").length
	const disabled = ports.filter((port) => portState(port) === "disabled").length
	const unknown = Math.max(0, ports.length - up - down - disabled)
	return (
		<div className="grid gap-3 border-t border-border pt-3">
			<div className="grid grid-cols-4 gap-2 text-sm tabular-nums">
				<PortCount label={<Trans>Total</Trans>} value={ports.length} className="text-foreground" />
				<PortCount label={<Trans>Up</Trans>} value={up} className="text-blue-700 dark:text-blue-300" />
				<PortCount label={<Trans>Down</Trans>} value={down} className="text-red-600 dark:text-red-300" />
				<PortCount label={<Trans>Disabled</Trans>} value={disabled + unknown} className="text-muted-foreground" />
			</div>
			<div className="max-h-28 overflow-auto text-sm leading-6">
				{ports.map((port, index) => {
					const id = port.ID ?? port.id ?? ""
					return (
						<span key={id || index}>
							<Link
								href={getPagePath($router, "network_port", { id })}
								title={portTrafficTitle(port, portTraffic[id], trafficView)}
								className={cn("hover:underline", portColor(port), !selected.has(id) && "opacity-45")}
							>
								{portLabel(port)}
							</Link>
							{index < ports.length - 1 ? <span className="text-muted-foreground">, </span> : null}
						</span>
					)
				})}
			</div>
		</div>
	)
}

function PortCount({ label, value, className }: { label: React.ReactNode; value: number; className?: string }) {
	return (
		<div className={cn("flex items-center gap-2", className)}>
			<span className="text-muted-foreground">{label}</span>
			<span className="font-medium">{value}</span>
		</div>
	)
}

function DeviceSensorsTable({ deviceId }: { deviceId: string }) {
	const { t } = useLingui()
	const [sensors, setSensors] = useState<NetworkDeviceSensor[]>([])
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [health, setHealth] = useState("all")
	const [sort, setSort] = useState("class:asc")
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [total, setTotal] = useState(0)
	const [counts, setCounts] = useState<SensorCounts>({ total: 0, problems: 0 })
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const load = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<NetworkDeviceSensorsResponse>(`/api/v1/devices/${deviceId}/sensors`, {
				query: {
					q: query || undefined,
					health: health === "all" ? undefined : health,
					sort: sortField,
					order,
					limit: pageSize,
					offset: page * pageSize,
				},
			})
			if (sequence !== requestSequence.current) return
			setSensors(data.items ?? [])
			setTotal(data.total ?? 0)
			if (data.counts) setCounts(data.counts)
		} catch (requestError) {
			if (sequence !== requestSequence.current) return
			setSensors([])
			setTotal(0)
			setError(requestError instanceof Error ? requestError.message : String(requestError))
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [deviceId, health, page, pageSize, query, sort])

	useEffect(() => {
		load()
	}, [load])

	const records = useMemo(
		() =>
			sensors.map((sensor) => {
				const name = sensor.Name ?? sensor.name ?? "—"
				const sensorClass = (sensor.Class ?? sensor.class ?? "other").toLowerCase()
				const status = sensor.Status ?? sensor.status ?? "—"
				const value = formatSensorValue(sensor)
				return {
					id: sensor.ID ?? sensor.id ?? "",
					health: ["", "ok", "up", "normal", "1"].includes(status.trim().toLowerCase()) ? "healthy" : "problem",
					sensorClass,
					name,
					status,
					value,
					searchText: `${sensorClass} ${name} ${status} ${value}`.toLowerCase(),
				}
			}),
		[sensors]
	)
	const columns = useMemo(
		() => [
			{ field: "health", title: t`Health`, width: 120, style: denseCellStyle() },
			{ field: "sensorClass", title: t`Class`, width: 130, style: denseCellStyle() },
			{ field: "name", title: t`Sensor`, width: 340, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 130, style: denseCellStyle() },
			{ field: "value", title: t`Value`, width: 180, style: denseCellStyle() },
		],
		[t]
	)
	const serverFiltering = useMemo(() => ({
		options: { health: ["healthy", "problem"].map((value) => ({ value })) },
		selected: { health: health === "all" ? [] : [health] },
		selection: { health: "single" as const },
		onColumnFilterChange: (_field: string, values: unknown[]) => {
			setHealth(values.length > 0 ? String(values[0]) : "all")
			setPage(0)
		},
		onClearAll: () => { setHealth("all"); setPage(0) },
	}), [health])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => ({
		field: sortField,
		direction: sortDirection,
		fields: { sensorClass: "class", name: "name", status: "status", value: "value" },
		onSortChange: (field: string, direction: "asc" | "desc") => { setSort(`${field}:${direction}`); setPage(0) },
	}), [sortDirection, sortField])
	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center gap-2">
				<Select
					value={health}
					onValueChange={(value) => {
						setHealth(value)
						setPage(0)
					}}
				>
					<SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
					<SelectContent>
						<SelectItem value="all"><Trans>All health</Trans></SelectItem>
						<SelectItem value="healthy"><Trans>Healthy</Trans></SelectItem>
						<SelectItem value="problem"><Trans>Problems</Trans></SelectItem>
					</SelectContent>
				</Select>
				<Select
					value={sort}
					onValueChange={(value) => {
						setSort(value)
						setPage(0)
					}}
				>
					<SelectTrigger className="w-48"><SelectValue /></SelectTrigger>
					<SelectContent>
						<SelectItem value="class:asc"><Trans>Class ascending</Trans></SelectItem>
						<SelectItem value="name:asc"><Trans>Name ascending</Trans></SelectItem>
						<SelectItem value="name:desc"><Trans>Name descending</Trans></SelectItem>
						<SelectItem value="status:asc"><Trans>Status ascending</Trans></SelectItem>
						<SelectItem value="value:desc"><Trans>Value descending</Trans></SelectItem>
					</SelectContent>
				</Select>
				<Badge variant={counts.problems > 0 ? "danger" : "success"}>
					{counts.problems}/{counts.total} <Trans>problems</Trans>
				</Badge>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No sensors discovered.`}
				searchPlaceholder={t`Search sensors...`}
				searchValue={search}
				onSearchChange={setSearch}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => {
						setPageSize(value)
						setPage(0)
					},
				}}
			/>
		</div>
	)
}

function DevicePortsTable({
	deviceId,
	portTraffic,
	trafficView,
}: {
	deviceId: string
	portTraffic: Record<string, { in?: number; out?: number }>
	trafficView: TrafficViewMode
}) {
	const { t } = useLingui()
	const [ports, setPorts] = useState<NetworkPort[]>([])
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [adminStatus, setAdminStatus] = useState("all")
	const [operStatus, setOperStatus] = useState("all")
	const [addressFamily, setAddressFamily] = useState("all")
	const [sort, setSort] = useState("if_index:asc")
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [total, setTotal] = useState(0)
	const [counts, setCounts] = useState<PortCounts>({ total: 0, up: 0, down: 0 })
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const load = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<NetworkPortsResponse>(`/api/v1/devices/${deviceId}/ports`, {
				query: {
					q: query || undefined,
					admin_status: adminStatus === "all" ? undefined : adminStatus,
					oper_status: operStatus === "all" ? undefined : operStatus,
					address_family: addressFamily === "all" ? undefined : addressFamily,
					sort: sortField,
					order,
					limit: pageSize,
					offset: page * pageSize,
				},
			})
			if (sequence !== requestSequence.current) return
			setPorts(data.items ?? [])
			setTotal(data.total ?? 0)
			if (data.counts) setCounts(data.counts)
		} catch (requestError) {
			if (sequence !== requestSequence.current) return
			setPorts([])
			setTotal(0)
			setError(requestError instanceof Error ? requestError.message : String(requestError))
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [addressFamily, adminStatus, deviceId, operStatus, page, pageSize, query, sort])

	useEffect(() => {
		load()
	}, [load])
	const records = useMemo(
		() =>
			ports.map((port) => {
				const id = port.ID ?? port.id ?? ""
				const name = port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? "—"
				const alias = port.IfAlias ?? port.if_alias ?? "—"
				const admin = port.AdminStatus ?? port.admin_status ?? "—"
				const oper = port.OperStatus ?? port.oper_status ?? "—"
				const speed = port.SpeedBps ?? port.speed_bps ?? 0
				const traffic = portTraffic[id] ?? {}
				const rateBase = trafficViewRateBase(trafficView)
				const addresses = port.Addresses ?? port.addresses ?? []
				const ipv4 = formatInterfaceAddresses(addresses, "ipv4")
				const ipv6 = formatInterfaceAddresses(addresses, "ipv6")
				return {
					id,
					name,
					alias,
					admin,
					oper,
					ipv4,
					ipv6,
					inRate: traffic.in == null ? "—" : formatBitsPerSecond(traffic.in, rateBase),
					outRate: traffic.out == null ? "—" : formatBitsPerSecond(traffic.out, rateBase),
					speed: formatBitsPerSecond(speed),
					searchText: `${name} ${alias} ${admin} ${oper} ${ipv4} ${ipv6}`.toLowerCase(),
				}
			}),
		[portTraffic, ports, trafficView]
	)
	const columns = useMemo(
		() => [
			{ field: "name", title: t`Port`, width: 160, style: denseCellStyle() },
			{ field: "alias", title: t`Alias`, width: 210, style: denseCellStyle() },
			{ field: "ipv4", title: "IPv4", width: 190, style: denseCellStyle() },
			{ field: "ipv6", title: "IPv6", width: 250, style: denseCellStyle() },
			{ field: "admin", title: t`Admin`, width: 100, style: denseCellStyle() },
			{ field: "oper", title: t`Oper`, width: 100, style: denseCellStyle() },
			{ field: "inRate", title: t`In`, width: 120, style: denseCellStyle() },
			{ field: "outRate", title: t`Out`, width: 120, style: denseCellStyle() },
			{ field: "speed", title: t`Speed`, width: 120, style: denseCellStyle() },
		],
		[t]
	)
	const openPort = useCallback((record: Record<string, unknown>) => {
		if (record.id) navigate(getPagePath($router, "network_port", { id: String(record.id) }))
	}, [])
	const portStates = ["up", "down"]
	const serverFiltering = useMemo(() => ({
		options: {
			admin: portStates.map((value) => ({ value })),
			oper: portStates.map((value) => ({ value })),
		},
		selected: {
			admin: adminStatus === "all" ? [] : [adminStatus],
			oper: operStatus === "all" ? [] : [operStatus],
		},
		selection: { admin: "single" as const, oper: "single" as const },
		onColumnFilterChange: (field: string, values: unknown[]) => {
			const value = values.length > 0 ? String(values[0]) : "all"
			if (field === "admin") setAdminStatus(value)
			if (field === "oper") setOperStatus(value)
			setPage(0)
		},
		onClearAll: () => { setAdminStatus("all"); setOperStatus("all"); setPage(0) },
	}), [adminStatus, operStatus])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => ({
		field: sortField,
		direction: sortDirection,
		fields: { name: "name", alias: "alias", admin: "admin_status", oper: "oper_status", speed: "speed" },
		onSortChange: (field: string, direction: "asc" | "desc") => { setSort(`${field}:${direction}`); setPage(0) },
	}), [sortDirection, sortField])
	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center gap-2">
				<Select
					value={operStatus}
					onValueChange={(value) => {
						setOperStatus(value)
						setPage(0)
					}}
				>
					<SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
					<SelectContent>
						<SelectItem value="all"><Trans>All states</Trans></SelectItem>
						<SelectItem value="up"><Trans>Up</Trans></SelectItem>
						<SelectItem value="down"><Trans>Down</Trans></SelectItem>
					</SelectContent>
				</Select>
				<Select
					value={addressFamily}
					onValueChange={(value) => {
						setAddressFamily(value)
						setPage(0)
					}}
				>
					<SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
					<SelectContent>
						<SelectItem value="all"><Trans>All addresses</Trans></SelectItem>
						<SelectItem value="ipv4">IPv4</SelectItem>
						<SelectItem value="ipv6">IPv6</SelectItem>
					</SelectContent>
				</Select>
				<Select
					value={sort}
					onValueChange={(value) => {
						setSort(value)
						setPage(0)
					}}
				>
					<SelectTrigger className="w-48"><SelectValue /></SelectTrigger>
					<SelectContent>
						<SelectItem value="if_index:asc"><Trans>Index ascending</Trans></SelectItem>
						<SelectItem value="name:asc"><Trans>Name ascending</Trans></SelectItem>
						<SelectItem value="name:desc"><Trans>Name descending</Trans></SelectItem>
						<SelectItem value="speed:desc"><Trans>Speed descending</Trans></SelectItem>
						<SelectItem value="oper_status:asc"><Trans>Status ascending</Trans></SelectItem>
					</SelectContent>
				</Select>
				<Badge variant={counts.down > 0 ? "secondary" : "success"}>
					{counts.up}/{counts.total} <Trans>up</Trans>
				</Badge>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No ports found.`}
				searchPlaceholder={t`Search ports, addresses...`}
				searchValue={search}
				onSearchChange={setSearch}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				height={560}
				onRowClick={openPort}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => {
						setPageSize(value)
						setPage(0)
					},
				}}
			/>
		</div>
	)
}

function BGPSessionsTable({ deviceId }: { deviceId: string }) {
	const { t } = useLingui()
	const [bgpSessions, setBGPSessions] = useState<BGPSession[]>([])
	const [counts, setCounts] = useState<BGPCounts>({ total: 0, established: 0 })
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [state, setState] = useState("all")
	const [sort, setSort] = useState("peer:asc")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const load = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<BGPSessionsResponse>(`/api/v1/devices/${deviceId}/bgp`, {
				query: {
					q: query || undefined,
					state: state === "all" ? undefined : state,
					sort: sortField,
					order,
					limit: pageSize,
					offset: page * pageSize,
				},
			})
			if (sequence !== requestSequence.current) return
			setBGPSessions(data.items ?? [])
			setTotal(data.total ?? 0)
			if (data.counts) setCounts(data.counts)
		} catch (requestError) {
			if (sequence !== requestSequence.current) return
			setBGPSessions([])
			setTotal(0)
			setError(requestError instanceof Error ? requestError.message : String(requestError))
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [deviceId, page, pageSize, query, sort, state])

	useEffect(() => {
		load()
	}, [load])

	const records = useMemo(
		() =>
			bgpSessions.map((session) => {
				const peer = session.PeerAddr ?? session.peer_addr ?? "—"
				const state = session.State ?? session.state ?? "—"
				const peerAS = formatNumber(session.PeerAS ?? session.peer_as)
				const localAS = formatNumber(session.LocalAS ?? session.local_as)
				const afi = session.AFI ?? session.afi ?? "—"
				const safi = session.SAFI ?? session.safi ?? "—"
				const accepted = formatNumber(session.AcceptedPrefixes ?? session.accepted_prefixes)
				const denied = formatNumber(session.DeniedPrefixes ?? session.denied_prefixes)
				const advertised = formatNumber(session.AdvertisedPrefixes ?? session.advertised_prefixes)
				return {
					id: session.ID ?? session.id ?? "",
					peer,
					state,
					peerAS,
					localAS,
					afi,
					safi,
					accepted,
					denied,
					advertised,
					searchText: `${peer} ${state} ${peerAS} ${localAS} ${afi} ${safi}`.toLowerCase(),
				}
			}),
		[bgpSessions]
	)
	const columns = useMemo(
		() => [
			{ field: "peer", title: t`BGP Peer`, width: 240, style: denseCellStyle() },
			{ field: "state", title: t`State`, width: 120, style: denseCellStyle() },
			{ field: "peerAS", title: t`Peer AS`, width: 110, style: denseCellStyle() },
			{ field: "localAS", title: t`Local AS`, width: 110, style: denseCellStyle() },
			{ field: "afi", title: "AFI", width: 100, style: denseCellStyle() },
			{ field: "safi", title: "SAFI", width: 140, style: denseCellStyle() },
			{ field: "accepted", title: t`Accepted`, width: 110, style: denseCellStyle() },
			{ field: "denied", title: t`Denied`, width: 110, style: denseCellStyle() },
			{ field: "advertised", title: t`Advertised`, width: 120, style: denseCellStyle() },
		],
		[t]
	)
	const serverFiltering = useMemo(() => ({
		options: { state: ["established", "idle", "active", "connect"].map((value) => ({ value })) },
		selected: { state: state === "all" ? [] : [state] },
		selection: { state: "single" as const },
		onColumnFilterChange: (_field: string, values: unknown[]) => {
			setState(values.length > 0 ? String(values[0]) : "all")
			setPage(0)
		},
		onClearAll: () => { setState("all"); setPage(0) },
	}), [state])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => ({
		field: sortField,
		direction: sortDirection,
		fields: { peer: "peer", peerAS: "peer_as", state: "state" },
		onSortChange: (field: string, direction: "asc" | "desc") => { setSort(`${field}:${direction}`); setPage(0) },
	}), [sortDirection, sortField])
	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center gap-2">
				<Select
					value={state}
					onValueChange={(value) => {
						setState(value)
						setPage(0)
					}}
				>
					<SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
					<SelectContent>
						<SelectItem value="all"><Trans>All states</Trans></SelectItem>
						<SelectItem value="established"><Trans>Established</Trans></SelectItem>
						<SelectItem value="idle"><Trans>Idle</Trans></SelectItem>
						<SelectItem value="active"><Trans>Active</Trans></SelectItem>
						<SelectItem value="connect"><Trans>Connect</Trans></SelectItem>
					</SelectContent>
				</Select>
				<Select
					value={sort}
					onValueChange={(value) => {
						setSort(value)
						setPage(0)
					}}
				>
					<SelectTrigger className="w-48"><SelectValue /></SelectTrigger>
					<SelectContent>
						<SelectItem value="peer:asc"><Trans>Peer ascending</Trans></SelectItem>
						<SelectItem value="peer:desc"><Trans>Peer descending</Trans></SelectItem>
						<SelectItem value="peer_as:asc"><Trans>Peer AS ascending</Trans></SelectItem>
						<SelectItem value="peer_as:desc"><Trans>Peer AS descending</Trans></SelectItem>
						<SelectItem value="state:asc"><Trans>State ascending</Trans></SelectItem>
					</SelectContent>
				</Select>
				<Badge variant={counts.established === counts.total && counts.total > 0 ? "success" : "secondary"}>
					{counts.established}/{counts.total} <Trans>established</Trans>
				</Badge>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No BGP sessions found.`}
				searchPlaceholder={t`Search BGP peers or AS numbers...`}
				searchValue={search}
				onSearchChange={setSearch}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => {
						setPageSize(value)
						setPage(0)
					},
				}}
			/>
		</div>
	)
}

function DeviceOverview({
	device,
	targetID,
	targetName,
	targetHost,
	targetStatus,
	targetLabels,
	resolvedLocation,
}: {
	device: NetworkDevice | null
	targetID: string
	targetName: string
	targetHost: string
	targetStatus: string
	targetLabels: string
	resolvedLocation: string
}) {
	const { t } = useLingui()
	const vendor = firstText(device?.Vendor, device?.vendor)
	const hardware = firstText(device?.Model, device?.model, device?.Platform, device?.platform)
	const platform = firstText(device?.Platform, device?.platform)
	const platformRow = platform && platform !== hardware ? platform : ""
	const location = resolvedLocation
	const sysDescr = firstText(device?.SysDescr, device?.sys_descr)
	const systemName = firstText(device?.SysName, device?.sys_name)
	const objectID = firstText(device?.SysObjectID, device?.sys_object_id)
	const uptime = formatDeviceUptime(device?.Uptime ?? device?.uptime)
	const snmpProfile = firstText(device?.SNMPProfileID, device?.snmp_profile_id)
	const snmpPort = formatNumber(device?.SNMPPort ?? device?.snmp_port)
	// The page header above already shows the logo, device name, status badge,
	// host, OS and uptime — these cards only carry the details, without
	// repeating that banner or truncating long values.
	return (
		<div className="grid items-start gap-4 lg:grid-cols-[minmax(0,1fr)_360px]">
			<div className="overflow-hidden rounded-md border border-border">
				<div className="border-b border-border bg-muted/30 px-4 py-3 text-sm font-semibold">
					<Trans>System Information</Trans>
				</div>
				<OverviewTable
					rows={[
						[t`System Name`, systemName],
						[t`Hardware`, hardware && vendor ? `${vendor} ${hardware}` : hardware || vendor],
						[t`Platform`, platformRow],
						[t`Operating System`, deviceOS(device)],
						["Object ID", objectID, true],
						[t`Uptime`, uptime],
						[t`Location`, location],
						[t`sysDescr`, sysDescr],
					]}
				/>
			</div>
			<div className="overflow-hidden rounded-md border border-border">
				<div className="border-b border-border bg-muted/30 px-4 py-3 text-sm font-semibold">
					<Trans>Collection Endpoint</Trans>
				</div>
				<OverviewTable
					rows={[
						[t`Host`, targetHost, true],
						[t`Status`, <StatusBadge key="status" value={targetStatus} />],
						[t`SNMP Version`, firstText(device?.SNMPProfileID, device?.snmp_profile_id) ? "v2c" : ""],
						["SNMP Profile", snmpProfile, true],
						["SNMP Port", snmpPort, true],
						[t`Target`, targetName || targetID],
						[t`Labels`, targetLabels],
					]}
				/>
			</div>
		</div>
	)
}

function OverviewTable({ rows }: { rows: [string, React.ReactNode, boolean?][] }) {
	const visibleRows = rows.filter(([, value]) => Boolean(value))
	return (
		<div className="divide-y divide-border/50 text-sm">
			{visibleRows.map(([label, value, mono]) => (
				<div key={label} className="grid grid-cols-[132px_minmax(0,1fr)]">
					<div className="bg-muted/20 px-4 py-2 text-muted-foreground">{label}</div>
					<div className={cn("min-w-0 break-words px-4 py-2", mono && "break-all font-mono text-xs leading-5")}>
						{value}
					</div>
				</div>
			))}
		</div>
	)
}

function StatusBadge({ value }: { value?: string }) {
	const normalized = value?.toLowerCase() ?? ""
	const variant: "success" | "danger" | "outline" =
		normalized === "up" ||
		normalized === "established" ||
		normalized === "ok" ||
		normalized === "1" ||
		normalized === "6"
			? "success"
			: normalized === "down" ||
					normalized === "idle" ||
					normalized === "unavailable" ||
					normalized === "nonoperational" ||
					normalized === "2"
				? "danger"
				: "outline"
	return <Badge variant={variant}>{value || "—"}</Badge>
}

function formatNumber(value?: number) {
	return typeof value === "number" ? String(value) : "—"
}

function formatDeviceUptime(value?: number) {
	if (!value || value < 0) {
		return ""
	}
	let seconds = value
	if (value > 1_000_000_000) {
		seconds = Math.floor(value / 1_000_000_000)
	}
	const days = Math.floor(seconds / 86_400)
	seconds %= 86_400
	const hours = Math.floor(seconds / 3_600)
	seconds %= 3_600
	const minutes = Math.floor(seconds / 60)
	const parts: string[] = []
	if (days) {
		parts.push(`${days}d`)
	}
	if (hours) {
		parts.push(`${hours}h`)
	}
	if (minutes) {
		parts.push(`${minutes}m`)
	}
	return parts.length ? parts.join(" ") : "<1m"
}

function deviceOS(device?: NetworkDevice | null) {
	const name = firstText(device?.OSName, device?.os_name)
	const version = firstText(device?.OSVersion, device?.os_version)
	if (!name) {
		return "—"
	}
	return version ? `${name} ${version}` : name
}

function formatSensorValue(sensor: NetworkDeviceSensor) {
	const value = sensor.Value ?? sensor.value
	if (typeof value !== "number") {
		return "—"
	}
	const unit = sensor.Unit ?? sensor.unit ?? ""
	return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}${unit ? ` ${unit}` : ""}`
}

function formatInterfaceAddresses(addresses: NetworkInterfaceAddress[], family: "ipv4" | "ipv6") {
	const values = addresses
		.filter((address) => (address.Family ?? address.family ?? "").toLowerCase() === family)
		.map((address) => {
			const ip = address.Address ?? address.address
			if (!ip) return ""
			const prefix = address.PrefixLength ?? address.prefix_length
			return typeof prefix === "number" ? `${ip}/${prefix}` : ip
		})
		.filter(Boolean)
	return values.length > 0 ? values.join("\n") : "—"
}

function portLabel(port: NetworkPort) {
	return (
		firstText(
			port.IfName,
			port.if_name,
			port.IfAlias,
			port.if_alias,
			port.IfDescr,
			port.if_descr,
			String(port.IfIndex ?? port.if_index ?? "")
		) || "—"
	)
}

function portState(port: NetworkPort) {
	const admin = (port.AdminStatus ?? port.admin_status ?? "").toLowerCase()
	const oper = (port.OperStatus ?? port.oper_status ?? "").toLowerCase()
	if (admin === "down" || admin === "2") {
		return "disabled"
	}
	if (oper === "up" || oper === "1") {
		return "up"
	}
	if (oper === "down" || oper === "2") {
		return "down"
	}
	return "unknown"
}

function portColor(port: NetworkPort) {
	const state = portState(port)
	if (state === "up") {
		return "text-blue-700 dark:text-blue-300"
	}
	if (state === "down") {
		return "text-red-600 dark:text-red-300"
	}
	return "text-muted-foreground"
}

function portTrafficTitle(
	port: NetworkPort,
	traffic: { in?: number; out?: number } | undefined,
	trafficView: TrafficViewMode
) {
	const rateBase = trafficViewRateBase(trafficView)
	return [
		portLabel(port),
		`In: ${formatBitsPerSecond(traffic?.in, rateBase)}`,
		`Out: ${formatBitsPerSecond(traffic?.out, rateBase)}`,
		`Admin: ${port.AdminStatus ?? port.admin_status ?? "—"}`,
		`Oper: ${port.OperStatus ?? port.oper_status ?? "—"}`,
		`Speed: ${formatBitsPerSecond(port.SpeedBps ?? port.speed_bps)}`,
	].join("\n")
}

function createAggregateGraphID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `aggr_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

type PhysicalEntity = {
	Index?: number
	Name?: string
	Description?: string
	Class?: string
	SerialNumber?: string
	ModelName?: string
	ManufacturerName?: string
	HardwareRevision?: string
	IsFRU?: boolean
}

type DeviceVLAN = {
	VLANID?: number
	Name?: string
	Status?: string
}

type DeviceLAGGroup = {
	AggregateIndex?: number
	MACAddress?: string
	Mode?: string
}

function DeviceVLANsLAG({ deviceId }: { deviceId: string }) {
	return (
		<div className="grid gap-6">
			<DeviceVLANTable deviceId={deviceId} />
			<DeviceLAGTable deviceId={deviceId} />
		</div>
	)
}

function DeviceVLANTable({ deviceId }: { deviceId: string }) {
	const { t } = useLingui()
	const [vlans, setVlans] = useState<DeviceVLAN[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [status, setStatus] = useState("all")
	const [sort, setSort] = useState("vlan_id:asc")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const load = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<{ items?: DeviceVLAN[]; total?: number }>(
				`/api/v1/devices/${deviceId}/vlans`,
				{
					query: {
						q: query || undefined,
						status: status === "all" ? undefined : status,
						sort: sortField,
						order,
						limit: pageSize,
						offset: page * pageSize,
					},
				}
			)
			if (sequence !== requestSequence.current) return
			setVlans(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (requestError) {
			if (sequence !== requestSequence.current) return
			setVlans([])
			setTotal(0)
			setError(requestError instanceof Error ? requestError.message : String(requestError))
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [deviceId, page, pageSize, query, sort, status])

	useEffect(() => {
		load()
	}, [load])

	const vlanRecords = useMemo(
		() =>
			vlans.map((vlan, index) => ({
				id: String(vlan.VLANID ?? index),
				vlanID: vlan.VLANID ?? "—",
				name: vlan.Name || "—",
				status: vlan.Status || "—",
				searchText: `${vlan.VLANID ?? ""} ${vlan.Name ?? ""} ${vlan.Status ?? ""}`.toLowerCase(),
			})),
		[vlans]
	)
	const vlanColumns = useMemo(
		() => [
			{ field: "vlanID", title: t`VLAN ID`, width: 140, style: denseCellStyle() },
			{ field: "name", title: t`Name`, width: 320, style: denseCellStyle() },
			{ field: "status", title: t`Status`, width: 160, style: denseCellStyle() },
		],
		[t]
	)
	const serverFiltering = useMemo(() => ({
		options: { status: [{ value: "active" }] },
		selected: { status: status === "all" ? [] : [status] },
		selection: { status: "single" as const },
		onColumnFilterChange: (_field: string, values: unknown[]) => {
			setStatus(values.length > 0 ? String(values[0]) : "all")
			setPage(0)
		},
		onClearAll: () => { setStatus("all"); setPage(0) },
	}), [status])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => ({
		field: sortField,
		direction: sortDirection,
		fields: { vlanID: "vlan_id", name: "name", status: "status" },
		onSortChange: (field: string, direction: "asc" | "desc") => { setSort(`${field}:${direction}`); setPage(0) },
	}), [sortDirection, sortField])
	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center justify-between gap-2">
				<div className="text-sm font-medium"><Trans>VLANs</Trans></div>
				<div className="flex flex-wrap items-center gap-2">
					<Select
						value={status}
						onValueChange={(value) => {
							setStatus(value)
							setPage(0)
						}}
					>
						<SelectTrigger className="w-36"><SelectValue /></SelectTrigger>
						<SelectContent>
							<SelectItem value="all"><Trans>All statuses</Trans></SelectItem>
							<SelectItem value="active"><Trans>Active</Trans></SelectItem>
						</SelectContent>
					</Select>
					<Select
						value={sort}
						onValueChange={(value) => {
							setSort(value)
							setPage(0)
						}}
					>
						<SelectTrigger className="w-44"><SelectValue /></SelectTrigger>
						<SelectContent>
							<SelectItem value="vlan_id:asc"><Trans>VLAN ID ascending</Trans></SelectItem>
							<SelectItem value="vlan_id:desc"><Trans>VLAN ID descending</Trans></SelectItem>
							<SelectItem value="name:asc"><Trans>Name ascending</Trans></SelectItem>
							<SelectItem value="name:desc"><Trans>Name descending</Trans></SelectItem>
						</SelectContent>
					</Select>
				</div>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={vlanRecords}
				columns={vlanColumns}
				loading={loading}
				emptyText={t`No VLANs discovered (Q-BRIDGE-MIB not supported or empty).`}
				searchPlaceholder={t`Search VLANs...`}
				height={300}
				searchValue={search}
				onSearchChange={setSearch}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => {
						setPageSize(value)
						setPage(0)
					},
				}}
			/>
		</div>
	)
}

function DeviceLAGTable({ deviceId }: { deviceId: string }) {
	const { t } = useLingui()
	const [lags, setLags] = useState<DeviceLAGGroup[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [mode, setMode] = useState("all")
	const [sort, setSort] = useState("aggregate_index:asc")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const load = useCallback(async () => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		try {
			const data = await api.send<{ items?: DeviceLAGGroup[]; total?: number }>(
				`/api/v1/devices/${deviceId}/lags`,
				{
					query: {
						q: query || undefined,
						mode: mode === "all" ? undefined : mode,
						sort: sortField,
						order,
						limit: pageSize,
						offset: page * pageSize,
					},
				}
			)
			if (sequence !== requestSequence.current) return
			setLags(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (requestError) {
			if (sequence !== requestSequence.current) return
			setLags([])
			setTotal(0)
			setError(requestError instanceof Error ? requestError.message : String(requestError))
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [deviceId, mode, page, pageSize, query, sort])

	useEffect(() => {
		load()
	}, [load])

	const lagRecords = useMemo(
		() =>
			lags.map((lag, index) => ({
				id: String(lag.AggregateIndex ?? index),
				aggregate: lag.AggregateIndex ?? "—",
				mac: lag.MACAddress || "—",
				mode: lag.Mode || "—",
				searchText: `${lag.AggregateIndex ?? ""} ${lag.MACAddress ?? ""} ${lag.Mode ?? ""}`.toLowerCase(),
			})),
		[lags]
	)
	const lagColumns = useMemo(
		() => [
			{ field: "aggregate", title: t`Aggregate`, width: 180, style: denseCellStyle() },
			{ field: "mac", title: t`MAC Address`, width: 260, style: denseCellStyle() },
			{ field: "mode", title: t`Mode`, width: 180, style: denseCellStyle() },
		],
		[t]
	)
	const serverFiltering = useMemo(() => ({
		options: { mode: ["lacp", "active", "passive", "unknown"].map((value) => ({ value })) },
		selected: { mode: mode === "all" ? [] : [mode] },
		selection: { mode: "single" as const },
		onColumnFilterChange: (_field: string, values: unknown[]) => {
			setMode(values.length > 0 ? String(values[0]) : "all")
			setPage(0)
		},
		onClearAll: () => { setMode("all"); setPage(0) },
	}), [mode])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => ({
		field: sortField,
		direction: sortDirection,
		fields: { aggregate: "aggregate_index", mac: "mac_address", mode: "mode" },
		onSortChange: (field: string, direction: "asc" | "desc") => { setSort(`${field}:${direction}`); setPage(0) },
	}), [sortDirection, sortField])
	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center justify-between gap-2">
				<div className="text-sm font-medium"><Trans>LAG groups</Trans></div>
				<div className="flex flex-wrap items-center gap-2">
					<Select
						value={mode}
						onValueChange={(value) => {
							setMode(value)
							setPage(0)
						}}
					>
						<SelectTrigger className="w-36"><SelectValue /></SelectTrigger>
						<SelectContent>
							<SelectItem value="all"><Trans>All modes</Trans></SelectItem>
							<SelectItem value="lacp">LACP</SelectItem>
							<SelectItem value="active"><Trans>Active</Trans></SelectItem>
							<SelectItem value="passive"><Trans>Passive</Trans></SelectItem>
							<SelectItem value="unknown"><Trans>Unknown</Trans></SelectItem>
						</SelectContent>
					</Select>
					<Select
						value={sort}
						onValueChange={(value) => {
							setSort(value)
							setPage(0)
						}}
					>
						<SelectTrigger className="w-48"><SelectValue /></SelectTrigger>
						<SelectContent>
							<SelectItem value="aggregate_index:asc"><Trans>Aggregate ascending</Trans></SelectItem>
							<SelectItem value="aggregate_index:desc"><Trans>Aggregate descending</Trans></SelectItem>
							<SelectItem value="mac_address:asc"><Trans>MAC ascending</Trans></SelectItem>
							<SelectItem value="mode:asc"><Trans>Mode ascending</Trans></SelectItem>
						</SelectContent>
					</Select>
				</div>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={lagRecords}
				columns={lagColumns}
				loading={loading}
				emptyText={t`No LAG groups discovered (IEEE8023-LAG-MIB not supported or empty).`}
				searchPlaceholder={t`Search LAG groups...`}
				height={300}
				searchValue={search}
				onSearchChange={setSearch}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => {
						setPageSize(value)
						setPage(0)
					},
				}}
			/>
		</div>
	)
}

type SNMPEventEntry = {
	ID?: string
	Severity?: string
	EventType?: string
	Message?: string
	Source?: string
	OccurredAt?: string
}

function DeviceEventLog({ deviceId, targetID: _targetID }: { deviceId: string; targetID: string }) {
	return <DeviceEventsTable deviceId={deviceId} alertOnly={false} />
}

function DeviceAlertLog({ deviceId, targetID: _targetID }: { deviceId: string; targetID: string }) {
	return <DeviceEventsTable deviceId={deviceId} alertOnly />
}

function DeviceEventsTable({ deviceId, alertOnly }: { deviceId: string; alertOnly: boolean }) {
	const { t } = useLingui()
	const [events, setEvents] = useState<SNMPEventEntry[]>([])
	const [total, setTotal] = useState(0)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [columnFilters, setColumnFilters] = useState<Record<string, unknown[]>>({})
	const [sort, setSort] = useState("occurred_at:desc")
	const [pageSize, setPageSize] = useState(25)
	const [page, setPage] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setQuery(search.trim())
			setPage(0)
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const fetchEvents = useCallback(async () => {
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		try {
			const [sortField, sortDirection] = sort.split(":")
			const params = new URLSearchParams({
				limit: String(pageSize),
				offset: String(page * pageSize),
				sort: sortField,
				order: sortDirection,
			})
			if (query) params.set("q", query)
			for (const field of ["severity", "event_type", "source"]) {
				const selected = columnFilters[field]?.map(String) ?? []
				const values = field === "severity" && alertOnly && selected.length === 0
					? ["warning", "error", "critical"]
					: selected
				if (values.length > 0) params.set(`filter.${field}`, values.join(","))
			}
			const data = await api.send<{ items?: SNMPEventEntry[]; total?: number }>(
				`/api/v1/devices/${deviceId}/events?${params.toString()}`,
				{}
			)
			if (sequence !== requestSequence.current) return
			setEvents(data.items ?? [])
			setTotal(data.total ?? 0)
		} catch (requestError) {
			if (sequence !== requestSequence.current) return
			setEvents([])
			setTotal(0)
			setError(requestError instanceof Error ? requestError.message : String(requestError))
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [alertOnly, columnFilters, deviceId, page, pageSize, query, sort])

	useEffect(() => {
		fetchEvents()
	}, [fetchEvents])
	const records = useMemo(
		() =>
			events.map((event, index) => {
				const time = event.OccurredAt ? new Date(event.OccurredAt).toLocaleString() : "—"
				const severity = event.Severity ?? "—"
				const type = event.EventType ?? "—"
				const source = event.Source ?? "—"
				const message = event.Message ?? "—"
				return {
					id: event.ID ?? String(index),
					time,
					severity,
					type,
					source,
					message,
					searchText: `${time} ${severity} ${type} ${source} ${message}`.toLowerCase(),
				}
			}),
		[events]
	)
	const columns = useMemo(
		() => [
			{ field: "time", title: t`Time`, width: 190, filter: false, style: denseCellStyle() },
			{ field: "severity", title: t`Severity`, width: 120, style: denseCellStyle() },
			{ field: "type", filterField: "event_type", title: t`Type`, width: 180, style: denseCellStyle() },
			{ field: "source", title: t`Source`, width: 130, style: denseCellStyle() },
			{ field: "message", title: t`Message`, width: 520, filter: false, style: denseCellStyle() },
		],
		[t]
	)
	const emptyText = alertOnly
		? t`No active warning, error, or critical events.`
		: t`No events yet. Events appear on interface status changes and SNMP traps.`
	const serverFiltering = useMemo(() => ({
		options: {
			severity: (alertOnly ? ["warning", "error", "critical"] : ["info", "warning", "error", "critical"])
				.map((value) => ({ value })),
			event_type: [],
			source: [],
		},
		selected: columnFilters,
		selection: { severity: "multiple" as const, event_type: "multiple" as const, source: "multiple" as const },
		loadOptions: async (field: string, facetSearch: string, signal: AbortSignal) => {
			if (field === "severity") {
				return (alertOnly ? ["warning", "error", "critical"] : ["info", "warning", "error", "critical"])
					.filter((value) => value.includes(facetSearch.toLowerCase()))
					.map((value) => ({ value }))
			}
			const params = new URLSearchParams({ field, q: facetSearch, limit: "100" })
			if (query) params.set("search", query)
			for (const filterField of ["severity", "event_type", "source"]) {
				if (filterField === field) continue
				const selected = columnFilters[filterField]?.map(String) ?? []
				const values = filterField === "severity" && alertOnly && selected.length === 0
					? ["warning", "error", "critical"]
					: selected
				if (values.length > 0) params.set(`filter.${filterField}`, values.join(","))
			}
			const data = await api.send<{ items?: { value: string; count: number }[] }>(
				`/api/v1/devices/${deviceId}/events/facets?${params.toString()}`,
				{ signal }
			)
			return (data.items ?? []).map((item) => ({ value: item.value, count: item.count }))
		},
		onColumnFilterChange: (field: string, values: unknown[]) => {
			setColumnFilters((current) => {
				const next = { ...current }
				if (values.length > 0) next[field] = values
				else delete next[field]
				return next
			})
			setPage(0)
		},
		onClearAll: () => { setColumnFilters({}); setPage(0) },
	}), [alertOnly, columnFilters, deviceId, query])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => ({
		field: sortField,
		direction: sortDirection,
		fields: { time: "occurred_at", severity: "severity", type: "event_type", source: "source", message: "message" },
		onSortChange: (field: string, direction: "asc" | "desc") => { setSort(`${field}:${direction}`); setPage(0) },
	}), [sortDirection, sortField])
	return (
		<div className="grid gap-3">
		{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={emptyText}
				searchPlaceholder={alertOnly ? t`Search alerts...` : t`Search events...`}
				searchValue={search}
				onSearchChange={setSearch}
				serverFiltering={serverFiltering}
				serverSorting={serverSorting}
				serverPagination={{
					page,
					pageSize,
					totalCount: total,
					onPageChange: setPage,
					onPageSizeChange: (value) => { setPageSize(value); setPage(0) },
				}}
			/>
		</div>
	)
}

function DeviceInventory({ deviceId }: { deviceId: string }) {
	const { t } = useLingui()
	const [entities, setEntities] = useState<PhysicalEntity[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [entityClass, setEntityClass] = useState("")
	const [classQuery, setClassQuery] = useState("")
	const [fru, setFRU] = useState("all")
	const [sort, setSort] = useState("entity_index:asc")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
			setClassQuery(entityClass.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [entityClass, search])

	const load = useCallback(async () => {
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		try {
			const [sortColumn, order] = sort.split(":")
			const params = new URLSearchParams({
				limit: String(pageSize),
				offset: String(page * pageSize),
				sort: sortColumn,
				order,
			})
			if (query) params.set("q", query)
			if (classQuery) params.set("class", classQuery)
			if (fru !== "all") params.set("fru", fru)
			const data = await api.send<{ items?: PhysicalEntity[]; total?: number }>(
				`/api/v1/devices/${deviceId}/inventory?${params}`,
				{}
			)
			if (sequence === requestSequence.current) {
				setEntities(data.items ?? [])
				setTotal(data.total ?? 0)
			}
		} catch (err) {
			if (sequence === requestSequence.current) {
				setEntities([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load inventory`)
			}
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [classQuery, deviceId, fru, page, pageSize, query, sort, t])

	useEffect(() => {
		load()
	}, [load])

	const records = useMemo(
		() =>
			entities.map((entity, index) => {
				const name = entity.Name || entity.Description || "—"
				const entityClass = entity.Class || "—"
				const model = entity.ModelName || "—"
				const serial = entity.SerialNumber || "—"
				const manufacturer = entity.ManufacturerName || "—"
				const hardwareRevision = entity.HardwareRevision || "—"
				return {
					id: String(entity.Index ?? index),
					index: entity.Index ?? "—",
					name,
					entityClass,
					model,
					serial,
					manufacturer,
					hardwareRevision,
					fru: entity.IsFRU ? t`Yes` : t`No`,
					searchText: `${name} ${entityClass} ${model} ${serial} ${manufacturer} ${hardwareRevision}`.toLowerCase(),
				}
			}),
		[entities, t]
	)
	const columns = useMemo(
		() => [
			{ field: "index", title: t`Index`, width: 90, style: denseCellStyle() },
			{ field: "name", title: t`Name`, width: 300, style: denseCellStyle() },
			{ field: "entityClass", title: t`Class`, width: 140, style: denseCellStyle() },
			{ field: "model", title: t`Model`, width: 180, style: denseCellStyle() },
			{ field: "serial", title: t`Serial`, width: 190, style: denseCellStyle() },
			{ field: "manufacturer", title: t`Manufacturer`, width: 180, style: denseCellStyle() },
			{ field: "hardwareRevision", title: t`HW Rev`, width: 130, style: denseCellStyle() },
			{ field: "fru", title: "FRU", width: 90, style: denseCellStyle() },
		],
		[t]
	)
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(() => ({
		options: { fru: [{ value: "true", label: t`Yes` }, { value: "false", label: t`No` }] },
		selected: { fru: fru === "all" ? [] : [fru] },
		selection: { fru: "single" as const },
		onColumnFilterChange: (_field: string, values: unknown[]) =>
			resetPage(() => setFRU(values.length > 0 ? String(values[0]) : "all")),
		onClearAll: () => resetPage(() => setFRU("all")),
	}), [fru, t])
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(() => ({
		field: sortField,
		direction: sortDirection,
		fields: {
			index: "entity_index", name: "name", entityClass: "class",
			model: "model", serial: "serial", manufacturer: "manufacturer",
		},
		onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
	}), [sortDirection, sortField])
	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap gap-2">
				<Input
					value={entityClass}
					onChange={(event) => setEntityClass(event.target.value)}
					placeholder={t`Class (exact)`}
					aria-label={t`Inventory class`}
					className="w-44"
				/>
				<Select value={fru} onValueChange={(value) => resetPage(() => setFRU(value))}>
					<SelectTrigger className="w-36" aria-label="FRU">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">{t`All inventory`}</SelectItem>
						<SelectItem value="true">{t`FRU only`}</SelectItem>
						<SelectItem value="false">{t`Non-FRU only`}</SelectItem>
					</SelectContent>
				</Select>
				<Select value={sort} onValueChange={(value) => resetPage(() => setSort(value))}>
					<SelectTrigger className="w-44" aria-label={t`Sort inventory`}>
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="entity_index:asc">{t`Index ascending`}</SelectItem>
						<SelectItem value="name:asc">{t`Name A–Z`}</SelectItem>
						<SelectItem value="class:asc">{t`Class A–Z`}</SelectItem>
						<SelectItem value="manufacturer:asc">{t`Manufacturer A–Z`}</SelectItem>
						<SelectItem value="serial:asc">{t`Serial A–Z`}</SelectItem>
					</SelectContent>
				</Select>
			</div>
			{error ? <div className="text-sm text-destructive">{error}</div> : null}
			<PagedVTable
				records={records}
				columns={columns}
				loading={loading}
				emptyText={t`No physical entities discovered. Click Rediscover to probe ENTITY-MIB.`}
				searchPlaceholder={t`Search inventory...`}
				searchValue={search}
				onSearchChange={setSearch}
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
			/>
		</div>
	)
}

// DeviceSavedGraphs links the saved aggregate graphs whose port membership
// includes this device, plus the full library.
function DeviceSavedGraphs({ deviceId }: { deviceId: string }) {
	const [graphs, setGraphs] = useState<{ ID?: string; Name?: string; Devices?: { ID?: string }[] }[]>([])

	useEffect(() => {
		let cancelled = false
		api.send<{ items?: { ID?: string; Name?: string; Devices?: { ID?: string }[] }[] }>("/api/v1/aggregate-graphs", {})
			.then((data) => {
				if (!cancelled) {
					setGraphs((data.items ?? []).filter((graph) => (graph.Devices ?? []).some((d) => d.ID === deviceId)))
				}
			})
			.catch(() => {})
		return () => {
			cancelled = true
		}
	}, [deviceId])

	return (
		<div className="overflow-hidden rounded-md border border-border">
			<div className="flex items-center justify-between border-b border-border bg-muted/30 px-4 py-3 text-sm font-semibold">
				<Trans>Saved Graphs</Trans>
				<Link
					className="text-xs font-normal text-muted-foreground hover:underline"
					href={getPagePath($router, "aggregate_graphs")}
				>
					<Trans>View all</Trans>
				</Link>
			</div>
			<div className="flex flex-wrap gap-2 p-4">
				{graphs.length === 0 ? (
					<span className="text-sm text-muted-foreground">
						<Trans>No saved graphs include this device yet.</Trans>
					</span>
				) : (
					graphs.map((graph) => (
						<Link
							key={graph.ID}
							href={getPagePath($router, "aggregate_graph", { id: graph.ID ?? "" })}
							className="rounded-md border border-border px-3 py-1.5 text-sm hover:bg-muted/40"
						>
							{graph.Name ?? graph.ID}
						</Link>
					))
				)}
			</div>
		</div>
	)
}

function formatTargetLabels(target: TargetRecord | null) {
	const labels = target?.Labels ?? target?.labels ?? {}
	const entries = Object.entries(labels)
	if (entries.length === 0) {
		return ""
	}
	return entries.map(([key, value]) => `${key}=${value}`).join(", ")
}

function firstValue(...values: (string | undefined)[]) {
	return values.find((value) => value?.trim()) ?? "—"
}

function firstText(...values: (string | undefined)[]) {
	return values.find((value) => value?.trim())?.trim() ?? ""
}

function denseCellStyle() {
	return {
		textAlign: "left" as const,
		textBaseline: "middle" as const,
		fontSize: 13,
		lineHeight: 18,
		autoWrapText: true,
		lineClamp: 3,
		padding: [4, 8, 4, 8],
	}
}

async function queryMetric(baseParams: URLSearchParams, metric: string) {
	const params = new URLSearchParams(baseParams)
	params.set("metric", metric)
	return await api.send<VMRangeResponse>(`/api/v1/metrics/query?${params.toString()}`, {})
}

function latestPortValues(response: VMRangeResponse) {
	const values: Record<string, number> = {}
	for (const result of response.data?.result ?? []) {
		const portID = result.metric?.port_id
		const latest = result.values?.at(-1)
		if (!portID || !latest) {
			continue
		}
		const value = Number(latest[1])
		if (Number.isFinite(value)) {
			values[portID] = value
		}
	}
	return values
}

function mergePortTraffic(inValues: Record<string, number>, outValues: Record<string, number>) {
	const merged: Record<string, { in?: number; out?: number }> = {}
	for (const [portID, value] of Object.entries(inValues)) {
		merged[portID] = { ...merged[portID], in: value }
	}
	for (const [portID, value] of Object.entries(outValues)) {
		merged[portID] = { ...merged[portID], out: value }
	}
	return merged
}
