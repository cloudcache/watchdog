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
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { isAdmin, pb } from "@/lib/api"
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
}

type NetworkPortsResponse = {
	items?: NetworkPort[]
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
}

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
}

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

export default memo(({ id }: DeviceDetailProps) => {
	const { t } = useLingui()
	const [device, setDevice] = useState<NetworkDevice | null>(null)
	const [target, setTarget] = useState<TargetRecord | null>(null)
	const [ports, setPorts] = useState<NetworkPort[]>([])
	const [bgpSessions, setBGPSessions] = useState<BGPSession[]>([])
	const [sensors, setSensors] = useState<NetworkDeviceSensor[]>([])
	const [chartWindow, setChartWindow] = useState("24h")
	const [portTraffic, setPortTraffic] = useState<Record<string, { in?: number; out?: number }>>({})
	const [dashboard, setDashboard] = useState<GraphDashboard | null>(null)
	const [activeTab, setActiveTab] = useState("overview")
	const [trafficView, setTrafficView] = useState<TrafficViewMode>("customer")
	const [loading, setLoading] = useState(true)
	const [rediscovering, setRediscovering] = useState(false)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [deviceData, portsData, bgpData, sensorData, targetsData, dashboardData] = await Promise.all([
				pb.send<NetworkDevice>(`/api/v1/network/devices/${id}`, {}),
				pb.send<NetworkPortsResponse>(`/api/v1/network/devices/${id}/ports`, {}),
				pb.send<BGPSessionsResponse>(`/api/v1/network/devices/${id}/bgp`, {}),
				pb.send<NetworkDeviceSensorsResponse>(`/api/v1/network/devices/${id}/sensors`, {}).catch(() => ({ items: [] })),
				pb.send<TargetsResponse>("/api/v1/targets", {}),
				pb.send<GraphDashboard>(`/api/v1/graph/devices/${id}/overview`, {}).catch(() => null),
			])
			setDevice(deviceData)
			const targetID = deviceData.TargetID ?? deviceData.target_id ?? ""
			setTarget((targetsData.items ?? []).find((item) => (item.ID ?? item.id) === targetID) ?? null)
			setPorts(portsData.items ?? [])
			setBGPSessions(bgpData.items ?? [])
			setSensors(sensorData.items ?? [])
			setDashboard(dashboardData)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load network device`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Network Device`} / WatchDog`
		refresh()
	}, [refresh, t])

	const targetID = device?.TargetID ?? device?.target_id ?? ""
	const deviceID = device?.ID ?? device?.id ?? id
	const portIDs = ports.map((port) => port.ID ?? port.id ?? "").filter(Boolean)

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
	}, [targetID, deviceID, portIDs.join(","), trafficView])

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
			await pb.send(`/api/v1/aggregate-graphs`, {
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
			await pb.send(`/api/v1/aggregate-graphs/${graphID}/items`, {
				method: "PUT",
				body: {
					items: [
						{ metric: "watchdog_snmp_if_in_bps", direction: "in", label: "In", total: true },
						{ metric: "watchdog_snmp_if_out_bps", direction: "out", label: "Out", total: true },
					],
				},
			})
			await pb.send(`/api/v1/aggregate-graphs/${graphID}/ports`, {
				method: "PUT",
				body: { ports: effectivePortIDs.map((portID) => ({ PortID: portID })) },
			})
			navigate(getPagePath($router, "aggregate_graph", { id: graphID }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save aggregate graph`)
		}
	}

	const deleteDevice = async () => {
		if (!globalThis.confirm(t`Delete this network device?`)) {
			return
		}
		setLoading(true)
		setError("")
		try {
			await pb.send(`/api/v1/network/devices/${id}`, { method: "DELETE" })
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
			await pb.send(`/api/v1/network/devices/${id}/snmp/discover`, { method: "POST" })
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
								{firstText(device?.SysLocation, device?.sys_location) ? (
									<span>{firstText(device?.SysLocation, device?.sys_location)}</span>
								) : null}
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
						<Button variant="ghost" size="sm" onClick={deleteDevice} disabled={loading}>
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
					<span>{trafficViewLabel(trafficView)}</span>
					<span>{trafficViewValueMode(trafficView, isAdmin())}</span>
				</div>
			</div>

			<GraphContextProvider
				value={{
					deviceId: deviceID,
					targetId: targetID,
					portIds: portIDs,
					trafficView,
					valueMode: trafficViewValueMode(trafficView, isAdmin()),
				}}
			>
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
										/>
									))
							: null}
						<PortStatusOverview
							ports={ports}
							selectedPortIDs={portIDs}
							portTraffic={portTraffic}
							trafficView={trafficView}
						/>
						<DeviceStatusSummary ports={ports} sensors={sensors} bgpSessions={bgpSessions} />
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
						<DeviceHealthPanel ports={ports} sensors={sensors} bgpSessions={bgpSessions} />
						<SensorsTable loading={loading} sensors={sensors} />
					</TabsContent>

					<TabsContent value="ports">
						<PortsTable
							loading={loading}
							ports={ports}
							portTraffic={portTraffic}
							deviceID={deviceID}
							targetID={targetID}
							trafficView={trafficView}
						/>
					</TabsContent>

					<TabsContent value="vlans" className="grid gap-3">
						<DeviceVLANsLAG deviceId={id} />
					</TabsContent>

					<TabsContent value="bgp" className="grid gap-3">
						<BGPSessionsTable loading={loading} bgpSessions={bgpSessions} />
					</TabsContent>

					<TabsContent value="inventory" className="grid gap-3">
						<DeviceOverview
							device={device}
							targetID={targetID}
							targetName={targetName}
							targetHost={targetHost}
							targetStatus={targetStatus}
							targetLabels={formatTargetLabels(target)}
						/>
						<DeviceInventory deviceId={id} />
					</TabsContent>

					<TabsContent value="logs" className="grid gap-3">
						<DeviceEventLog deviceId={id} targetID={targetID} />
					</TabsContent>

					<TabsContent value="alerts" className="grid gap-3">
						<DeviceAlertLog
							deviceId={id}
							targetID={targetID}
							portCount={ports.length}
							sensorCount={sensors.length}
							bgpCount={bgpSessions.length}
						/>
					</TabsContent>
				</Tabs>
			</GraphContextProvider>
		</div>
	)
})

function DeviceStatusSummary({
	ports,
	sensors,
	bgpSessions,
}: {
	ports: NetworkPort[]
	sensors: NetworkDeviceSensor[]
	bgpSessions: BGPSession[]
}) {
	const upPorts = ports.filter((port) => {
		const s = (port.OperStatus ?? port.oper_status ?? "").toString().toLowerCase()
		return s === "up" || s === "1"
	}).length
	const downPorts = ports.filter((port) => {
		const s = (port.OperStatus ?? port.oper_status ?? "").toString().toLowerCase()
		return s === "down" || s === "2"
	}).length
	const establishedBGP = bgpSessions.filter(
		(session) => (session.State ?? session.state ?? "").toLowerCase() === "established"
	).length
	return (
		<div className="grid gap-3 md:grid-cols-4">
			<SummaryTile
				label={<Trans>Ports</Trans>}
				value={`${upPorts}/${ports.length}`}
				detail={<Trans>up / total</Trans>}
			/>
			<SummaryTile label={<Trans>Down Ports</Trans>} value={String(downPorts)} detail={<Trans>oper down</Trans>} />
			<SummaryTile label={<Trans>Sensors</Trans>} value={String(sensors.length)} detail={<Trans>discovered</Trans>} />
			<SummaryTile
				label={<Trans>BGP</Trans>}
				value={`${establishedBGP}/${bgpSessions.length}`}
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
	sensors,
	bgpSessions,
}: {
	ports: NetworkPort[]
	sensors: NetworkDeviceSensor[]
	bgpSessions: BGPSession[]
}) {
	const downPorts = ports.filter((port) => {
		const s = (port.OperStatus ?? port.oper_status ?? "").toString().toLowerCase()
		return s === "down" || s === "2"
	})
	const disabledPorts = ports.filter((port) => {
		const s = (port.AdminStatus ?? port.admin_status ?? "").toString().toLowerCase()
		return s === "down" || s === "2"
	})
	const sensorProblems = sensors.filter((sensor) => !isHealthyStatus(sensor.Status ?? sensor.status))
	const bgpProblems = bgpSessions.filter(
		(session) => (session.State ?? session.state ?? "").toLowerCase() !== "established"
	)
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
					value={String(sensorProblems.length)}
					detail={<Trans>not ok</Trans>}
				/>
				<SummaryTile
					label={<Trans>BGP Alerts</Trans>}
					value={String(bgpProblems.length)}
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

function SensorsTable({ loading, sensors }: { loading: boolean; sensors: NetworkDeviceSensor[] }) {
	const grouped = useMemo(() => {
		const map: Record<string, NetworkDeviceSensor[]> = {}
		for (const s of sensors) {
			const cls = (s.Class ?? s.class ?? "other").toLowerCase()
			if (!map[cls]) {
				map[cls] = []
			}
			map[cls].push(s)
		}
		return Object.entries(map).sort(([a], [b]) => a.localeCompare(b))
	}, [sensors])

	if (loading) {
		return (
			<div className="text-sm text-muted-foreground">
				<Trans>Loading...</Trans>
			</div>
		)
	}
	if (sensors.length === 0) {
		return (
			<div className="flex h-[120px] items-center justify-center rounded-md border border-border bg-muted/20 text-sm text-muted-foreground">
				<Trans>No sensors discovered.</Trans>
			</div>
		)
	}
	return (
		<div className="grid gap-3">
			{grouped.map(([cls, items]) => (
				<div key={cls} className="overflow-hidden rounded-md bg-card">
					<div className="flex items-center justify-between border-b border-border bg-muted/30 px-4 py-2">
						<span className="text-sm font-medium capitalize">{cls}</span>
						<Badge variant="outline">{items.length}</Badge>
					</div>
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>
									<Trans>Sensor</Trans>
								</TableHead>
								<TableHead>
									<Trans>Status</Trans>
								</TableHead>
								<TableHead className="text-right">
									<Trans>Value</Trans>
								</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{items.map((sensor) => (
								<TableRow key={sensor.ID ?? sensor.id}>
									<TableCell className="font-medium">{sensor.Name ?? sensor.name ?? "—"}</TableCell>
									<TableCell>
										<StatusBadge value={sensor.Status ?? sensor.status} />
									</TableCell>
									<TableCell className="text-right font-mono text-xs">{formatSensorValue(sensor)}</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>
			))}
		</div>
	)
}

function PortsTable({
	loading,
	ports,
	portTraffic,
	deviceID: _deviceID,
	targetID: _targetID,
	trafficView,
}: {
	loading: boolean
	ports: NetworkPort[]
	portTraffic: Record<string, { in?: number; out?: number }>
	deviceID: string
	targetID: string
	trafficView: TrafficViewMode
}) {
	const { t } = useLingui()
	const [search, setSearch] = useState("")
	const [operFilter, setOperFilter] = useState("all")
	const filtered = useMemo(() => {
		const q = search.trim().toLowerCase()
		return ports.filter((port) => {
			const oper = (port.OperStatus ?? port.oper_status ?? "").toLowerCase()
			if (operFilter === "up" && oper !== "up") return false
			if (operFilter === "down" && oper !== "down") return false
			if (!q) return true
			const name = (port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? "").toLowerCase()
			const alias = (port.IfAlias ?? port.if_alias ?? "").toLowerCase()
			return name.includes(q) || alias.includes(q)
		})
	}, [ports, search, operFilter])
	const upCount = ports.filter((p) => (p.OperStatus ?? p.oper_status ?? "").toLowerCase() === "up").length

	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center gap-2">
				<Input
					value={search}
					onChange={(e) => setSearch(e.target.value)}
					placeholder={t`Search ports...`}
					className="max-w-xs"
				/>
				<Select value={operFilter} onValueChange={setOperFilter}>
					<SelectTrigger className="w-28">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">
							{t`All`} ({ports.length})
						</SelectItem>
						<SelectItem value="up">
							{t`Up`} ({upCount})
						</SelectItem>
						<SelectItem value="down">
							{t`Down`} ({ports.length - upCount})
						</SelectItem>
					</SelectContent>
				</Select>
			</div>
			<div className="overflow-hidden rounded-md bg-card">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>
								<Trans>Port</Trans>
							</TableHead>
							<TableHead>
								<Trans>Alias</Trans>
							</TableHead>
							<TableHead>
								<Trans>Oper</Trans>
							</TableHead>
							<TableHead className="text-right">
								<Trans>In</Trans>
							</TableHead>
							<TableHead className="text-right">
								<Trans>Out</Trans>
							</TableHead>
							<TableHead>
								<Trans>Speed</Trans>
							</TableHead>
							<TableHead className="text-right">
								<Trans>Actions</Trans>
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{loading ? (
							<TableRow>
								<TableCell colSpan={7} className="text-muted-foreground">
									<Trans>Loading...</Trans>
								</TableCell>
							</TableRow>
						) : filtered.length === 0 ? (
							<TableRow>
								<TableCell colSpan={7} className="text-muted-foreground">
									{search || operFilter !== "all" ? t`No ports match the filter.` : t`No ports found.`}
								</TableCell>
							</TableRow>
						) : (
							filtered.map((port) => {
								const portID = port.ID ?? port.id ?? ""
								const traffic = portTraffic[portID] ?? {}
								const speed = port.SpeedBps ?? port.speed_bps ?? 0
								const rateBase = trafficViewRateBase(trafficView)
								return (
									<TableRow key={portID}>
										<TableCell className="font-medium">
											<Link className="hover:underline" href={getPagePath($router, "network_port", { id: portID })}>
												{port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? "—"}
											</Link>
										</TableCell>
										<TableCell className="max-w-[14rem] truncate">{port.IfAlias ?? port.if_alias ?? "—"}</TableCell>
										<TableCell>
											<StatusBadge value={port.OperStatus ?? port.oper_status} />
										</TableCell>
										<TableCell className="text-right font-mono text-xs">
											{traffic.in != null ? <TrafficCell bps={traffic.in} speed={speed} rateBase={rateBase} /> : "—"}
										</TableCell>
										<TableCell className="text-right font-mono text-xs">
											{traffic.out != null ? <TrafficCell bps={traffic.out} speed={speed} rateBase={rateBase} /> : "—"}
										</TableCell>
										<TableCell className="text-xs">{formatBitsPerSecond(speed)}</TableCell>
										<TableCell className="text-right">
											<div className="flex justify-end gap-1">
												<Link
													href={getPagePath($router, "network_port_edit", { id: portID })}
													className={cn(buttonVariants({ variant: "ghost", size: "sm" }))}
												>
													<PencilIcon className="h-3.5 w-3.5" />
												</Link>
												<Link
													href={getPagePath($router, "network_port_policy", { id: portID })}
													className={cn(buttonVariants({ variant: "ghost", size: "sm" }))}
												>
													<SlidersHorizontalIcon className="h-3.5 w-3.5" />
												</Link>
											</div>
										</TableCell>
									</TableRow>
								)
							})
						)}
					</TableBody>
				</Table>
			</div>
		</div>
	)
}

function TrafficCell({ bps, speed, rateBase }: { bps: number; speed: number; rateBase: number }) {
	const pct = speed > 0 ? (bps / speed) * 100 : 0
	const color =
		pct >= 80
			? "text-red-600 dark:text-red-400"
			: pct >= 50
				? "text-yellow-600 dark:text-yellow-400"
				: "text-foreground"
	return <span className={color}>{formatBitsPerSecond(bps, rateBase)}</span>
}

function BGPSessionsTable({ loading, bgpSessions }: { loading: boolean; bgpSessions: BGPSession[] }) {
	return (
		<div className="overflow-hidden rounded-md bg-card">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead>
							<Trans>BGP Peer</Trans>
						</TableHead>
						<TableHead>
							<Trans>State</Trans>
						</TableHead>
						<TableHead>
							<Trans>Peer AS</Trans>
						</TableHead>
						<TableHead>AFI/SAFI</TableHead>
						<TableHead>
							<Trans>Accepted</Trans>
						</TableHead>
						<TableHead>
							<Trans>Advertised</Trans>
						</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{loading ? (
						<TableRow>
							<TableCell colSpan={6} className="text-muted-foreground">
								<Trans>Loading...</Trans>
							</TableCell>
						</TableRow>
					) : bgpSessions.length === 0 ? (
						<TableRow>
							<TableCell colSpan={6} className="text-muted-foreground">
								<Trans>No BGP sessions found.</Trans>
							</TableCell>
						</TableRow>
					) : (
						bgpSessions.map((session) => (
							<TableRow key={session.ID ?? session.id}>
								<TableCell className="font-mono text-xs">{session.PeerAddr ?? session.peer_addr ?? "—"}</TableCell>
								<TableCell>
									<StatusBadge value={session.State ?? session.state} />
								</TableCell>
								<TableCell>{formatNumber(session.PeerAS ?? session.peer_as)}</TableCell>
								<TableCell>
									{session.AFI ?? session.afi ?? "—"}/{session.SAFI ?? session.safi ?? "—"}
								</TableCell>
								<TableCell>{formatNumber(session.AcceptedPrefixes ?? session.accepted_prefixes)}</TableCell>
								<TableCell>{formatNumber(session.AdvertisedPrefixes ?? session.advertised_prefixes)}</TableCell>
							</TableRow>
						))
					)}
				</TableBody>
			</Table>
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
}: {
	device: NetworkDevice | null
	targetID: string
	targetName: string
	targetHost: string
	targetStatus: string
	targetLabels: string
}) {
	const { t } = useLingui()
	const vendor = firstText(device?.Vendor, device?.vendor)
	const hardware = firstText(device?.Model, device?.model, device?.Platform, device?.platform)
	const platform = firstText(device?.Platform, device?.platform)
	const platformRow = platform && platform !== hardware ? platform : ""
	const location = firstText(device?.SysLocation, device?.sys_location)
	const sysDescr = firstText(device?.SysDescr, device?.sys_descr)
	const systemName = firstText(device?.SysName, device?.sys_name)
	const objectID = firstText(device?.SysObjectID, device?.sys_object_id)
	const uptime = formatDeviceUptime(device?.Uptime ?? device?.uptime)
	const snmpProfile = firstText(device?.SNMPProfileID, device?.snmp_profile_id)
	const snmpPort = formatNumber(device?.SNMPPort ?? device?.snmp_port)
	const logo = vendorLogoFor(vendor)
	return (
		<div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_360px]">
			<div className="overflow-hidden rounded-md border border-border">
				<div className="flex items-start gap-4 border-b border-border bg-muted/30 p-4">
					{logo ? <img src={logo} alt={vendor || "vendor"} className="h-14 w-20 shrink-0 object-contain" /> : null}
					<div className="min-w-0">
						<div className="text-base font-semibold leading-6">
							{hardware || platform || vendor || targetName || "—"}
						</div>
						{sysDescr ? <div className="mt-1 break-words text-sm text-muted-foreground">{sysDescr}</div> : null}
					</div>
				</div>
				<OverviewTable
					rows={[
						[t`System Name`, systemName],
						[t`Hostname`, targetHost, true],
						[t`Hardware`, hardware],
						[t`Operating System`, deviceOS(device)],
						[t`Platform`, platformRow],
						["Object ID", objectID, true],
						[t`Uptime`, uptime],
						[t`Location`, location],
						[t`sysDescr`, sysDescr?.slice(0, 80)],
					]}
				/>
			</div>
			<div className="overflow-hidden rounded-md border border-border">
				<div className="border-b border-border bg-muted/30 px-4 py-3 text-sm font-semibold">
					<Trans>Collection Endpoint</Trans>
				</div>
				<OverviewTable
					rows={[
						[t`Target`, targetName || targetID, true],
						[t`Host`, targetHost, true],
						[t`Status`, targetStatus],
						[t`Labels`, targetLabels],
						["SNMP Profile", snmpProfile, true],
						["SNMP Port", snmpPort, true],
						[t`Vendor`, vendor],
						[t`SNMP Version`, firstText(device?.SNMPProfileID, device?.snmp_profile_id) ? "v2c" : ""],
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
				<div key={label} className="grid grid-cols-[150px_minmax(0,1fr)]">
					<div className="bg-muted/20 px-4 py-2 text-muted-foreground">{label}</div>
					<div className={cn("min-w-0 px-4 py-2", mono && "font-mono text-xs")}>{value}</div>
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

function isHealthyStatus(value?: string) {
	const normalized = value?.toLowerCase() ?? ""
	return (
		normalized === "" || normalized === "ok" || normalized === "up" || normalized === "normal" || normalized === "1"
	)
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
	const [vlans, setVlans] = useState<DeviceVLAN[]>([])
	const [lags, setLags] = useState<DeviceLAGGroup[]>([])
	const [loading, setLoading] = useState(true)

	useEffect(() => {
		let cancelled = false
		const load = async () => {
			try {
				const [vlanData, lagData] = await Promise.all([
					pb
						.send<{ items?: DeviceVLAN[] }>(`/api/v1/network/devices/${deviceId}/vlans`, {})
						.catch(() => ({ items: [] })),
					pb
						.send<{ items?: DeviceLAGGroup[] }>(`/api/v1/network/devices/${deviceId}/lags`, {})
						.catch(() => ({ items: [] })),
				])
				if (!cancelled) {
					setVlans(vlanData.items ?? [])
					setLags(lagData.items ?? [])
				}
			} finally {
				if (!cancelled) setLoading(false)
			}
		}
		load().catch(() => {
			if (!cancelled) setLoading(false)
		})
		return () => {
			cancelled = true
		}
	}, [deviceId])

	if (loading) {
		return (
			<div className="text-sm text-muted-foreground">
				<Trans>Loading...</Trans>
			</div>
		)
	}
	return (
		<div className="grid gap-3">
			{vlans.length > 0 ? (
				<div className="overflow-hidden rounded-md bg-card">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>
									<Trans>VLAN ID</Trans>
								</TableHead>
								<TableHead>
									<Trans>Name</Trans>
								</TableHead>
								<TableHead>
									<Trans>Status</Trans>
								</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{vlans.map((vlan, index) => (
								<TableRow key={index}>
									<TableCell className="font-mono">{vlan.VLANID ?? "—"}</TableCell>
									<TableCell className="font-medium">{vlan.Name || "—"}</TableCell>
									<TableCell>{vlan.Status || "—"}</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>
			) : (
				<div className="flex h-[80px] items-center justify-center rounded-md border border-border bg-muted/20 text-sm text-muted-foreground">
					<Trans>No VLANs discovered (Q-BRIDGE-MIB not supported or empty).</Trans>
				</div>
			)}
			{lags.length > 0 ? (
				<div className="overflow-hidden rounded-md bg-card">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>
									<Trans>Aggregate</Trans>
								</TableHead>
								<TableHead>
									<Trans>MAC Address</Trans>
								</TableHead>
								<TableHead>
									<Trans>Mode</Trans>
								</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{lags.map((lag, index) => (
								<TableRow key={index}>
									<TableCell className="font-mono">{lag.AggregateIndex ?? "—"}</TableCell>
									<TableCell className="font-mono text-xs">{lag.MACAddress || "—"}</TableCell>
									<TableCell>{lag.Mode || "—"}</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>
			) : (
				<div className="flex h-[80px] items-center justify-center rounded-md border border-border bg-muted/20 text-sm text-muted-foreground">
					<Trans>No LAG groups discovered (IEEE8023-LAG-MIB not supported or empty).</Trans>
				</div>
			)}
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
	const [events, setEvents] = useState<SNMPEventEntry[]>([])
	const [loading, setLoading] = useState(true)

	useEffect(() => {
		let cancelled = false
		const load = async () => {
			try {
				const data = await pb.send<{ items?: SNMPEventEntry[] }>(
					`/api/v1/network/devices/${deviceId}/events?limit=50`,
					{}
				)
				if (!cancelled) setEvents(data.items ?? [])
			} catch {
				if (!cancelled) setEvents([])
			} finally {
				if (!cancelled) setLoading(false)
			}
		}
		load()
		return () => {
			cancelled = true
		}
	}, [deviceId])

	if (loading)
		return (
			<div className="text-sm text-muted-foreground">
				<Trans>Loading...</Trans>
			</div>
		)
	if (events.length === 0) {
		return (
			<div className="flex h-[120px] items-center justify-center rounded-md border border-border bg-muted/20 text-sm text-muted-foreground">
				<Trans>No events yet. Events appear on interface status changes and SNMP traps.</Trans>
			</div>
		)
	}
	return (
		<div className="overflow-hidden rounded-md bg-card">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead>
							<Trans>Time</Trans>
						</TableHead>
						<TableHead>
							<Trans>Severity</Trans>
						</TableHead>
						<TableHead>
							<Trans>Type</Trans>
						</TableHead>
						<TableHead>
							<Trans>Message</Trans>
						</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{events.map((event, i) => (
						<TableRow key={event.ID ?? i}>
							<TableCell className="whitespace-nowrap text-xs">{formatRelative(event.OccurredAt)}</TableCell>
							<TableCell>
								<StatusBadge value={event.Severity} />
							</TableCell>
							<TableCell className="font-mono text-xs">{event.EventType ?? ""}</TableCell>
							<TableCell className="max-w-[28rem] truncate text-xs">{event.Message ?? ""}</TableCell>
						</TableRow>
					))}
				</TableBody>
			</Table>
		</div>
	)
}

function DeviceAlertLog({
	deviceId: _deviceId,
	targetID: _targetID,
	portCount,
	sensorCount,
	bgpCount,
}: {
	deviceId: string
	targetID: string
	portCount: number
	sensorCount: number
	bgpCount: number
}) {
	const { t } = useLingui()
	const alerts = useMemo(() => {
		const items: { severity: string; message: string }[] = []
		if (portCount > 0) {
			items.push({ severity: "info", message: t`${portCount} ports discovered` })
		}
		if (sensorCount > 0) {
			items.push({ severity: "info", message: t`${sensorCount} sensors discovered` })
		}
		if (bgpCount > 0) {
			items.push({ severity: "info", message: t`${bgpCount} BGP sessions discovered` })
		}
		return items
	}, [portCount, sensorCount, bgpCount, t])

	if (alerts.length === 0) {
		return (
			<div className="flex h-[120px] items-center justify-center rounded-md border border-border bg-muted/20 text-sm text-muted-foreground">
				<Trans>No alerts. Threshold-based alerting is not yet configured for this device.</Trans>
			</div>
		)
	}
	return (
		<div className="grid gap-2">
			{alerts.map((alert, i) => (
				<div key={i} className="flex items-center gap-3 rounded-md border border-border p-3">
					<div
						className={cn(
							"h-2 w-2 shrink-0 rounded-full",
							alert.severity === "info" ? "bg-blue-500" : alert.severity === "warning" ? "bg-yellow-500" : "bg-red-500"
						)}
					/>
					<span className="text-sm">{alert.message}</span>
				</div>
			))}
		</div>
	)
}

function formatRelative(isoTime?: string): string {
	if (!isoTime) return "—"
	const date = new Date(isoTime)
	if (Number.isNaN(date.getTime())) return "—"
	const diff = Date.now() - date.getTime()
	if (diff < 60_000) return "<1m ago"
	if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
	if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
	return date.toLocaleString()
}

function DeviceInventory({ deviceId }: { deviceId: string }) {
	const [entities, setEntities] = useState<PhysicalEntity[]>([])
	const [loading, setLoading] = useState(true)

	useEffect(() => {
		let cancelled = false
		const load = async () => {
			try {
				const data = await pb.send<{ items?: PhysicalEntity[] }>(`/api/v1/network/devices/${deviceId}/inventory`, {})
				if (!cancelled) {
					setEntities(data.items ?? [])
				}
			} catch {
				if (!cancelled) setEntities([])
			} finally {
				if (!cancelled) setLoading(false)
			}
		}
		load().catch(() => {
			if (!cancelled) setLoading(false)
		})
		return () => {
			cancelled = true
		}
	}, [deviceId])

	if (loading) {
		return (
			<div className="text-sm text-muted-foreground">
				<Trans>Loading inventory...</Trans>
			</div>
		)
	}
	if (entities.length === 0) {
		return (
			<div className="flex h-[120px] items-center justify-center rounded-md border border-border bg-muted/20 text-sm text-muted-foreground">
				<Trans>No physical entities discovered. Click Rediscover to probe ENTITY-MIB.</Trans>
			</div>
		)
	}
	return (
		<div className="overflow-hidden rounded-md bg-card">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead>
							<Trans>Name</Trans>
						</TableHead>
						<TableHead>
							<Trans>Class</Trans>
						</TableHead>
						<TableHead>
							<Trans>Model</Trans>
						</TableHead>
						<TableHead>
							<Trans>Serial</Trans>
						</TableHead>
						<TableHead>
							<Trans>Manufacturer</Trans>
						</TableHead>
						<TableHead>
							<Trans>HW Rev</Trans>
						</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{entities.map((entity, index) => (
						<TableRow key={index}>
							<TableCell className="font-medium">{entity.Name || entity.Description || "—"}</TableCell>
							<TableCell>{entity.Class || "—"}</TableCell>
							<TableCell>{entity.ModelName || "—"}</TableCell>
							<TableCell className="font-mono text-xs">{entity.SerialNumber || "—"}</TableCell>
							<TableCell>{entity.ManufacturerName || "—"}</TableCell>
							<TableCell className="font-mono text-xs">{entity.HardwareRevision || "—"}</TableCell>
						</TableRow>
					))}
				</TableBody>
			</Table>
		</div>
	)
}

// DeviceSavedGraphs links the saved aggregate graphs whose port membership
// includes this device, plus the full library.
function DeviceSavedGraphs({ deviceId }: { deviceId: string }) {
	const [graphs, setGraphs] = useState<{ ID?: string; Name?: string; Devices?: { ID?: string }[] }[]>([])

	useEffect(() => {
		let cancelled = false
		pb.send<{ items?: { ID?: string; Name?: string; Devices?: { ID?: string }[] }[] }>("/api/v1/aggregate-graphs", {})
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

async function queryMetric(baseParams: URLSearchParams, metric: string) {
	const params = new URLSearchParams(baseParams)
	params.set("metric", metric)
	return await pb.send<VMRangeResponse>(`/api/v1/metrics/query?${params.toString()}`, {})
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
