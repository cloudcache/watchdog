import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, CableIcon, PencilIcon, RefreshCwIcon, SlidersHorizontalIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { GraphContextProvider, GraphPanelRenderer, type GraphDashboard } from "@/components/graph/graph-panel-renderer"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { formatBitsPerSecond } from "@/lib/metric-format"
import { cn } from "@/lib/utils"

type NetworkDevice = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	SysName?: string
	sys_name?: string
	TargetID?: string
	target_id?: string
	Vendor?: string
	vendor?: string
	Model?: string
	model?: string
}

type NetworkPort = {
	ID?: string
	id?: string
	DeviceID?: string
	device_id?: string
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
	Metadata?: Record<string, string>
	metadata?: Record<string, string>
}

type NetworkPortTransceiver = {
	ModuleType?: string
	module_type?: string
	Vendor?: string
	vendor?: string
	Model?: string
	model?: string
	Serial?: string
	serial?: string
	WavelengthNM?: number
	wavelength_nm?: number
	DistanceM?: number
	distance_m?: number
	Connector?: string
	connector?: string
	UpdatedAt?: string
	updated_at?: string
}

type NetworkPortResponse = {
	port?: NetworkPort
	device?: NetworkDevice
	transceiver?: NetworkPortTransceiver | null
}

type PortDetailProps = {
	id: string
}

export default memo(({ id }: PortDetailProps) => {
	const { t } = useLingui()
	const [port, setPort] = useState<NetworkPort | null>(null)
	const [device, setDevice] = useState<NetworkDevice | null>(null)
	const [transceiver, setTransceiver] = useState<NetworkPortTransceiver | null>(null)
	const [dashboard, setDashboard] = useState<GraphDashboard | null>(null)
	const [rangeWindow, setRangeWindow] = useState("24h")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [data, dashboardData] = await Promise.all([
				pb.send<NetworkPortResponse>(`/api/v1/network/ports/${id}`, {}),
				pb.send<GraphDashboard>(`/api/v1/graph/ports/${id}/overview`, {}).catch(() => null),
			])
			setPort(data.port ?? null)
			setDevice(data.device ?? null)
			setTransceiver(data.transceiver ?? null)
			setDashboard(dashboardData)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load network port`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Network Port`} / Beszel`
		refresh()
	}, [refresh, t])

	const deviceID = device?.ID ?? device?.id ?? port?.DeviceID ?? port?.device_id ?? ""
	const targetID = device?.TargetID ?? device?.target_id ?? ""
	const title = port?.IfName ?? port?.if_name ?? port?.IfDescr ?? port?.if_descr ?? id

	const deletePort = async () => {
		if (!globalThis.confirm(t`Delete this network port?`)) {
			return
		}
		setLoading(true)
		setError("")
		try {
			await pb.send(`/api/v1/network/ports/${id}`, { method: "DELETE" })
			navigate(deviceID ? getPagePath($router, "network_device", { id: deviceID }) : getPagePath($router, "network"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete network port`)
			setLoading(false)
		}
	}

	return (
		<div className="grid gap-4">
			<div
				className={cn(
					"overflow-hidden rounded-md border border-border",
					(port?.OperStatus ?? port?.oper_status ?? "").toLowerCase() === "up" && "border-l-4 border-l-green-500",
					(port?.OperStatus ?? port?.oper_status ?? "").toLowerCase() === "down" && "border-l-4 border-l-red-500",
				)}
			>
				<div className="flex items-center justify-between gap-4 p-4">
					<div className="flex min-w-0 items-center gap-2">
						<Link
							href={deviceID ? getPagePath($router, "network_device", { id: deviceID }) : getPagePath($router, "network")}
							className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
							aria-label={t`Back to network device`}
						>
							<ArrowLeftIcon className="h-4 w-4" />
						</Link>
						<CableIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
						<div className="min-w-0">
							<div className="flex items-center gap-2">
								<h1 className="truncate text-lg font-semibold">{title}</h1>
								<StatusBadge label={t`Oper`} value={port?.OperStatus ?? port?.oper_status} />
							</div>
							<div className="mt-0.5 flex flex-wrap items-center gap-x-3 text-xs text-muted-foreground">
								{device?.SysName ?? device?.sys_name ? <span>{device.SysName ?? device.sys_name}</span> : null}
								{port?.SpeedBps ?? port?.speed_bps ? <span>{formatBitsPerSecond(port.SpeedBps ?? port.speed_bps)}</span> : null}
								{transceiver?.ModuleType ?? transceiver?.module_type ? <span>{transceiver.ModuleType ?? transceiver.module_type}</span> : null}
							</div>
						</div>
					</div>
					<div className="flex shrink-0 items-center gap-1.5">
						<Link href={getPagePath($router, "network_port_edit", { id })} className={cn(buttonVariants({ variant: "ghost", size: "sm" }))}>
							<PencilIcon className="h-4 w-4" />
						</Link>
						<Link href={getPagePath($router, "network_port_policy", { id })} className={cn(buttonVariants({ variant: "ghost", size: "sm" }))}>
							<SlidersHorizontalIcon className="h-4 w-4" />
						</Link>
						<Button variant="ghost" size="sm" onClick={deletePort} disabled={loading}>
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

			<div className="grid gap-3 md:grid-cols-4">
				<InfoCell label={t`Device`} value={device?.SysName ?? device?.sys_name ?? device?.Name ?? device?.name} />
				<InfoCell label={t`Vendor`} value={device?.Vendor ?? device?.vendor} />
				<InfoCell label={t`Model`} value={device?.Model ?? device?.model} />
				<InfoCell label={t`ifIndex`} value={formatNumber(port?.IfIndex ?? port?.if_index)} mono />
				<InfoCell label={t`Alias`} value={port?.IfAlias ?? port?.if_alias} />
				<InfoCell label={t`Description`} value={port?.IfDescr ?? port?.if_descr} />
				<InfoCell label={t`Speed`} value={formatBitsPerSecond(port?.SpeedBps ?? port?.speed_bps)} />
				<InfoCell label={t`Metadata`} value={formatMetadata(port?.Metadata ?? port?.metadata)} />
				<InfoCell label={t`Module`} value={transceiverModule(transceiver)} />
				<InfoCell label={t`Serial`} value={transceiver?.Serial ?? transceiver?.serial} mono />
				<InfoCell label={t`Wavelength`} value={formatNM(transceiver?.WavelengthNM ?? transceiver?.wavelength_nm)} />
				<InfoCell label={t`Distance`} value={formatMeters(transceiver?.DistanceM ?? transceiver?.distance_m)} />
				<div className="rounded-md border border-border p-3">
					<div className="text-xs text-muted-foreground">
						<Trans>Status</Trans>
					</div>
					<div className="mt-2 flex gap-2">
						<StatusBadge label={t`Admin`} value={port?.AdminStatus ?? port?.admin_status} />
						<StatusBadge label={t`Oper`} value={port?.OperStatus ?? port?.oper_status} />
					</div>
				</div>
			</div>

			<GraphContextProvider
				value={{
					deviceId: deviceID,
					targetId: targetID,
					portIds: [id],
					valueMode: "corrected",
				}}
			>
				<div className="flex justify-end">
					<Select value={rangeWindow} onValueChange={setRangeWindow}>
						<SelectTrigger className="w-28">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="5m">5m</SelectItem>
							<SelectItem value="15m">15m</SelectItem>
							<SelectItem value="1h">1h</SelectItem>
							<SelectItem value="6h">6h</SelectItem>
							<SelectItem value="24h">24h</SelectItem>
							<SelectItem value="7d">7d</SelectItem>
							<SelectItem value="30d">30d</SelectItem>
						</SelectContent>
					</Select>
				</div>
				{dashboard
					? dashboard.panels.map((panel) => (
							<GraphPanelRenderer
								key={panel.id}
								panel={panel}
								range={rangeWindow}
								refreshInterval={dashboard.refresh}
							/>
						))
					: null}
			</GraphContextProvider>
		</div>
	)
})

function InfoCell({ label, value, mono }: { label: string; value?: string; mono?: boolean }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className={cn("mt-1 truncate text-sm", mono && "font-mono text-xs")}>{value || "—"}</div>
		</div>
	)
}

function StatusBadge({ label, value }: { label: string; value?: string }) {
	const normalized = value?.toLowerCase() ?? ""
	const variant: "success" | "danger" | "outline" =
		normalized === "up" || normalized === "1"
			? "success"
			: normalized === "down" || normalized === "2"
				? "danger"
				: "outline"
	return (
		<Badge variant={variant}>
			{label}: {value || "—"}
		</Badge>
	)
}

function formatNumber(value?: number) {
	return typeof value === "number" ? String(value) : undefined
}

function formatMetadata(metadata?: Record<string, string>) {
	if (!metadata || Object.keys(metadata).length === 0) {
		return undefined
	}
	return Object.entries(metadata)
		.map(([key, value]) => `${key}=${value}`)
		.join(", ")
}

function transceiverModule(transceiver: NetworkPortTransceiver | null) {
	if (!transceiver) {
		return undefined
	}
	return [transceiver.ModuleType ?? transceiver.module_type, transceiver.Vendor ?? transceiver.vendor, transceiver.Model ?? transceiver.model]
		.filter(Boolean)
		.join(" ")
}

function formatNM(value?: number) {
	return typeof value === "number" && value > 0 ? `${value} nm` : undefined
}

function formatMeters(value?: number) {
	return typeof value === "number" && value > 0 ? `${value} m` : undefined
}

