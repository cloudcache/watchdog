import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, FileDownIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { TrafficViewSwitcher } from "@/components/traffic-view-switcher"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { isAdmin, pb } from "@/lib/api"
import {
	trafficViewExportAggregation,
	trafficViewExportStep,
	trafficViewLabel,
	trafficViewValueMode,
	type TrafficViewMode,
} from "@/lib/traffic-view"
import { cn } from "@/lib/utils"

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Host?: string
	host?: string
}

type TargetsResponse = {
	items?: TargetRecord[]
}

type NetworkDevice = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	Name?: string
	name?: string
	SysName?: string
	sys_name?: string
}

type NetworkPort = {
	ID?: string
	id?: string
	IfName?: string
	if_name?: string
	IfAlias?: string
	if_alias?: string
	IfDescr?: string
	if_descr?: string
}

export default memo(() => {
	const { t } = useLingui()
	const [targets, setTargets] = useState<TargetRecord[]>([])
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [portsByDevice, setPortsByDevice] = useState<Record<string, NetworkPort[]>>({})
	const [trafficView, setTrafficView] = useState<TrafficViewMode>("customer")
	const [form, setForm] = useState({
		targetID: "",
		portID: "",
		rangeStart: defaultDateTime(-24),
		rangeEnd: defaultDateTime(0),
		step: trafficViewExportStep("customer"),
		aggregation: "p95_5m",
		valueMode: "corrected",
		format: "csv",
	})
	const [creating, setCreating] = useState(false)
	const [error, setError] = useState("")

	useEffect(() => {
		document.title = `${t`Create Export`} / Watchdog`
		Promise.all([
			pb.send<TargetsResponse>("/api/v1/targets", {}),
			pb.send<{ items?: NetworkDevice[] }>("/api/v1/network/devices", {}),
		])
			.then(([targetData, deviceData]) => {
				setTargets(targetData.items ?? [])
				setDevices(deviceData.items ?? [])
			})
			.catch(() => {
				setTargets([])
				setDevices([])
			})
	}, [t])

	const targetDevices = devices.filter((device) => deviceTargetID(device) === form.targetID)
	const targetPorts = targetDevices.flatMap((device) => portsByDevice[deviceID(device)] ?? [])
	const canUseRaw = isAdmin()

	const applyTrafficView = useCallback(
		(mode: TrafficViewMode) => {
			setTrafficView(mode)
			setForm((current) => ({
				...current,
				aggregation: trafficViewExportAggregation(mode),
				step: trafficViewExportStep(mode),
				valueMode: trafficViewValueMode(mode, canUseRaw),
			}))
		},
		[canUseRaw]
	)

	useEffect(() => {
		setForm((current) => ({ ...current, portID: "" }))
		if (!form.targetID) {
			return
		}
		const missingDeviceIDs = targetDevices.map(deviceID).filter((id) => id && !portsByDevice[id])
		if (missingDeviceIDs.length === 0) {
			return
		}
		Promise.all(
			missingDeviceIDs.map(async (id) => {
				const data = await pb.send<{ items?: NetworkPort[] }>(`/api/v1/network/devices/${id}/ports`, {})
				return [id, data.items ?? []] as const
			})
		)
			.then((loaded) => {
				setPortsByDevice((current) => {
					const next = { ...current }
					for (const [id, ports] of loaded) {
						next[id] = ports
					}
					return next
				})
			})
			.catch(() => {})
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [form.targetID])

	const createExport = useCallback(async () => {
		setCreating(true)
		setError("")
		try {
			const task = await pb.send<{ ID?: string; id?: string }>("/api/v1/exports", {
				method: "POST",
				body: {
					TargetID: form.targetID,
					PortID: form.portID,
					PeriodType: "custom",
					RangeStart: toISOString(form.rangeStart),
					RangeEnd: toISOString(form.rangeEnd),
					Step: Number(form.step),
					Aggregation: form.aggregation,
					ValueMode: form.valueMode,
					Format: form.format,
				},
			})
			const id = task.ID ?? task.id ?? ""
			navigate(id ? getPagePath($router, "export_detail", { id }) : getPagePath($router, "exports"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to create export`)
		} finally {
			setCreating(false)
		}
	}, [form, t])

	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<Link
					href={getPagePath($router, "exports")}
					className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
					aria-label={t`Back to exports`}
				>
					<ArrowLeftIcon className="h-4 w-4" />
				</Link>
				<FileDownIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
				<h1 className="text-xl font-semibold tracking-normal">
					<Trans>Create Export</Trans>
				</h1>
			</div>

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="flex flex-wrap items-center justify-between gap-3">
					<TrafficViewSwitcher value={trafficView} onChange={applyTrafficView} allowRaw={canUseRaw} />
					<Link
						href={getPagePath($router, "aggregate_charts")}
						className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
					>
						<Trans>Open Charts</Trans>
					</Link>
				</div>
				<div className="grid gap-3 md:grid-cols-4">
					<ViewSummary label={t`View`} value={trafficViewLabel(trafficView)} />
					<ViewSummary label={t`Value`} value={form.valueMode} />
					<ViewSummary label={t`Aggregation`} value={form.aggregation} />
					<ViewSummary label={t`Step`} value={form.step === "60000000000" ? "1m" : "5m"} />
				</div>
				<div className="grid gap-3 md:grid-cols-2">
					<Select
						value={form.targetID}
						onValueChange={(targetID) => setForm((current) => ({ ...current, targetID, portID: "" }))}
					>
						<SelectTrigger>
							<SelectValue placeholder={t`Target`} />
						</SelectTrigger>
						<SelectContent>
							{targets.map((target) => {
								const id = target.ID ?? target.id ?? ""
								return (
									<SelectItem key={id} value={id}>
										{target.Name ?? target.name ?? target.Host ?? target.host ?? id}
									</SelectItem>
								)
							})}
						</SelectContent>
					</Select>
					<Select
						value={form.portID || "__target__"}
						onValueChange={(portID) =>
							setForm((current) => ({ ...current, portID: portID === "__target__" ? "" : portID }))
						}
					>
						<SelectTrigger>
							<SelectValue placeholder={t`Port`} />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="__target__">
								<Trans>Target summary</Trans>
							</SelectItem>
							{targetPorts.map((port) => {
								const id = portID(port)
								return (
									<SelectItem key={id} value={id}>
										{portLabel(port)}
									</SelectItem>
								)
							})}
						</SelectContent>
					</Select>
					<Input
						type="datetime-local"
						value={form.rangeStart}
						onChange={(event) => setForm((current) => ({ ...current, rangeStart: event.target.value }))}
					/>
					<Input
						type="datetime-local"
						value={form.rangeEnd}
						onChange={(event) => setForm((current) => ({ ...current, rangeEnd: event.target.value }))}
					/>
				</div>
				<div className="flex flex-wrap items-center gap-2">
					<Select value={form.step} onValueChange={(step) => setForm((current) => ({ ...current, step }))}>
						<SelectTrigger className="w-28">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="60000000000">1m</SelectItem>
							<SelectItem value="300000000000">5m</SelectItem>
						</SelectContent>
					</Select>
					<Select
						value={form.aggregation}
						onValueChange={(aggregation) => setForm((current) => ({ ...current, aggregation }))}
					>
						<SelectTrigger className="w-44">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="p95_5m">p95_5m</SelectItem>
							<SelectItem value="avg_5m">avg_5m</SelectItem>
							<SelectItem value="fourth_peak_5m">fourth_peak_5m</SelectItem>
							<SelectItem value="daily_p95">daily_p95</SelectItem>
							<SelectItem value="daily_avg">daily_avg</SelectItem>
						</SelectContent>
					</Select>
					<Select
						value={form.valueMode}
						onValueChange={(valueMode) => setForm((current) => ({ ...current, valueMode }))}
					>
						<SelectTrigger className="w-36">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="corrected">corrected</SelectItem>
							{isAdmin() ? <SelectItem value="raw">raw</SelectItem> : null}
							{isAdmin() ? <SelectItem value="both">both</SelectItem> : null}
						</SelectContent>
					</Select>
					<Select value={form.format} onValueChange={(format) => setForm((current) => ({ ...current, format }))}>
						<SelectTrigger className="w-28">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="csv">csv</SelectItem>
						</SelectContent>
					</Select>
				</div>
				{error ? <div className="text-sm text-destructive">{error}</div> : null}
				<div>
					<Button onClick={createExport} disabled={creating || !form.targetID}>
						<FileDownIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Button>
					{!form.targetID ? (
						<div className="mt-2 text-sm text-muted-foreground">
							<Trans>Select a target before creating an export.</Trans>
						</div>
					) : null}
				</div>
			</div>
		</div>
	)
})

function ViewSummary({ label, value }: { label: string; value: string }) {
	return (
		<div className="rounded-md border border-border px-3 py-2">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className="mt-1 truncate text-sm font-medium">{value}</div>
		</div>
	)
}

function defaultDateTime(offsetHours: number) {
	const date = new Date(Date.now() + offsetHours * 60 * 60 * 1000)
	return toDateTimeLocal(date)
}

function toDateTimeLocal(date: Date) {
	const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
	return local.toISOString().slice(0, 16)
}

function toISOString(value: string) {
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toISOString()
}

function deviceID(device: NetworkDevice) {
	return device.ID ?? device.id ?? ""
}

function deviceTargetID(device: NetworkDevice) {
	return device.TargetID ?? device.target_id ?? ""
}

function portID(port: NetworkPort) {
	return port.ID ?? port.id ?? ""
}

function portLabel(port: NetworkPort) {
	const name = port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? portID(port)
	const alias = port.IfAlias ?? port.if_alias
	return alias ? `${name} · ${alias}` : name
}
