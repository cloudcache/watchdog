import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, CalendarPlusIcon, PencilIcon, ReceiptTextIcon, SaveIcon } from "lucide-react"
import { memo, useCallback, useEffect, useState } from "react"
import { $router, Link } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { cn } from "@/lib/utils"

type BillingAccount = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Status?: string
	status?: string
	BillingDay?: number
	billing_day?: number
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	QuotaBytes?: number
	quota_bytes?: number
	Notes?: string
	notes?: string
}

type BillingAccountPort = {
	PortID?: string
	port_id?: string
	Direction?: string
	direction?: string
}

type NetworkDevice = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	SysName?: string
	sys_name?: string
	TargetID?: string
	target_id?: string
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
	OperStatus?: string
	oper_status?: string
}

type BillingAccountDetailProps = {
	id: string
}

export default memo(({ id }: BillingAccountDetailProps) => {
	const { t } = useLingui()
	const [account, setAccount] = useState<BillingAccount | null>(null)
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [portsByDevice, setPortsByDevice] = useState<Record<string, NetworkPort[]>>({})
	const [selectedPorts, setSelectedPorts] = useState<Record<string, string>>({})
	const [period, setPeriod] = useState({
		id: createPeriodID(),
		rangeStart: defaultDateTime(-24 * 30),
		rangeEnd: defaultDateTime(0),
	})
	const [loading, setLoading] = useState(true)
	const [savingPorts, setSavingPorts] = useState(false)
	const [creatingPeriod, setCreatingPeriod] = useState(false)
	const [message, setMessage] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setMessage("")
		try {
			const [accountData, bindingData, deviceData] = await Promise.all([
				pb.send<BillingAccount>(`/api/v1/billing/accounts/${id}`, {}),
				pb
					.send<{ items?: BillingAccountPort[] }>(`/api/v1/billing/accounts/${id}/ports`, {})
					.catch(() => ({ items: [] })),
				pb.send<{ items?: NetworkDevice[] }>("/api/v1/network/devices", {}),
			])
			setAccount(accountData)
			setDevices(deviceData.items ?? [])
			setSelectedPorts(
				Object.fromEntries(
					(bindingData.items ?? []).map((port) => [billingPortID(port), billingPortDirection(port) || "max"])
				)
			)
			const loaded = await Promise.all(
				(deviceData.items ?? []).map(async (device) => {
					const deviceIDValue = deviceID(device)
					const data = await pb.send<{ items?: NetworkPort[] }>(`/api/v1/network/devices/${deviceIDValue}/ports`, {})
					return [deviceIDValue, data.items ?? []] as const
				})
			)
			setPortsByDevice(Object.fromEntries(loaded))
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to load billing account`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${t`Billing Account`} / Beszel`
		refresh()
	}, [refresh, t])

	const savePorts = async () => {
		setSavingPorts(true)
		setMessage("")
		try {
			await pb.send(`/api/v1/billing/accounts/${id}/ports`, {
				method: "PUT",
				body: {
					ports: Object.entries(selectedPorts).map(([portID, direction]) => ({
						PortID: portID,
						Direction: direction,
					})),
				},
			})
			setMessage(t`Saved`)
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to save billing ports`)
		} finally {
			setSavingPorts(false)
		}
	}

	const createPeriod = async () => {
		setCreatingPeriod(true)
		setMessage("")
		try {
			await pb.send(`/api/v1/billing/accounts/${id}/periods`, {
				method: "POST",
				body: {
					ID: period.id.trim(),
					RangeStart: toISOString(period.rangeStart),
					RangeEnd: toISOString(period.rangeEnd),
				},
			})
			setMessage(t`Created`)
			setPeriod((current) => ({ ...current, id: createPeriodID() }))
		} catch (err) {
			setMessage(err instanceof Error ? err.message : t`Failed to create billing period`)
		} finally {
			setCreatingPeriod(false)
		}
	}

	const name = account?.Name ?? account?.name ?? id
	const discoveredPorts = devices.flatMap((device) =>
		(portsByDevice[deviceID(device)] ?? []).map((port) => ({
			...port,
			deviceName: device.SysName ?? device.sys_name ?? device.Name ?? device.name ?? deviceID(device),
		}))
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "billing")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to billing`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<ReceiptTextIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">{name}</h1>
				</div>
				<Link
					href={getPagePath($router, "billing_edit", { id })}
					className={buttonVariants({ variant: "outline", size: "sm" })}
				>
					<PencilIcon className="me-2 h-4 w-4" />
					<Trans>Edit</Trans>
				</Link>
			</div>

			{message ? (
				<div className="rounded-md border border-border p-3 text-sm text-muted-foreground">{message}</div>
			) : null}

			<div className="grid gap-3 md:grid-cols-4">
				<InfoCell label={t`Status`} value={<AccountStatus value={account?.Status ?? account?.status} />} />
				<InfoCell label={t`Billing Day`} value={String(account?.BillingDay ?? account?.billing_day ?? "-")} />
				<InfoCell label={t`Aggregation`} value={account?.Aggregation ?? account?.aggregation ?? "-"} />
				<InfoCell label={t`Value`} value={account?.ValueMode ?? account?.value_mode ?? "corrected"} />
				<InfoCell label={t`Quota`} value={formatBytes(account?.QuotaBytes ?? account?.quota_bytes)} />
				<InfoCell label="ID" value={id} mono />
			</div>

			<div className="grid gap-4 lg:grid-cols-2">
				<div className="grid gap-3 rounded-md border border-border p-4">
					<div className="flex items-center justify-between gap-3">
						<h2 className="text-base font-medium">
							<Trans>Billing Ports</Trans>
						</h2>
						<Button size="sm" variant="outline" onClick={savePorts} disabled={loading || savingPorts}>
							<SaveIcon className="me-2 h-4 w-4" />
							<Trans>Save</Trans>
						</Button>
					</div>
					{discoveredPorts.length === 0 ? (
						<div className="rounded-md border border-border p-3 text-sm text-muted-foreground">
							<Trans>No discovered ports. Discover or import SNMP interfaces before computing traffic billing.</Trans>
						</div>
					) : (
						<div className="grid max-h-[420px] gap-2 overflow-auto pe-1">
							{discoveredPorts.map((port) => {
								const id = portID(port)
								const checked = id in selectedPorts
								return (
									<div key={id} className="grid gap-2 rounded-md border border-border p-3 md:grid-cols-[1fr_150px]">
										<label className="flex min-w-0 items-start gap-2">
											<Checkbox
												checked={checked}
												onCheckedChange={() =>
													setSelectedPorts((current) => {
														if (id in current) {
															const next = { ...current }
															delete next[id]
															return next
														}
														return { ...current, [id]: "max" }
													})
												}
											/>
											<span className="min-w-0">
												<span className="block truncate text-sm font-medium">{portLabel(port)}</span>
												<span className="block truncate text-xs text-muted-foreground">
													{port.deviceName} · {port.OperStatus ?? port.oper_status ?? "unknown"}
												</span>
											</span>
										</label>
										<Select
											value={selectedPorts[id] ?? "max"}
											onValueChange={(direction) =>
												setSelectedPorts((current) => ({ ...current, [id]: direction }))
											}
											disabled={!checked}
										>
											<SelectTrigger>
												<SelectValue />
											</SelectTrigger>
											<SelectContent>
												<SelectItem value="max">max(in,out)</SelectItem>
												<SelectItem value="sum">in + out</SelectItem>
												<SelectItem value="in">in</SelectItem>
												<SelectItem value="out">out</SelectItem>
											</SelectContent>
										</Select>
									</div>
								)
							})}
						</div>
					)}
					<div className="text-sm text-muted-foreground">
						{Object.keys(selectedPorts).length} <Trans>ports selected</Trans>
					</div>
				</div>

				<div className="grid gap-3 rounded-md border border-border p-4">
					<div className="flex items-center justify-between gap-3">
						<h2 className="text-base font-medium">
							<Trans>Billing Period</Trans>
						</h2>
						<Button
							size="sm"
							variant="outline"
							onClick={createPeriod}
							disabled={loading || creatingPeriod}
						>
							<CalendarPlusIcon className="me-2 h-4 w-4" />
							<Trans>Create</Trans>
						</Button>
					</div>
					<div className="grid gap-3 sm:grid-cols-2">
						<Field label={t`Range Start`}>
							<Input
								type="datetime-local"
								value={period.rangeStart}
								onChange={(event) => setPeriod((current) => ({ ...current, rangeStart: event.target.value }))}
							/>
						</Field>
						<Field label={t`Range End`}>
							<Input
								type="datetime-local"
								value={period.rangeEnd}
								onChange={(event) => setPeriod((current) => ({ ...current, rangeEnd: event.target.value }))}
							/>
						</Field>
					</div>
				</div>
			</div>
		</div>
	)
})

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function InfoCell({ label, value, mono }: { label: string; value?: React.ReactNode; mono?: boolean }) {
	return (
		<div className="rounded-md border border-border p-3">
			<div className="text-xs text-muted-foreground">{label}</div>
			<div className={cn("mt-1 truncate text-sm", mono && "font-mono text-xs")}>{value || "-"}</div>
		</div>
	)
}

function AccountStatus({ value }: { value?: string }) {
	const normalized = value?.toLowerCase() ?? ""
	const variant: "success" | "danger" | "outline" =
		normalized === "active" ? "success" : normalized === "paused" ? "danger" : "outline"
	return <Badge variant={variant}>{value || "-"}</Badge>
}

function createPeriodID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `period_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

function billingPortID(port: BillingAccountPort) {
	return port.PortID ?? port.port_id ?? ""
}

function billingPortDirection(port: BillingAccountPort) {
	return port.Direction ?? port.direction ?? ""
}

function deviceID(device: NetworkDevice) {
	return device.ID ?? device.id ?? ""
}

function portID(port: NetworkPort) {
	return port.ID ?? port.id ?? ""
}

function portLabel(port: NetworkPort) {
	const name = port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? portID(port)
	const alias = port.IfAlias ?? port.if_alias
	return alias ? `${name} · ${alias}` : name
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

function formatBytes(value?: number) {
	if (!value || value <= 0) {
		return "-"
	}
	if (value >= 1_000_000_000_000) {
		return `${(value / 1_000_000_000_000).toFixed(2)} TB`
	}
	if (value >= 1_000_000_000) {
		return `${(value / 1_000_000_000).toFixed(2)} GB`
	}
	if (value >= 1_000_000) {
		return `${(value / 1_000_000).toFixed(2)} MB`
	}
	return `${value} B`
}
