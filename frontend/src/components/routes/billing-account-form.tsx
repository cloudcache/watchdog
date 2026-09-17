import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, ReceiptTextIcon, SaveIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import { BYTES_PER_GB, formatBillingUnit, parseBillingUnit, reconciliationUnit } from "@/lib/billing-units"
import { cn } from "@/lib/utils"

type BillingAccount = {
	id: string
	party_id?: string
	name: string
	status: string
	measurement_type: "bandwidth" | "traffic"
	billing_method: "package_port" | "monthly_95th" | "daily_95th" | "monthly_average"
	algorithm: "95th" | "daily_95th" | "average" | "total"
	billing_day: number
	timezone: string
	direction: "in" | "out" | "agg"
	default_layer: "raw" | "supplier" | "customer"
	price_currency: string
	unit_price: string
	minimum_percent: number
	traffic_allowance_bytes?: number
	reconcile_abs: number
	reconcile_percent: number
	ref: string
	notes: string
}
type Party = { id: string; name: string; kind: string; status: string }
type BillingDirection = "in" | "out" | "agg"
type BillingPort = { port_id: string; direction: BillingDirection }
type NetworkDevice = {
	id?: string
	ID?: string
	name?: string
	Name?: string
	sys_name?: string
	SysName?: string
	host?: string
	Host?: string
}
type NetworkPort = {
	id?: string
	ID?: string
	if_index?: number
	IfIndex?: number
	if_name?: string
	IfName?: string
	if_alias?: string
	IfAlias?: string
	oper_status?: string
	OperStatus?: string
}
type Page<T> = { items?: T[]; total?: number }
type FormState = Omit<
	BillingAccount,
	"id" | "billing_day" | "minimum_percent" | "traffic_allowance_bytes" | "reconcile_abs" | "reconcile_percent"
> & {
	billing_day: string
	minimum_percent: string
	traffic_allowance_gb: string
	reconcile_absolute: string
	reconcile_percent: string
}

const emptyForm: FormState = {
	party_id: "",
	name: "",
	status: "active",
	measurement_type: "bandwidth",
	billing_method: "monthly_95th",
	algorithm: "95th",
	billing_day: "1",
	timezone: "UTC",
	direction: "agg",
	default_layer: "customer",
	price_currency: "CNY",
	unit_price: "0",
	minimum_percent: "0",
	traffic_allowance_gb: "",
	reconcile_absolute: "0",
	reconcile_percent: "5",
	ref: "",
	notes: "",
}

export default memo(({ id }: { id?: string }) => {
	const { t } = useLingui()
	const [form, setForm] = useState<FormState>(emptyForm)
	const [parties, setParties] = useState<Party[]>([])
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [portsByDevice, setPortsByDevice] = useState<Record<string, NetworkPort[]>>({})
	const [activeDeviceID, setActiveDeviceID] = useState("")
	const [deviceSearch, setDeviceSearch] = useState("")
	const [bindings, setBindings] = useState<Record<string, BillingDirection>>({})
	const [loading, setLoading] = useState(Boolean(id))
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	const etag = useRef("")
	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [partyPage, devicePage, detail] = await Promise.all([
				api.send<Page<Party>>("/api/v1/billing/parties", {
					query: { limit: 500, offset: 0, status: "active", sort: "name", order: "asc" },
				}),
				api.send<Page<NetworkDevice>>("/api/v1/devices", {
					query: { kind: "network", limit: 500, offset: 0 },
				}),
				id
					? api.send<{ account: BillingAccount; ports: BillingPort[] }>(`/api/v1/billing/accounts/${id}`, {
							onResponse: (raw) => {
								etag.current = raw.headers.get("ETag") ?? ""
							},
						})
					: Promise.resolve(null),
			])
			setParties(partyPage.items ?? [])
			const loadedDevices = devicePage.items ?? []
			setDevices(loadedDevices)
			const portEntries = await Promise.all(
				loadedDevices.map(async (device) => {
					const deviceID = networkDeviceID(device)
					if (!deviceID) return ["", [] as NetworkPort[]] as const
					const page = await api.send<Page<NetworkPort>>(`/api/v1/devices/${deviceID}/ports`, {
						query: { limit: 500, offset: 0 },
					})
					return [deviceID, page.items ?? []] as const
				})
			)
			const loadedPorts = Object.fromEntries(portEntries.filter(([deviceID]) => Boolean(deviceID)))
			setPortsByDevice(loadedPorts)
			const loadedBindings: Record<string, BillingDirection> = Object.fromEntries(
				(detail?.ports ?? []).map((port) => [port.port_id, port.direction])
			)
			setBindings(loadedBindings)
			const firstBoundDevice = loadedDevices.find((device) =>
				(loadedPorts[networkDeviceID(device)] ?? []).some((port) => networkPortID(port) in loadedBindings)
			)
			setActiveDeviceID(networkDeviceID(firstBoundDevice ?? loadedDevices[0]))
			if (!detail) return
			const item = detail.account
			setForm({
				party_id: item.party_id ?? "",
				name: item.name,
				status: item.status,
				measurement_type: item.measurement_type,
				billing_method: item.billing_method,
				algorithm: item.algorithm,
				billing_day: String(item.billing_day),
				timezone: item.timezone,
				direction: item.direction,
				default_layer: item.default_layer,
				price_currency: item.price_currency ?? "CNY",
				unit_price: item.unit_price ?? "0",
				minimum_percent: String(item.minimum_percent ?? 0),
				traffic_allowance_gb: formatBillingUnit(item.traffic_allowance_bytes, BYTES_PER_GB),
				reconcile_absolute: formatBillingUnit(item.reconcile_abs, reconciliationUnit(item.algorithm).multiplier),
				reconcile_percent: String(item.reconcile_percent),
				ref: item.ref,
				notes: item.notes,
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load billing account`)
		} finally {
			setLoading(false)
		}
	}, [id, t])
	useEffect(() => {
		document.title = `${id ? t`Edit Billing Account` : t`Create Billing Account`} / Watchdog`
		load()
	}, [id, load, t])
	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const activePorts = useMemo(() => portsByDevice[activeDeviceID] ?? [], [activeDeviceID, portsByDevice])
	const filteredDevices = useMemo(() => {
		const query = deviceSearch.trim().toLocaleLowerCase()
		if (!query) return devices
		return devices.filter((device) => networkDeviceSearchText(device).includes(query))
	}, [deviceSearch, devices])
	const selectedPortCount = Object.keys(bindings).length
	const selectedOnActiveDevice = activePorts.filter((port) => networkPortID(port) in bindings).length
	const allActivePortsSelected = activePorts.length > 0 && selectedOnActiveDevice === activePorts.length
	const togglePort = (portID: string) => {
		setBindings((current) => {
			const next = { ...current }
			if (portID in next) delete next[portID]
			else next[portID] = form.direction
			return next
		})
	}
	const toggleAllActivePorts = () => {
		setBindings((current) => {
			const next = { ...current }
			if (allActivePortsSelected) {
				for (const port of activePorts) delete next[networkPortID(port)]
			} else {
				for (const port of activePorts) {
					const portID = networkPortID(port)
					if (portID) next[portID] = next[portID] ?? form.direction
				}
			}
			return next
		})
	}
	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const trafficAllowance =
				form.measurement_type === "traffic"
					? parseBillingUnit(form.traffic_allowance_gb, BYTES_PER_GB)
					: undefined
			const reconcileAbs = parseBillingUnit(form.reconcile_absolute, reconciliationUnit(form.algorithm).multiplier)
			if ((form.measurement_type === "traffic" && trafficAllowance === undefined) || reconcileAbs === undefined) {
				throw new Error(t`Enter valid billing and reconciliation values`)
			}
			const body = {
				party_id: form.party_id || undefined,
				name: form.name.trim(),
				status: form.status,
				measurement_type: form.measurement_type,
				billing_method: form.billing_method,
				billing_day: Number(form.billing_day),
				timezone: form.timezone.trim(),
				direction: form.direction,
				default_layer: form.default_layer,
				price_currency: form.price_currency.trim().toUpperCase(),
				unit_price: form.unit_price.trim(),
				minimum_percent: Number(form.minimum_percent),
				traffic_allowance_bytes: trafficAllowance,
				reconcile_abs: reconcileAbs,
				reconcile_percent: Number(form.reconcile_percent),
				ref: form.ref.trim(),
				notes: form.notes.trim(),
				items: Object.entries(bindings).map(([port_id, direction]) => ({ port_id, direction })),
			}
			const saved = await api.send<BillingAccount>(id ? `/api/v1/billing/accounts/${id}` : "/api/v1/billing/accounts", {
				method: id ? "PATCH" : "POST",
				headers: id && etag.current ? { "If-Match": etag.current } : undefined,
				body,
			})
			navigate(getPagePath($router, "billing_detail", { id: saved.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save billing account`)
		} finally {
			setSaving(false)
		}
	}
	const remove = async () => {
		if (!id || !etag.current || !window.confirm(t`Delete this billing account?`)) return
		try {
			await api.send(`/api/v1/billing/accounts/${id}`, { method: "DELETE", headers: { "If-Match": etag.current } })
			navigate(getPagePath($router, "billing"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete billing account`)
		}
	}
	const minimumPercent = Number(form.minimum_percent)
	const validMinimumPercent =
		form.minimum_percent.trim() !== "" && Number.isFinite(minimumPercent) && minimumPercent >= 0 && minimumPercent <= 100
	const validTrafficAllowance =
		form.measurement_type !== "traffic" ||
		parseBillingUnit(form.traffic_allowance_gb, BYTES_PER_GB) !== undefined
	const validReconcileAbsolute =
		parseBillingUnit(form.reconcile_absolute, reconciliationUnit(form.algorithm).multiplier) !== undefined
	const reconcilePercent = Number(form.reconcile_percent)
	const validReconcilePercent =
		form.reconcile_percent.trim() !== "" &&
		Number.isFinite(reconcilePercent) &&
		reconcilePercent >= 0 &&
		reconcilePercent <= 100
	const validPrice = /^(0|[1-9][0-9]{0,13})(\.[0-9]{1,6})?$/.test(form.unit_price.trim())
	const canSave =
		form.name.trim() &&
		form.timezone.trim() &&
		Number(form.billing_day) >= 1 &&
		Number(form.billing_day) <= 31 &&
		validMinimumPercent &&
		validTrafficAllowance &&
		validReconcileAbsolute &&
		validReconcilePercent &&
		validPrice &&
		/^[A-Za-z]{3}$/.test(form.price_currency.trim()) &&
		selectedPortCount > 0
	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={id ? getPagePath($router, "billing_detail", { id }) : getPagePath($router, "billing")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to billing`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<ReceiptTextIcon className="h-5 w-5 text-muted-foreground" />
					<h1 className="truncate text-xl font-semibold">
						{id ? <Trans>Edit Billing Account</Trans> : <Trans>Create Billing Account</Trans>}
					</h1>
				</div>
				<div className="flex gap-2">
					{id ? (
						<Button size="sm" variant="destructive" onClick={remove}>
							<Trash2Icon className="me-2 h-4 w-4" />
							<Trans>Delete</Trans>
						</Button>
					) : null}
					<Button size="sm" onClick={save} disabled={loading || saving || !canSave}>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Save</Trans>
					</Button>
				</div>
			</div>
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="grid gap-4 rounded-md border border-border p-4 md:grid-cols-2 xl:grid-cols-3">
				<Field label={t`Name`}>
					<Input value={form.name} onChange={(event) => update({ name: event.target.value })} disabled={loading} />
				</Field>
				<Field label={t`Party`}>
					<Select
						value={form.party_id || "none"}
						onValueChange={(value) => update({ party_id: value === "none" ? "" : value })}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="none">—</SelectItem>
							{parties.map((party) => (
								<SelectItem key={party.id} value={party.id}>
									{party.name} ({party.kind})
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Status`}>
					<Select value={form.status} onValueChange={(status) => update({ status })}>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="active">active</SelectItem>
							<SelectItem value="paused">paused</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Measurement type`}>
					<Select
						value={form.measurement_type}
						onValueChange={(value: "bandwidth" | "traffic") => {
							const method = value === "traffic" ? "package_port" : form.billing_method
							update({
								measurement_type: value,
								billing_method: method,
								algorithm: billingAlgorithm(value, method),
								minimum_percent: method === "package_port" ? "0" : form.minimum_percent,
								reconcile_absolute: "0",
							})
						}}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="bandwidth">
								<Trans>Bandwidth</Trans>
							</SelectItem>
							<SelectItem value="traffic">
								<Trans>Traffic</Trans>
							</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Billing method`}>
					<Select
						value={form.billing_method}
						onValueChange={(value: "package_port" | "monthly_95th" | "daily_95th" | "monthly_average") =>
							update({
								billing_method: value,
								algorithm: billingAlgorithm(form.measurement_type, value),
								minimum_percent: value === "package_port" ? "0" : form.minimum_percent,
								reconcile_absolute: "0",
							})
						}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							{form.measurement_type === "bandwidth" ? (
								<>
									<SelectItem value="package_port">
										<Trans>Port package</Trans>
									</SelectItem>
									<SelectItem value="monthly_95th">
										<Trans>Monthly 95th</Trans>
									</SelectItem>
									<SelectItem value="daily_95th">
										<Trans>Daily 95th</Trans>
									</SelectItem>
									<SelectItem value="monthly_average">
										<Trans>Monthly average</Trans>
									</SelectItem>
								</>
							) : (
								<SelectItem value="package_port">
									<Trans>Port package</Trans>
								</SelectItem>
							)}
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Billing Day`}>
					<Input
						type="number"
						min="1"
						max="31"
						value={form.billing_day}
						onChange={(event) => update({ billing_day: event.target.value })}
					/>
				</Field>
				<Field label={t`Timezone`}>
					<Input
						value={form.timezone}
						onChange={(event) => update({ timezone: event.target.value })}
						placeholder="Asia/Singapore"
					/>
				</Field>
				<Field label={t`Direction`}>
					<Select value={form.direction} onValueChange={(value: "in" | "out" | "agg") => update({ direction: value })}>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="in">in</SelectItem>
							<SelectItem value="out">out</SelectItem>
							<SelectItem value="agg">in + out</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Value strategy`}>
					<Select
						value={form.default_layer}
						onValueChange={(value: "raw" | "supplier" | "customer") => update({ default_layer: value })}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="raw">
								<Trans>Raw</Trans>
							</SelectItem>
							<SelectItem value="supplier">
								<Trans>Supplier</Trans>
							</SelectItem>
							<SelectItem value="customer">
								<Trans>Customer</Trans>
							</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Currency`}>
					<Input
						maxLength={3}
						value={form.price_currency}
						onChange={(event) => update({ price_currency: event.target.value.toUpperCase() })}
						placeholder="CNY"
					/>
				</Field>
				<Field
					label={
						form.billing_method === "package_port"
							? t`Price per port / billing cycle`
							: form.billing_method === "daily_95th"
								? t`Price per Mbps / day`
								: t`Price per Mbps / billing cycle`
					}
				>
					<Input
						inputMode="decimal"
						value={form.unit_price}
						onChange={(event) => update({ unit_price: event.target.value })}
						placeholder="0.000000"
					/>
				</Field>
				{form.measurement_type === "bandwidth" && form.billing_method !== "package_port" ? (
					<Field
						label={t`Minimum usage (%)`}
						hint={t`Percentage of the combined nominal bandwidth of the selected ports.`}
					>
						<Input
							type="number"
							min="0"
							max="100"
							step="0.01"
							value={form.minimum_percent}
							onChange={(event) => update({ minimum_percent: event.target.value })}
						/>
					</Field>
				) : form.measurement_type === "traffic" ? (
					<Field label={t`Traffic allowance (GB)`} hint={t`Total traffic above this allowance is period overuse.`}>
						<Input
							type="number"
							min="0"
							step="0.001"
							value={form.traffic_allowance_gb}
							onChange={(event) => update({ traffic_allowance_gb: event.target.value })}
						/>
					</Field>
				) : null}
				<div className="md:col-span-2 xl:col-span-3">
					<h2 className="text-sm font-medium">
						<Trans>Reconciliation alert thresholds</Trans>
					</h2>
					<p className="text-xs text-muted-foreground">
						<Trans>
							These thresholds only flag differences between SNMP, Flow, and external evidence. They never change usage
							or price.
						</Trans>
					</p>
				</div>
				<Field label={t`Absolute difference threshold (${reconciliationUnit(form.algorithm).label})`}>
					<Input
						type="number"
						min="0"
						step="0.001"
						value={form.reconcile_absolute}
						onChange={(event) => update({ reconcile_absolute: event.target.value })}
					/>
				</Field>
				<Field label={t`Difference threshold (%)`}>
					<Input
						type="number"
						min="0"
						max="100"
						step="0.01"
						value={form.reconcile_percent}
						onChange={(event) => update({ reconcile_percent: event.target.value })}
					/>
				</Field>
				<Field label={t`Reference`}>
					<Input value={form.ref} onChange={(event) => update({ ref: event.target.value })} />
				</Field>
				<div className="md:col-span-2 xl:col-span-3">
					<Field label={t`Notes`}>
						<Textarea value={form.notes} onChange={(event) => update({ notes: event.target.value })} />
					</Field>
				</div>
			</div>
			<div className="grid gap-3 rounded-md border border-border p-4">
				<div className="flex items-center justify-between gap-3">
					<div>
						<h2 className="font-medium">
							<Trans>Billing ports and metrics</Trans>
						</h2>
						<p className="text-sm text-muted-foreground">
							<Trans>Select ports from any device. Each port can use inbound, outbound, or combined traffic.</Trans>
						</p>
					</div>
					<Badge variant="outline">
						<Trans>{selectedPortCount} selected</Trans>
					</Badge>
				</div>
				<div className="grid gap-3 md:grid-cols-2">
					<Field label={t`Search devices`}>
						<Input
							value={deviceSearch}
							onChange={(event) => setDeviceSearch(event.target.value)}
							placeholder={t`Search by device name or IP...`}
						/>
					</Field>
					<Field label={t`Device`}>
						<Select
							value={activeDeviceID}
							onValueChange={(value) => {
								setActiveDeviceID(value)
								setDeviceSearch("")
							}}
							disabled={devices.length === 0}
						>
							<SelectTrigger>
								<SelectValue placeholder={t`Select a device`} />
							</SelectTrigger>
							<SelectContent>
								{filteredDevices.map((device) => {
									const deviceID = networkDeviceID(device)
									return (
										<SelectItem key={deviceID} value={deviceID}>
											{networkDeviceOptionLabel(device)}
										</SelectItem>
									)
								})}
								{filteredDevices.length === 0 ? (
									<div className="px-2 py-1.5 text-sm text-muted-foreground">
										<Trans>No matching devices.</Trans>
									</div>
								) : null}
							</SelectContent>
						</Select>
					</Field>
				</div>
				{activePorts.length > 0 ? (
					<div className="flex items-center justify-between rounded-md bg-muted/40 px-3 py-2">
						<label htmlFor="billing-select-all-ports" className="flex items-center gap-2 text-sm font-medium">
							<Checkbox
								id="billing-select-all-ports"
								checked={allActivePortsSelected ? true : selectedOnActiveDevice > 0 ? "indeterminate" : false}
								onCheckedChange={toggleAllActivePorts}
							/>
							<Trans>Select all ports on this device</Trans>
						</label>
						<span className="text-xs text-muted-foreground">
							{selectedOnActiveDevice} / {activePorts.length}
						</span>
					</div>
				) : null}
				<div className="grid max-h-[420px] gap-2 overflow-auto pe-1 md:grid-cols-2">
					{devices.length === 0 ? (
						<div className="text-sm text-muted-foreground">
							<Trans>No accessible network devices.</Trans>
						</div>
					) : activePorts.length === 0 ? (
						<div className="text-sm text-muted-foreground">
							<Trans>No discovered ports on this device.</Trans>
						</div>
					) : (
						activePorts.map((port) => {
							const portID = networkPortID(port)
							const checked = portID in bindings
							return (
								<div
									key={portID}
									className="grid grid-cols-[minmax(0,1fr)_130px] gap-2 rounded-md border border-border p-3"
								>
									<label htmlFor={`billing-port-${portID}`} className="flex min-w-0 items-start gap-2">
										<Checkbox
											id={`billing-port-${portID}`}
											checked={checked}
											onCheckedChange={() => togglePort(portID)}
										/>
										<span className="min-w-0">
											<span className="block truncate text-sm font-medium">{networkPortLabel(port)}</span>
											<span className="block truncate text-xs text-muted-foreground">
												ifIndex {port.IfIndex ?? port.if_index ?? "—"} ·{" "}
												{port.OperStatus ?? port.oper_status ?? "unknown"}
											</span>
										</span>
									</label>
									<Select
										disabled={!checked}
										value={bindings[portID] ?? form.direction}
										onValueChange={(value: BillingDirection) =>
											setBindings((current) => ({ ...current, [portID]: value }))
										}
									>
										<SelectTrigger aria-label={t`Billing metric`}>
											<SelectValue />
										</SelectTrigger>
										<SelectContent>
											<SelectItem value="in">
												<Trans>Inbound</Trans>
											</SelectItem>
											<SelectItem value="out">
												<Trans>Outbound</Trans>
											</SelectItem>
											<SelectItem value="agg">
												<Trans>In + out</Trans>
											</SelectItem>
										</SelectContent>
									</Select>
								</div>
							)
						})
					)}
				</div>
				{selectedPortCount === 0 ? (
					<p className="text-sm text-destructive">
						<Trans>Select at least one billing port.</Trans>
					</p>
				) : null}
			</div>
		</div>
	)
})

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
			{hint ? <p className="text-xs text-muted-foreground">{hint}</p> : null}
		</div>
	)
}

function billingAlgorithm(
	measurement: FormState["measurement_type"],
	method: FormState["billing_method"]
): FormState["algorithm"] {
	if (measurement === "traffic") return "total"
	if (method === "monthly_95th") return "95th"
	if (method === "daily_95th") return "daily_95th"
	return "average"
}

function networkDeviceID(device?: NetworkDevice) {
	return device?.ID ?? device?.id ?? ""
}

function networkDeviceLabel(device: NetworkDevice) {
	return (
		device.SysName ??
		device.sys_name ??
		device.Name ??
		device.name ??
		device.Host ??
		device.host ??
		networkDeviceID(device)
	)
}

function networkDeviceOptionLabel(device: NetworkDevice) {
	const label = networkDeviceLabel(device)
	const host = device.Host ?? device.host ?? ""
	return host && host !== label ? `${label} · ${host}` : label
}

function networkDeviceSearchText(device: NetworkDevice) {
	return [device.SysName, device.sys_name, device.Name, device.name, device.Host, device.host, networkDeviceID(device)]
		.filter(Boolean)
		.join(" ")
		.toLocaleLowerCase()
}

function networkPortID(port: NetworkPort) {
	return port.ID ?? port.id ?? ""
}

function networkPortLabel(port: NetworkPort) {
	const name = port.IfName ?? port.if_name ?? networkPortID(port)
	const alias = port.IfAlias ?? port.if_alias
	return alias ? `${name} · ${alias}` : name
}
