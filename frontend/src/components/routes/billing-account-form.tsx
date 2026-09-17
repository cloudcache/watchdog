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
import { cn } from "@/lib/utils"

type BillingAccount = {
	id: string
	party_id?: string
	name: string
	status: string
	bill_type: "cdr" | "quota"
	algorithm: "95th" | "average" | "total"
	billing_day: number
	timezone: string
	direction: "in" | "out" | "agg"
	default_layer: "raw" | "supplier" | "customer"
	pricing_model: "flat_port" | "usage_95th"
	price_currency: string
	unit_price: string
	cdr_bps?: number
	quota_bytes?: number
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
	"id" | "billing_day" | "cdr_bps" | "quota_bytes" | "reconcile_abs" | "reconcile_percent"
> & {
	billing_day: string
	cdr_bps: string
	quota_bytes: string
	reconcile_abs: string
	reconcile_percent: string
}

const emptyForm: FormState = {
	party_id: "",
	name: "",
	status: "active",
	bill_type: "cdr",
	algorithm: "95th",
	billing_day: "1",
	timezone: "UTC",
	direction: "agg",
	default_layer: "customer",
	pricing_model: "usage_95th",
	price_currency: "CNY",
	unit_price: "0",
	cdr_bps: "",
	quota_bytes: "",
	reconcile_abs: "0",
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
				bill_type: item.bill_type,
				algorithm: item.algorithm,
				billing_day: String(item.billing_day),
				timezone: item.timezone,
				direction: item.direction,
				default_layer: item.default_layer,
				pricing_model: item.pricing_model ?? "flat_port",
				price_currency: item.price_currency ?? "CNY",
				unit_price: item.unit_price ?? "0",
				cdr_bps: String(item.cdr_bps ?? ""),
				quota_bytes: String(item.quota_bytes ?? ""),
				reconcile_abs: String(item.reconcile_abs),
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
			const body = {
				party_id: form.party_id || undefined,
				name: form.name.trim(),
				status: form.status,
				bill_type: form.bill_type,
				algorithm: form.algorithm,
				billing_day: Number(form.billing_day),
				timezone: form.timezone.trim(),
				direction: form.direction,
				default_layer: form.default_layer,
				pricing_model: form.pricing_model,
				price_currency: form.price_currency.trim().toUpperCase(),
				unit_price: form.unit_price.trim(),
				cdr_bps: form.bill_type === "cdr" ? Number(form.cdr_bps) : undefined,
				quota_bytes: form.bill_type === "quota" ? Number(form.quota_bytes) : undefined,
				reconcile_abs: Number(form.reconcile_abs),
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
	const allowance = form.bill_type === "cdr" ? form.cdr_bps : form.quota_bytes
	const validAllowance = allowance.trim() !== "" && Number.isFinite(Number(allowance)) && Number(allowance) >= 0
	const validPrice = /^(0|[1-9][0-9]{0,13})(\.[0-9]{1,6})?$/.test(form.unit_price.trim())
	const canSave =
		form.name.trim() &&
		form.timezone.trim() &&
		Number(form.billing_day) >= 1 &&
		Number(form.billing_day) <= 31 &&
		validAllowance &&
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
				<Field label={t`Billing type`}>
					<Select
						value={form.bill_type}
						onValueChange={(value: "cdr" | "quota") =>
							update({
								bill_type: value,
								algorithm: value === "quota" ? "total" : form.algorithm === "total" ? "95th" : form.algorithm,
								pricing_model: value === "quota" ? "flat_port" : form.pricing_model,
							})
						}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="cdr">cdr</SelectItem>
							<SelectItem value="quota">quota</SelectItem>
						</SelectContent>
					</Select>
				</Field>
				<Field label={t`Algorithm`}>
					<Select
						value={form.algorithm}
						onValueChange={(value: "95th" | "average" | "total") =>
							update({ algorithm: value, pricing_model: value === "95th" ? form.pricing_model : "flat_port" })
						}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							{form.bill_type === "cdr" ? (
								<>
									<SelectItem value="95th">95th</SelectItem>
									<SelectItem value="average">average</SelectItem>
								</>
							) : (
								<SelectItem value="total">total</SelectItem>
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
				<Field label={t`Pricing model`}>
					<Select
						value={form.pricing_model}
						onValueChange={(value: "flat_port" | "usage_95th") =>
							update({
								pricing_model: value,
								bill_type: value === "usage_95th" ? "cdr" : form.bill_type,
								algorithm: value === "usage_95th" ? "95th" : form.algorithm,
							})
						}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="flat_port">
								<Trans>Fixed price per port</Trans>
							</SelectItem>
							<SelectItem value="usage_95th">
								<Trans>95th percentile per Mbps</Trans>
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
						form.pricing_model === "flat_port" ? t`Price per port / billing cycle` : t`Price per Mbps / billing cycle`
					}
				>
					<Input
						inputMode="decimal"
						value={form.unit_price}
						onChange={(event) => update({ unit_price: event.target.value })}
						placeholder="0.000000"
					/>
				</Field>
				{form.bill_type === "cdr" ? (
					<Field label="CDR (bps)">
						<Input
							type="number"
							min="0"
							value={form.cdr_bps}
							onChange={(event) => update({ cdr_bps: event.target.value })}
						/>
					</Field>
				) : (
					<Field label={t`Quota bytes`}>
						<Input
							type="number"
							min="0"
							value={form.quota_bytes}
							onChange={(event) => update({ quota_bytes: event.target.value })}
						/>
					</Field>
				)}
				<Field label={t`Absolute threshold`}>
					<Input
						type="number"
						min="0"
						value={form.reconcile_abs}
						onChange={(event) => update({ reconcile_abs: event.target.value })}
					/>
				</Field>
				<Field label={t`Percent threshold`}>
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
				<Field label={t`Device`}>
					<Select value={activeDeviceID} onValueChange={setActiveDeviceID} disabled={devices.length === 0}>
						<SelectTrigger>
							<SelectValue placeholder={t`Select a device`} />
						</SelectTrigger>
						<SelectContent>
							{devices.map((device) => {
								const deviceID = networkDeviceID(device)
								return (
									<SelectItem key={deviceID} value={deviceID}>
										{networkDeviceLabel(device)}
									</SelectItem>
								)
							})}
						</SelectContent>
					</Select>
				</Field>
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

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
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

function networkPortID(port: NetworkPort) {
	return port.ID ?? port.id ?? ""
}

function networkPortLabel(port: NetworkPort) {
	const name = port.IfName ?? port.if_name ?? networkPortID(port)
	const alias = port.IfAlias ?? port.if_alias
	return alias ? `${name} · ${alias}` : name
}
