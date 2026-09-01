import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, PlusIcon, SaveIcon, Trash2Icon } from "lucide-react"
import type React from "react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Badge } from "@/components/ui/badge"
import { Button, buttonVariants } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { isAdmin, pb } from "@/lib/api"
import { cn } from "@/lib/utils"

type MetricDefinition = {
	Name?: string
	name?: string
	Scope?: string
	scope?: string
	Unit?: string
	unit?: string
	Description?: string
	description?: string
}

type NetworkDevice = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
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
	OperStatus?: string
	oper_status?: string
}

type AggregateGraph = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Metric?: string
	metric?: string
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	Unit?: string
	unit?: string
	Description?: string
	description?: string
}

type AggregateGraphPort = {
	PortID?: string
	port_id?: string
}

type AggregateGraphItem = {
	ID?: string
	id?: string
	Metric?: string
	metric?: string
	Direction?: string
	direction?: string
	Label?: string
	label?: string
	Total?: boolean
	total?: boolean
}

type ItemState = {
	id: string
	metric: string
	direction: string
	label: string
	total: boolean
}

type FormState = {
	id: string
	name: string
	aggregation: string
	valueMode: string
	unit: string
	description: string
}

type AggregateGraphFormProps = {
	id?: string
}

export default memo(({ id }: AggregateGraphFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? createAggregateGraphID(),
		name: "",
		aggregation: "sum",
		valueMode: "corrected",
		unit: "bps",
		description: "",
	}))
	const [items, setItems] = useState<ItemState[]>(() => defaultTrafficItems())
	const [metrics, setMetrics] = useState<MetricDefinition[]>([])
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [portsByDevice, setPortsByDevice] = useState<Record<string, NetworkPort[]>>({})
	const [selectedPorts, setSelectedPorts] = useState<string[]>([])
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")

	const loadCatalog = useCallback(async () => {
		const [metricData, deviceData] = await Promise.all([
			pb.send<{ items?: MetricDefinition[] }>("/api/v1/metrics/catalog", {}),
			pb.send<{ items?: NetworkDevice[] }>("/api/v1/network/devices", {}),
		])
		setMetrics(metricData.items ?? [])
		setDevices(deviceData.items ?? [])
		return { metrics: metricData.items ?? [], devices: deviceData.items ?? [] }
	}, [])

	const loadAllPorts = useCallback(async (deviceList: NetworkDevice[]) => {
		const entries = await Promise.all(
			deviceList.map(async (device) => {
				const deviceID = device.ID ?? device.id ?? ""
				if (!deviceID) {
					return [deviceID, []] as const
				}
				const data = await pb.send<{ items?: NetworkPort[] }>(`/api/v1/network/devices/${deviceID}/ports`, {})
				return [deviceID, data.items ?? []] as const
			})
		)
		const next: Record<string, NetworkPort[]> = {}
		for (const [deviceID, ports] of entries) {
			if (deviceID) {
				next[deviceID] = ports
			}
		}
		setPortsByDevice(next)
	}, [])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit Graph` : t`Create Graph`} / Beszel`
		let cancelled = false
		const load = async () => {
			setLoading(true)
			setError("")
			try {
				const { devices: deviceList } = await loadCatalog()
				await loadAllPorts(deviceList)
				if (id) {
					const [graph, portLinks, itemLinks] = await Promise.all([
						pb.send<AggregateGraph>(`/api/v1/aggregate-graphs/${id}`, {}),
						pb.send<{ items?: AggregateGraphPort[] }>(`/api/v1/aggregate-graphs/${id}/ports`, {}),
						pb.send<{ items?: AggregateGraphItem[] }>(`/api/v1/aggregate-graphs/${id}/items`, {}),
					])
					if (cancelled) return
					setForm({
						id,
						name: graph.Name ?? graph.name ?? "",
						aggregation: graph.Aggregation ?? graph.aggregation ?? "sum",
						valueMode: graph.ValueMode ?? graph.value_mode ?? "corrected",
						unit: graph.Unit ?? graph.unit ?? "",
						description: graph.Description ?? graph.description ?? "",
					})
					setSelectedPorts((portLinks.items ?? []).map((link) => link.PortID ?? link.port_id ?? "").filter(Boolean))
					const loaded = itemLinks.items ?? []
					if (loaded.length > 0) {
						setItems(
							loaded.map((item, index) => ({
								id: item.ID ?? item.id ?? `item_${index}`,
								metric: item.Metric ?? item.metric ?? "",
								direction: item.Direction ?? item.direction ?? "other",
								label: item.Label ?? item.label ?? "",
								total: Boolean(item.Total ?? item.total),
							}))
						)
					}
				}
			} catch (err) {
				if (!cancelled) {
					setError(err instanceof Error ? err.message : t`Failed to load aggregate graph`)
				}
			} finally {
				if (!cancelled) {
					setLoading(false)
				}
			}
		}
		load()
		return () => {
			cancelled = true
		}
	}, [id, loadAllPorts, loadCatalog, t, isEditing])

	const visiblePorts = useMemo(
		() =>
			devices.flatMap((device) =>
				(portsByDevice[device.ID ?? device.id ?? ""] ?? []).map((port) => ({
					...port,
					deviceID: device.ID ?? device.id ?? "",
					deviceName: device.SysName ?? device.sys_name ?? device.ID ?? device.id ?? "",
				}))
			),
		[devices, portsByDevice]
	)

	const update = <K extends keyof FormState>(key: K, value: FormState[K]) => {
		setForm((current) => ({ ...current, [key]: value }))
	}

	const updateItem = (index: number, patch: Partial<ItemState>) => {
		setItems((current) => current.map((item, i) => (i === index ? { ...item, ...patch } : item)))
	}
	const addItem = () => {
		setItems((current) => [
			...current,
			{ id: `item_${current.length}_${Math.random().toString(36).slice(2, 8)}`, metric: "", direction: "other", label: "", total: false },
		])
	}
	const removeItem = (index: number) => {
		setItems((current) => current.filter((_, i) => i !== index))
	}

	const togglePort = (portID: string) => {
		setSelectedPorts((current) =>
			current.includes(portID) ? current.filter((item) => item !== portID) : [...current, portID]
		)
	}

	const submit = async (event: React.FormEvent) => {
		event.preventDefault()
		if (!form.name.trim()) {
			setError(t`Name is required`)
			return
		}
		if (items.filter((item) => item.metric).length === 0) {
			setError(t`Add at least one data source (metric)`)
			return
		}
		if (selectedPorts.length === 0) {
			setError(t`Select at least one port`)
			return
		}
		setSaving(true)
		setError("")
		try {
			const body = {
				Name: form.name.trim(),
				Aggregation: form.aggregation,
				ValueMode: form.valueMode,
				Unit: form.unit.trim(),
				Description: form.description.trim(),
			}
			if (isEditing) {
				await pb.send(`/api/v1/aggregate-graphs/${id}`, { method: "PATCH", body })
			} else {
				await pb.send(`/api/v1/aggregate-graphs`, { method: "POST", body: { ...body, id: form.id } })
			}
			await pb.send(`/api/v1/aggregate-graphs/${form.id}/items`, {
				method: "PUT",
				body: {
					items: items
						.filter((item) => item.metric)
						.map((item) => ({
							id: item.id,
							metric: item.metric,
							direction: item.direction,
							label: item.label,
							total: item.total,
						})),
				},
			})
			await pb.send(`/api/v1/aggregate-graphs/${form.id}/ports`, {
				method: "PUT",
				body: { ports: selectedPorts.map((portID) => ({ PortID: portID })) },
			})
			navigate(getPagePath($router, "aggregate_graph", { id: form.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save aggregate graph`)
		} finally {
			setSaving(false)
		}
	}

	if (loading) {
		return (
			<div className="grid gap-4">
				<div className="text-sm text-muted-foreground">
					<Trans>Loading...</Trans>
				</div>
			</div>
		)
	}

	return (
		<div className="grid gap-4">
			<div className="flex items-center gap-2">
				<Link
					href={getPagePath($router, "aggregate_graphs")}
					className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
					aria-label={t`Back to saved graphs`}
				>
					<ArrowLeftIcon className="h-4 w-4" />
				</Link>
				<h1 className="text-xl font-semibold tracking-normal">
					{isEditing ? t`Edit Graph` : t`Create Graph`}
				</h1>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}

			<form onSubmit={submit} className="grid gap-4">
				<div className="grid gap-3 rounded-md border border-border p-4">
					<Field label={t`Name`}>
						<Input value={form.name} onChange={(event) => update("name", event.target.value)} />
					</Field>
					<Field label={t`Aggregation`}>
						<Select value={form.aggregation} onValueChange={(value) => update("aggregation", value)}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="sum">sum</SelectItem>
								<SelectItem value="avg">avg</SelectItem>
								<SelectItem value="max">max</SelectItem>
								<SelectItem value="min">min</SelectItem>
								<SelectItem value="count">count</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<div className="grid gap-3 md:grid-cols-2">
						<Field label={t`Value mode`}>
							<Select
								value={form.valueMode}
								onValueChange={(value) => update("valueMode", value)}
								disabled={!isAdmin() && form.valueMode !== "corrected"}
							>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="corrected">corrected</SelectItem>
									<SelectItem value="raw">raw</SelectItem>
									<SelectItem value="both">both</SelectItem>
								</SelectContent>
							</Select>
						</Field>
						<Field label={t`Unit`}>
							<Input value={form.unit} onChange={(event) => update("unit", event.target.value)} placeholder="bps" />
						</Field>
					</div>
				<Field label={t`Description`}>
					<Textarea
						value={form.description}
						onChange={(event) => update("description", event.target.value)}
						rows={2}
					/>
				</Field>
			</div>

			<div className="grid gap-3 rounded-md border border-border p-4">
				<div className="flex items-center justify-between gap-3">
					<h2 className="text-base font-medium">
						<Trans>Data Sources</Trans>
					</h2>
					<Button type="button" variant="outline" size="sm" onClick={addItem}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Add</Trans>
					</Button>
				</div>
				<div className="grid gap-2">
					{items.map((item, index) => (
						<div key={item.id} className="grid gap-2 rounded-md border border-border p-2 md:grid-cols-[1fr_120px_120px_auto]">
							<Select value={item.metric} onValueChange={(value) => updateItem(index, { metric: value })}>
								<SelectTrigger>
									<SelectValue placeholder={t`Metric`} />
								</SelectTrigger>
								<SelectContent>
									{metrics.map((metric) => {
										const name = metric.Name ?? metric.name ?? ""
										return (
											<SelectItem key={name} value={name}>
												{metric.Description ?? metric.description ?? name}
											</SelectItem>
										)
									})}
								</SelectContent>
							</Select>
							<Select value={item.direction} onValueChange={(value) => updateItem(index, { direction: value })}>
								<SelectTrigger>
									<SelectValue />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="in">in</SelectItem>
									<SelectItem value="out">out</SelectItem>
									<SelectItem value="other">other</SelectItem>
								</SelectContent>
							</Select>
							<Input
								value={item.label}
								onChange={(event) => updateItem(index, { label: event.target.value })}
								placeholder={t`Label`}
							/>
							<div className="flex items-center gap-2">
								<label className="flex items-center gap-1.5 whitespace-nowrap text-xs text-muted-foreground">
									<Checkbox checked={item.total} onCheckedChange={(value) => updateItem(index, { total: value === true })} />
									<Trans>Total</Trans>
								</label>
								<Button type="button" variant="ghost" size="icon" onClick={() => removeItem(index)}>
									<Trash2Icon className="h-4 w-4" />
								</Button>
							</div>
						</div>
					))}
				</div>
			</div>

			<div className="grid gap-3 rounded-md border border-border p-4">
				<div className="flex items-center justify-between gap-3">
					<h2 className="text-base font-medium">
						<Trans>Ports</Trans>
					</h2>
					<Badge variant="outline">
						{selectedPorts.length} / {visiblePorts.length}
					</Badge>
				</div>
					<div className="grid max-h-[360px] gap-1 overflow-auto pe-1">
						{visiblePorts.length === 0 ? (
							<div className="px-2 py-1 text-sm text-muted-foreground">
								<Trans>No discovered ports.</Trans>
							</div>
						) : (
							visiblePorts.map((port) => {
								const portID = port.ID ?? port.id ?? ""
								return (
									<label key={portID} className="flex items-start gap-2 rounded-md px-2 py-1.5 hover:bg-muted/60">
										<Checkbox checked={selectedPorts.includes(portID)} onCheckedChange={() => togglePort(portID)} />
										<span className="min-w-0">
											<span className="block truncate text-sm font-medium">{portLabel(port)}</span>
											<span className="block truncate text-xs text-muted-foreground">
												{port.deviceName} · {port.OperStatus ?? port.oper_status ?? "unknown"}
											</span>
										</span>
									</label>
								)
							})
						)}
					</div>
				</div>

				<div className="flex items-center justify-end gap-2">
					<Button type="submit" disabled={saving}>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Save</Trans>
					</Button>
				</div>
			</form>
		</div>
	)
})

function Field({ label, children }: { label: React.ReactNode; children: React.ReactNode }) {
	return (
		<div className="grid gap-1.5">
			<Label>{label}</Label>
			{children}
		</div>
	)
}

function portLabel(port: NetworkPort & { deviceName?: string }) {
	const name = port.IfName ?? port.if_name ?? port.IfDescr ?? port.if_descr ?? port.ID ?? port.id ?? ""
	const alias = port.IfAlias ?? port.if_alias
	return alias ? `${name} · ${alias}` : name
}

function defaultTrafficItems(): ItemState[] {
	return [
		{ id: "item_in", metric: "watchdog_snmp_if_in_bps", direction: "in", label: "In", total: true },
		{ id: "item_out", metric: "watchdog_snmp_if_out_bps", direction: "out", label: "Out", total: true },
	]
}

function createAggregateGraphID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `aggr_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}
