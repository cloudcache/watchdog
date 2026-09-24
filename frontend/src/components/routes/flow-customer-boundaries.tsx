import { Trans, useLingui } from "@lingui/react/macro"
import { PencilIcon, PlusIcon, RefreshCwIcon, SaveIcon, Trash2Icon, XIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api, can } from "@/lib/api"

type FlowExporter = {
	device_id: string
	device_name: string
	device_host: string
	protocol: string
	source_prefix: string
	enabled: boolean
}

type NetworkDevice = {
	id: string
	name?: string
	sys_name?: string
	host: string
	kind: string
}

type Customer = { id: string; name: string; ref?: string }
type SourceRange = { id: string; cidr: string; family: number; prefix_length: number }
type CustomerBoundary = {
	id: string
	device_id: string
	device_name: string
	device_host: string
	customer_id: string
	customer_name: string
	customer_ref?: string
	source_ranges: SourceRange[]
	row_version: number
}
type Page<T> = { items?: T[]; total?: number }

const newCustomerValue = "__new_customer__"

export default memo(function FlowCustomerBoundaries() {
	const { t } = useLingui()
	const [devices, setDevices] = useState<NetworkDevice[]>([])
	const [exportersByDevice, setExportersByDevice] = useState<Map<string, FlowExporter>>(new Map())
	const [customers, setCustomers] = useState<Customer[]>([])
	const [boundaries, setBoundaries] = useState<CustomerBoundary[]>([])
	const [selectedDeviceID, setSelectedDeviceID] = useState("")
	const [editingID, setEditingID] = useState("")
	const [customerID, setCustomerID] = useState("")
	const [newCustomerName, setNewCustomerName] = useState("")
	const [sourceRanges, setSourceRanges] = useState("")
	const [confirmDeleteID, setConfirmDeleteID] = useState("")
	const [loading, setLoading] = useState(true)
	const [boundariesLoading, setBoundariesLoading] = useState(false)
	const [boundaryRevision, setBoundaryRevision] = useState(0)
	const [working, setWorking] = useState(false)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")
	const boundaryRequest = useRef(0)
	const canManage = can("address.manage")

	const loadReferences = useCallback(async () => {
		const [devicePage, exporters, customerPage] = await Promise.all([
			api.send<Page<NetworkDevice>>("/api/v1/devices", {
				query: { kind: "network", limit: 500, sort: "name", order: "asc" },
			}),
			api.send<Page<FlowExporter>>("/api/v1/flow/devices", {
				query: { enabled: true, limit: 500, sort: "device", order: "asc" },
			}),
			api.send<Page<Customer>>("/api/v1/flow/customers", {
				query: { limit: 500, sort: "name", order: "asc" },
			}),
		])
		const unique = new Map<string, FlowExporter>()
		for (const exporter of exporters.items ?? []) {
			if (!unique.has(exporter.device_id)) unique.set(exporter.device_id, exporter)
		}
		const nextDevices = (devicePage.items ?? []).filter((device) => unique.has(device.id))
		setDevices(nextDevices)
		setExportersByDevice(unique)
		setCustomers(customerPage.items ?? [])
		setSelectedDeviceID((current) =>
			current && nextDevices.some((device) => device.id === current) ? current : nextDevices[0]?.id || ""
		)
	}, [])

	const loadBoundaries = useCallback(async (deviceID: string, signal?: AbortSignal) => {
		if (!deviceID) {
			setBoundaries([])
			return
		}
		const request = ++boundaryRequest.current
		const page = await api.send<Page<CustomerBoundary>>("/api/v1/flow/customer-bindings", {
			query: { device_id: deviceID, limit: 500, sort: "customer", order: "asc" },
			signal,
		})
		if (request === boundaryRequest.current) setBoundaries(page.items ?? [])
	}, [])

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			await loadReferences()
			setBoundaryRevision((revision) => revision + 1)
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to load Flow customer boundaries`)
		} finally {
			setLoading(false)
		}
	}, [loadReferences, t])

	useEffect(() => {
		refresh()
	}, [refresh])

	useEffect(() => {
		const controller = new AbortController()
		setBoundariesLoading(Boolean(selectedDeviceID))
		loadBoundaries(selectedDeviceID, controller.signal)
			.catch((cause) => {
				if (!controller.signal.aborted) {
					setError(cause instanceof Error ? cause.message : t`Failed to load Flow customer boundaries`)
				}
			})
			.finally(() => {
				if (!controller.signal.aborted) setBoundariesLoading(false)
			})
		return () => controller.abort()
	}, [boundaryRevision, loadBoundaries, selectedDeviceID, t])

	const selectedDevice = useMemo(
		() => devices.find((device) => device.id === selectedDeviceID),
		[devices, selectedDeviceID]
	)
	const selectedExporter = exportersByDevice.get(selectedDeviceID)
	const deviceLabel = (device: NetworkDevice) => {
		const name = device.sys_name || device.name || device.host
		return name === device.host ? device.host : `${name} · ${device.host}`
	}

	const resetForm = () => {
		setEditingID("")
		setCustomerID("")
		setNewCustomerName("")
		setSourceRanges("")
	}

	const edit = (item: CustomerBoundary) => {
		setEditingID(item.id)
		setCustomerID(item.customer_id)
		setNewCustomerName("")
		setSourceRanges(item.source_ranges.map((prefix) => prefix.cidr).join("\n"))
		setError("")
		setNotice("")
	}

	const save = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			let selectedCustomerID = customerID
			if (customerID === newCustomerValue) {
				const created = await api.send<Customer>("/api/v1/flow/customers", {
					method: "POST",
					body: { name: newCustomerName.trim(), ref: "", notes: "" },
				})
				selectedCustomerID = created.id
			}
			const ranges = sourceRanges
				.split(/\r?\n/)
				.map((value) => value.trim())
				.filter(Boolean)
			if (!selectedDeviceID || !selectedCustomerID) throw new Error(t`Select a Flow device and customer`)
			if (ranges.length === 0) throw new Error(t`Enter at least one customer source range`)
			const existing = boundaries.find((item) => item.id === editingID)
			await api.send(editingID ? `/api/v1/flow/customer-bindings/${editingID}` : "/api/v1/flow/customer-bindings", {
				method: editingID ? "PATCH" : "POST",
				headers: existing ? { "If-Match": `"${existing.row_version}"` } : undefined,
				body: {
					device_id: selectedDeviceID,
					customer_id: selectedCustomerID,
					source_ranges: ranges,
				},
			})
			resetForm()
			await Promise.all([loadReferences(), loadBoundaries(selectedDeviceID)])
			setNotice(t`Customer source ranges saved.`)
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to save customer source ranges`)
		} finally {
			setWorking(false)
		}
	}

	const remove = async (item: CustomerBoundary) => {
		if (confirmDeleteID !== item.id) {
			setConfirmDeleteID(item.id)
			return
		}
		setWorking(true)
		setError("")
		try {
			await api.send(`/api/v1/flow/customer-bindings/${item.id}`, {
				method: "DELETE",
				headers: { "If-Match": `"${item.row_version}"` },
			})
			setConfirmDeleteID("")
			await loadBoundaries(selectedDeviceID)
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to delete customer source ranges`)
		} finally {
			setWorking(false)
		}
	}

	return (
		<section id="flow-customer-boundaries" className="grid gap-4 rounded-md border border-border bg-card p-4">
			<div className="flex flex-wrap items-start justify-between gap-3">
				<div>
					<h2 className="text-lg font-semibold">
						<Trans>Device customer boundaries</Trans>
					</h2>
					<p className="text-sm text-muted-foreground">
						<Trans>
							Each Flow device can serve multiple customers. Enter one or more source CIDRs per customer here; they do
							not enter the shared Geo address library.
						</Trans>
					</p>
				</div>
				<Button type="button" variant="outline" size="sm" onClick={refresh} disabled={working || loading}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>
			<div className="grid gap-2">
				<Label htmlFor="flow-customer-device">
					<Trans>Flow observation device</Trans>
				</Label>
				<Select
					value={selectedDeviceID || undefined}
					onValueChange={(value) => {
						setSelectedDeviceID(value)
						setBoundaries([])
						setEditingID("")
						setCustomerID("")
						setNewCustomerName("")
						setSourceRanges("")
					}}
					disabled={loading}
				>
					<SelectTrigger id="flow-customer-device">
						<SelectValue placeholder={t`Select a Flow-enabled device`} />
					</SelectTrigger>
					<SelectContent>
						{devices.map((device) => (
							<SelectItem key={device.id} value={device.id}>
								{deviceLabel(device)} · Flow
							</SelectItem>
						))}
					</SelectContent>
				</Select>
				<p className="text-xs text-muted-foreground">
					<Trans>
						This list comes from the network device inventory. Flow exporter identity is maintained independently.
					</Trans>
					{selectedExporter ? ` ${selectedExporter.protocol.toUpperCase()} · ${selectedExporter.source_prefix}` : ""}
				</p>
			</div>
			{devices.length === 0 && !loading ? (
				<p className="rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-sm text-amber-800">
					<Trans>No network devices are available.</Trans>
				</p>
			) : null}
			{selectedDevice && !selectedExporter ? (
				<p className="rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-sm text-amber-800">
					<Trans>
						This device has no enabled Flow exporter binding. Configure the binding before saving customer boundaries.
					</Trans>
				</p>
			) : null}
			<div className="grid gap-3">
				{boundariesLoading ? (
					<p className="text-sm text-muted-foreground">
						<Trans>Loading customer source ranges...</Trans>
					</p>
				) : null}
				{boundaries.map((item) => (
					<div
						key={item.id}
						className="flex flex-wrap items-start justify-between gap-3 rounded-md border border-border p-3"
					>
						<div className="grid gap-2">
							<div className="font-medium">
								{item.customer_name}
								{item.customer_ref ? ` · ${item.customer_ref}` : ""}
							</div>
							<div className="flex flex-wrap gap-2">
								{item.source_ranges.map((prefix) => (
									<code key={prefix.id} className="rounded bg-muted px-2 py-1 text-xs">
										{prefix.cidr}
									</code>
								))}
							</div>
						</div>
						{canManage ? (
							<div className="flex gap-2">
								<Button type="button" variant="outline" size="sm" onClick={() => edit(item)}>
									<PencilIcon className="h-4 w-4" />
									<span className="sr-only">
										<Trans>Edit</Trans>
									</span>
								</Button>
								<Button
									type="button"
									variant={confirmDeleteID === item.id ? "destructive" : "outline"}
									size="sm"
									onClick={() => remove(item)}
									disabled={working}
								>
									<Trash2Icon className="me-1 h-4 w-4" />
									{confirmDeleteID === item.id ? <Trans>Confirm delete</Trans> : <Trans>Delete</Trans>}
								</Button>
							</div>
						) : null}
					</div>
				))}
				{boundaries.length === 0 && selectedDeviceID && !loading && !boundariesLoading ? (
					<p className="text-sm text-muted-foreground">
						<Trans>No customers are configured for this Flow device.</Trans>
					</p>
				) : null}
			</div>
			{canManage && selectedDeviceID ? (
				<div className="grid gap-3 rounded-md border border-border bg-muted/20 p-4 md:grid-cols-2">
					<div className="grid content-start gap-2">
						<Label htmlFor="flow-boundary-customer">
							<Trans>Customer</Trans>
						</Label>
						<Select
							value={customerID || undefined}
							onValueChange={setCustomerID}
							disabled={working || Boolean(editingID)}
						>
							<SelectTrigger id="flow-boundary-customer">
								<SelectValue placeholder={t`Select or create a customer`} />
							</SelectTrigger>
							<SelectContent>
								{customers
									.filter(
										(customer) =>
											!boundaries.some((item) => item.customer_id === customer.id) || customer.id === customerID
									)
									.map((customer) => (
										<SelectItem key={customer.id} value={customer.id}>
											{customer.name}
										</SelectItem>
									))}
								{!editingID ? (
									<SelectItem value={newCustomerValue}>
										<Trans>+ Create customer</Trans>
									</SelectItem>
								) : null}
							</SelectContent>
						</Select>
						{customerID === newCustomerValue ? (
							<Input
								value={newCustomerName}
								onChange={(event) => setNewCustomerName(event.target.value)}
								placeholder={t`Customer name`}
							/>
						) : null}
					</div>
					<div className="grid gap-2">
						<Label htmlFor="flow-boundary-ranges">
							<Trans>Customer source ranges</Trans>
						</Label>
						<textarea
							id="flow-boundary-ranges"
							className="min-h-32 rounded-md border border-input bg-background px-3 py-2 font-mono text-sm"
							value={sourceRanges}
							onChange={(event) => setSourceRanges(event.target.value)}
							placeholder={t`One CIDR per line, for example 192.0.2.0/24`}
						/>
						<p className="text-xs text-muted-foreground">
							<Trans>Overlapping ranges between different customers on the same device are rejected.</Trans>
						</p>
					</div>
					<div className="flex gap-2 md:col-span-2">
						<Button type="button" onClick={save} disabled={working || !selectedExporter}>
							{editingID ? <SaveIcon className="me-2 h-4 w-4" /> : <PlusIcon className="me-2 h-4 w-4" />}
							{editingID ? <Trans>Save changes</Trans> : <Trans>Add customer boundary</Trans>}
						</Button>
						{editingID ? (
							<Button type="button" variant="outline" onClick={resetForm}>
								<XIcon className="me-2 h-4 w-4" />
								<Trans>Cancel</Trans>
							</Button>
						) : null}
					</div>
				</div>
			) : null}
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}
		</section>
	)
})
