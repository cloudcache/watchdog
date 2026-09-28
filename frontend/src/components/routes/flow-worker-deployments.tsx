import { Trans, useLingui } from "@lingui/react/macro"
import { RefreshCwIcon, RocketIcon, ShieldCheckIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { api, can } from "@/lib/api"

type Page<T> = { items?: T[]; total?: number }

type FlowWorker = {
	id: string
	name: string
	status: string
	health: string
	last_seen?: string
}

type FlowExporter = {
	device_id: string
	device_name: string
	device_host: string
	enabled: boolean
}

type FlowWorkerBinding = {
	device_id: string
	worker_id: string
	worker_name: string
}

type DeploymentValidation = {
	valid: boolean
	worker_ids: string[]
	device_ids: string[]
	address_snapshot_id: string
	address_version: number
	profile_row_version: number
	device_boundary_digests: Record<string, string>
	recommended_worker_ids: string[]
	artifact_count: number
}

type OperationJob = {
	id: string
	status: string
	progress_done: number
	progress_total: number
	last_error_detail?: string
}

type FlowDeployment = {
	id: string
	worker_id: string
	worker_name: string
	generation: number
	effective_from: string
	state: string
	worker_health: string
	worker_last_seen?: string
	device_names: string
	ack_state?: string
	ack_attempted_at?: string
	installed_at?: string
	failure_stage?: string
	error_code?: string
	error_message?: string
}

export default memo(function FlowWorkerDeployments() {
	const { t } = useLingui()
	const [workers, setWorkers] = useState<FlowWorker[]>([])
	const [devices, setDevices] = useState<FlowExporter[]>([])
	const [bindings, setBindings] = useState<FlowWorkerBinding[]>([])
	const [deployments, setDeployments] = useState<FlowDeployment[]>([])
	const [selectedWorkers, setSelectedWorkers] = useState<string[]>([])
	const [selectedDevices, setSelectedDevices] = useState<string[]>([])
	const [effectiveFrom, setEffectiveFrom] = useState(defaultEffectiveFrom)
	const [validation, setValidation] = useState<DeploymentValidation | null>(null)
	const [job, setJob] = useState<OperationJob | null>(null)
	const [loading, setLoading] = useState(true)
	const [working, setWorking] = useState(false)
	const [error, setError] = useState("")
	const [notice, setNotice] = useState("")
	const canPublish = can("address.publish")

	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [workerPage, exporterPage, bindingPage, deploymentPage] = await Promise.all([
				api.send<Page<FlowWorker>>("/api/v1/agents", {
					query: { kind: "flow_worker", limit: 500, sort: "name", order: "asc" },
				}),
				api.send<Page<FlowExporter>>("/api/v1/flow/devices", {
					query: { enabled: true, limit: 500, sort: "device", order: "asc" },
				}),
				api.send<Page<FlowWorkerBinding>>("/api/v1/flow/worker-device-bindings", {
					query: { limit: 500, sort: "device", order: "asc" },
				}),
				api.send<Page<FlowDeployment>>("/api/v1/flow/deployments", {
					query: { limit: 100, sort: "created_at", order: "desc" },
				}),
			])
			const activeWorkers = (workerPage.items ?? []).filter((worker) => worker.status === "active")
			const uniqueDevices = new Map<string, FlowExporter>()
			for (const exporter of exporterPage.items ?? []) {
				if (!uniqueDevices.has(exporter.device_id)) uniqueDevices.set(exporter.device_id, exporter)
			}
			const nextDevices = [...uniqueDevices.values()]
			const nextBindings = bindingPage.items ?? []
			setWorkers(activeWorkers)
			setDevices(nextDevices)
			setBindings(nextBindings)
			setDeployments(deploymentPage.items ?? [])
			setSelectedDevices((current) =>
				current.length > 0
					? current.filter((id) => uniqueDevices.has(id))
					: nextDevices.map((device) => device.device_id)
			)
			setSelectedWorkers((current) => {
				const available = new Set(activeWorkers.map((worker) => worker.id))
				if (current.length > 0) return current.filter((id) => available.has(id))
				return uniqueStrings(nextBindings.map((binding) => binding.worker_id)).filter((id) => available.has(id))
			})
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to load Flow deployment state`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		load()
	}, [load])

	useEffect(() => {
		setValidation(null)
	}, [effectiveFrom, selectedDevices, selectedWorkers])

	const requestBody = () => ({
		worker_ids: selectedWorkers,
		device_ids: selectedDevices,
		effective_from: parseEffectiveFrom(effectiveFrom),
	})

	const validate = async () => {
		setWorking(true)
		setError("")
		setNotice("")
		try {
			if (selectedWorkers.length === 0 || selectedDevices.length === 0) {
				throw new Error(t`Select at least one active Flow worker and one Flow device`)
			}
			setValidation(
				await api.send<DeploymentValidation>("/api/v1/flow/deployments/validate", {
					method: "POST",
					body: requestBody(),
				})
			)
			setNotice(t`Deployment is valid and ready to publish.`)
		} catch (cause) {
			setValidation(null)
			setError(cause instanceof Error ? cause.message : t`Deployment validation failed`)
		} finally {
			setWorking(false)
		}
	}

	const publish = async () => {
		if (!validation || !confirm(t`Publish this immutable deployment to the selected Flow workers?`)) return
		setWorking(true)
		setError("")
		setNotice("")
		try {
			const response = await api.send<{ job: OperationJob }>("/api/v1/flow/deployments", {
				method: "POST",
				body: requestBody(),
			})
			setJob(response.job)
			setNotice(t`Deployment job queued: ${response.job.id}`)
			const completed = await waitForJob(response.job.id)
			setJob(completed)
			if (completed.status === "failed" || completed.status === "canceled") {
				throw new Error(completed.last_error_detail || t`Deployment job failed`)
			}
			if (completed.status === "succeeded") {
				setNotice(t`Desired deployment created; waiting for worker installation acknowledgements.`)
			}
			await load()
		} catch (cause) {
			setError(cause instanceof Error ? cause.message : t`Failed to publish Flow deployment`)
		} finally {
			setWorking(false)
		}
	}

	const recommendedWorkers = useMemo(() => {
		const selected = new Set(selectedDevices)
		return uniqueStrings(
			bindings.filter((binding) => selected.has(binding.device_id)).map((binding) => binding.worker_id)
		)
	}, [bindings, selectedDevices])
	const bindingByDevice = useMemo(() => new Map(bindings.map((binding) => [binding.device_id, binding])), [bindings])

	return (
		<section className="grid gap-4 rounded-md border border-border bg-card p-4">
			<div className="flex flex-wrap items-start justify-between gap-3">
				<div>
					<div className="flex items-center gap-2">
						<RocketIcon className="h-5 w-5 text-muted-foreground" />
						<h2 className="text-lg font-semibold">
							<Trans>Flow worker deployments</Trans>
						</h2>
					</div>
					<p className="text-sm text-muted-foreground">
						<Trans>
							Validate and publish one signed manifest that atomically combines the active address catalog, policy, and
							selected device boundaries.
						</Trans>
					</p>
				</div>
				<Button type="button" variant="outline" size="sm" onClick={load} disabled={loading || working}>
					<RefreshCwIcon className="me-2 h-4 w-4" />
					<Trans>Refresh</Trans>
				</Button>
			</div>

			<div className="grid gap-4 lg:grid-cols-2">
				<SelectionPanel
					title={t`Flow devices`}
					empty={t`No enabled Flow devices.`}
					items={devices.map((device) => ({
						id: device.device_id,
						label: device.device_name || device.device_host || device.device_id,
						detail: bindingByDevice.get(device.device_id)?.worker_name || t`No recommended worker`,
					}))}
					selected={selectedDevices}
					onChange={setSelectedDevices}
					disabled={!canPublish || working}
				/>
				<SelectionPanel
					title={t`Active Flow workers`}
					empty={t`No active Flow workers are registered.`}
					items={workers.map((worker) => ({
						id: worker.id,
						label: worker.name || worker.id,
						detail: `${worker.health || "unknown"}${worker.last_seen ? ` · ${formatDate(worker.last_seen)}` : ""}`,
					}))}
					selected={selectedWorkers}
					onChange={setSelectedWorkers}
					disabled={!canPublish || working}
				/>
			</div>

			{canPublish ? (
				<div className="grid gap-3 rounded-md border border-border bg-muted/20 p-4 md:grid-cols-[minmax(240px,1fr)_auto_auto_auto] md:items-end">
					<div className="grid gap-2">
						<Label htmlFor="flow-deployment-effective-from">
							<Trans>Effective from</Trans>
						</Label>
						<Input
							id="flow-deployment-effective-from"
							type="datetime-local"
							value={effectiveFrom}
							onChange={(event) => setEffectiveFrom(event.target.value)}
							disabled={working}
						/>
					</div>
					<Button
						type="button"
						variant="outline"
						onClick={() =>
							setSelectedWorkers(recommendedWorkers.filter((id) => workers.some((worker) => worker.id === id)))
						}
						disabled={working || recommendedWorkers.length === 0}
					>
						<Trans>Use recommendations</Trans>
					</Button>
					<Button type="button" variant="outline" onClick={validate} disabled={working}>
						<ShieldCheckIcon className="me-2 h-4 w-4" />
						<Trans>Validate</Trans>
					</Button>
					<Button type="button" onClick={publish} disabled={working || !validation}>
						<RocketIcon className="me-2 h-4 w-4" />
						<Trans>Publish deployment</Trans>
					</Button>
				</div>
			) : null}

			{validation ? (
				<div className="rounded-md border border-green-500/30 bg-green-500/5 p-3 text-sm text-green-700">
					<Trans>
						Validated address version {validation.address_version}, policy revision {validation.profile_row_version},
						and {validation.artifact_count} immutable artifacts.
					</Trans>
				</div>
			) : null}
			{job ? (
				<div className="text-sm text-muted-foreground">
					<Trans>Deployment job</Trans> <code>{job.id}</code>: {job.status} ({job.progress_done}/{job.progress_total})
				</div>
			) : null}
			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			{notice ? <div className="rounded-md border border-green-500/30 p-3 text-sm text-green-700">{notice}</div> : null}

			<div className="overflow-x-auto rounded-md border border-border">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>
								<Trans>Worker</Trans>
							</TableHead>
							<TableHead>
								<Trans>Generation</Trans>
							</TableHead>
							<TableHead>
								<Trans>Devices</Trans>
							</TableHead>
							<TableHead>
								<Trans>Desired</Trans>
							</TableHead>
							<TableHead>
								<Trans>Worker health</Trans>
							</TableHead>
							<TableHead>
								<Trans>ACK</Trans>
							</TableHead>
							<TableHead>
								<Trans>Installed</Trans>
							</TableHead>
							<TableHead>
								<Trans>Error</Trans>
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{deployments.length === 0 && !loading ? (
							<TableRow>
								<TableCell colSpan={8} className="text-center text-sm text-muted-foreground">
									<Trans>No Flow worker deployments.</Trans>
								</TableCell>
							</TableRow>
						) : (
							deployments.map((deployment) => (
								<TableRow key={deployment.id}>
									<TableCell>{deployment.worker_name || deployment.worker_id}</TableCell>
									<TableCell>{deployment.generation}</TableCell>
									<TableCell className="max-w-72 text-xs">{deployment.device_names || "—"}</TableCell>
									<TableCell>
										<Badge variant="outline">{deployment.state}</Badge>
									</TableCell>
									<TableCell>{deployment.worker_health || "unknown"}</TableCell>
									<TableCell>{deployment.ack_state || "pending"}</TableCell>
									<TableCell className="whitespace-nowrap text-xs">{formatDate(deployment.installed_at)}</TableCell>
									<TableCell className="max-w-72 text-xs text-destructive">
										{[deployment.failure_stage, deployment.error_code, deployment.error_message]
											.filter(Boolean)
											.join(": ") || "—"}
									</TableCell>
								</TableRow>
							))
						)}
					</TableBody>
				</Table>
			</div>
		</section>
	)
})

function SelectionPanel({
	title,
	empty,
	items,
	selected,
	onChange,
	disabled,
}: {
	title: string
	empty: string
	items: { id: string; label: string; detail: string }[]
	selected: string[]
	onChange: (value: string[]) => void
	disabled: boolean
}) {
	const selectedSet = new Set(selected)
	return (
		<div className="grid content-start gap-2 rounded-md border border-border p-3">
			<div className="flex items-center justify-between gap-2">
				<div className="font-medium">{title}</div>
				<div className="flex gap-1">
					<Button
						type="button"
						variant="ghost"
						size="sm"
						onClick={() => onChange(items.map((item) => item.id))}
						disabled={disabled}
					>
						<Trans>All</Trans>
					</Button>
					<Button type="button" variant="ghost" size="sm" onClick={() => onChange([])} disabled={disabled}>
						<Trans>None</Trans>
					</Button>
				</div>
			</div>
			{items.length === 0 ? <p className="text-sm text-muted-foreground">{empty}</p> : null}
			{items.map((item) => (
				<label
					key={item.id}
					htmlFor={`flow-deployment-selection-${item.id}`}
					className="flex cursor-pointer items-start gap-2 rounded p-1.5 hover:bg-muted/50"
				>
					<Checkbox
						id={`flow-deployment-selection-${item.id}`}
						checked={selectedSet.has(item.id)}
						disabled={disabled}
						onCheckedChange={(checked) =>
							onChange(checked ? uniqueStrings([...selected, item.id]) : selected.filter((id) => id !== item.id))
						}
					/>
					<span className="grid min-w-0 gap-0.5">
						<span className="truncate text-sm">{item.label}</span>
						<span className="truncate text-xs text-muted-foreground">{item.detail}</span>
					</span>
				</label>
			))}
		</div>
	)
}

async function waitForJob(jobID: string): Promise<OperationJob> {
	let current: OperationJob = { id: jobID, status: "queued", progress_done: 0, progress_total: 0 }
	for (let attempt = 0; attempt < 120; attempt++) {
		current = await api.send<OperationJob>(`/api/v1/operation-jobs/${encodeURIComponent(jobID)}`, {})
		if (["succeeded", "failed", "canceled"].includes(current.status)) return current
		await new Promise((resolve) => globalThis.setTimeout(resolve, 1000))
	}
	return current
}

function uniqueStrings(values: string[]) {
	return [...new Set(values.filter(Boolean))].sort()
}

function defaultEffectiveFrom() {
	const date = new Date(Math.ceil(Date.now() / 60_000) * 60_000 + 60_000)
	const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
	return local.toISOString().slice(0, 16)
}

function parseEffectiveFrom(value: string) {
	const date = new Date(value)
	if (!value || Number.isNaN(date.getTime()) || date.getSeconds() !== 0 || date.getMilliseconds() !== 0) {
		throw new Error("Effective time must be a minute boundary")
	}
	return date.toISOString()
}

function formatDate(value?: string) {
	if (!value) return "—"
	const date = new Date(value)
	return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}
