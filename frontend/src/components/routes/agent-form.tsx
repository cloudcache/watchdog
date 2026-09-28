import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { ArrowLeftIcon, BracesIcon, PlugZapIcon, SaveIcon, ShieldOffIcon, Trash2Icon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import { agentCapabilities, compatibleDeviceKind } from "@/lib/agent-control"
import { cn } from "@/lib/utils"

type AgentRecord = {
	id: string
	device_id?: string
	kind: string
	mode?: string
	endpoint?: string
	status: string
}

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	kind?: string
}

type TargetsResponse = {
	items?: TargetRecord[]
}

type AgentFormProps = {
	id?: string
}

type FormState = {
	id: string
	agentType: string
	targetID: string
	mode: string
	endpoint: string
	status: string
}

export default memo(({ id }: AgentFormProps) => {
	const { t } = useLingui()
	const isEditing = Boolean(id)
	const [targets, setTargets] = useState<TargetRecord[]>([])
	const [form, setForm] = useState<FormState>(() => ({
		id: id ?? createAgentID(),
		agentType: "snmp",
		targetID: "",
		mode: "push",
		endpoint: "",
		status: "registered",
	}))
	const [loading, setLoading] = useState(true)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	// Weak ETag from the last load, echoed as If-Match on save (optimistic
	// concurrency): a concurrent edit is rejected (412), not overwritten.
	const etagRef = useRef("")

	const load = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const targetsData = await api.send<TargetsResponse>("/api/v1/devices", {})
			const targetItems = targetsData.items ?? []
			setTargets(targetItems)
			if (!id) {
				setForm((current) => ({
					...current,
					targetID: current.targetID || firstTargetIDForKind(targetItems, current.agentType),
				}))
				return
			}
			const agent = await api.send<AgentRecord>(`/api/v1/agents/${id}`, {
				onResponse: (response) => {
					etagRef.current = response.headers.get("ETag") ?? ""
				},
			})
			setForm({
				id: agent.id ?? id,
				agentType: agent.kind ?? "snmp",
				targetID: agent.device_id ?? "",
				mode: agent.mode ?? "push",
				endpoint: agent.endpoint ?? "",
				status: agent.status ?? "registered",
			})
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load agent`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	useEffect(() => {
		document.title = `${isEditing ? t`Edit Agent` : t`Create Agent`} / Watchdog`
		load()
	}, [isEditing, load, t])

	const save = async () => {
		setSaving(true)
		setError("")
		try {
			const body = {
				id: form.id.trim(),
				device_id: form.targetID,
				kind: form.agentType,
				mode: form.mode,
				endpoint: form.endpoint.trim(),
				status: form.status,
				api_version: "v1",
				capabilities: agentCapabilities(form.agentType),
			}
			const saved = await api.send<AgentRecord>(id ? `/api/v1/agents/${id}` : "/api/v1/agents", {
				method: id ? "PATCH" : "POST",
				headers: id && etagRef.current ? { "If-Match": etagRef.current } : undefined,
				body,
			})
			navigate(getPagePath($router, "agent_edit", { id: saved.id ?? form.id }))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to save agent`)
		} finally {
			setSaving(false)
		}
	}

	const revokeAgent = async () => {
		if (!id || !window.confirm(t`Revoke this agent?`)) return
		setSaving(true)
		setError("")
		try {
			await api.send(`/api/v1/agents/${id}/revoke`, {
				method: "POST",
				headers: etagRef.current ? { "If-Match": etagRef.current } : undefined,
			})
			await load()
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to revoke agent`)
		} finally {
			setSaving(false)
		}
	}

	const deleteAgent = async () => {
		if (!id || !window.confirm(t`Delete this agent?`)) {
			return
		}
		setSaving(true)
		setError("")
		try {
			await api.send(`/api/v1/agents/${id}`, {
				method: "DELETE",
				headers: etagRef.current ? { "If-Match": etagRef.current } : undefined,
			})
			navigate(getPagePath($router, "agents"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to delete agent`)
		} finally {
			setSaving(false)
		}
	}

	const update = (patch: Partial<FormState>) => setForm((current) => ({ ...current, ...patch }))
	const targetOptions = targets.filter((target) => targetKind(target) === agentTargetKind(form.agentType))
	const canSave = Boolean(form.id.trim())

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex min-w-0 items-center gap-2">
					<Link
						href={getPagePath($router, "agents")}
						className={cn(buttonVariants({ variant: "ghost", size: "icon" }), "shrink-0")}
						aria-label={t`Back to agents`}
					>
						<ArrowLeftIcon className="h-4 w-4" />
					</Link>
					<PlugZapIcon className="h-5 w-5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="truncate text-xl font-semibold tracking-normal">
						{isEditing ? <Trans>Edit Agent</Trans> : <Trans>Create Agent</Trans>}
					</h1>
				</div>
				<div className="flex items-center gap-2">
					{isEditing ? (
						<>
							<Button
								size="sm"
								variant="outline"
								onClick={() => navigate(getPagePath($router, "agent_plans", { id: id ?? form.id }))}
							>
								<BracesIcon className="me-2 h-4 w-4" />
								<Trans>Plans</Trans>
							</Button>
							<Button
								size="sm"
								variant="outline"
								onClick={revokeAgent}
								disabled={loading || saving || form.status === "revoked"}
							>
								<ShieldOffIcon className="me-2 h-4 w-4" />
								<Trans>Revoke</Trans>
							</Button>
							<Button size="sm" variant="outline" onClick={deleteAgent} disabled={loading || saving}>
								<Trash2Icon className="me-2 h-4 w-4" />
								<Trans>Delete</Trans>
							</Button>
						</>
					) : null}
					<Button size="sm" onClick={save} disabled={loading || saving || !canSave}>
						<SaveIcon className="me-2 h-4 w-4" />
						<Trans>Save</Trans>
					</Button>
				</div>
			</div>

			{error ? <div className="rounded-md border border-border p-3 text-sm text-destructive">{error}</div> : null}

			<div className="grid gap-4 rounded-md border border-border p-4">
				<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
					<Field label="ID">
						<Input
							value={form.id}
							onChange={(event) => update({ id: event.target.value })}
							disabled={isEditing || loading}
						/>
					</Field>
					<Field label={t`Agent Type`}>
						<Select
							value={form.agentType}
							onValueChange={(agentType) => update({ agentType, targetID: firstTargetIDForKind(targets, agentType) })}
							disabled={loading || isEditing}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="snmp">snmp</SelectItem>
								<SelectItem value="system">system</SelectItem>
								<SelectItem value="flow_collect">flow_collect</SelectItem>
								<SelectItem value="flow_worker">flow_worker</SelectItem>
								<SelectItem value="probe">probe</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Target`}>
						<Select
							value={form.targetID || "__unbound__"}
							onValueChange={(targetID) => update({ targetID: targetID === "__unbound__" ? "" : targetID })}
							disabled={loading}
						>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="__unbound__">
									<Trans>Unbound</Trans>
								</SelectItem>
								{targetOptions.map((target) => {
									const targetID = target.ID ?? target.id ?? ""
									return (
										<SelectItem key={targetID} value={targetID}>
											{target.Name ?? target.name ?? targetID} · {targetKind(target)}
										</SelectItem>
									)
								})}
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Mode`}>
						<Select value={form.mode} onValueChange={(mode) => update({ mode })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="push">push</SelectItem>
								<SelectItem value="pull">pull</SelectItem>
							</SelectContent>
						</Select>
					</Field>
					<Field label={t`Endpoint`}>
						<Input
							value={form.endpoint}
							onChange={(event) => update({ endpoint: event.target.value })}
							disabled={loading}
						/>
					</Field>
					<Field label={t`Status`}>
						<Select value={form.status} onValueChange={(status) => update({ status })} disabled={loading}>
							<SelectTrigger>
								<SelectValue />
							</SelectTrigger>
							<SelectContent>
								<SelectItem value="registered">registered</SelectItem>
								<SelectItem value="active">active</SelectItem>
								<SelectItem value="draining">draining</SelectItem>
								{form.status === "revoked" ? <SelectItem value="revoked">revoked</SelectItem> : null}
							</SelectContent>
						</Select>
					</Field>
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

function createAgentID() {
	const bytes = new Uint8Array(8)
	crypto.getRandomValues(bytes)
	return `agent_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`
}

function targetKind(target: TargetRecord) {
	return target.kind ?? "system"
}

function agentTargetKind(agentType: string) {
	return compatibleDeviceKind(agentType)
}

function firstTargetIDForKind(targets: TargetRecord[], agentType: string) {
	const target = targets.find((item) => targetKind(item) === agentTargetKind(agentType))
	return target?.ID ?? target?.id ?? ""
}
