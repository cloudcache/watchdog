import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { KeyRoundIcon, PlugZapIcon, PlusIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog"
import { InputCopy } from "@/components/ui/input-copy"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { api } from "@/lib/api"
import {
	agentActivationCommand,
	agentServiceName,
	compatibleDeviceKind,
	defaultAgentID,
	registryArgumentsText,
} from "@/lib/agent-control"
import type { ColumnDefine } from "@/lib/vtable"

type AgentRecord = {
	id: string
	device_id?: string
	kind: string
	mode?: string
	endpoint?: string
	status: string
	health?: string
	desired_plan_version?: number
	acked_plan_version?: number
	last_seen?: string
	last_run?: string
	last_success?: string
	last_error?: string
	run_count?: number
	failure_count?: number
	updated_at?: string
}

type AgentsResponse = {
	items?: AgentRecord[]
	total?: number
}

type EnrollmentTarget = {
	id: string
	name?: string
	host?: string
	kind: string
}

function requestedAgentKind() {
	const value = new URLSearchParams(window.location.search).get("enroll") ?? ""
	return ["system", "snmp", "flow_collect", "flow_worker", "probe"].includes(value) ? value : ""
}

export default memo(() => {
	const { t } = useLingui()
	const [agents, setAgents] = useState<AgentRecord[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [agentType, setAgentType] = useState("all")
	const [status, setStatus] = useState("all")
	const [sort, setSort] = useState("updated_at:desc")
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestedEnrollmentKind = requestedAgentKind()
	const [enrollmentOpen, setEnrollmentOpen] = useState(Boolean(requestedEnrollmentKind))
	const requestSequence = useRef(0)

	useEffect(() => {
		const timer = window.setTimeout(() => {
			setPage(0)
			setQuery(search.trim())
		}, 300)
		return () => window.clearTimeout(timer)
	}, [search])

	const refresh = useCallback(async () => {
		const sequence = ++requestSequence.current
		setLoading(true)
		setError("")
		try {
			const [sortColumn, order] = sort.split(":")
			const params = new URLSearchParams({
				limit: String(pageSize),
				offset: String(page * pageSize),
				sort: sortColumn,
				order,
			})
			if (query) params.set("q", query)
			if (agentType !== "all") params.set("kind", agentType)
			if (status !== "all") params.set("status", status)
			const data = await api.send<AgentsResponse>(`/api/v1/agents?${params}`, {})
			if (sequence === requestSequence.current) {
				setAgents(data.items ?? [])
				setTotal(data.total ?? 0)
			}
		} catch (err) {
			if (sequence === requestSequence.current) {
				setAgents([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load agents`)
			}
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [agentType, page, pageSize, query, sort, status, t])

	useEffect(() => {
		document.title = `${t`Agents`} / Watchdog`
		refresh()
	}, [refresh, t])

	const records = useMemo(
		() =>
			agents.map((agent) => ({
				id: agent.id,
				agentType: agent.kind,
				target: agent.device_id ?? "—",
				mode: agent.mode ?? "—",
				endpoint: agent.endpoint ?? "—",
				status: agent.status,
				health: agent.health ?? "unknown",
				plan: `${agent.acked_plan_version ?? 0} / ${agent.desired_plan_version ?? 0}`,
				lastSeen: formatTime(agent.last_seen),
				lastRun: formatTime(agent.last_run),
				lastSuccess: formatTime(agent.last_success),
				runCount: agent.run_count ?? 0,
				failureCount: agent.failure_count ?? 0,
				lastError: agent.last_error ?? "",
				updated: formatTime(agent.updated_at),
			})),
		[agents]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "id", title: "ID", width: 220 },
			{ field: "agentType", title: t`Type`, width: 100 },
			{ field: "target", title: t`Target`, width: 220 },
			{ field: "mode", title: t`Mode`, width: 100 },
			{ field: "endpoint", title: t`Endpoint`, width: 260 },
			{ field: "status", title: t`Status`, width: 120 },
			{ field: "health", title: t`Health`, width: 120 },
			{ field: "plan", title: t`ACK / desired`, width: 130 },
			{ field: "lastSeen", title: t`Last Seen`, width: 180 },
			{ field: "lastRun", title: t`Last Run`, width: 180 },
			{ field: "lastSuccess", title: t`Last Success`, width: 180 },
			{ field: "runCount", title: t`Runs`, width: 90 },
			{ field: "failureCount", title: t`Failures`, width: 90 },
			{ field: "lastError", title: t`Last Error`, width: 320 },
			{ field: "updated", title: t`Updated`, width: 180 },
		],
		[t]
	)

	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(
		() => ({
			options: {
				agentType: ["snmp", "system", "flow_collect", "flow_worker", "probe"].map((value) => ({ value })),
				status: ["registered", "active", "draining", "revoked"].map((value) => ({ value })),
			},
			selected: {
				agentType: agentType === "all" ? [] : [agentType],
				status: status === "all" ? [] : [status],
			},
			selection: { agentType: "single" as const, status: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) => {
				const value = values.length > 0 ? String(values[0]) : "all"
				resetPage(() => {
					if (field === "agentType") setAgentType(value)
					if (field === "status") setStatus(value)
				})
			},
			onClearAll: () =>
				resetPage(() => {
					setAgentType("all")
					setStatus("all")
				}),
		}),
		[agentType, status]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				id: "id",
				agentType: "kind",
				target: "device_id",
				mode: "mode",
				status: "status",
				lastSeen: "last_seen_at",
				runCount: "run_count",
				failureCount: "failure_count",
				updated: "updated_at",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => setEnrollmentOpen(true)}>
						<KeyRoundIcon className="me-2 h-4 w-4" />
						<Trans>Enroll</Trans>
					</Button>
					<PlugZapIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Agents</Trans> ({total})
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "agent_new"))}>
						<PlusIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap gap-2">
				<AgentFilter label={t`Type`} value={agentType} onChange={(value) => resetPage(() => setAgentType(value))}>
					<SelectItem value="all">{t`All types`}</SelectItem>
					<SelectItem value="snmp">SNMP</SelectItem>
					<SelectItem value="system">{t`System`}</SelectItem>
					<SelectItem value="flow_collect">flow_collect</SelectItem>
					<SelectItem value="flow_worker">flow_worker</SelectItem>
					<SelectItem value="probe">probe</SelectItem>
				</AgentFilter>
				<AgentFilter label={t`Status`} value={status} onChange={(value) => resetPage(() => setStatus(value))}>
					<SelectItem value="all">{t`All statuses`}</SelectItem>
					{["registered", "active", "draining", "revoked"].map((value) => (
						<SelectItem key={value} value={value}>
							{value}
						</SelectItem>
					))}
				</AgentFilter>
				<AgentFilter label={t`Sort`} value={sort} onChange={(value) => resetPage(() => setSort(value))} wide>
					<SelectItem value="updated_at:desc">{t`Recently updated`}</SelectItem>
					<SelectItem value="id:asc">{t`ID A–Z`}</SelectItem>
					<SelectItem value="device_id:asc">{t`Target A–Z`}</SelectItem>
					<SelectItem value="status:asc">{t`Status`}</SelectItem>
					<SelectItem value="last_seen_at:desc">{t`Last seen`}</SelectItem>
					<SelectItem value="failure_count:desc">{t`Most failures`}</SelectItem>
				</AgentFilter>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No agents found.`}
					searchPlaceholder={t`Search agent, target, endpoint, or error...`}
					searchValue={search}
					onSearchChange={setSearch}
					serverFiltering={serverFiltering}
					serverSorting={serverSorting}
					serverPagination={{
						page,
						pageSize,
						totalCount: total,
						onPageChange: setPage,
						onPageSizeChange: (value) => {
							setPage(0)
							setPageSize(value)
						},
					}}
					height={560}
					onRowClick={(record) => navigate(getPagePath($router, "agent_runs", { id: String(record.id) }))}
				/>
			</div>
			<EnrollmentDialog
				open={enrollmentOpen}
				initialKind={requestedEnrollmentKind || "system"}
				onClose={() => setEnrollmentOpen(false)}
			/>
		</div>
	)
})

function EnrollmentDialog({ open, initialKind, onClose }: { open: boolean; initialKind: string; onClose: () => void }) {
	const { t } = useLingui()
	const [kind, setKind] = useState(initialKind)
	const [agentID, setAgentID] = useState(() => defaultAgentID(initialKind))
	const [deviceID, setDeviceID] = useState("")
	const [targets, setTargets] = useState<EnrollmentTarget[]>([])
	const [token, setToken] = useState("")
	const [publicKey, setPublicKey] = useState("")
	const [keyID, setKeyID] = useState("")
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState("")
	useEffect(() => {
		if (!open) return
		setKind(initialKind)
		setAgentID(defaultAgentID(initialKind))
		setToken("")
		setError("")
		Promise.all([
			api.send<{ items?: EnrollmentTarget[] }>("/api/v1/devices", { query: { limit: 500 } }),
			api.send<{ key_id?: string; public_key?: string }>("/api/v1/agents/plan-public-key", {}),
		])
			.then(([devices, trust]) => {
				setTargets(devices.items ?? [])
				setKeyID(trust.key_id ?? "")
				setPublicKey(trust.public_key ?? "")
			})
			.catch((err) => setError(err instanceof Error ? err.message : t`Failed to load targets`))
	}, [initialKind, open, t])
	const compatibleTargets = targets.filter((target) => compatibleAgentTarget(kind, target.kind))
	const create = async () => {
		setSaving(true)
		setError("")
		try {
			if (!agentID.trim() || agentID.trim().length > 26) throw new Error(t`Agent ID must contain 1–26 characters.`)
			const result = await api.send<{ token: string }>("/api/v1/agents/enrollment-tokens", {
				method: "POST",
				body: { kind, device_id: deviceID, expires_in_seconds: 900 },
			})
			setToken(result.token)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to create enrollment token`)
		} finally {
			setSaving(false)
		}
	}
	const apiURL = new URL(api.buildURL("/api/v1"), window.location.origin).origin
	const registryArgs = token ? registryArgumentsText(kind, agentID.trim(), apiURL) : ""
	const serviceName = agentServiceName(kind)
	const activationCommand = token ? agentActivationCommand(kind, agentID.trim(), publicKey) : ""
	return (
		<Dialog open={open} onOpenChange={(value) => !value && onClose()}>
			<DialogContent className="max-w-2xl">
				<DialogHeader>
					<DialogTitle>
						<Trans>Enroll Agent</Trans>
					</DialogTitle>
					<DialogDescription>
						<Trans>Create a 15-minute, one-time enrollment token and connect an independently managed process.</Trans>
					</DialogDescription>
				</DialogHeader>
				<div className="grid gap-3">
					<div className="grid gap-1.5">
						<Label htmlFor="agent-enrollment-id">
							<Trans>Agent ID</Trans>
						</Label>
						<Input
							id="agent-enrollment-id"
							value={agentID}
							maxLength={26}
							disabled={Boolean(token)}
							onChange={(event) => setAgentID(event.target.value)}
						/>
					</div>
					<Select
						value={kind}
						onValueChange={(value) => {
							setKind(value)
							setAgentID(defaultAgentID(value))
							setDeviceID((current) => {
								const selected = targets.find((target) => target.id === current)
								return selected && compatibleAgentTarget(value, selected.kind) ? current : ""
							})
							setToken("")
						}}
					>
						<SelectTrigger>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							{["system", "snmp", "flow_collect", "flow_worker", "probe"].map((value) => (
								<SelectItem key={value} value={value}>
									{value}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
					<Select
						value={deviceID || "__unbound__"}
						onValueChange={(value) => {
							setDeviceID(value === "__unbound__" ? "" : value)
							setToken("")
						}}
					>
						<SelectTrigger aria-label={t`Target`}>
							<SelectValue placeholder={t`Target`} />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="__unbound__">
								<Trans>Unbound</Trans>
							</SelectItem>
							{compatibleTargets.map((target) => (
								<SelectItem key={target.id} value={target.id}>
									{target.name || target.host || target.id} · {target.kind}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
					{token ? (
						<div className="grid gap-3 rounded-md border border-border p-3">
							<div className="text-sm font-medium">
								<Trans>Bootstrap this process</Trans>
							</div>
							<p className="text-xs text-muted-foreground">
								<Trans>
									The platform manages identity, signed configuration, ACK and health. systemd or a container runtime
									owns process start, restart and host privileges.
								</Trans>
							</p>
							<div className="grid gap-1.5">
								<Label>
									<Trans>One-time enrollment token</Trans>
								</Label>
								<InputCopy id="agent-enrollment-token" name="agent-enrollment-token" value={token} />
							</div>
							{activationCommand ? (
								<div className="grid gap-1.5">
									<Label>
										<Trans>Host activation command</Trans>
									</Label>
									<InputCopy id="agent-activation-command" name="agent-activation-command" value={activationCommand} />
									<p className="text-xs text-muted-foreground">
										<Trans>
											Run this command on the Watchdog host, then paste the one-time token when prompted. The token is
											not echoed or stored in shell history.
										</Trans>
									</p>
								</div>
							) : null}
							<details className="rounded-md border border-border p-3">
								<summary className="cursor-pointer text-sm font-medium">
									<Trans>Advanced manual setup</Trans>
								</summary>
								<div className="mt-3 grid gap-3">
									<div className="grid gap-1.5">
										<Label>
											<Trans>Agent plan public key</Trans> ({keyID || "-"})
										</Label>
										<InputCopy id="agent-plan-public-key" name="agent-plan-public-key" value={publicKey} />
									</div>
									<div className="grid gap-1.5">
										<Label>
											<Trans>Registry arguments</Trans>
										</Label>
										<InputCopy id="agent-registry-arguments" name="agent-registry-arguments" value={registryArgs} />
									</div>
									<p className="text-xs text-muted-foreground">
										{serviceName ? (
											<Trans>
												For automated deployment, save the token and public key to protected files, add the registry
												arguments to {serviceName}, then enable that service.
											</Trans>
										) : (
											<Trans>
												Install the matching process, save the token and public key, then start it with the registry
												arguments above.
											</Trans>
										)}
									</p>
								</div>
							</details>
						</div>
					) : null}
					{error ? <div className="text-sm text-destructive">{error}</div> : null}
				</div>
				<DialogFooter>
					<Button variant="outline" onClick={onClose}>
						<Trans>Close</Trans>
					</Button>
					<Button onClick={create} disabled={saving || Boolean(token)}>
						<Trans>Create token</Trans>
					</Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	)
}

function AgentFilter({
	label,
	value,
	onChange,
	children,
	wide = false,
}: {
	label: string
	value: string
	onChange: (value: string) => void
	children: React.ReactNode
	wide?: boolean
}) {
	return (
		<Select value={value} onValueChange={onChange}>
			<SelectTrigger className={wide ? "w-44" : "w-36"} aria-label={label}>
				<SelectValue placeholder={label} />
			</SelectTrigger>
			<SelectContent>{children}</SelectContent>
		</Select>
	)
}

function formatTime(value?: string) {
	if (!value) return "—"
	const time = new Date(value)
	return Number.isNaN(time.getTime()) || time.getFullYear() <= 1 ? "—" : time.toLocaleString()
}

function compatibleAgentTarget(agentKind: string, deviceKind: string) {
	const expected = compatibleDeviceKind(agentKind)
	return expected === "" || deviceKind === expected
}
