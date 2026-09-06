import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PlugZapIcon, PlusIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import type { ColumnDefine } from "@/lib/vtable"

type AgentRecord = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	AgentType?: string
	agent_type?: string
	Mode?: string
	mode?: string
	Endpoint?: string
	endpoint?: string
	Status?: string
	status?: string
	LastSeen?: string
	last_seen?: string
	LastRun?: string
	last_run?: string
	LastSuccess?: string
	last_success?: string
	LastError?: string
	last_error?: string
	RunCount?: number
	run_count?: number
	FailureCount?: number
	failure_count?: number
	UpdatedAt?: string
	updated_at?: string
}

type AgentsResponse = {
	items?: AgentRecord[]
	total?: number
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
			if (agentType !== "all") params.set("agent_type", agentType)
			if (status !== "all") params.set("status", status)
			const data = await pb.send<AgentsResponse>(`/api/v1/agent-registry?${params}`, {})
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
				id: agent.ID ?? agent.id ?? "",
				agentType: agent.AgentType ?? agent.agent_type ?? "snmp",
				target: agent.TargetID ?? agent.target_id ?? "—",
				mode: agent.Mode ?? agent.mode ?? "—",
				endpoint: agent.Endpoint ?? agent.endpoint ?? "—",
				status: agent.Status ?? agent.status ?? "—",
				lastSeen: formatTime(agent.LastSeen ?? agent.last_seen),
				lastRun: formatTime(agent.LastRun ?? agent.last_run),
				lastSuccess: formatTime(agent.LastSuccess ?? agent.last_success),
				runCount: agent.RunCount ?? agent.run_count ?? 0,
				failureCount: agent.FailureCount ?? agent.failure_count ?? 0,
				lastError: agent.LastError ?? agent.last_error ?? "",
				updated: formatTime(agent.UpdatedAt ?? agent.updated_at),
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

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
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
				</AgentFilter>
				<AgentFilter label={t`Status`} value={status} onChange={(value) => resetPage(() => setStatus(value))}>
					<SelectItem value="all">{t`All statuses`}</SelectItem>
					{["pending", "up", "down", "error", "disabled"].map((value) => (
						<SelectItem key={value} value={value}>
							{value}
						</SelectItem>
					))}
				</AgentFilter>
				<AgentFilter label={t`Sort`} value={sort} onChange={(value) => resetPage(() => setSort(value))} wide>
					<SelectItem value="updated_at:desc">{t`Recently updated`}</SelectItem>
					<SelectItem value="id:asc">{t`ID A–Z`}</SelectItem>
					<SelectItem value="target_id:asc">{t`Target A–Z`}</SelectItem>
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
		</div>
	)
})

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
