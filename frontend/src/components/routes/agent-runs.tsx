import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@/lib/page-path"
import { ArrowLeftIcon, BracesIcon, PencilIcon, PlugZapIcon, RefreshCwIcon } from "lucide-react"
import { memo, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { api } from "@/lib/api"
import type { ColumnDefine } from "@/lib/vtable"

type AgentRecord = {
	id: string
	device_id?: string
	mode?: string
	status: string
	last_run?: string
	last_error?: string
	run_count?: number
	failure_count?: number
}

type AgentRunRecord = {
	ID?: string
	id?: string
	Status?: string
	status?: string
	Error?: string
	error?: string
	Seen?: boolean
	seen?: boolean
	StartedAt?: string
	started_at?: string
	EndedAt?: string
	ended_at?: string
	DurationMS?: number
	duration_ms?: number
	summary?: Record<string, unknown>
}

type AgentRunsProps = {
	id: string
}

export default memo(({ id }: AgentRunsProps) => {
	const { t } = useLingui()
	const [agent, setAgent] = useState<AgentRecord | null>(null)
	const [runs, setRuns] = useState<AgentRunRecord[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [debouncedSearch, setDebouncedSearch] = useState("")
	const [statusFilter, setStatusFilter] = useState("")
	const [seenFilter, setSeenFilter] = useState("")
	const [sort, setSort] = useState("ended:desc")
	const [reloadKey, setReloadKey] = useState(0)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")
	const requestSequence = useRef(0)

	type RunsResponse = { items?: AgentRunRecord[]; total?: number }

	useEffect(() => {
		const handle = setTimeout(() => {
			setPage(0)
			setDebouncedSearch(search.trim())
		}, 300)
		return () => clearTimeout(handle)
	}, [search])

	useEffect(() => {
		document.title = `${id} / ${t`Agent Runs`} / Watchdog`
		api
			.send<AgentRecord>(`/api/v1/agents/${id}`, {})
			.then(setAgent)
			.catch((err) => setError(err instanceof Error ? err.message : t`Failed to load agent runs`))
	}, [id, reloadKey, t])

	useEffect(() => {
		const sequence = ++requestSequence.current
		const [sortField, order] = sort.split(":")
		setLoading(true)
		setError("")
		api
			.send<RunsResponse>(`/api/v1/agents/${id}/runs`, {
				query: {
					q: debouncedSearch || undefined,
					status: statusFilter || undefined,
					seen: seenFilter || undefined,
					sort: sortField,
					order,
					limit: pageSize,
					offset: page * pageSize || undefined,
				},
			})
			.then((data) => {
				if (sequence !== requestSequence.current) return
				setRuns(data.items ?? [])
				setTotal(data.total ?? 0)
			})
			.catch((err) => {
				if (sequence !== requestSequence.current) return
				setRuns([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load agent runs`)
			})
			.finally(() => {
				if (sequence === requestSequence.current) setLoading(false)
			})
	}, [debouncedSearch, id, page, pageSize, reloadKey, seenFilter, sort, statusFilter, t])

	const records = useMemo(
		() =>
			runs.map((run) => ({
				id: run.ID ?? run.id ?? "",
				status: run.Status ?? run.status ?? "-",
				seen: (run.Seen ?? run.seen) ? t`yes` : t`no`,
				started: formatDate(run.StartedAt ?? run.started_at),
				ended: formatDate(run.EndedAt ?? run.ended_at),
				duration: formatDuration(run.DurationMS ?? run.duration_ms ?? 0),
				workload: formatWorkload(run.summary),
				error: run.Error ?? run.error ?? "",
			})),
		[runs, t]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "status", title: t`Status`, width: 120 },
			{ field: "started", title: t`Started`, width: 190 },
			{ field: "ended", title: t`Ended`, width: 190 },
			{ field: "duration", title: t`Duration`, width: 110 },
			{ field: "workload", title: t`Workload`, width: 460 },
			{ field: "seen", title: t`Seen`, width: 90 },
			{ field: "error", title: t`Error`, width: 420 },
			{ field: "id", title: "ID", width: 180 },
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
				status: [{ value: "success" }, { value: "failure" }],
				seen: [
					{ value: "true", label: t`yes` },
					{ value: "false", label: t`no` },
				],
			},
			selected: { status: statusFilter ? [statusFilter] : [], seen: seenFilter ? [seenFilter] : [] },
			selection: { status: "single" as const, seen: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) =>
				resetPage(() => {
					const value = values.length > 0 ? String(values[0]) : ""
					if (field === "status") setStatusFilter(value)
					if (field === "seen") setSeenFilter(value)
				}),
			onClearAll: () =>
				resetPage(() => {
					setStatusFilter("")
					setSeenFilter("")
				}),
		}),
		[seenFilter, statusFilter, t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				status: "status",
				started: "started",
				ended: "ended",
				duration: "duration",
				seen: "seen",
				id: "id",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)
	const status = agent?.status ?? "-"
	const targetID = agent?.device_id ?? "-"

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Button variant="ghost" size="icon" onClick={() => navigate(getPagePath($router, "agents"))}>
						<ArrowLeftIcon className="h-4 w-4" />
					</Button>
					<PlugZapIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Agent Runs</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "agent_plans", { id }))}>
						<BracesIcon className="me-2 h-4 w-4" />
						<Trans>Plans</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "agent_edit", { id }))}>
						<PencilIcon className="me-2 h-4 w-4" />
						<Trans>Edit</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={() => setReloadKey((value) => value + 1)} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="grid gap-3 md:grid-cols-4">
				<Summary label={t`Agent`} value={id} />
				<Summary label={t`Target`} value={targetID} />
				<Summary label={t`Status`} value={localizedStatus(status)} />
				<Summary label={t`Runs`} value={`${agent?.run_count ?? 0} / ${agent?.failure_count ?? 0}`} />
			</div>

			<div className="rounded-md border border-border bg-card p-3">
				{error ? <div className="p-3 text-sm text-destructive">{error}</div> : null}
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No agent runs found.`}
					searchPlaceholder={t`Search by run ID or error...`}
					height={560}
					rowHeight={46}
					searchValue={search}
					onSearchChange={setSearch}
					serverPagination={{
						page,
						pageSize,
						totalCount: total,
						onPageChange: setPage,
						onPageSizeChange: (value) => resetPage(() => setPageSize(value)),
					}}
					serverFiltering={serverFiltering}
					serverSorting={serverSorting}
				/>
			</div>
		</div>
	)
})

function Summary({ label, value }: { label: string; value: string }) {
	return (
		<div className="rounded-md border border-border bg-card p-3">
			<div className="text-xs font-medium text-muted-foreground">{label}</div>
			<div className="mt-1 break-words text-sm font-medium">{value || "-"}</div>
		</div>
	)
}

function localizedStatus(status: string) {
	if (!status) {
		return "-"
	}
	return status === "success" || status === "up"
		? `● ${status}`
		: status === "failure" || status === "error"
			? `● ${status}`
			: status
}

function formatDate(value?: string) {
	if (!value) {
		return "-"
	}
	const date = new Date(value)
	if (Number.isNaN(date.getTime())) {
		return "-"
	}
	return date.toLocaleString()
}

function formatDuration(value: number) {
	if (!value) {
		return "-"
	}
	if (value < 1000) {
		return `${value} ms`
	}
	return `${(value / 1000).toFixed(1)} s`
}

// formatWorkload renders a run's processing summary (agent_runs.summary_json) as a
// compact key=value line. Keys differ by agent kind (worker: records/kafka_lag_records,
// collector: sflow_received/netflow_received, snmp: samples/failed), so it renders every
// reported counter generically rather than hard-coding one shape.
function formatWorkload(summary?: Record<string, unknown>) {
	if (!summary || typeof summary !== "object") {
		return ""
	}
	const parts: string[] = []
	for (const [key, value] of Object.entries(summary)) {
		if (value === null || value === undefined) {
			continue
		}
		parts.push(`${key}=${typeof value === "number" ? value.toLocaleString() : String(value)}`)
	}
	return parts.join(" · ")
}
