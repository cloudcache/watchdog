import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { ArrowLeftIcon, PencilIcon, PlugZapIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { pb } from "@/lib/api"
import { createListTable, disposeTable, type ListTable } from "@/lib/vtable"

type AgentRecord = {
	ID?: string
	id?: string
	TargetID?: string
	target_id?: string
	Mode?: string
	mode?: string
	Status?: string
	status?: string
	LastRun?: string
	last_run?: string
	LastError?: string
	last_error?: string
	RunCount?: number
	run_count?: number
	FailureCount?: number
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
}

type AgentRunsProps = {
	id: string
}

const RUNS_PAGE_SIZE = 200

export default memo(({ id }: AgentRunsProps) => {
	const { t } = useLingui()
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [agent, setAgent] = useState<AgentRecord | null>(null)
	const [runs, setRuns] = useState<AgentRunRecord[]>([])
	const [cursor, setCursor] = useState("")
	const [loading, setLoading] = useState(true)
	const [loadingMore, setLoadingMore] = useState(false)
	const [error, setError] = useState("")

	type RunsResponse = { items?: AgentRunRecord[]; next_cursor?: string }

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const [agentData, runData] = await Promise.all([
				pb.send<AgentRecord>(`/api/v1/agent-registry/${id}`, {}),
				pb.send<RunsResponse>(`/api/v1/agent-registry/${id}/runs`, { query: { limit: RUNS_PAGE_SIZE } }),
			])
			setAgent(agentData)
			setRuns(runData.items ?? [])
			setCursor(runData.next_cursor ?? "")
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load agent runs`)
		} finally {
			setLoading(false)
		}
	}, [id, t])

	const loadMore = useCallback(async () => {
		if (!cursor || loadingMore) return
		setLoadingMore(true)
		try {
			const data = await pb.send<RunsResponse>(`/api/v1/agent-registry/${id}/runs`, {
				query: { limit: RUNS_PAGE_SIZE, cursor },
			})
			setRuns((current) => [...current, ...(data.items ?? [])])
			setCursor(data.next_cursor ?? "")
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load agent runs`)
		} finally {
			setLoadingMore(false)
		}
	}, [cursor, id, loadingMore, t])

	useEffect(() => {
		document.title = `${id} / ${t`Agent Runs`} / Watchdog`
		refresh()
	}, [id, refresh, t])

	const records = useMemo(
		() =>
			runs.map((run) => ({
				id: run.ID ?? run.id ?? "",
				status: localizedStatus(run.Status ?? run.status ?? ""),
				seen: (run.Seen ?? run.seen) ? t`yes` : t`no`,
				started: formatDate(run.StartedAt ?? run.started_at),
				ended: formatDate(run.EndedAt ?? run.ended_at),
				duration: formatDuration(run.DurationMS ?? run.duration_ms ?? 0),
				error: run.Error ?? run.error ?? "",
			})),
		[runs, t]
	)

	useEffect(() => {
		if (!tableRef.current || loading || error) {
			return
		}
		disposeTable(tableInstance.current)
		tableInstance.current = createListTable(tableRef.current, {
			records,
			rowHeight: 46,
			headerRowHeight: 38,
			widthMode: "adaptive",
			columns: [
				{ field: "status", title: t`Status`, width: 120 },
				{ field: "ended", title: t`Ended`, width: 190 },
				{ field: "duration", title: t`Duration`, width: 110 },
				{ field: "seen", title: t`Seen`, width: 90 },
				{ field: "error", title: t`Error`, width: 520 },
				{ field: "id", title: "ID", width: 180 },
			],
		})
		return () => disposeTable(tableInstance.current)
	}, [error, loading, records, t])

	const status = agent?.Status ?? agent?.status ?? "-"
	const targetID = agent?.TargetID ?? agent?.target_id ?? "-"

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
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "agent_edit", { id }))}>
						<PencilIcon className="me-2 h-4 w-4" />
						<Trans>Edit</Trans>
					</Button>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="grid gap-3 md:grid-cols-4">
				<Summary label={t`Agent`} value={id} />
				<Summary label={t`Target`} value={targetID} />
				<Summary label={t`Status`} value={localizedStatus(status)} />
				<Summary
					label={t`Runs`}
					value={`${agent?.RunCount ?? agent?.run_count ?? 0} / ${agent?.FailureCount ?? agent?.failure_count ?? 0}`}
				/>
			</div>

			<div className="rounded-md border border-border bg-card">
				{loading ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>Loading...</Trans>
					</div>
				) : null}
				{error ? <div className="p-3 text-sm text-destructive">{error}</div> : null}
				{!loading && !error && runs.length === 0 ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>No agent runs found.</Trans>
					</div>
				) : null}
				<div ref={tableRef} className="h-[560px] w-full" />
			</div>

			{cursor ? (
				<div className="flex justify-center">
					<Button variant="outline" size="sm" onClick={loadMore} disabled={loadingMore}>
						{loadingMore ? <Trans>Loading...</Trans> : <Trans>Load more</Trans>}
					</Button>
				</div>
			) : null}
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
