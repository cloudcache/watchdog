import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PlugZapIcon, PlusIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { pb } from "@/lib/api"
import { createListTable, disposeTable, getRowRecord, type ListTable } from "@/lib/vtable"

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
}

export default memo(() => {
	const { t } = useLingui()
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [agents, setAgents] = useState<AgentRecord[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<AgentsResponse>("/api/v1/agent-registry", {})
			setAgents(data.items ?? [])
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load agents`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Agents`} / Beszel`
		refresh()
	}, [refresh, t])

	useEffect(() => {
		if (!tableRef.current || loading || error) {
			return
		}
		const records = agents.map((agent) => ({
			id: agent.ID ?? agent.id ?? "",
			agentType: agent.AgentType ?? agent.agent_type ?? "snmp",
			target: agent.TargetID ?? agent.target_id ?? "—",
			mode: agent.Mode ?? agent.mode ?? "—",
			endpoint: agent.Endpoint ?? agent.endpoint ?? "—",
			status: agent.Status ?? agent.status ?? "—",
			lastSeen: agent.LastSeen ?? agent.last_seen ?? "—",
			lastRun: agent.LastRun ?? agent.last_run ?? "—",
			lastSuccess: agent.LastSuccess ?? agent.last_success ?? "—",
			runCount: agent.RunCount ?? agent.run_count ?? 0,
			failureCount: agent.FailureCount ?? agent.failure_count ?? 0,
			lastError: agent.LastError ?? agent.last_error ?? "",
			updated: agent.UpdatedAt ?? agent.updated_at ?? "—",
		}))
		disposeTable(tableInstance.current)
		tableInstance.current = createListTable(tableRef.current, {
			records,
			columns: [
				{ field: "id", title: "ID", width: 220 },
				{ field: "agentType", title: t`Type`, width: 100 },
				{ field: "target", title: t`Target`, width: 220 },
				{ field: "mode", title: t`Mode`, width: 100 },
				{ field: "endpoint", title: t`Endpoint`, width: 260 },
				{ field: "status", title: t`Status`, width: 120 },
				{ field: "lastSeen", title: t`Last Seen`, width: 220 },
				{ field: "lastRun", title: t`Last Run`, width: 220 },
				{ field: "lastSuccess", title: t`Last Success`, width: 220 },
				{ field: "runCount", title: t`Runs`, width: 90 },
				{ field: "failureCount", title: t`Failures`, width: 90 },
				{ field: "lastError", title: t`Last Error`, width: 360 },
				{ field: "updated", title: t`Updated`, width: 220 },
			],
		})
		tableInstance.current.on("click_cell", (args: { col: number; row: number }) => {
			const record = getRowRecord(tableInstance.current, args)
			if (record?.id) {
				navigate(getPagePath($router, "agent_runs", { id: record.id }))
			}
		})
		return () => disposeTable(tableInstance.current)
	}, [agents, error, loading, t])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<PlugZapIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Agents</Trans>
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

			<div className="rounded-md border border-border bg-card">
				{loading ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>Loading...</Trans>
					</div>
				) : null}
				{error ? <div className="p-3 text-sm text-destructive">{error}</div> : null}
				{!loading && !error && agents.length === 0 ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>No agents found.</Trans>
					</div>
				) : null}
				<div ref={tableRef} className="h-[520px] w-full" />
			</div>
		</div>
	)
})
