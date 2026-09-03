import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { CrosshairIcon, PlusIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { pb } from "@/lib/api"
import { createListTable, disposeTable, getRowRecord, type ListTable } from "@/lib/vtable"

type TargetRecord = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Type?: string
	target_type?: string
	Host?: string
	host?: string
	Status?: string
	status?: string
	Labels?: Record<string, string>
	labels?: Record<string, string>
	UpdatedAt?: string
	updated_at?: string
}

type TargetsResponse = {
	items?: TargetRecord[]
}

export default memo(() => {
	const { t } = useLingui()
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [targets, setTargets] = useState<TargetRecord[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const data = await pb.send<TargetsResponse>("/api/v1/targets", {})
			// Network targets live on the Network page (device view); listing
			// them here too duplicated the same box in two lists.
			setTargets((data.items ?? []).filter((target) => (target.Type ?? target.target_type) !== "network"))
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load targets`)
		} finally {
			setLoading(false)
		}
	}, [t])

	useEffect(() => {
		document.title = `${t`Targets`} / Watchdog`
		refresh()
	}, [refresh, t])

	useEffect(() => {
		if (!tableRef.current || loading || error) {
			return
		}
		const records = targets.map((target) => {
			const id = target.ID ?? target.id ?? ""
			const type = target.Type ?? target.target_type ?? ""
			return {
				id,
				name: target.Name ?? target.name ?? "—",
				type: type || "—",
				host: target.Host ?? target.host ?? "—",
				status: target.Status ?? target.status ?? "—",
				labels: formatLabels(target.Labels ?? target.labels),
				updated: target.UpdatedAt ?? target.updated_at ?? "—",
			}
		})
		disposeTable(tableInstance.current)
		tableInstance.current = createListTable(tableRef.current, {
			records,
			columns: [
				{ field: "name", title: t`Name`, width: 220 },
				{ field: "type", title: t`Type`, width: 120 },
				{ field: "host", title: t`Host`, width: 220 },
				{ field: "status", title: t`Status`, width: 120 },
				{ field: "labels", title: t`Labels`, width: 260 },
				{ field: "updated", title: t`Updated`, width: 220 },
			],
		})
		tableInstance.current.on("click_cell", (args: { col: number; row: number }) => {
			const record = getRowRecord(tableInstance.current, args)
			if (record?.id) {
				navigate(getPagePath($router, "target_detail", { id: record.id }))
			}
		})
		return () => disposeTable(tableInstance.current)
	}, [error, loading, t, targets])

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<CrosshairIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Targets</Trans>
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Button variant="outline" size="sm" onClick={() => navigate(getPagePath($router, "target_new"))}>
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
				{!loading && !error && targets.length === 0 ? (
					<div className="p-3 text-sm text-muted-foreground">
						<Trans>No targets found.</Trans>
					</div>
				) : null}
				<div ref={tableRef} className="h-[520px] w-full" />
			</div>
		</div>
	)
})

function formatLabels(labels?: Record<string, string>) {
	if (!labels || Object.keys(labels).length === 0) {
		return "—"
	}
	return Object.entries(labels)
		.map(([key, value]) => `${key}=${value}`)
		.join(", ")
}
