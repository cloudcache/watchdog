import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PlusIcon, RefreshCwIcon, ServerIcon } from "lucide-react"
import { memo, useCallback, useEffect, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { fetchTargetsPage, type TargetListItem } from "@/lib/api"
import { createListTable, disposeTable, getRowRecord, type ListTable } from "@/lib/vtable"

// Network targets live on the Network page (device view); the Hosts list
// excludes them server-side so pagination pages over host targets only.
const PAGE_SIZE = 200

export default memo(() => {
	const { t } = useLingui()
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [targets, setTargets] = useState<TargetListItem[]>([])
	const [cursor, setCursor] = useState("")
	const [loading, setLoading] = useState(true)
	const [loadingMore, setLoadingMore] = useState(false)
	const [error, setError] = useState("")

	const refresh = useCallback(async () => {
		setLoading(true)
		setError("")
		try {
			const { items, nextCursor } = await fetchTargetsPage({ limit: PAGE_SIZE, excludeKind: "network" })
			setTargets(items)
			setCursor(nextCursor)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load targets`)
		} finally {
			setLoading(false)
		}
	}, [t])

	const loadMore = useCallback(async () => {
		if (!cursor || loadingMore) {
			return
		}
		setLoadingMore(true)
		try {
			const { items, nextCursor } = await fetchTargetsPage({ limit: PAGE_SIZE, cursor, excludeKind: "network" })
			setTargets((current) => [...current, ...items])
			setCursor(nextCursor)
		} catch (err) {
			setError(err instanceof Error ? err.message : t`Failed to load targets`)
		} finally {
			setLoadingMore(false)
		}
	}, [cursor, loadingMore])

	useEffect(() => {
		document.title = `${t`Hosts`} / Watchdog`
		refresh()
	}, [refresh, t])

	useEffect(() => {
		if (!tableRef.current || loading || error) {
			return
		}
		const records = targets.map((target) => {
			const id = target.id
			return {
				id,
				name: target.name || "—",
				type: target.kind || "—",
				host: target.host || "—",
				status: target.status || "—",
				labels: formatLabels(target.labels),
				updated: target.updated_at || "—",
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
					<ServerIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Hosts</Trans>
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
						<Trans>No hosts found.</Trans>
					</div>
				) : null}
				<div ref={tableRef} className="h-[520px] w-full" />
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

function formatLabels(labels?: Record<string, string>) {
	if (!labels || Object.keys(labels).length === 0) {
		return "—"
	}
	return Object.entries(labels)
		.map(([key, value]) => `${key}=${value}`)
		.join(", ")
}
