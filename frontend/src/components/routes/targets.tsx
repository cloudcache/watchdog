import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { PlusIcon, RefreshCwIcon, ServerIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, navigate } from "@/components/router"
import { Button } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { fetchTargetsPage, type TargetListItem } from "@/lib/api"
import type { ColumnDefine } from "@/lib/vtable"

// Network targets live on the Network page (device view); the Hosts list
// excludes them server-side so pagination pages over host targets only.

export default memo(() => {
	const { t } = useLingui()
	const [targets, setTargets] = useState<TargetListItem[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [status, setStatus] = useState("all")
	const [sort, setSort] = useState("name:asc")
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
			const [sortField, order] = sort.split(":") as [string, "asc" | "desc"]
			const { items, total: responseTotal } = await fetchTargetsPage({
				limit: pageSize,
				offset: page * pageSize,
				excludeKind: "network",
				search: query,
				status: status === "all" ? undefined : status,
				sort: sortField,
				order,
			})
			if (sequence !== requestSequence.current) return
			setTargets(items)
			setTotal(responseTotal ?? 0)
		} catch (err) {
			if (sequence !== requestSequence.current) return
			setTargets([])
			setTotal(0)
			setError(err instanceof Error ? err.message : t`Failed to load targets`)
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [page, pageSize, query, sort, status, t])

	useEffect(() => {
		document.title = `${t`Hosts`} / Watchdog`
		refresh()
	}, [refresh, t])

	const records = useMemo(
		() =>
			targets.map((target) => ({
				id: target.id,
				name: target.name || "—",
				type: target.kind || "—",
				host: target.host || "—",
				status: target.status || "—",
				labels: formatLabels(target.labels),
				updated: target.updated_at || "—",
			})),
		[targets]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "name", title: t`Name`, width: 220 },
			{ field: "type", title: t`Type`, width: 120 },
			{ field: "host", title: t`Host`, width: 220 },
			{ field: "status", title: t`Status`, width: 120 },
			{ field: "labels", title: t`Labels`, width: 260 },
			{ field: "updated", title: t`Updated`, width: 220 },
		],
		[t]
	)
	const resetPage = (update: () => void) => {
		setPage(0)
		update()
	}
	const serverFiltering = useMemo(
		() => ({
			options: { status: ["pending", "up", "down", "paused"].map((value) => ({ value })) },
			selected: { status: status === "all" ? [] : [status] },
			selection: { status: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetPage(() => setStatus(values.length > 0 ? String(values[0]) : "all")),
			onClearAll: () => resetPage(() => setStatus("all")),
		}),
		[status]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: { name: "name", type: "kind", host: "host", status: "status", updated: "updated_at" },
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

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
			<div className="flex flex-wrap gap-2">
				<Select value={status} onValueChange={(value) => resetPage(() => setStatus(value))}>
					<SelectTrigger className="w-40">
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">{t`All statuses`}</SelectItem>
						{["pending", "up", "down", "paused"].map((value) => (
							<SelectItem key={value} value={value}>
								{value}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="rounded-md border border-border bg-card p-3">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No hosts found.`}
					searchPlaceholder={t`Search hosts by name, host, type, or status...`}
					searchValue={search}
					onSearchChange={setSearch}
					height={520}
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
					onRowClick={(record) => {
						const id = String(record.id ?? "")
						if (id) navigate(getPagePath($router, "target_detail", { id }))
					}}
				/>
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
