import { Trans, useLingui } from "@lingui/react/macro"
import { useStore } from "@nanostores/react"
import { getPagePath } from "@/lib/page-path"
import { LayoutDashboardIcon, PlusIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { isReadOnlyUser, api } from "@/lib/api"
import { normalizeDashboardLayout } from "@/lib/dashboard-ui"
import { $platformIdentity } from "@/lib/platform-auth"
import type { ColumnDefine } from "@/lib/vtable"
import { cn } from "@/lib/utils"

type Dashboard = {
	id: string
	owner_id?: string
	name: string
	description?: string
	layout: unknown
	version: number
	created_at: string
	updated_at: string
}

type DashboardListResponse = {
	items?: Dashboard[]
	total?: number
}

export default memo(() => {
	const { t } = useLingui()
	const identity = useStore($platformIdentity)
	const [items, setItems] = useState<Dashboard[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [owner, setOwner] = useState<"all" | "mine">("all")
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
			if (owner === "mine" && identity.current?.userID) params.set("owner_id", identity.current.userID)
			const data = await api.send<DashboardListResponse>(`/api/v1/dashboards?${params}`, {})
			if (sequence === requestSequence.current) {
				setItems(data.items ?? [])
				setTotal(data.total ?? 0)
			}
		} catch (err) {
			if (sequence === requestSequence.current) {
				setItems([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load dashboards`)
			}
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [identity.current?.userID, owner, page, pageSize, query, sort, t])

	useEffect(() => {
		document.title = `${t`Dashboards`} / Watchdog`
		refresh()
	}, [refresh, t])

	const remove = useCallback(
		async (dashboard: Dashboard) => {
			if (!globalThis.confirm(t`Delete this dashboard?`)) return
			setError("")
			try {
				await api.send(`/api/v1/dashboards/${dashboard.id}`, {
					method: "DELETE",
					headers: { "If-Match": `"${dashboard.version}"` },
				})
				if (items.length === 1 && page > 0) setPage((value) => value - 1)
				else refresh()
			} catch (err) {
				setError(err instanceof Error ? err.message : t`Failed to delete dashboard`)
			}
		},
		[items.length, page, refresh, t]
	)

	const records = useMemo(
		() =>
			items.map((dashboard) => ({
				name: dashboard.name,
				description: dashboard.description || "—",
				panels: normalizeDashboardLayout(dashboard.layout).panels.length,
				owner: dashboard.owner_id || "—",
				version: dashboard.version,
				updated: formatTime(dashboard.updated_at),
				action: isReadOnlyUser() ? "Open" : "Delete",
				dashboard,
			})),
		[items]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "name", title: t`Name`, width: 220, style: denseCellStyle() },
			{ field: "description", title: t`Description`, width: 280, style: denseCellStyle() },
			{ field: "panels", title: t`Panels`, width: 90, style: denseCellStyle() },
			{ field: "owner", title: t`Owner`, width: 190, style: denseCellStyle() },
			{ field: "version", title: t`Version`, width: 90, style: denseCellStyle() },
			{ field: "updated", title: t`Updated`, width: 180, style: denseCellStyle() },
			{ field: "action", title: t`Actions`, width: 100, filter: false, style: denseCellStyle() },
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
				owner: identity.current?.userID ? [{ value: identity.current.userID, label: t`Me` }] : [],
			},
			selected: { owner: owner === "mine" && identity.current?.userID ? [identity.current.userID] : [] },
			selection: { owner: "single" as const },
			onColumnFilterChange: (_field: string, values: unknown[]) =>
				resetPage(() => setOwner(values.length > 0 ? "mine" : "all")),
			onClearAll: () => resetPage(() => setOwner("all")),
		}),
		[identity.current?.userID, owner, t]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: { name: "name", owner: "owner_id", version: "version", updated: "updated_at" },
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<LayoutDashboardIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Dashboards</Trans> ({total})
					</h1>
				</div>
				<div className="flex items-center gap-2">
					{!isReadOnlyUser() ? (
						<Link href={getPagePath($router, "dashboard_new")} className={cn(buttonVariants({ size: "sm" }))}>
							<PlusIcon className="me-2 h-4 w-4" />
							<Trans>Create</Trans>
						</Link>
					) : null}
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap gap-2">
				<Select value={owner} onValueChange={(value) => resetPage(() => setOwner(value as "all" | "mine"))}>
					<SelectTrigger className="w-36" aria-label={t`Owner`}>
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="all">{t`All owners`}</SelectItem>
						<SelectItem value="mine">{t`Owned by me`}</SelectItem>
					</SelectContent>
				</Select>
				<Select value={sort} onValueChange={(value) => resetPage(() => setSort(value))}>
					<SelectTrigger className="w-44" aria-label={t`Sort`}>
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="updated_at:desc">{t`Recently updated`}</SelectItem>
						<SelectItem value="name:asc">{t`Name A–Z`}</SelectItem>
						<SelectItem value="created_at:desc">{t`Recently created`}</SelectItem>
						<SelectItem value="version:desc">{t`Highest version`}</SelectItem>
					</SelectContent>
				</Select>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No dashboards found.`}
					searchPlaceholder={t`Search dashboard name or description...`}
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
					onCellClick={(record, field) => {
						const dashboard = record.dashboard as Dashboard
						if (field === "action" && !isReadOnlyUser()) remove(dashboard)
						else navigate(getPagePath($router, "dashboard_edit", { id: dashboard.id }))
					}}
				/>
			</div>
		</div>
	)
})

function formatTime(value: string) {
	const time = new Date(value)
	return Number.isNaN(time.getTime()) ? value : time.toLocaleString()
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}
