import { Trans, useLingui } from "@lingui/react/macro"
import { getPagePath } from "@nanostores/router"
import { FileDownIcon, RefreshCwIcon } from "lucide-react"
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { $router, Link, navigate } from "@/components/router"
import { Button, buttonVariants } from "@/components/ui/button"
import { PagedVTable } from "@/components/ui/paged-vtable"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { pb } from "@/lib/api"
import { trafficViewFromValue, trafficViewLabel } from "@/lib/traffic-view"
import type { ColumnDefine } from "@/lib/vtable"
import { cn } from "@/lib/utils"
import {
	exportDownloadURL,
	exportID,
	exportStatus,
	exportValueLayer,
	formatExportRange,
	type ExportTask,
} from "./export-types"

type ExportTasksResponse = {
	items?: ExportTask[]
	total?: number
	limit?: number
	offset?: number
}

export default memo(() => {
	const { t } = useLingui()
	const [tasks, setTasks] = useState<ExportTask[]>([])
	const [total, setTotal] = useState(0)
	const [page, setPage] = useState(0)
	const [pageSize, setPageSize] = useState(25)
	const [search, setSearch] = useState("")
	const [query, setQuery] = useState("")
	const [status, setStatus] = useState("all")
	const [valueLayer, setValueLayer] = useState("all")
	const [format, setFormat] = useState("all")
	const [sort, setSort] = useState("created_at:desc")
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
			const [sortBy, sortDirection] = sort.split(":")
			const params = new URLSearchParams({
				limit: String(pageSize),
				offset: String(page * pageSize),
				sort_by: sortBy,
				sort_direction: sortDirection,
			})
			if (query) params.set("q", query)
			if (status !== "all") params.set("status", status)
			if (valueLayer !== "all") params.set("value_layer", valueLayer)
			if (format !== "all") params.set("format", format)
			const data = await pb.send<ExportTasksResponse>(`/api/v1/exports?${params}`, {})
			if (sequence === requestSequence.current) {
				setTasks(data.items ?? [])
				setTotal(data.total ?? 0)
			}
		} catch (err) {
			if (sequence === requestSequence.current) {
				setTasks([])
				setTotal(0)
				setError(err instanceof Error ? err.message : t`Failed to load exports`)
			}
		} finally {
			if (sequence === requestSequence.current) setLoading(false)
		}
	}, [format, page, pageSize, query, sort, status, t, valueLayer])

	useEffect(() => {
		document.title = `${t`Exports`} / Watchdog`
		refresh()
	}, [refresh, t])

	const records = useMemo(
		() =>
			tasks.map((task) => ({
				id: exportID(task),
				status: exportStatus(task) || "—",
				target: task.TargetID ?? task.target_id ?? "—",
				port: task.PortID ?? task.port_id ?? "—",
				range: formatExportRange(task),
				aggregation: task.Aggregation ?? task.aggregation ?? "—",
				view: trafficViewLabel(exportTrafficView(task)),
				value: task.ValueMode ?? task.value_mode ?? "corrected",
				format: task.Format ?? task.format ?? "—",
				action: exportStatus(task) === "complete" ? "Download" : "Open",
				task,
			})),
		[tasks]
	)
	const columns = useMemo<ColumnDefine[]>(
		() => [
			{ field: "status", title: t`Status`, width: 110, style: denseCellStyle() },
			{ field: "target", title: t`Target`, width: 180, style: denseCellStyle() },
			{ field: "port", title: t`Port`, width: 180, style: denseCellStyle() },
			{ field: "range", title: t`Range`, width: 270, style: denseCellStyle() },
			{ field: "aggregation", title: t`Aggregation`, width: 130, style: denseCellStyle() },
			{ field: "view", title: t`View`, width: 110, style: denseCellStyle() },
			{ field: "value", title: t`Value`, width: 110, style: denseCellStyle() },
			{ field: "format", title: t`Format`, width: 100, style: denseCellStyle() },
			{ field: "action", title: t`Actions`, width: 110, filter: false, style: denseCellStyle() },
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
				status: ["pending", "running", "complete", "failed", "canceled"].map((value) => ({ value })),
				view: ["raw", "supplier", "customer"].map((value) => ({ value })),
				format: ["csv", "parquet"].map((value) => ({ value })),
			},
			selected: {
				status: status === "all" ? [] : [status],
				view: valueLayer === "all" ? [] : [valueLayer],
				format: format === "all" ? [] : [format],
			},
			selection: { status: "single" as const, view: "single" as const, format: "single" as const },
			onColumnFilterChange: (field: string, values: unknown[]) => {
				const value = values.length > 0 ? String(values[0]) : "all"
				resetPage(() => {
					if (field === "status") setStatus(value)
					if (field === "view") setValueLayer(value)
					if (field === "format") setFormat(value)
				})
			},
			onClearAll: () =>
				resetPage(() => {
					setStatus("all")
					setValueLayer("all")
					setFormat("all")
				}),
		}),
		[format, status, valueLayer]
	)
	const [sortField, sortDirection] = sort.split(":") as [string, "asc" | "desc"]
	const serverSorting = useMemo(
		() => ({
			field: sortField,
			direction: sortDirection,
			fields: {
				status: "status",
				target: "target_id",
				range: "range_start",
				view: "value_layer",
				format: "format",
			},
			onSortChange: (field: string, direction: "asc" | "desc") => resetPage(() => setSort(`${field}:${direction}`)),
		}),
		[sortDirection, sortField]
	)

	return (
		<div className="grid gap-4">
			<div className="flex items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<FileDownIcon className="h-5 w-5 text-muted-foreground" strokeWidth={1.75} />
					<h1 className="text-xl font-semibold tracking-normal">
						<Trans>Exports</Trans> ({total})
					</h1>
				</div>
				<div className="flex items-center gap-2">
					<Link href={getPagePath($router, "export_new")} className={cn(buttonVariants({ size: "sm" }))}>
						<FileDownIcon className="me-2 h-4 w-4" />
						<Trans>Create</Trans>
					</Link>
					<Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
						<RefreshCwIcon className="me-2 h-4 w-4" />
						<Trans>Refresh</Trans>
					</Button>
				</div>
			</div>

			<div className="flex flex-wrap gap-2">
				<ExportFilter value={status} onChange={(value) => resetPage(() => setStatus(value))} label={t`Status`}>
					<SelectItem value="all">
						<Trans>All</Trans>
					</SelectItem>
					{["pending", "running", "complete", "failed", "canceled"].map((value) => (
						<SelectItem key={value} value={value}>
							{value}
						</SelectItem>
					))}
				</ExportFilter>
				<ExportFilter value={valueLayer} onChange={(value) => resetPage(() => setValueLayer(value))} label={t`View`}>
					<SelectItem value="all">
						<Trans>All</Trans>
					</SelectItem>
					{["raw", "supplier", "customer"].map((value) => (
						<SelectItem key={value} value={value}>
							{value}
						</SelectItem>
					))}
				</ExportFilter>
				<ExportFilter value={format} onChange={(value) => resetPage(() => setFormat(value))} label={t`Format`}>
					<SelectItem value="all">
						<Trans>All</Trans>
					</SelectItem>
					<SelectItem value="csv">csv</SelectItem>
					<SelectItem value="parquet">parquet</SelectItem>
				</ExportFilter>
				<ExportFilter value={sort} onChange={(value) => resetPage(() => setSort(value))} label={t`Sort`} wide>
					<SelectItem value="created_at:desc">
						<Trans>Newest first</Trans>
					</SelectItem>
					<SelectItem value="created_at:asc">
						<Trans>Oldest first</Trans>
					</SelectItem>
					<SelectItem value="status:asc">
						<Trans>Status</Trans>
					</SelectItem>
					<SelectItem value="target_id:asc">
						<Trans>Target</Trans>
					</SelectItem>
				</ExportFilter>
			</div>

			{error ? (
				<div className="rounded-md border border-destructive/30 p-3 text-sm text-destructive">{error}</div>
			) : null}
			<div className="overflow-hidden rounded-md border border-border bg-card">
				<PagedVTable
					records={records}
					columns={columns}
					loading={loading}
					emptyText={t`No exports found.`}
					searchPlaceholder={t`Search export, target, port, or dataset...`}
					searchValue={search}
					onSearchChange={(value) => resetPage(() => setSearch(value))}
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
						const task = record.task as ExportTask
						if (field === "action" && exportStatus(task) === "complete") downloadExport(task)
						else navigate(getPagePath($router, "export_detail", { id: exportID(task) }))
					}}
				/>
			</div>
		</div>
	)
})

function ExportFilter({
	value,
	onChange,
	label,
	children,
	wide = false,
}: {
	value: string
	onChange: (value: string) => void
	label: string
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

function exportTrafficView(task: ExportTask) {
	const layer = exportValueLayer(task)
	if (layer === "raw" || layer === "supplier" || layer === "customer") return layer
	return trafficViewFromValue(task.ValueMode ?? task.value_mode, task.Aggregation ?? task.aggregation)
}

function downloadExport(task: ExportTask) {
	const url = exportDownloadURL(task)
	if (url) globalThis.location.href = url
}

function denseCellStyle() {
	return { padding: [8, 10, 8, 10], textBaseline: "middle", autoWrapText: false }
}
