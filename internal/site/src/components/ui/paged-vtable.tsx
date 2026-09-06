import { SearchIcon } from "lucide-react"
import { useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { createListTable, disposeTable, getRowRecord, type ColumnDefine, type ListTable } from "@/lib/vtable"

export function PagedVTable({
	records,
	columns,
	loading = false,
	emptyText,
	searchPlaceholder = "Search...",
	height = 440,
	rowHeight = 42,
	onRowClick,
	onCellClick,
	showSearch = true,
}: {
	records: Record<string, unknown>[]
	columns: ColumnDefine[]
	loading?: boolean
	emptyText: string
	searchPlaceholder?: string
	height?: number
	rowHeight?: number
	onRowClick?: (record: Record<string, unknown>) => void
	onCellClick?: (record: Record<string, unknown>, field: string) => void
	showSearch?: boolean
}) {
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [search, setSearch] = useState("")
	const [pageSize, setPageSize] = useState(25)
	const [page, setPage] = useState(0)
	const [filteredCount, setFilteredCount] = useState(records.length)

	const searchedRecords = useMemo(() => {
		const query = search.trim().toLocaleLowerCase()
		if (!query) return records
		return records.filter((record) =>
			String(record.searchText ?? Object.values(record).join(" "))
				.toLocaleLowerCase()
				.includes(query)
		)
	}, [records, search])

	useEffect(() => {
		setPage(0)
		setFilteredCount(searchedRecords.length)
	}, [searchedRecords, pageSize])

	useEffect(() => {
		if (!tableRef.current || loading || searchedRecords.length === 0) return
		if (tableInstance.current) {
			disposeTable(tableInstance.current)
			tableInstance.current = null
		}
		const table = createListTable(tableRef.current, {
			records: searchedRecords,
			columns,
			rowHeight,
			headerRowHeight: 38,
			widthMode: "adaptive",
			pagination: { perPageCount: pageSize, currentPage: 0, totalCount: searchedRecords.length },
			onFilteredCountChange: setFilteredCount,
			onFilterApplied: () => setPage(0),
		})
		tableInstance.current = table
		if (onRowClick || onCellClick) {
			table.on("click_cell", (args: { col: number; row: number }) => {
				const record = getRowRecord(table, args) as Record<string, unknown> | null
				if (!record) return
				onCellClick?.(record, String(columns[args.col]?.field ?? ""))
				onRowClick?.(record)
			})
		}
		return () => {
			if (tableInstance.current === table) {
				tableInstance.current = null
			}
			disposeTable(table)
		}
	}, [columns, loading, onCellClick, onRowClick, pageSize, rowHeight, searchedRecords])

	useEffect(() => {
		tableInstance.current?.updatePagination({ perPageCount: pageSize, currentPage: page, totalCount: filteredCount })
	}, [filteredCount, page, pageSize])

	const pageCount = Math.max(1, Math.ceil(filteredCount / pageSize))
	const safePage = Math.min(page, pageCount - 1)

	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center justify-between gap-2">
				{showSearch ? (
					<div className="relative w-full max-w-sm">
						<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
						<Input
							value={search}
							onChange={(event) => setSearch(event.target.value)}
							placeholder={searchPlaceholder}
							className="pl-9"
						/>
					</div>
				) : (
					<div />
				)}
				<div className="flex items-center gap-2 text-sm text-muted-foreground">
					<span>{filteredCount} items</span>
					<Select value={String(pageSize)} onValueChange={(value) => setPageSize(Number(value))}>
						<SelectTrigger className="h-9 w-24">
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="25">25 / page</SelectItem>
							<SelectItem value="50">50 / page</SelectItem>
							<SelectItem value="100">100 / page</SelectItem>
						</SelectContent>
					</Select>
				</div>
			</div>
			<div className="overflow-hidden rounded-md bg-card">
				{loading ? <div className="p-3 text-sm text-muted-foreground">Loading...</div> : null}
				{!loading && searchedRecords.length === 0 ? (
					<div className="p-4 text-sm text-muted-foreground">{emptyText}</div>
				) : null}
				<div
					ref={tableRef}
					className="w-full"
					style={{ height: searchedRecords.length > 0 && !loading ? height : 0 }}
				/>
			</div>
			{searchedRecords.length > 0 ? (
				<div className="flex items-center justify-end gap-2 text-sm">
					<Button
						variant="outline"
						size="sm"
						disabled={safePage === 0}
						onClick={() => setPage((value) => Math.max(0, value - 1))}
					>
						Previous
					</Button>
					<span className="min-w-24 text-center text-muted-foreground">
						Page {safePage + 1} of {pageCount}
					</span>
					<Button
						variant="outline"
						size="sm"
						disabled={safePage + 1 >= pageCount}
						onClick={() => setPage((value) => Math.min(pageCount - 1, value + 1))}
					>
						Next
					</Button>
				</div>
			) : null}
		</div>
	)
}
