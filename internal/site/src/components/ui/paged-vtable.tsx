import { SearchIcon } from "lucide-react"
import { useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
	createListTable,
	disposeTable,
	getRowRecord,
	SELECTION_FIELD,
	type ColumnDefine,
	type EditableOptions,
	type ListTable,
	type SelectableOptions,
	type ServerFiltering,
	type ServerSorting,
} from "@/lib/vtable"

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
	showPagination = true,
	serverPagination,
	searchValue,
	onSearchChange,
	onSearchSubmit,
	serverFiltering,
	serverSorting,
	editable,
	selectable,
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
	showPagination?: boolean
	serverPagination?: {
		page: number
		pageSize: number
		totalCount?: number
		hasNextPage?: boolean
		onPageChange: (page: number) => void
		onPageSizeChange: (pageSize: number) => void
	}
	searchValue?: string
	onSearchChange?: (value: string) => void
	onSearchSubmit?: (value: string) => void
	serverFiltering?: ServerFiltering
	serverSorting?: ServerSorting
	editable?: EditableOptions
	selectable?: SelectableOptions
}) {
	const tableRef = useRef<HTMLDivElement>(null)
	const tableInstance = useRef<ListTable | null>(null)
	const [search, setSearch] = useState("")
	const [pageSize, setPageSize] = useState(25)
	const [page, setPage] = useState(0)
	const [filteredCount, setFilteredCount] = useState(records.length)

	const serverMode = Boolean(serverPagination)
	const effectiveSearch = searchValue ?? search
	const effectivePageSize = serverPagination?.pageSize ?? pageSize
	const effectivePage = serverPagination?.page ?? page
	const totalCount = serverMode ? serverPagination?.totalCount : filteredCount
	const searchedRecords = useMemo(() => {
		if (serverMode) return records
		const query = effectiveSearch.trim().toLocaleLowerCase()
		if (!query) return records
		return records.filter((record) =>
			String(record.searchText ?? Object.values(record).join(" "))
				.toLocaleLowerCase()
				.includes(query)
		)
	}, [effectiveSearch, records, serverMode])

	const effectiveColumns = useMemo(
		() =>
			selectable
				? [
						{
							field: SELECTION_FIELD,
							title: "",
							width: 44,
							cellType: "checkbox",
							headerType: "checkbox",
							filter: false,
						},
						...columns,
					]
				: columns,
		[columns, selectable]
	)

	useEffect(() => {
		if (!serverMode) setPage(0)
		setFilteredCount(searchedRecords.length)
	}, [searchedRecords, effectivePageSize, serverMode])

	useEffect(() => {
		if (!tableRef.current || loading || searchedRecords.length === 0) return
		if (tableInstance.current) {
			disposeTable(tableInstance.current)
			tableInstance.current = null
		}
		const table = createListTable(tableRef.current, {
			records: searchedRecords,
			columns: effectiveColumns,
			rowHeight,
			headerRowHeight: 38,
			widthMode: "adaptive",
			pagination: {
				perPageCount: serverMode ? Math.max(1, searchedRecords.length) : effectivePageSize,
				currentPage: 0,
				totalCount: searchedRecords.length,
			},
			onFilteredCountChange: setFilteredCount,
			onFilterApplied: () => {
				if (!serverMode) setPage(0)
			},
			serverFiltering,
			serverSorting,
			editable,
			selectable,
		})
		tableInstance.current = table
		if (onRowClick || onCellClick) {
			table.on("click_cell", (args: { col: number; row: number }) => {
				const record = getRowRecord(table, args) as Record<string, unknown> | null
				if (!record) return
				onCellClick?.(record, String(effectiveColumns[args.col]?.field ?? ""))
				onRowClick?.(record)
			})
		}
		return () => {
			if (tableInstance.current === table) {
				tableInstance.current = null
			}
			disposeTable(table)
		}
	}, [
		effectiveColumns,
		effectivePageSize,
		loading,
		onCellClick,
		onRowClick,
		rowHeight,
		searchedRecords,
		serverFiltering,
		serverSorting,
		serverMode,
		editable,
		selectable,
	])

	useEffect(() => {
		tableInstance.current?.updatePagination({
			perPageCount: serverMode ? Math.max(1, searchedRecords.length) : effectivePageSize,
			currentPage: serverMode ? 0 : effectivePage,
			totalCount: serverMode ? searchedRecords.length : filteredCount,
		})
	}, [effectivePage, effectivePageSize, filteredCount, searchedRecords.length, serverMode])

	const pageCount =
		totalCount == null
			? Math.max(1, effectivePage + 1 + (serverPagination?.hasNextPage ? 1 : 0))
			: Math.max(1, Math.ceil(totalCount / effectivePageSize))
	const safePage = Math.min(effectivePage, pageCount - 1)

	return (
		<div className="grid gap-3">
			<div className="flex flex-wrap items-center justify-between gap-2">
				{showSearch ? (
					<div className="relative w-full max-w-sm">
						<SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
						<Input
							value={effectiveSearch}
							onChange={(event) => {
								if (onSearchChange) onSearchChange(event.target.value)
								else setSearch(event.target.value)
							}}
							onKeyDown={(event) => {
								if (event.key === "Enter") onSearchSubmit?.(effectiveSearch)
							}}
							placeholder={searchPlaceholder}
							className="pl-9"
						/>
					</div>
				) : (
					<div />
				)}
				<div className="flex items-center gap-2 text-sm text-muted-foreground">
					<span>{totalCount == null ? `${records.length} items on this page` : `${totalCount} items`}</span>
					<Select
						value={String(effectivePageSize)}
						onValueChange={(value) => {
							const next = Number(value)
							if (serverPagination) serverPagination.onPageSizeChange(next)
							else setPageSize(next)
						}}
					>
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
			{showPagination && searchedRecords.length > 0 ? (
				<div className="flex items-center justify-end gap-2 text-sm">
					<Button
						variant="outline"
						size="sm"
						disabled={safePage === 0}
						onClick={() => {
							if (serverPagination) serverPagination.onPageChange(Math.max(0, safePage - 1))
							else setPage((value) => Math.max(0, value - 1))
						}}
					>
						Previous
					</Button>
					<span className="min-w-24 text-center text-muted-foreground">
						Page {safePage + 1} of {pageCount}
					</span>
					<Button
						variant="outline"
						size="sm"
						disabled={totalCount == null ? !serverPagination?.hasNextPage : safePage + 1 >= pageCount}
						onClick={() => {
							if (serverPagination) serverPagination.onPageChange(Math.min(pageCount - 1, safePage + 1))
							else setPage((value) => Math.min(pageCount - 1, value + 1))
						}}
					>
						Next
					</Button>
				</div>
			) : null}
		</div>
	)
}
