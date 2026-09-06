// biome-ignore-all lint/suspicious/noExplicitAny: VisActor VTable option and event types are unstable across minor versions.
import * as VTable from "@visactor/vtable"

export type ListTable = InstanceType<typeof VTable.ListTable>
export type ColumnDefine = any

type FilterColumn = {
	field: string
	title: string
}

type FilterValue = {
	key: string
	label: string
	count: number
}

const tableCleanup = new WeakMap<ListTable, () => void>()
let closeOpenFilter: (() => void) | null = null

const filterIcon = {
	type: "svg",
	svg: '<svg viewBox="0 0 24 24" fill="none" stroke="#737373" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 5h16l-6.5 7.5V19l-3 1.5v-8z"/></svg>',
	width: 14,
	height: 14,
	positionType: "absoluteRight",
	marginRight: 8,
	name: "watchdog-column-filter",
	funcType: "watchdog-column-filter",
	hover: { width: 20, height: 20, bgColor: "rgba(115,115,115,0.12)" },
	cursor: "pointer",
	visibleTime: "always",
	interactive: true,
} as const

const activeFilterIcon = {
	...filterIcon,
	svg: filterIcon.svg.replace('stroke="#737373"', 'stroke="#2d74ff"'),
}

export interface CreateTableOptions {
	records: any[]
	columns: ColumnDefine[]
	rowHeight?: number
	headerRowHeight?: number
	widthMode?: "standard" | "autoWidth" | "adaptive"
	columnResize?: boolean
	theme?: any
	pagination?: { totalCount?: number; perPageCount: number; currentPage?: number }
	onFilteredCountChange?: (count: number) => void
	onFilterApplied?: () => void
}

export function createListTable(dom: HTMLElement, options: CreateTableOptions): ListTable {
	const isDark = document.documentElement.classList.contains("dark")
	dom.classList.add("vtable-surface")
	const baseTheme = options.theme ?? (isDark ? (VTable.themes as any).DARK : (VTable.themes as any).DEFAULT)
	const borderColor = isDark ? "rgba(255,255,255,0.08)" : "#e5e5e5"
	const headerBgColor = isDark ? "#0d0d0d" : "#fafafa"
	// One border system: every section draws the same 1px line and the outer
	// frame is off, so the cell borders form the grid on their own. Leaving
	// the builtin defaultStyle/frame active stacked a second, differently
	// colored line onto the seams (visually 2px at the corners).
	const theme = {
		...baseTheme,
		defaultStyle: {
			...(baseTheme as any).defaultStyle,
			borderColor,
			borderLineWidth: 1,
		},
		bodyStyle: {
			...(baseTheme as any).bodyStyle,
			borderColor,
			borderLineWidth: 1,
		},
		headerStyle: {
			...(baseTheme as any).headerStyle,
			borderColor,
			borderLineWidth: 1,
			bgColor: headerBgColor,
			fontSize: 13,
			fontWeight: 600,
		},
		rowHeaderStyle: {
			...(baseTheme as any).rowHeaderStyle,
			borderColor,
			borderLineWidth: 1,
		},
		cornerHeaderStyle: {
			...(baseTheme as any).cornerHeaderStyle,
			borderColor,
			borderLineWidth: 1,
		},
		frameStyle: {
			...(baseTheme as any).frameStyle,
			borderColor: "transparent",
			borderLineWidth: 0,
			shadowBlur: 0,
		},
	}
	const filterColumns: FilterColumn[] = []
	const activeFilters = new Map<string, Set<string>>()
	const columns = options.columns.map((column, index) => {
		const { filter: filterEnabled = true, filterField, ...tableColumn } = column
		const field = String(filterField ?? tableColumn.field ?? "")
		if (!filterEnabled || !field) {
			return tableColumn
		}
		filterColumns[index] = { field, title: String(tableColumn.title ?? field) }
		return {
			...tableColumn,
			headerIcon: (args: any) =>
				appendHeaderIcon(
					typeof tableColumn.headerIcon === "function" ? tableColumn.headerIcon(args) : tableColumn.headerIcon,
					activeFilters.has(field) ? activeFilterIcon : filterIcon
				),
		}
	})
	const table = new VTable.ListTable({
		container: dom,
		records: options.records,
		columns,
		defaultRowHeight: options.rowHeight ?? 40,
		defaultHeaderRowHeight: options.headerRowHeight ?? 36,
		theme,
		hover: { highlightMode: "row" },
		widthMode: options.widthMode ?? "adaptive",
		autoFillWidth: true,
		columnResizeMode: options.columnResize === false ? "none" : "all",
		pagination: options.pagination,
	} as any)
	if (filterColumns.some(Boolean)) {
		tableCleanup.set(
			table,
			enableColumnFilters(
				table,
				dom,
				options.records,
				filterColumns,
				activeFilters,
				options.onFilteredCountChange,
				options.onFilterApplied
			)
		)
	}
	return table
}

export function getRowRecord(table: ListTable | null, args: any): any | null {
	if (!table || !args || typeof args.row !== "number" || args.row < 1) {
		return null
	}
	const instance = table as any
	if (typeof instance.getCellOriginRecord === "function") {
		try {
			const record = instance.getCellOriginRecord(args.col, args.row)
			if (record) {
				return record
			}
		} catch {}
	}
	if (typeof instance.getRecordByRowCol === "function") {
		try {
			const record = instance.getRecordByRowCol(args.row, args.col)
			if (record) {
				return record
			}
		} catch {}
	}
	return Array.isArray(instance.records) ? (instance.records[args.row - 1] ?? null) : null
}

export function disposeTable(table: ListTable | null) {
	try {
		if (table) {
			tableCleanup.get(table)?.()
			tableCleanup.delete(table)
		}
		;(table as any)?.release?.()
	} catch {}
}

function appendHeaderIcon(existing: any, icon: any): any {
	if (Array.isArray(existing)) {
		return [...existing, icon]
	}
	return existing ? [existing, icon] : [icon]
}

function enableColumnFilters(
	table: ListTable,
	dom: HTMLElement,
	records: any[],
	columns: FilterColumn[],
	activeFilters: Map<string, Set<string>>,
	onFilteredCountChange?: (count: number) => void,
	onFilterApplied?: () => void
): () => void {
	let ownedClose: (() => void) | null = null

	const applyFilters = () => {
		if (activeFilters.size === 0) {
			table.updateFilterRules([])
			table.refreshHeader()
			onFilteredCountChange?.(records.length)
			onFilterApplied?.()
			return
		}
		table.updateFilterRules([
			{
				filterFunc: (record: any) => matchesFilters(record, activeFilters),
			},
		])
		table.refreshHeader()
		onFilteredCountChange?.(records.filter((record) => matchesFilters(record, activeFilters)).length)
		onFilterApplied?.()
	}

	const handleIconClick = (args: any) => {
		if (args?.name !== filterIcon.name || args.row !== 0) {
			return
		}
		const column = columns[args.col]
		if (!column) {
			return
		}
		closeOpenFilter?.()
		ownedClose = openFilterPopover({
			anchor: getFilterAnchor(dom, args),
			column,
			records: records.filter((record) => matchesFilters(record, activeFilters, column.field)),
			selected: activeFilters.get(column.field),
			hasAnyFilter: activeFilters.size > 0,
			onApply: (selected, allValues) => {
				if (selected.size === allValues.size) {
					activeFilters.delete(column.field)
				} else {
					activeFilters.set(column.field, selected)
				}
				applyFilters()
			},
			onClear: () => {
				activeFilters.delete(column.field)
				applyFilters()
			},
			onClearAll: () => {
				activeFilters.clear()
				applyFilters()
			},
			onClose: () => {
				ownedClose = null
			},
		})
		closeOpenFilter = ownedClose
	}

	;(table as any).on?.("icon_click", handleIconClick)
	return () => {
		ownedClose?.()
		;(table as any).off?.("icon_click", handleIconClick)
	}
}

function matchesFilters(record: any, filters: Map<string, Set<string>>, excludedField?: string): boolean {
	for (const [field, selected] of filters) {
		if (field !== excludedField && !selected.has(filterValueKey(record?.[field]))) {
			return false
		}
	}
	return true
}

function collectFilterValues(records: any[], field: string): FilterValue[] {
	const values = new Map<string, FilterValue>()
	for (const record of records) {
		const raw = record?.[field]
		const key = filterValueKey(raw)
		const existing = values.get(key)
		if (existing) {
			existing.count++
		} else {
			values.set(key, { key, label: filterValueLabel(raw), count: 1 })
		}
	}
	return [...values.values()].sort((a, b) => a.label.localeCompare(b.label, undefined, { numeric: true }))
}

function filterValueKey(value: unknown): string {
	if (value == null || value === "") return "empty:"
	if (typeof value === "object") return `object:${JSON.stringify(value)}`
	return `${typeof value}:${String(value)}`
}

function filterValueLabel(value: unknown): string {
	if (value == null || value === "") return "(Blank)"
	if (typeof value === "object") return JSON.stringify(value)
	return String(value).replace(/\s*\n\s*/g, " · ")
}

type OpenFilterPopoverOptions = {
	anchor: { x: number; y: number }
	column: FilterColumn
	records: any[]
	selected?: Set<string>
	hasAnyFilter: boolean
	onApply: (selected: Set<string>, allValues: Set<string>) => void
	onClear: () => void
	onClearAll: () => void
	onClose: () => void
}

function openFilterPopover(options: OpenFilterPopoverOptions): () => void {
	const values = collectFilterValues(options.records, options.column.field)
	const allValues = new Set(values.map((value) => value.key))
	const selected = options.selected ? new Set(options.selected) : new Set(allValues)
	const labels = filterLabels()
	const popover = document.createElement("div")
	popover.className = "vtable-filter-popover"
	popover.setAttribute("role", "dialog")
	popover.setAttribute("aria-label", `${labels.filter}: ${options.column.title}`)

	const heading = document.createElement("div")
	heading.className = "vtable-filter-heading"
	const title = document.createElement("strong")
	title.textContent = options.column.title
	const clearButton = makeButton(labels.clear, "vtable-filter-clear")
	heading.append(title, clearButton)

	const search = document.createElement("input")
	search.className = "vtable-filter-search"
	search.type = "search"
	search.placeholder = labels.search
	search.setAttribute("aria-label", `${labels.search} ${options.column.title}`)

	const selectAllRow = document.createElement("label")
	selectAllRow.className = "vtable-filter-option vtable-filter-select-all"
	const selectAll = document.createElement("input")
	selectAll.type = "checkbox"
	const selectAllText = document.createElement("span")
	selectAllText.textContent = labels.selectAll
	selectAllRow.append(selectAll, selectAllText)

	const list = document.createElement("div")
	list.className = "vtable-filter-options"

	const footer = document.createElement("div")
	footer.className = "vtable-filter-footer"
	const summary = document.createElement("span")
	summary.className = "vtable-filter-summary"
	const actions = document.createElement("div")
	actions.className = "vtable-filter-actions"
	const cancelButton = makeButton(labels.cancel)
	const applyButton = makeButton(labels.apply, "vtable-filter-apply")
	actions.append(cancelButton, applyButton)
	footer.append(summary, actions)
	popover.append(heading, search, selectAllRow, list, footer)
	document.body.append(popover)

	let visibleValues = values
	let closed = false
	const renderOptions = () => {
		const query = search.value.trim().toLocaleLowerCase()
		visibleValues = query ? values.filter((value) => value.label.toLocaleLowerCase().includes(query)) : values
		list.replaceChildren()
		for (const value of visibleValues) {
			const row = document.createElement("label")
			row.className = "vtable-filter-option"
			const checkbox = document.createElement("input")
			checkbox.type = "checkbox"
			checkbox.checked = selected.has(value.key)
			checkbox.addEventListener("change", () => {
				if (checkbox.checked) selected.add(value.key)
				else selected.delete(value.key)
				updateSummary()
			})
			const valueText = document.createElement("span")
			valueText.className = "vtable-filter-option-label"
			valueText.textContent = value.label
			valueText.title = value.label
			const count = document.createElement("span")
			count.className = "vtable-filter-count"
			count.textContent = String(value.count)
			row.append(checkbox, valueText, count)
			list.append(row)
		}
		if (visibleValues.length === 0) {
			const empty = document.createElement("div")
			empty.className = "vtable-filter-empty"
			empty.textContent = labels.noValues
			list.append(empty)
		}
		updateSummary()
	}
	const updateSummary = () => {
		const selectedVisible = visibleValues.filter((value) => selected.has(value.key)).length
		selectAll.checked = visibleValues.length > 0 && selectedVisible === visibleValues.length
		selectAll.indeterminate = selectedVisible > 0 && selectedVisible < visibleValues.length
		summary.textContent = labels.selected(selected.size, values.length)
	}
	const close = () => {
		if (closed) return
		closed = true
		popover.remove()
		document.removeEventListener("pointerdown", handleOutsidePointer)
		document.removeEventListener("keydown", handleKeydown)
		window.removeEventListener("resize", close)
		window.removeEventListener("scroll", handleOutsideScroll, true)
		if (closeOpenFilter === close) closeOpenFilter = null
		options.onClose()
	}
	const handleOutsidePointer = (event: Event) => {
		if (!popover.contains(event.target as Node)) close()
	}
	// Close when the page or table scrolls out from under the popover, but not
	// when the user scrolls the popover's own (overflow: auto) option list —
	// the capture-phase listener sees those inner scrolls too.
	const handleOutsideScroll = (event: Event) => {
		if (!popover.contains(event.target as Node)) close()
	}
	const handleKeydown = (event: KeyboardEvent) => {
		if (event.key === "Escape") close()
	}

	search.addEventListener("input", renderOptions)
	selectAll.addEventListener("change", () => {
		for (const value of visibleValues) {
			if (selectAll.checked) selected.add(value.key)
			else selected.delete(value.key)
		}
		renderOptions()
	})
	clearButton.addEventListener("click", () => {
		options.onClear()
		close()
	})
	cancelButton.addEventListener("click", close)
	applyButton.addEventListener("click", () => {
		options.onApply(new Set(selected), allValues)
		close()
	})
	if (options.hasAnyFilter) {
		const clearAllButton = makeButton(labels.clearAll, "vtable-filter-clear-all")
		clearAllButton.addEventListener("click", () => {
			options.onClearAll()
			close()
		})
		heading.insertBefore(clearAllButton, clearButton)
	}

	renderOptions()
	positionFilterPopover(popover, options.anchor)
	document.addEventListener("pointerdown", handleOutsidePointer)
	document.addEventListener("keydown", handleKeydown)
	window.addEventListener("resize", close)
	window.addEventListener("scroll", handleOutsideScroll, true)
	queueMicrotask(() => search.focus())
	return close
}

function getFilterAnchor(dom: HTMLElement, args: any): { x: number; y: number } {
	const event = args?.event as MouseEvent | undefined
	if (Number.isFinite(event?.clientX) && Number.isFinite(event?.clientY)) {
		return { x: event?.clientX ?? 0, y: event?.clientY ?? 0 }
	}
	const rect = dom.getBoundingClientRect()
	return { x: rect.left + Number(args?.x ?? 0), y: rect.top + Number(args?.y ?? 0) }
}

function positionFilterPopover(popover: HTMLElement, anchor: { x: number; y: number }) {
	const { left, top } = calculateFilterPopoverPosition(
		anchor,
		{ width: popover.offsetWidth, height: popover.offsetHeight },
		{ width: window.innerWidth, height: window.innerHeight }
	)
	popover.style.left = `${Math.round(left)}px`
	popover.style.top = `${Math.round(top)}px`
}

export function calculateFilterPopoverPosition(
	anchor: { x: number; y: number },
	popover: { width: number; height: number },
	viewport: { width: number; height: number }
): { left: number; top: number } {
	const gutter = 8
	const gap = 10
	const maxLeft = Math.max(gutter, viewport.width - popover.width - gutter)
	const left = Math.min(Math.max(gutter, anchor.x - popover.width + 20), maxLeft)
	const below = anchor.y + gap
	const above = anchor.y - popover.height - gap
	const maxTop = Math.max(gutter, viewport.height - popover.height - gutter)
	const top = below + popover.height <= viewport.height - gutter ? below : Math.min(Math.max(gutter, above), maxTop)
	return { left, top }
}

function makeButton(text: string, className = ""): HTMLButtonElement {
	const button = document.createElement("button")
	button.type = "button"
	button.className = className
	button.textContent = text
	return button
}

function filterLabels() {
	const isChinese = /^zh\b/i.test(document.documentElement.lang || navigator.language)
	return isChinese
		? {
				filter: "筛选",
				search: "搜索值",
				selectAll: "全选当前结果",
				clear: "清除本列",
				clearAll: "清除全部",
				cancel: "取消",
				apply: "应用",
				noValues: "没有匹配值",
				selected: (selected: number, total: number) => `已选 ${selected}/${total}`,
			}
		: {
				filter: "Filter",
				search: "Search values",
				selectAll: "Select all results",
				clear: "Clear column",
				clearAll: "Clear all",
				cancel: "Cancel",
				apply: "Apply",
				noValues: "No matching values",
				selected: (selected: number, total: number) => `${selected} of ${total} selected`,
			}
}
