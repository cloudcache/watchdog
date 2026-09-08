// biome-ignore-all lint/suspicious/noExplicitAny: VisActor VTable option and event types are unstable across minor versions.
import * as VTable from "@visactor/vtable"
import { InputEditor } from "@visactor/vtable-editors"
import { calculateFilterPopoverPosition } from "./vtable-position"

export { calculateFilterPopoverPosition } from "./vtable-position"

export type ListTable = InstanceType<typeof VTable.ListTable>

// Inline cell editing (EdgeManager-style double-click editing). Register the
// text editor once; columns opt in via `editable.fields` and commits arrive on
// the change_cell_value event, which the caller turns into a PATCH.
let editorsRegistered = false
function ensureEditorsRegistered() {
	if (editorsRegistered) return
	editorsRegistered = true
	;(VTable.register as any).editor("watchdog-input", new InputEditor({}))
}

export type EditableOptions = {
	fields: string[]
	onEdit: (record: Record<string, unknown>, field: string, value: string) => void
}

// Row multi-select via a checkbox column. The caller prepends the checkbox
// column (field "__selected"); this binds the toggle event and reports the
// currently checked records so a bulk action (e.g. delete) can act on them.
export const SELECTION_FIELD = "__selected"
export type SelectableOptions = {
	onSelectionChange: (records: Record<string, unknown>[]) => void
}
export type ColumnDefine = any

type FilterColumn = {
	field: string
	title: string
}

type FilterValue = {
	key: string
	label: string
	count: number
	raw: unknown
}

export type ServerFilterOption = { value: unknown; label?: string; count?: number }

export type ServerFiltering = {
	options: Record<string, ServerFilterOption[]>
	selected: Record<string, unknown[]>
	selection?: Record<string, "single" | "multiple">
	loadOptions?: (field: string, search: string, signal: AbortSignal) => Promise<ServerFilterOption[]>
	onColumnFilterChange: (field: string, values: unknown[]) => void
	onClearAll: () => void
}

export type ServerSorting = {
	field: string
	direction: "asc" | "desc"
	fields: Record<string, string>
	onSortChange: (field: string, direction: "asc" | "desc") => void
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
	serverFiltering?: ServerFiltering
	serverSorting?: ServerSorting
	editable?: EditableOptions
	selectable?: SelectableOptions
	onCellDblClick?: (record: Record<string, unknown>, field: string) => void
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
	for (const [field, values] of Object.entries(options.serverFiltering?.selected ?? {})) {
		if (values.length > 0) activeFilters.set(field, new Set(values.map(filterValueKey)))
	}
	const columns = options.columns.map((column, index) => {
		const { filter: filterEnabled = true, filterField, ...tableColumn } = column
		const tableField = String(tableColumn.field ?? "")
		const filterKey = String(filterField ?? tableField)
		const filterAvailable = !options.serverFiltering || Object.hasOwn(options.serverFiltering.options, filterKey)
		const sortField = options.serverSorting?.fields[tableField]
		const resolvedColumn = options.serverSorting ? { ...tableColumn, sort: Boolean(sortField) } : tableColumn
		if (!filterEnabled || !filterKey || !filterAvailable) {
			return resolvedColumn
		}
		filterColumns[index] = { field: filterKey, title: String(tableColumn.title ?? filterKey) }
		return {
			...resolvedColumn,
			headerIcon: (args: any) =>
				appendHeaderIcon(
					typeof tableColumn.headerIcon === "function" ? tableColumn.headerIcon(args) : tableColumn.headerIcon,
					activeFilters.has(filterKey) ? activeFilterIcon : filterIcon
				),
		}
	})
	const activeSortField = options.serverSorting
		? Object.entries(options.serverSorting.fields).find(
				([, serverField]) => serverField === options.serverSorting?.field
			)?.[0]
		: undefined
	if (options.editable) {
		ensureEditorsRegistered()
		const editSet = new Set(options.editable.fields)
		for (const column of columns) {
			if (column && editSet.has(String((column as any).field))) {
				;(column as any).editor = "watchdog-input"
			}
		}
	}
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
		editCellTrigger: options.editable ? "doubleclick" : undefined,
		pagination: options.pagination,
		sortState:
			activeSortField && options.serverSorting
				? { field: activeSortField, order: options.serverSorting.direction }
				: undefined,
	} as any)
	const cleanups: (() => void)[] = []
	if (options.editable) {
		const editable = options.editable
		const handleCellEdit = (arg: any) => {
			const record = getRowRecord(table, arg) as Record<string, unknown> | null
			const field = String(columns[arg?.col]?.field ?? "")
			if (!record || !editable.fields.includes(field)) return
			editable.onEdit(record, field, String(arg?.changedValue ?? arg?.currentValue ?? ""))
		}
		;(table as any).on?.("change_cell_value", handleCellEdit)
		cleanups.push(() => (table as any).off?.("change_cell_value", handleCellEdit))
	}
	if (options.selectable) {
		const selectable = options.selectable
		const handleCheckbox = () => {
			const checked = ((table as any).getCheckboxState?.(SELECTION_FIELD) ?? []) as Record<string, unknown>[]
			selectable.onSelectionChange(checked)
		}
		;(table as any).on?.("checkbox_state_change", handleCheckbox)
		cleanups.push(() => (table as any).off?.("checkbox_state_change", handleCheckbox))
	}
	if (options.onCellDblClick) {
		const onCellDblClick = options.onCellDblClick
		const handleDblClick = (arg: any) => {
			if (!arg || typeof arg.row !== "number" || arg.row < 1) return
			const record = getRowRecord(table, arg) as Record<string, unknown> | null
			if (record) onCellDblClick(record, String(columns[arg.col]?.field ?? ""))
		}
		;(table as any).on?.("dblclick_cell", handleDblClick)
		cleanups.push(() => (table as any).off?.("dblclick_cell", handleDblClick))
	}
	if (filterColumns.some(Boolean)) {
		cleanups.push(
			enableColumnFilters(
				table,
				dom,
				options.records,
				filterColumns,
				activeFilters,
				options.onFilteredCountChange,
				options.onFilterApplied,
				options.serverFiltering
			)
		)
	}
	if (options.serverSorting) {
		cleanups.push(enableServerSorting(table, options.serverSorting))
	}
	if (cleanups.length > 0) {
		tableCleanup.set(table, () => {
			for (const cleanup of cleanups) cleanup()
		})
	}
	return table
}

function enableServerSorting(table: ListTable, sorting: ServerSorting): () => void {
	const handleSort = (state: { field?: unknown; order?: unknown }) => {
		const tableField = String(state?.field ?? "")
		const serverField = sorting.fields[tableField]
		if (!serverField) return false
		const direction = state.order === "desc" ? "desc" : "asc"
		;(table as any).updateSortState?.({ field: tableField, order: direction }, false)
		sorting.onSortChange(serverField, direction)
		// Prevent VisActor from sorting only the currently loaded page.
		return false
	}
	;(table as any).on?.("sort_click", handleSort)
	return () => (table as any).off?.("sort_click", handleSort)
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
	onFilterApplied?: () => void,
	serverFiltering?: ServerFiltering
): () => void {
	let ownedClose: (() => void) | null = null

	const applyFilters = () => {
		if (serverFiltering) {
			table.refreshHeader()
			onFilterApplied?.()
			return
		}
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
		const singleSelect = serverFiltering?.selection?.[column.field] === "single"
		closeOpenFilter?.()
		ownedClose = openFilterPopover({
			anchor: getFilterAnchor(dom, args),
			column,
			values: serverFiltering?.options[column.field]?.map((option) => ({
				key: filterValueKey(option.value),
				label: option.label ?? filterValueLabel(option.value),
				count: option.count ?? -1,
				raw: option.value,
			})),
			loadValues: serverFiltering?.loadOptions
				? async (search, signal) =>
						((await serverFiltering.loadOptions?.(column.field, search, signal)) ?? []).map((option) => ({
							key: filterValueKey(option.value),
							label: option.label ?? filterValueLabel(option.value),
							count: option.count ?? -1,
							raw: option.value,
						}))
				: undefined,
			records: records.filter((record) => matchesFilters(record, activeFilters, column.field)),
			selected: activeFilters.get(column.field),
			singleSelect,
			hasAnyFilter: activeFilters.size > 0,
			onApply: (selected, allValues, availableValues, unconstrained) => {
				// An empty server-side array means "no constraint" in every list
				// contract. Treat both zero and all selected as clearing this column
				// so the UI cannot display an active filter while the API returns all.
				const cleared =
					selected.size === 0 ||
					Boolean(serverFiltering?.loadOptions ? unconstrained : !singleSelect && selected.size === allValues.size)
				if (cleared) {
					activeFilters.delete(column.field)
				} else {
					activeFilters.set(column.field, selected)
				}
				if (serverFiltering) {
					const options = [
						...(serverFiltering.options[column.field] ?? []),
						...availableValues.map((value) => ({ value: value.raw })),
					]
					serverFiltering.onColumnFilterChange(
						column.field,
						cleared
							? []
							: options.filter((option) => selected.has(filterValueKey(option.value))).map((option) => option.value)
					)
				}
				applyFilters()
			},
			onClear: () => {
				activeFilters.delete(column.field)
				serverFiltering?.onColumnFilterChange(column.field, [])
				applyFilters()
			},
			onClearAll: () => {
				activeFilters.clear()
				serverFiltering?.onClearAll()
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
			values.set(key, { key, label: filterValueLabel(raw), count: 1, raw })
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
	values?: FilterValue[]
	loadValues?: (search: string, signal: AbortSignal) => Promise<FilterValue[]>
	selected?: Set<string>
	singleSelect?: boolean
	hasAnyFilter: boolean
	onApply: (selected: Set<string>, allValues: Set<string>, values: FilterValue[], unconstrained: boolean) => void
	onClear: () => void
	onClearAll: () => void
	onClose: () => void
}

function openFilterPopover(options: OpenFilterPopoverOptions): () => void {
	let values = options.values ?? collectFilterValues(options.records, options.column.field)
	const allValues = new Set(values.map((value) => value.key))
	const valueByKey = new Map(values.map((value) => [value.key, value]))
	let unconstrained = options.selected == null
	const selected = options.selected
		? new Set(options.selected)
		: options.singleSelect
			? new Set<string>()
			: new Set(allValues)
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
	if (options.singleSelect) selectAllRow.hidden = true

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
	let loadTimer: ReturnType<typeof setTimeout> | undefined
	let loadController: AbortController | undefined
	let loadingValues = false
	const reposition = () => positionFilterPopover(popover, options.anchor)
	const renderOptions = () => {
		const query = search.value.trim().toLocaleLowerCase()
		visibleValues = options.loadValues
			? values
			: query
				? values.filter((value) => value.label.toLocaleLowerCase().includes(query))
				: values
		list.replaceChildren()
		if (loadingValues) {
			const loading = document.createElement("div")
			loading.className = "vtable-filter-empty"
			loading.textContent = labels.loading
			list.append(loading)
			updateSummary()
			reposition()
			return
		}
		for (const value of visibleValues) {
			const row = document.createElement("label")
			row.className = "vtable-filter-option"
			const checkbox = document.createElement("input")
			checkbox.type = "checkbox"
			checkbox.checked = selected.has(value.key)
			checkbox.addEventListener("change", () => {
				unconstrained = false
				if (checkbox.checked) {
					if (options.singleSelect) selected.clear()
					selected.add(value.key)
				} else selected.delete(value.key)
				renderOptions()
			})
			const valueText = document.createElement("span")
			valueText.className = "vtable-filter-option-label"
			valueText.textContent = value.label
			valueText.title = value.label
			const count = document.createElement("span")
			count.className = "vtable-filter-count"
			count.textContent = value.count >= 0 ? String(value.count) : ""
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
		reposition()
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
		if (loadTimer) clearTimeout(loadTimer)
		loadController?.abort()
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

	const loadRemoteValues = () => {
		if (!options.loadValues) {
			renderOptions()
			return
		}
		if (loadTimer) clearTimeout(loadTimer)
		loadController?.abort()
		loadController = undefined
		loadTimer = setTimeout(async () => {
			const controller = new AbortController()
			loadController = controller
			loadingValues = true
			renderOptions()
			try {
				const loaded = await options.loadValues?.(search.value.trim(), controller.signal)
				if (closed || controller.signal.aborted || !loaded) return
				for (const value of loaded) valueByKey.set(value.key, value)
				values = loaded
				allValues.clear()
				for (const value of loaded) {
					allValues.add(value.key)
					if (unconstrained) selected.add(value.key)
				}
			} catch (reason) {
				if (!controller.signal.aborted) values = []
			} finally {
				if (!closed && loadController === controller) {
					loadingValues = false
					renderOptions()
				}
			}
		}, 200)
	}
	search.addEventListener("input", loadRemoteValues)
	selectAll.addEventListener("change", () => {
		unconstrained = false
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
		options.onApply(new Set(selected), allValues, [...valueByKey.values()], unconstrained)
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

	if (options.loadValues) loadRemoteValues()
	else renderOptions()
	if (options.loadValues) reposition()
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
				loading: "正在加载…",
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
				loading: "Loading…",
				selected: (selected: number, total: number) => `${selected} of ${total} selected`,
			}
}
