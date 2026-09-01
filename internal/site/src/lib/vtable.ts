// biome-ignore-all lint/suspicious/noExplicitAny: VisActor VTable option and event types are unstable across minor versions.
import * as VTable from "@visactor/vtable"

export type ListTable = InstanceType<typeof VTable.ListTable>
export type ColumnDefine = any

export interface CreateTableOptions {
	records: any[]
	columns: ColumnDefine[]
	rowHeight?: number
	headerRowHeight?: number
	widthMode?: "standard" | "autoWidth" | "adaptive"
	columnResize?: boolean
	theme?: any
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
	return new VTable.ListTable({
		container: dom,
		records: options.records,
		columns: options.columns,
		defaultRowHeight: options.rowHeight ?? 40,
		defaultHeaderRowHeight: options.headerRowHeight ?? 36,
		theme,
		hover: { highlightMode: "row" },
		widthMode: options.widthMode ?? "adaptive",
		autoFillWidth: true,
		columnResizeMode: options.columnResize === false ? "none" : "all",
	} as any)
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
		;(table as any)?.release?.()
	} catch {}
}
