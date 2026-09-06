export type DashboardPanel = {
	id: string
	graph_id: string
	title?: string
	span?: 1 | 2
	[key: string]: unknown
}

export type DashboardLayout = {
	panels: DashboardPanel[]
	[key: string]: unknown
}

export type DashboardGraph = {
	ID?: string
	id?: string
	Name?: string
	name?: string
	Description?: string
	description?: string
	Aggregation?: string
	aggregation?: string
	ValueMode?: string
	value_mode?: string
	Unit?: string
	unit?: string
}

export type DashboardGraphReference = {
	graph_id: string
	exists: boolean
	graph?: DashboardGraph
	series?: Array<{ ID?: string; id?: string; Label?: string; label?: string; Metric?: string; metric?: string }>
}

export function graphID(graph: DashboardGraph) {
	return graph.ID ?? graph.id ?? ""
}

export function graphName(graph: DashboardGraph) {
	return graph.Name ?? graph.name ?? graphID(graph)
}

export function normalizeDashboardLayout(value: unknown): DashboardLayout {
	const source = isRecord(value) ? value : {}
	const rawPanels = Array.isArray(source.panels) ? source.panels : []
	const panels = rawPanels.flatMap((value, index) => {
		if (!isRecord(value) || typeof value.graph_id !== "string" || !value.graph_id.trim()) return []
		const normalizedGraphID = value.graph_id.trim()
		const span = value.span === 2 ? 2 : 1
		return [
			{
				...value,
				id: typeof value.id === "string" && value.id.trim() ? value.id : `panel-${index}-${normalizedGraphID}`,
				graph_id: normalizedGraphID,
				title: typeof value.title === "string" ? value.title : undefined,
				span,
			} as DashboardPanel,
		]
	})
	return { ...source, panels }
}

export function moveDashboardPanel(panels: DashboardPanel[], index: number, direction: -1 | 1) {
	const destination = index + direction
	if (index < 0 || index >= panels.length || destination < 0 || destination >= panels.length) return panels
	const next = [...panels]
	;[next[index], next[destination]] = [next[destination], next[index]]
	return next
}

export function addDashboardPanel(panels: DashboardPanel[], panel: DashboardPanel) {
	return panels.some((item) => item.id === panel.id) ? panels : [...panels, panel]
}

export function removeDashboardPanel(panels: DashboardPanel[], id: string) {
	return panels.filter((panel) => panel.id !== id)
}

export function referenceMap(references: DashboardGraphReference[]) {
	return new Map(references.map((reference) => [reference.graph_id, reference]))
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value)
}
