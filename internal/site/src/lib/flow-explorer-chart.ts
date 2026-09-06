// biome-ignore-all lint/suspicious/noExplicitAny: VChart specs intentionally use its open schema.
import VChart from "@visactor/vchart"
import type { FlowSeries } from "./flow-explorer-model"

export type FlowGraphType = "lines" | "stacked" | "heatmap" | "sankey"

export function createFlowExplorerChart(
	dom: HTMLElement,
	graphType: FlowGraphType,
	series: FlowSeries[],
	formatValue: (value: number) => string
) {
	const spec = buildFlowExplorerChartSpec(graphType, series, formatValue)
	const chart = new VChart(spec as any, { dom })
	chart.renderSync()
	return chart
}

export function buildFlowExplorerChartSpec(
	graphType: FlowGraphType,
	series: FlowSeries[],
	formatValue: (value: number) => string
) {
	if (graphType === "sankey") {
		const sankey = buildSankeyData(series)
		return {
			type: "sankey",
			background: "transparent",
			data: [{ id: "flow", values: [sankey] }],
			categoryField: "name",
			valueField: "value",
			sourceField: "source",
			targetField: "target",
			nodeKey: "name",
			nodeAlign: "justify",
			dropIsolatedNode: true,
			overflow: "scroll-y",
			label: { visible: true },
			emphasis: { enable: true, trigger: "hover", effect: "related" },
			tooltip: {
				mark: {
					content: [{ key: { field: "name" }, value: (datum: { value?: number }) => formatValue(datum.value ?? 0) }],
				},
			},
		}
	}
	const values = series.flatMap((item) =>
		item.values.map((point) => ({
			time: point.time,
			series: item.name,
			value: point.value,
			valueLabel: formatValue(point.value),
		}))
	)
	if (graphType === "heatmap") {
		return {
			type: "heatmap",
			background: "transparent",
			data: [{ id: "flow", values }],
			xField: "time",
			yField: "series",
			valueField: "value",
			color: { type: "linear", range: ["#eff6ff", "#60a5fa", "#1d4ed8"] },
			legends: { visible: true, orient: "right" },
			tooltip: { mark: { content: [{ key: { field: "series" }, value: { field: "valueLabel" } }] } },
		}
	}
	return {
		type: graphType === "stacked" ? "area" : "line",
		background: "transparent",
		data: [{ id: "flow", values }],
		xField: "time",
		yField: "value",
		seriesField: "series",
		stack: graphType === "stacked",
		point: { visible: false },
		line: { style: { lineWidth: 2 }, smooth: true },
		area: { style: { fillOpacity: 0.55 } },
		legends: { visible: true, orient: "top", position: "start" },
		axes: [
			{ orient: "bottom", type: "time", label: { formatMethod: (value: number) => new Date(value).toLocaleString() } },
			{ orient: "left", label: { formatMethod: formatValue }, grid: { visible: true, style: { lineDash: [3, 3] } } },
		],
		tooltip: { dimension: { content: [{ key: { field: "series" }, value: { field: "valueLabel" } }] } },
	}
}

export function buildSankeyData(series: FlowSeries[]) {
	const nodes = new Set<string>()
	const links = new Map<string, { source: string; target: string; value: number }>()
	for (const item of series) {
		if (item.path.length < 2 || !Number.isFinite(item.sankeyValue) || item.sankeyValue <= 0) continue
		for (let index = 0; index < item.path.length - 1; index += 1) {
			const source = `${index + 1}: ${item.path[index]}`
			const target = `${index + 2}: ${item.path[index + 1]}`
			nodes.add(source)
			nodes.add(target)
			const key = JSON.stringify([source, target])
			const current = links.get(key) ?? { source, target, value: 0 }
			current.value += item.sankeyValue
			links.set(key, current)
		}
	}
	return {
		nodes: [...nodes].sort().map((name) => ({ name })),
		links: [...links.values()].sort(
			(left, right) => left.source.localeCompare(right.source) || left.target.localeCompare(right.target)
		),
	}
}
