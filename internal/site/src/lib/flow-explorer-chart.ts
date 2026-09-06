// biome-ignore-all lint/suspicious/noExplicitAny: VChart specs intentionally use its open schema.
import VChart from "@visactor/vchart"
import type { FlowSeries } from "./flow-explorer-model"

export type FlowGraphType = "lines" | "stacked" | "heatmap"

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
	const values = series.flatMap((item) =>
		item.values.map((point) => ({
			time: point.time,
			series: item.name.split("|")[0],
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
