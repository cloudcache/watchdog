import assert from "node:assert/strict"
import test from "node:test"
import { buildFlowExplorerChartSpec, buildSankeyData } from "./flow-explorer-chart.ts"

const series = [
	{
		name: "CN|snapshot|geo|1",
		label: "CN → 4134",
		path: ["CN", "4134"],
		values: [{ time: 1, value: 2 }],
		minimum: 2,
		maximum: 2,
		last: 2,
		average: 2,
		p95: 2,
		total: 2,
		receivedRecords: 1,
		unknownSamplingRecords: 0,
		qualityRecords: 0,
		sankeyValue: 2,
	},
]

test("flow chart model exposes distinct line, stacked, heatmap and true-tuple sankey specifications", () => {
	assert.equal(buildFlowExplorerChartSpec("lines", series, String).type, "line")
	const stacked = buildFlowExplorerChartSpec("stacked", series, String)
	assert.equal(stacked.type, "area")
	assert.equal("stack" in stacked && stacked.stack, true)
	assert.equal(buildFlowExplorerChartSpec("heatmap", series, String).type, "heatmap")
	assert.equal(buildFlowExplorerChartSpec("sankey", series, String).type, "sankey")
	assert.deepEqual(buildSankeyData(series), {
		nodes: [{ name: "1: CN" }, { name: "2: 4134" }],
		links: [{ source: "1: CN", target: "2: 4134", value: 2 }],
	})
})
