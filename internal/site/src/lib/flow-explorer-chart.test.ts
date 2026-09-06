import assert from "node:assert/strict"
import test from "node:test"
import { buildFlowExplorerChartSpec } from "./flow-explorer-chart.ts"

const series = [
	{
		name: "CN|snapshot|geo|1",
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
	},
]

test("flow chart model exposes distinct line, stacked and heatmap specifications", () => {
	assert.equal(buildFlowExplorerChartSpec("lines", series, String).type, "line")
	const stacked = buildFlowExplorerChartSpec("stacked", series, String)
	assert.equal(stacked.type, "area")
	assert.equal("stack" in stacked && stacked.stack, true)
	assert.equal(buildFlowExplorerChartSpec("heatmap", series, String).type, "heatmap")
})
