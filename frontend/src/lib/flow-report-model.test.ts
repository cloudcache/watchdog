import assert from "node:assert/strict"
import test from "node:test"
import { i18n } from "@lingui/core"
import {
	buildReportSeries,
	FLOW_REPORT_CATEGORIES,
	FLOW_REPORT_URL_STATE_VERSION,
	flowDimensionValueLabel,
	flowReportCategoryLabel,
	initialFlowReportDisplayMode,
	initialFlowReportRange,
	reportDirectionTotals,
	reportPanelUnit,
	reportSeriesStats,
	resolveFlowReportRange,
	transformReportSeries,
	type FlowReportPanel,
} from "./flow-report-model.ts"

test("legacy implicit one-hour report URLs migrate to the 24-hour default", () => {
	assert.equal(initialFlowReportRange(""), "24h")
	assert.equal(initialFlowReportRange("?range=1h"), "24h")
	assert.equal(initialFlowReportRange(`?range=1h&state_version=${FLOW_REPORT_URL_STATE_VERSION}`), "1h")
	assert.equal(initialFlowReportRange("?range=6h"), "6h")
})

test("report display mode never restores the removed signed-difference view", () => {
	assert.equal(initialFlowReportDisplayMode(""), "value")
	assert.equal(initialFlowReportDisplayMode("?display=value"), "value")
	assert.equal(initialFlowReportDisplayMode("?display=share"), "share")
	assert.equal(initialFlowReportDisplayMode("?display=difference"), "value")
	assert.equal(initialFlowReportDisplayMode("?display=unexpected"), "value")
})

test("report panel unit falls back to the embedded metric definition", () => {
	const panel = {
		status: "ready",
		meta: {},
		data: { metric: { name: "estimated_bps", unit: "bits_per_second" } },
	} as FlowReportPanel
	assert.equal(reportPanelUnit(panel), "bits_per_second")
	panel.meta.unit = "bps"
	assert.equal(reportPanelUnit(panel), "bps")
})

test("report categories use the frozen English product wording", () => {
	i18n.load("en", {})
	i18n.activate("en")
	assert.deepEqual(
		[
			"on_net_local_city",
			"on_net_cross_city",
			"on_net_cross_province",
			"off_net_in_province",
			"off_net_cross_province",
			"overseas",
			"unknown",
			"internal",
			"transit",
			"ambiguous",
		].map((category) => flowReportCategoryLabel(category)),
		[
			"On-net · local city",
			"On-net · cross-city",
			"On-net · cross-province",
			"Off-net · within province",
			"Off-net · cross-province",
			"Cross-border",
			"Unknown",
			"Internal",
			"Transit",
			"Unattributed",
		]
	)
})

test("report categories resolve through the active zh-CN catalog", () => {
	i18n.load("zh-CN", {
		on_net_local_city: "本网・本市",
		on_net_cross_city: "本网・跨市",
		on_net_cross_province: "本网・跨省",
		off_net_in_province: "异网・省内",
		off_net_cross_province: "异网・跨省",
		overseas: "跨境",
	})
	i18n.activate("zh-CN")
	assert.deepEqual(
		FLOW_REPORT_CATEGORIES.map((category) => flowReportCategoryLabel(category, i18n)),
		["本网・本市", "本网・跨市", "本网・跨省", "异网・省内", "异网・跨省", "跨境"]
	)
})

test("Flow VTable values localize stable keys without changing unknown values", () => {
	i18n.load("zh-CN", {
		on_net_local_city: "本网・本市",
		off_net_in_province: "异网・省内",
		"flow.direction.inbound": "流入",
		"flow.direction.outbound": "流出",
		"flow.value.other": "其他",
		"flow.value.unassigned": "未归属",
	})
	i18n.activate("zh-CN")
	assert.equal(flowDimensionValueLabel("category", "on_net_local_city", i18n), "本网・本市")
	assert.equal(flowDimensionValueLabel("category", "off_net_in_province", i18n), "异网・省内")
	assert.equal(flowDimensionValueLabel("business_direction", "in", i18n), "流入")
	assert.equal(flowDimensionValueLabel("direction", "out", i18n), "流出")
	assert.equal(flowDimensionValueLabel("protocol", "6", i18n), "TCP (6)")
	assert.equal(flowDimensionValueLabel("category", "_other", i18n), "其他")
	assert.equal(flowDimensionValueLabel("category", "_unassigned", i18n), "未归属")
	assert.equal(flowDimensionValueLabel("geo.country", "CN", i18n), "CN")
})

test("report range presets align closed UTC minutes and calendar boundaries", () => {
	const now = new Date("2026-09-08T10:17:42Z")
	assert.deepEqual(resolveFlowReportRange("1h", "", "", "UTC", now), {
		start: "2026-09-08T09:17:00.000Z",
		end: "2026-09-08T10:17:00.000Z",
	})
	assert.deepEqual(resolveFlowReportRange("today", "", "", "Asia/Singapore", now), {
		start: "2026-09-07T16:00:00.000Z",
		end: "2026-09-08T10:17:00.000Z",
	})
	assert.deepEqual(resolveFlowReportRange("previous_month", "", "", "Asia/Singapore", now), {
		start: "2026-07-31T16:00:00.000Z",
		end: "2026-08-31T16:00:00.000Z",
	})
})

test("report series preserves category identity and exact bucket statistics", () => {
	const panel = {
		status: "ready",
		data: {
			points: [1, 2, 3, 100].map((value, index) => ({
				bucket: new Date(Date.UTC(2026, 8, 8, 9, index)).toISOString(),
				dimension_value: "overseas",
				other: false,
				value,
				dimension_snapshot_id: "snapshot-1",
				geo_version: "geo-1",
				classification_version: 1,
				received_records: 1,
				unknown_sampling_records: 0,
				quality_records: 0,
				generated_at: "2026-09-08T10:00:00Z",
			})),
		},
	} as FlowReportPanel
	const series = buildReportSeries(panel)
	assert.equal(series.length, 1)
	assert.deepEqual(reportSeriesStats(series[0]), { current: 100, average: 26.5, p95: 100, maximum: 100, total: 106 })
})

test("report series displays immutable geography names instead of storage IDs", () => {
	const panel = {
		id: "dimension_in",
		status: "ready",
		meta: {},
		data: {
			points: [
				{
					bucket: "2026-09-19T00:00:00Z",
					dimension_value: "supplier/province/CN/320000",
					other: false,
					value: 268_360_000,
					dimension_snapshot_id: "snapshot-7",
					geo_version: "geo-7",
					classification_version: 7,
					received_records: 1,
					unknown_sampling_records: 0,
					quality_records: 0,
					generated_at: "2026-09-19T00:01:00Z",
				},
			],
			dimension_labels: {
				"geo-7:supplier/province/CN/320000": {
					code: "supplier/province/CN/320000",
					name: "Jiangsu",
					kind: "province",
					path: [
						{ id: "supplier/country/CN", name: "China", kind: "country" },
						{ id: "supplier/province/CN/320000", name: "Jiangsu", kind: "province" },
					],
					additive: true,
					version: "geo-7",
				},
			},
		},
	} as FlowReportPanel
	assert.equal(buildReportSeries(panel)[0]?.name, "Jiangsu")
})

test("share uses a same-direction denominator without changing traffic direction", () => {
	const inbound = [
		{ name: "a", values: [{ time: 1, value: 30 }] },
		{ name: "b", values: [{ time: 1, value: 70 }] },
	]
	const outbound = [
		{ name: "a", values: [{ time: 1, value: 50 }] },
		{ name: "b", values: [{ time: 1, value: 50 }] },
	]
	const shares = transformReportSeries(inbound, outbound, "share")
	assert.equal(shares.inbound[0].values[0].value, 0.3)
	assert.equal(shares.outbound[0].values[0].value, 0.5)
	const zero = transformReportSeries(
		[
			{ name: "a", values: [{ time: 2, value: 0 }] },
			{ name: "b", values: [{ time: 2, value: 0 }] },
		],
		[],
		"share"
	)
	assert.deepEqual(zero.inbound[0].values, [], "a zero denominator must remain unknown rather than becoming 0%")
})

test("direction total recognizes the provider's stable labels", () => {
	const panel = {
		status: "ready",
		data: {
			points: [
				{ bucket: "2026-09-08T09:00:00Z", dimension_value: "Inbound", value: 8 },
				{ bucket: "2026-09-08T09:00:00Z", dimension_value: "Outbound", value: 3 },
			],
		},
	} as FlowReportPanel
	assert.deepEqual(reportDirectionTotals(panel), { inbound: 8, outbound: 3 })
})
