import assert from "node:assert/strict"
import test from "node:test"
import { i18n } from "@lingui/core"
import {
	buildReportSeries,
	FLOW_REPORT_CATEGORIES,
	flowReportCategoryLabel,
	reportDirectionTotals,
	reportSeriesStats,
	resolveFlowReportRange,
	transformReportSeries,
	type FlowReportPanel,
} from "./flow-report-model.ts"

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
			"Local City",
			"Over City",
			"Over State",
			"Inner State",
			"Over State",
			"Over Sea",
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

test("share uses a same-direction denominator and difference is signed inbound minus outbound", () => {
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
	const difference = transformReportSeries(inbound, outbound, "difference")
	assert.equal(difference.difference.find((series) => series.name === "a")?.values[0].value, -20)
	assert.equal(difference.difference.find((series) => series.name === "b")?.values[0].value, 20)
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
