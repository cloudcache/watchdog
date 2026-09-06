import assert from "node:assert/strict"
import test from "node:test"
import {
	buildFlowCSV,
	buildFlowJointSeries,
	buildFlowQuickFilter,
	buildFlowSeries,
	FLOW_SURFACE_PRESETS,
	flowSurfacePreset,
	parseFlowFilter,
	resolveOverseasRange,
	resolveFlowTimeRange,
} from "./flow-explorer-model.ts"

test("six Flow surfaces have stable unique paths and query defaults", () => {
	assert.deepEqual(Object.keys(FLOW_SURFACE_PRESETS), [
		"overview",
		"dimensions",
		"source",
		"destination",
		"overseas",
		"vpn",
	])
	assert.equal(new Set(Object.values(FLOW_SURFACE_PRESETS).map((preset) => preset.path)).size, 6)
	assert.equal(flowSurfacePreset("source").queryMode, "src_ip")
	assert.equal(flowSurfacePreset("destination").queryMode, "dst_ip")
	assert.deepEqual(flowSurfacePreset("overseas"), {
		path: "/flow/overseas",
		queryMode: "advanced",
		dimension: "geo.country",
		filter: "category = overseas",
		advancedOpen: true,
	})
	assert.equal(flowSurfacePreset("vpn").queryMode, "vpn")
})

test("flow ranges are independent presets and custom ranges are minute-aligned", () => {
	const now = new Date("2026-09-06T12:37:45Z")
	assert.deepEqual(resolveFlowTimeRange("7d", "", "", now), {
		start: "2026-08-30T12:37:00.000Z",
		end: "2026-09-06T12:37:00.000Z",
	})
	assert.deepEqual(resolveFlowTimeRange("custom", "2026-09-06T10:03:49Z", "2026-09-06T11:04:59Z", now), {
		start: "2026-09-06T10:03:00.000Z",
		end: "2026-09-06T11:04:00.000Z",
	})
})

test("overseas ranges select a physical rollup and align closed UTC buckets", () => {
	assert.deepEqual(resolveOverseasRange("2026-09-06T10:03:49Z", "2026-09-06T11:04:59Z"), {
		start: "2026-09-06T10:03:00.000Z",
		end: "2026-09-06T11:04:00.000Z",
		bucket: "1m",
	})
	assert.deepEqual(resolveOverseasRange("2026-08-01T10:03:49Z", "2026-09-06T11:04:59Z"), {
		start: "2026-08-01T10:00:00.000Z",
		end: "2026-09-06T11:00:00.000Z",
		bucket: "1h",
	})
	assert.throws(() => resolveOverseasRange("invalid", "2026-09-06T11:04:59Z"), /valid overseas range/)
})

test("flow filter expression parses precedence, typed operators and IP/CIDR values", () => {
	assert.deepEqual(
		parseFlowFilter(
			`(geo.country=CN OR asn IN (AS4134, 4837)) AND NOT remote_ip IN (203.0.113.0/24, 2001:db8::1) AND protocol!=udp`
		),
		{
			op: "and",
			args: [
				{
					op: "or",
					args: [
						{ op: "predicate", field: "geo.country", operator: "eq", values: ["CN"] },
						{ op: "predicate", field: "asn", operator: "in", values: ["AS4134", "4837"] },
					],
				},
				{
					op: "not",
					args: [
						{
							op: "predicate",
							field: "remote_ip",
							operator: "in",
							values: ["203.0.113.0/24", "2001:db8::1"],
						},
					],
				},
				{ op: "predicate", field: "protocol", operator: "ne", values: ["udp"] },
			],
		}
	)
	assert.throws(() => parseFlowFilter("raw_sql=1"), /Unsupported filter field/)
	assert.throws(() => parseFlowFilter("src_ip IN 203.0.113.1"), /requires parentheses/)
	assert.equal(parseFlowFilter(""), undefined)
})

test("quick filters use published geo codes and the operator ASN set", () => {
	assert.deepEqual(
		buildFlowQuickFilter({
			countryCode: "CN",
			provinceCode: "330000",
			cityCode: "330100",
			operatorASNs: [4837, 4134, 4837, 0],
		}),
		{
			op: "and",
			args: [
				{ op: "predicate", field: "geo.country", operator: "eq", values: ["CN"] },
				{ op: "predicate", field: "geo.province", operator: "eq", values: ["330000"] },
				{ op: "predicate", field: "geo.city", operator: "eq", values: ["330100"] },
				{ op: "predicate", field: "asn", operator: "in", values: ["4134", "4837"] },
			],
		}
	)
	assert.equal(buildFlowQuickFilter({}), undefined)
})

test("flow series statistics use actual final bucket duration", () => {
	const base = {
		other: false,
		dimension_snapshot_id: "snapshot-1",
		geo_version: "geo-1",
		classification_version: 1,
		received_records: 1,
		unknown_sampling_records: 0,
		quality_records: 0,
		generated_at: "2026-09-06T10:03:00Z",
	}
	const series = buildFlowSeries(
		[
			{ ...base, bucket: "2026-09-06T10:00:00Z", dimension_value: "CN", value: 800 },
			{ ...base, bucket: "2026-09-06T10:05:00Z", dimension_value: "CN", value: 400 },
		],
		{
			requested_from: "2026-09-06T10:00:00Z",
			requested_to: "2026-09-06T10:07:00Z",
			effective_from: "2026-09-06T10:00:00Z",
			effective_to: "2026-09-06T10:07:00Z",
			source: "1m",
			source_seconds: 60,
			step_seconds: 300,
			target_points: 300,
		},
		"bits_per_second"
	)[0]
	assert.equal(series.total, (800 * 300) / 8 + (400 * 120) / 8)
	assert.equal(series.minimum, 400)
	assert.equal(series.maximum, 800)
	assert.equal(series.last, 400)
	assert.equal(series.p95, 800)
})

test("null wire points are treated as an empty Flow result", () => {
	assert.deepEqual(buildFlowSeries(null, undefined, "bits_per_second"), [])
	assert.deepEqual(buildFlowJointSeries(undefined, undefined, "bits_per_second"), [])
})

test("joint series preserves ordered tuple paths from the same facts", () => {
	const series = buildFlowJointSeries(
		[
			{
				bucket: "2026-09-06T10:00:00Z",
				dimension_values: ["CN", "4134"],
				other: false,
				value: 800,
				dimension_snapshot_id: "snapshot-1",
				geo_version: "geo-1",
				classification_version: 1,
				received_records: 2,
				unknown_sampling_records: 0,
				quality_records: 0,
				observed_at: "2026-09-06T10:01:00Z",
			},
		],
		{
			requested_from: "2026-09-06T10:00:00Z",
			requested_to: "2026-09-06T10:01:00Z",
			effective_from: "2026-09-06T10:00:00Z",
			effective_to: "2026-09-06T10:01:00Z",
			source: "flow_records",
			step_seconds: 60,
			target_points: 300,
		},
		"bits_per_second"
	)[0]
	assert.deepEqual(series.path, ["CN", "4134"])
	assert.equal(series.label, "CN → 4134")
	assert.equal(series.sankeyValue, 800)
})

test("missing tuple buckets are zero-filled for last and weighted average", () => {
	const series = buildFlowSeries(
		[
			{
				bucket: "2026-09-06T10:00:00Z",
				dimension_value: "CN",
				other: false,
				value: 800,
				dimension_snapshot_id: "snapshot-1",
				geo_version: "geo-1",
				classification_version: 1,
				received_records: 1,
				unknown_sampling_records: 0,
				quality_records: 0,
				generated_at: "2026-09-06T10:01:00Z",
			},
		],
		{
			requested_from: "2026-09-06T10:00:00Z",
			requested_to: "2026-09-06T10:02:00Z",
			effective_from: "2026-09-06T10:00:00Z",
			effective_to: "2026-09-06T10:02:00Z",
			source: "1m",
			source_seconds: 60,
			step_seconds: 60,
			target_points: 300,
		},
		"bits_per_second"
	)[0]
	assert.deepEqual(series.values, [
		{ time: Date.parse("2026-09-06T10:00:00Z"), value: 800 },
		{ time: Date.parse("2026-09-06T10:01:00Z"), value: 0 },
	])
	assert.equal(series.last, 0)
	assert.equal(series.average, 400)
	assert.equal(series.sankeyValue, 400)
})

test("current result CSV includes time-series and summary values and neutralizes formulas", () => {
	const csv = buildFlowCSV(
		[
			{
				name: "formula",
				label: "=cmd|'/C calc'!A0,\"quoted\"",
				path: ["formula"],
				values: [{ time: Date.parse("2026-09-06T10:00:00Z"), value: 42 }],
				minimum: 1,
				maximum: 50,
				last: 42,
				average: 20,
				p95: 45,
				total: 120,
				receivedRecords: 8,
				unknownSamplingRecords: 2,
				qualityRecords: 1,
				sankeyValue: 20,
			},
		],
		"bits_per_second"
	)
	assert.match(csv, /^"series","bucket","value"/)
	assert.match(csv, /"'=cmd\|'\/C calc'!A0,""quoted"""/)
	assert.match(csv, /"2026-09-06T10:00:00\.000Z","42","bits_per_second"/)
	assert.match(csv, /"8","2","1"$/)
})
