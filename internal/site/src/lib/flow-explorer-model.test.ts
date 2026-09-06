import assert from "node:assert/strict"
import test from "node:test"
import { buildFlowJointSeries, buildFlowSeries, parseFlowFilter, resolveFlowTimeRange } from "./flow-explorer-model.ts"

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
