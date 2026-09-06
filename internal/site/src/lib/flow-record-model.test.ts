import assert from "node:assert/strict"
import test from "node:test"
import {
	buildFlowRecordRows,
	flowProtocolLabel,
	formatFlowBytes,
	updateFlowRecordCursors,
} from "./flow-record-model.ts"

test("Flow record rows preserve stable ids and format protocol and counters", () => {
	const rows = buildFlowRecordRows([
		{
			event_time: "2026-09-07T11:59:00Z",
			record_id: "record-a",
			values: {
				src_ip: "203.0.113.1",
				dst_ip: "2001:db8::1",
				ip_protocol: 6,
				estimated_bytes: 1_500_000,
				quality_flags: 2,
			},
		},
	])
	assert.equal(rows[0].record_id, "record-a")
	assert.equal(rows[0].src_ip, "203.0.113.1")
	assert.equal(rows[0].dst_ip, "2001:db8::1")
	assert.equal(rows[0].protocol, "TCP (6)")
	assert.equal(rows[0].estimated_bytes, "1.50 MB")
	assert.equal(rows[0].quality_flags, 2)
	assert.equal(flowProtocolLabel(58), "ICMPv6 (58)")
	assert.equal(formatFlowBytes("bad"), "")
})

test("Flow cursor pages retain only the reachable forward chain", () => {
	assert.deepEqual(updateFlowRecordCursors(["", "cursor-1", "stale-2", "stale-3"], 1, "cursor-2"), [
		"",
		"cursor-1",
		"cursor-2",
	])
	assert.deepEqual(updateFlowRecordCursors([""], 0), ["", ""])
})
