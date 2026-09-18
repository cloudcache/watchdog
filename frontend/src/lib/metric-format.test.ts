import assert from "node:assert/strict"
import test from "node:test"
import { formatBitsPerSecond, formatMetricValue } from "./metric-format.ts"

test("bit rates use explicit SI units instead of locale compact-number suffixes", () => {
	assert.equal(formatBitsPerSecond(0), "0 bps")
	assert.equal(formatBitsPerSecond(12_000_000_000), "12.00 Gbps")
	assert.equal(formatBitsPerSecond(1_500_000), "1.50 Mbps")
})

test("Flow wire units stay explicit on axes and tooltips", () => {
	assert.equal(formatMetricValue(120_000_000_000, "bits_per_second"), "120.00 Gbps")
	assert.equal(formatMetricValue(12_500_000, "packets_per_second"), "12.5 Mpps")
	assert.equal(formatMetricValue(12_500_000_000, "bytes"), "12.5 GB")
})
