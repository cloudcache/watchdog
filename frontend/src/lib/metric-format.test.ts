import assert from "node:assert/strict"
import test from "node:test"
import { formatBitsPerSecond } from "./metric-format.ts"

test("bit rates use explicit SI units instead of locale compact-number suffixes", () => {
	assert.equal(formatBitsPerSecond(0), "0 bps")
	assert.equal(formatBitsPerSecond(12_000_000_000), "12.00 Gbps")
	assert.equal(formatBitsPerSecond(1_500_000), "1.50 Mbps")
})
