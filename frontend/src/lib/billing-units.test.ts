import assert from "node:assert/strict"
import test from "node:test"
import { BPS_PER_MBPS, BYTES_PER_GB, formatBillingUnit, parseBillingUnit, reconciliationUnit } from "./billing-units.ts"

test("billing units round-trip API base units through operator units", () => {
	assert.equal(formatBillingUnit(1_500_000_000, BPS_PER_MBPS), "1500")
	assert.equal(parseBillingUnit("1500", BPS_PER_MBPS), 1_500_000_000)
	assert.equal(formatBillingUnit(12_500_000_000, BYTES_PER_GB), "12.5")
	assert.equal(parseBillingUnit("12.5", BYTES_PER_GB), 12_500_000_000)
})

test("billing units reject ambiguous or unsafe values", () => {
	assert.equal(parseBillingUnit("", BPS_PER_MBPS), undefined)
	assert.equal(parseBillingUnit("-1", BPS_PER_MBPS), undefined)
	assert.equal(parseBillingUnit("1e3", BPS_PER_MBPS), undefined)
	assert.equal(parseBillingUnit("999999999999", BYTES_PER_GB), undefined)
})

test("reconciliation threshold unit follows the account algorithm", () => {
	assert.deepEqual(reconciliationUnit("95th"), { label: "Mbps", multiplier: BPS_PER_MBPS })
	assert.deepEqual(reconciliationUnit("average"), { label: "Mbps", multiplier: BPS_PER_MBPS })
	assert.deepEqual(reconciliationUnit("total"), { label: "GB", multiplier: BYTES_PER_GB })
})
