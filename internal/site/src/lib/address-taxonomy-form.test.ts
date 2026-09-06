import assert from "node:assert/strict"
import test from "node:test"
import { parseASNList, toggleListValue } from "./address-taxonomy-form.ts"

test("address taxonomy normalizes, de-duplicates, and sorts ASNs", () => {
	assert.deepEqual(parseASNList("4134, 4837\n4134 9929"), [4134, 4837, 9929])
	assert.deepEqual(parseASNList("  "), [])
})

test("address taxonomy rejects invalid ASNs", () => {
	for (const value of ["0", "1.5", "4294967296", "AS4134"]) {
		assert.throws(() => parseASNList(value), new RegExp(`Invalid ASN: ${value.replace(".", "\\.")}`))
	}
})

test("address taxonomy list toggles do not duplicate values", () => {
	assert.deepEqual(toggleListValue([4], 4, true), [4])
	assert.deepEqual(toggleListValue([4], 6, true), [4, 6])
	assert.deepEqual(toggleListValue([4, 6], 4, false), [6])
})
