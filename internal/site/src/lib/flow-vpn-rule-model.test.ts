import assert from "node:assert/strict"
import test from "node:test"
import { optionalPositive, optionalRatio, parseIdentifiers, parsePositiveIntegers } from "./flow-vpn-rule-model.ts"

test("VPN rule list inputs normalize deterministically", () => {
	assert.deepEqual(parsePositiveIntegers("8443, 443,443", 65535), [443, 8443])
	assert.deepEqual(
		parseIdentifiers("us, SG,us", (value) => value.toUpperCase()),
		["SG", "US"]
	)
	assert.equal(parsePositiveIntegers("", 255), undefined)
})

test("VPN rule numeric inputs reject invalid values", () => {
	assert.throws(() => parsePositiveIntegers("0", 65535))
	assert.throws(() => parsePositiveIntegers("6.5", 255))
	assert.equal(optionalPositive("1000"), 1000)
	assert.throws(() => optionalPositive("0"))
	assert.equal(optionalRatio("0.75"), 0.75)
	assert.throws(() => optionalRatio("1.1"))
})
