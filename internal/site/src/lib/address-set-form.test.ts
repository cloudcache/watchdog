import assert from "node:assert/strict"
import test from "node:test"
import { parseAddressEntries, parsePrefixLabels, parseSetLabelSelector } from "./address-set-form.ts"

test("address form parsers normalize and deduplicate typed values", () => {
	assert.deepEqual(parseAddressEntries(" 192.0.2.0/24,\n2001:db8::/32,192.0.2.0/24 "), [
		"192.0.2.0/24",
		"2001:db8::/32",
	])
	assert.deepEqual(parsePrefixLabels("region=Hangzhou, type=customer"), {
		region: "Hangzhou",
		type: "customer",
	})
	assert.deepEqual(parseSetLabelSelector("region=east|west|east, type=customer"), {
		region: ["east", "west"],
		type: ["customer"],
	})
})

test("address form parsers reject malformed or duplicate labels", () => {
	assert.throws(() => parsePrefixLabels("region"), /Invalid label assignment/)
	assert.throws(() => parsePrefixLabels("region=east, region=west"), /Duplicate/)
	assert.throws(() => parseSetLabelSelector("region=east|"), /Empty value/)
})
