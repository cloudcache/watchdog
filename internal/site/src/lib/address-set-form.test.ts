import assert from "node:assert/strict"
import test from "node:test"
import {
	formatAddressSetSelector,
	formatSetLabelSelector,
	parseAddressEntries,
	parsePrefixLabels,
	parseSetLabelSelector,
	parseUnsignedIntegerEntries,
} from "./address-set-form.ts"

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
	assert.deepEqual(parseUnsignedIntegerEntries("64512, 64513,64512", 1, 4_294_967_295, "ASN"), [64512, 64513])
})

test("address set formatters tolerate legacy scalar label selectors", () => {
	assert.equal(
		formatSetLabelSelector({ type: "customer", provider: ["telecom", "mobile"] }),
		"type=customer,provider=telecom|mobile"
	)
	assert.equal(
		formatAddressSetSelector({ labels: { type: "customer" }, geo_node_ids: ["cn"], asns: [4134], families: [4, 6] }),
		"type=customer, geo:cn, asn:4134, IPv4|IPv6"
	)
	assert.equal(formatAddressSetSelector({ labels: { malformed: 7 }, families: null }), "—")
	assert.equal(formatAddressSetSelector(null), "—")
})

test("address form parsers reject malformed or duplicate labels", () => {
	assert.throws(() => parsePrefixLabels("region"), /Invalid label assignment/)
	assert.throws(() => parsePrefixLabels("region=east, region=west"), /Duplicate/)
	assert.throws(() => parseSetLabelSelector("region=east|"), /Empty value/)
	assert.throws(() => parseUnsignedIntegerEntries("64512.5", 1, 4_294_967_295, "ASN"), /integer between/)
	assert.throws(() => parseUnsignedIntegerEntries("0", 1, 4_294_967_295, "ASN"), /ASN/)
})
