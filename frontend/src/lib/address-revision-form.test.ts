import assert from "node:assert/strict"
import test from "node:test"
import { buildAddressPrefixRevisionOperations } from "./address-revision-form.ts"

test("address revision form builds stable delete and create operations", () => {
	assert.deepEqual(
		buildAddressPrefixRevisionOperations(
			[
				{ id: "prefix-b", row_version: 3 },
				{ id: "prefix-a", row_version: 2 },
			],
			{
				cidrs: "203.0.113.7/24\n2001:db8::/32",
				labels: "type=customer,region=east",
				source: " customer ",
				asn: "4134",
				geoLeafID: " geo-cn ",
				operatorID: " operator-telecom ",
			}
		),
		[
			{ action: "delete", prefix_id: "prefix-a", expected_version: 2 },
			{ action: "delete", prefix_id: "prefix-b", expected_version: 3 },
			{
				action: "create",
				cidr: "203.0.113.7/24",
				labels: { type: "customer", region: "east" },
				source: "customer",
				asn: 4134,
				geo_leaf_id: "geo-cn",
				operator_id: "operator-telecom",
			},
			{
				action: "create",
				cidr: "2001:db8::/32",
				labels: { type: "customer", region: "east" },
				source: "customer",
				asn: 4134,
				geo_leaf_id: "geo-cn",
				operator_id: "operator-telecom",
			},
		]
	)
})

test("address revision form rejects multiple ASNs", () => {
	assert.throws(
		() =>
			buildAddressPrefixRevisionOperations([], {
				cidrs: "203.0.113.0/24",
				labels: "",
				source: "manual",
				asn: "4134,4837",
				geoLeafID: "",
				operatorID: "",
			}),
		/Only one ASN/
	)
})
