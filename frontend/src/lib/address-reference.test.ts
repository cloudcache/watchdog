import assert from "node:assert/strict"
import test from "node:test"
import { addressReferenceSummary, mergeAddressReferenceOptions, toggleAddressReference } from "./address-reference.ts"

test("reference options merge pages by stable id and refresh labels", () => {
	assert.deepEqual(
		mergeAddressReferenceOptions(
			[
				{ id: "geo-a", label: "China" },
				{ id: "geo-b", label: "Old label" },
			],
			[
				{ id: "geo-b", label: "Beijing" },
				{ id: "geo-c", label: "Shanghai" },
			]
		),
		[
			{ id: "geo-a", label: "China" },
			{ id: "geo-b", label: "Beijing" },
			{ id: "geo-c", label: "Shanghai" },
		]
	)
})

test("reference selection supports single, multiple, and deselection", () => {
	assert.deepEqual(toggleAddressReference(["a"], "b", true, false), ["b"])
	assert.deepEqual(toggleAddressReference(["a"], "b", true, true), ["a", "b"])
	assert.deepEqual(toggleAddressReference(["a", "b"], "b", true, true), ["a", "b"])
	assert.deepEqual(toggleAddressReference(["a", "b"], "a", false, true), ["b"])
})

test("reference summary never exposes an unresolved raw id", () => {
	const options = [{ id: "geo-a", label: "country · China" }]
	assert.equal(addressReferenceSummary([], options, "Choose geography", "selected"), "Choose geography")
	assert.equal(addressReferenceSummary(["geo-a"], options, "Choose geography", "selected"), "country · China")
	assert.equal(addressReferenceSummary(["opaque-id"], options, "Choose geography", "selected"), "selected")
	assert.equal(addressReferenceSummary(["a", "b"], options, "Choose geography", "selected"), "2 selected")
})
