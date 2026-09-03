import assert from "node:assert/strict"
import test from "node:test"
import { vendorLogoFor } from "./vendor-logos.ts"

test("returns no image for an undiscovered vendor", () => {
	assert.equal(vendorLogoFor(""), "")
	assert.equal(vendorLogoFor("unknown vendor"), "")
})

test("returns the known vendor logo", () => {
	assert.equal(vendorLogoFor("Huawei Technologies"), "/static/vendor-logos/huawei.svg")
})
