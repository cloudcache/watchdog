import assert from "node:assert/strict"
import test from "node:test"

import { trafficViewQueryStep, trafficViewRateBase, trafficViewValueMode } from "./traffic-view.ts"

test("customer and supplier are corrected views of the same raw samples", () => {
	assert.equal(trafficViewValueMode("customer"), "corrected")
	assert.equal(trafficViewValueMode("supplier"), "corrected")
	assert.equal(trafficViewValueMode("raw"), "raw")
})

test("traffic views keep their accounting display bases", () => {
	assert.equal(trafficViewRateBase("customer"), 1000)
	assert.equal(trafficViewRateBase("supplier"), 1024)
	assert.equal(trafficViewRateBase("raw"), 1000)
	assert.equal(trafficViewQueryStep("supplier"), "300")
})
