import assert from "node:assert/strict"
import test from "node:test"
import { resolveAPIBase, responseFilename } from "./api-transport.ts"

test("API base preserves monolith and standalone deployment contracts", () => {
	assert.equal(resolveAPIBase(undefined, "/console/"), "/console/")
	assert.equal(resolveAPIBase("  ", "/console/"), "/console/")
	assert.equal(resolveAPIBase(" https://api.example/watchdog ", "/console/"), "https://api.example/watchdog")
})

test("download filenames are readable from exposed Content-Disposition", () => {
	assert.equal(responseFilename('attachment; filename="flows.csv"'), "flows.csv")
	assert.equal(responseFilename("attachment; filename*=UTF-8''flow%20report.csv"), "flow report.csv")
	assert.equal(responseFilename(null), "")
})
