import assert from "node:assert/strict"
import test from "node:test"
import { getPagePath } from "./page-path.ts"

const registry = {}

test("document paths are generated without a client router", () => {
	assert.equal(getPagePath(registry, "flow_overview"), "/flow")
	assert.equal(getPagePath(registry, "network_device", { id: "device / 1" }), "/network/devices/device%20%2F%201")
	assert.equal(getPagePath(registry, "settings"), "/settings")
	assert.equal(getPagePath(registry, "settings", { name: "general" }), "/settings/general")
})

test("required document path parameters fail before navigation", () => {
	assert.throws(() => getPagePath(registry, "network_device"), /Missing route parameter: id/)
})
