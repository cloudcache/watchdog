import assert from "node:assert/strict"
import test from "node:test"
import { graphPortIDFromLink, normalizeGraphPortStatus } from "./graph-port-links.ts"

test("graph port links preserve explicit ids and parse legacy hrefs", () => {
	assert.equal(graphPortIDFromLink({ port_id: " port-a ", href: "/network/ports/wrong" }), "port-a")
	assert.equal(graphPortIDFromLink({ href: "/network/ports/port%20b?range=24h" }), "port b")
	assert.equal(graphPortIDFromLink({ href: "/network/devices/device-a" }), "")
})

test("graph port status normalizes SNMP and management values", () => {
	assert.equal(normalizeGraphPortStatus("1"), "up")
	assert.equal(normalizeGraphPortStatus("down"), "down")
	assert.equal(normalizeGraphPortStatus("admin_down"), "disabled")
	assert.equal(normalizeGraphPortStatus("testing"), "unknown")
})
