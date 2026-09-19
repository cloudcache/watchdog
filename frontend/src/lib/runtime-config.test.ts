import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { runInNewContext } from "node:vm"
import test from "node:test"
import { resolveAPIBase } from "./api-transport.ts"

const source = readFileSync(new URL("../../public/watchdog-config.js", import.meta.url), "utf8")

type RuntimeConfig = { BASE_PATH: string; VERSION: string; API_URL: string }

function load(configSource: string, injected?: RuntimeConfig) {
	const context: { WATCHDOG?: RuntimeConfig } = injected ? { WATCHDOG: injected } : {}
	runInNewContext(configSource, context)
	if (!context.WATCHDOG) assert.fail("runtime config was not initialized")
	return context.WATCHDOG
}

test("runtime config preserves a host-injected configuration", () => {
	const config = load(source, { BASE_PATH: "/console/", VERSION: "test", API_URL: "https://same.example/console" })
	assert.equal(config.API_URL, "https://same.example/console")
	assert.equal(config.BASE_PATH, "/console/")
})

test("standalone runtime config defaults to the same origin", () => {
	const config = load(source)
	assert.equal(config.API_URL, "")
	assert.equal(config.BASE_PATH, "/")
	// The API client turns an empty origin into base-path-relative URLs.
	assert.equal(resolveAPIBase(config.API_URL, config.BASE_PATH), "/")
})

test("standalone runtime config accepts a split-origin API address", () => {
	const config = load(source.replace('API_URL: ""', 'API_URL: "https://api.example"'))
	assert.equal(config.API_URL, "https://api.example")
	assert.equal(config.BASE_PATH, "/")
})
