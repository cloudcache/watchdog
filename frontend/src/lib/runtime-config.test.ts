import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { runInNewContext } from "node:vm"
import test from "node:test"
import { resolveAPIBase } from "./api-transport.ts"

const source = readFileSync(new URL("../../public/watchdog-config.js", import.meta.url), "utf8")

type RuntimeConfig = {
	BASE_PATH: string
	VERSION: string
	API_URL: string
	API_BOOTSTRAP_ATTEMPT_TIMEOUT_MS?: number
	API_BOOTSTRAP_RETRIES?: number
	API_BOOTSTRAP_RETRY_DELAY_MS?: number
}

function load(configSource: string, injected?: RuntimeConfig) {
	const context: { WATCHDOG?: RuntimeConfig } = injected ? { WATCHDOG: injected } : {}
	runInNewContext(configSource, context)
	if (!context.WATCHDOG) assert.fail("runtime config was not initialized")
	return context.WATCHDOG
}

test("runtime config preserves host-injected API settings but keeps root MPA paths", () => {
	const config = load(source, { BASE_PATH: "/ignored-subpath/", VERSION: "test", API_URL: "https://same.example/api" })
	assert.equal(config.API_URL, "https://same.example/api")
	assert.equal(config.BASE_PATH, "/")
})

test("standalone runtime config defaults to the same origin", () => {
	const config = load(source)
	assert.equal(config.API_URL, "")
	assert.equal(config.BASE_PATH, "/")
	assert.equal(config.API_BOOTSTRAP_ATTEMPT_TIMEOUT_MS, 2_500)
	assert.equal(config.API_BOOTSTRAP_RETRIES, 2)
	assert.equal(config.API_BOOTSTRAP_RETRY_DELAY_MS, 250)
	assert.equal(resolveAPIBase(config.API_URL, config.BASE_PATH), "/")
})

test("standalone runtime config accepts a split-origin API address", () => {
	const config = load(source.replace('API_URL: ""', 'API_URL: "https://api.example"'))
	assert.equal(config.API_URL, "https://api.example")
	assert.equal(config.BASE_PATH, "/")
})
