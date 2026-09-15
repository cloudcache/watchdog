import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { runInNewContext } from "node:vm"
import test from "node:test"

const source = readFileSync(new URL("../../public/watchdog-config.js", import.meta.url), "utf8")

test("runtime config preserves injected same-origin defaults", () => {
	const context = {
		WATCHDOG: { BASE_PATH: "/console/", VERSION: "test", API_URL: "https://same.example/console" },
		location: { origin: "https://front.example" },
	}
	runInNewContext(source, context)
	assert.equal(context.WATCHDOG.API_URL, "https://same.example/console")
	assert.equal(context.WATCHDOG.BASE_PATH, "/console/")
})

test("standalone runtime config overrides the single API address", () => {
	const configuredSource = source.replace('API_URL: "http://127.0.0.1:8091"', 'API_URL: "https://api.example"')
	const context: {
		WATCHDOG: string | { BASE_PATH: string; VERSION: string; API_URL: string }
		location: { origin: string }
	} = { WATCHDOG: "{info}", location: { origin: "https://front.example" } }
	runInNewContext(configuredSource, context)
	if (typeof context.WATCHDOG === "string") assert.fail("runtime config was not initialized")
	assert.equal(context.WATCHDOG.API_URL, "https://api.example")
	assert.equal(context.WATCHDOG.BASE_PATH, "/")
})
