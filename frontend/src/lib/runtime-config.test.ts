import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { runInNewContext } from "node:vm"
import test from "node:test"

const source = readFileSync(new URL("../../public/watchdog-config.js", import.meta.url), "utf8")

test("runtime config preserves injected same-origin defaults", () => {
	const context = {
		WATCHDOG: { BASE_PATH: "/console/", VERSION: "test", API_URL: "https://same.example/console" },
		location: { origin: "https://front.example", protocol: "https:", hostname: "front.example" },
	}
	runInNewContext(source, context)
	assert.equal(context.WATCHDOG.API_URL, "https://same.example/console")
	assert.equal(context.WATCHDOG.BASE_PATH, "/console/")
})

test("standalone runtime config derives the API from the browser host", () => {
	const context: {
		WATCHDOG: string | { BASE_PATH: string; VERSION: string; API_URL: string }
		location: { origin: string; protocol: string; hostname: string }
	} = {
		WATCHDOG: "{info}",
		location: { origin: "http://103.83.64.18:8090", protocol: "http:", hostname: "103.83.64.18" },
	}
	runInNewContext(source, context)
	if (typeof context.WATCHDOG === "string") assert.fail("runtime config was not initialized")
	assert.equal(context.WATCHDOG.API_URL, "http://103.83.64.18:8091")
})

test("standalone runtime config overrides the single API address", () => {
	const configuredSource = source.replace(
		"API_URL: `${globalThis.location.protocol}//${globalThis.location.hostname}:8091`",
		'API_URL: "https://api.example"'
	)
	const context: {
		WATCHDOG: string | { BASE_PATH: string; VERSION: string; API_URL: string }
		location: { origin: string; protocol: string; hostname: string }
	} = {
		WATCHDOG: "{info}",
		location: { origin: "https://front.example", protocol: "https:", hostname: "front.example" },
	}
	runInNewContext(configuredSource, context)
	if (typeof context.WATCHDOG === "string") assert.fail("runtime config was not initialized")
	assert.equal(context.WATCHDOG.API_URL, "https://api.example")
	assert.equal(context.WATCHDOG.BASE_PATH, "/")
})
