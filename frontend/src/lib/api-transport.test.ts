import assert from "node:assert/strict"
import test from "node:test"
import {
	isHTMLAPIResponse,
	nonNegativeRuntimeInteger,
	positiveRuntimeInteger,
	requestDeadline,
	resolveAPIBase,
	responseFilename,
	retryTransient,
} from "./api-transport.ts"

test("API base preserves monolith and standalone deployment contracts", () => {
	assert.equal(resolveAPIBase(undefined, "/"), "/")
	assert.equal(resolveAPIBase("  ", "/"), "/")
	assert.equal(resolveAPIBase(" https://api.example/watchdog ", "/"), "https://api.example/watchdog")
})

test("download filenames are readable from exposed Content-Disposition", () => {
	assert.equal(responseFilename('attachment; filename="flows.csv"'), "flows.csv")
	assert.equal(responseFilename("attachment; filename*=UTF-8''flow%20report.csv"), "flow report.csv")
	assert.equal(responseFilename(null), "")
})

test("frontend HTML responses are never accepted as API JSON", () => {
	assert.equal(isHTMLAPIResponse("text/html; charset=utf-8", "anything"), true)
	assert.equal(isHTMLAPIResponse("application/octet-stream", "<!doctype html><title>Watchdog</title>"), true)
	assert.equal(isHTMLAPIResponse("application/json", '{"items":[]}'), false)
})

test("request deadlines abort stalled requests and report timeout ownership", async () => {
	const deadline = requestDeadline(undefined, 5)
	await new Promise((resolve) => setTimeout(resolve, 15))
	assert.equal(deadline.signal.aborted, true)
	assert.equal(deadline.didTimeout(), true)
	deadline.cleanup()
})

test("normal API requests do not install a transport deadline", async () => {
	const deadline = requestDeadline(undefined)
	await new Promise((resolve) => setTimeout(resolve, 10))
	assert.equal(deadline.signal.aborted, false)
	assert.equal(deadline.didTimeout(), false)
	deadline.cleanup()
})

test("caller cancellation remains distinct from request timeout", () => {
	const caller = new AbortController()
	const deadline = requestDeadline(caller.signal, 60_000)
	caller.abort("route changed")
	assert.equal(deadline.signal.aborted, true)
	assert.equal(deadline.signal.reason, "route changed")
	assert.equal(deadline.didTimeout(), false)
	deadline.cleanup()
})

test("runtime durations accept only positive safe integers", () => {
	assert.equal(positiveRuntimeInteger(2500, 1000), 2500)
	assert.equal(positiveRuntimeInteger(0, 1000), 1000)
	assert.equal(positiveRuntimeInteger(Number.NaN, 1000), 1000)
	assert.equal(nonNegativeRuntimeInteger(0, 2), 0)
	assert.equal(nonNegativeRuntimeInteger(-1, 2), 2)
})

test("transient retries are bounded and do not retry permanent errors", async () => {
	let attempts = 0
	const value = await retryTransient(
		() => {
			attempts++
			if (attempts < 3) return Promise.reject(new Error("transient"))
			return Promise.resolve("ready")
		},
		() => true,
		[0, 0]
	)
	assert.equal(value, "ready")
	assert.equal(attempts, 3)

	attempts = 0
	await assert.rejects(
		retryTransient(
			() => {
				attempts++
				return Promise.reject(new Error("permanent"))
			},
			() => false,
			[0, 0]
		),
		/permanent/
	)
	assert.equal(attempts, 1)
})
