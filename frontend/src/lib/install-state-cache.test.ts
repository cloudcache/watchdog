import assert from "node:assert/strict"
import test from "node:test"
import { readInstalledCache, writeInstalledCache } from "./install-state-cache.ts"

function memoryStorage() {
	const values = new Map<string, string>()
	return {
		getItem: (key: string) => values.get(key) ?? null,
		setItem: (key: string, value: string) => values.set(key, value),
		removeItem: (key: string) => values.delete(key),
	}
}

test("only a positive install result is cached", () => {
	const storage = memoryStorage()
	assert.equal(readInstalledCache(storage), false)
	writeInstalledCache(storage, true)
	assert.equal(readInstalledCache(storage), true)
	writeInstalledCache(storage, false)
	assert.equal(readInstalledCache(storage), false)
})

test("storage denial never blocks application startup", () => {
	const storage = {
		getItem: () => {
			throw new Error("denied")
		},
		setItem: () => {
			throw new Error("denied")
		},
		removeItem: () => {
			throw new Error("denied")
		},
	}
	assert.equal(readInstalledCache(storage), false)
	assert.doesNotThrow(() => writeInstalledCache(storage, true))
})
