import assert from "node:assert/strict"
import test from "node:test"
import { newUUID } from "./random.ts"

test("newUUID returns an RFC 4122 v4 identifier", () => {
	const value = newUUID()
	assert.match(value, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
})

test("newUUID falls back to getRandomValues when randomUUID is unavailable", () => {
	const fallbackCrypto = {
		getRandomValues: (bytes: Uint8Array) => {
			bytes.fill(0xaa)
			return bytes
		},
	} as Crypto
	assert.equal(newUUID(fallbackCrypto), "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa")
})
