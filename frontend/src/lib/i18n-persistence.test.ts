import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import test from "node:test"

const source = readFileSync(fileURLToPath(new URL("./i18n.ts", import.meta.url)), "utf8")

test("MPA bootstrap does not overwrite the persisted language", () => {
	assert.match(source, /function activateLocale\([^)]*persist = true\)/)
	assert.match(source, /if \(persist\) \{[\s\S]*localStorage\.setItem\("lang", locale\)/)
	assert.match(source, /activateLocale\("en", enMessages, false\)/)
})
