import assert from "node:assert/strict"
import { readdirSync, readFileSync } from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import test from "node:test"

const sourceRoot = fileURLToPath(new URL("../", import.meta.url))
const forbiddenAPIPrefixes = [
	"/api/v1/network/devices",
	"/api/v1/network/ports",
	"/api/v1/network/bgp",
	"/api/v1/network/traffic-policy-defaults",
	"/api/v1/targets",
]

function sourceFiles(directory: string): string[] {
	return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
		const absolute = path.join(directory, entry.name)
		if (entry.isDirectory()) {
			return entry.name === "locales" ? [] : sourceFiles(absolute)
		}
		return /\.(ts|tsx)$/.test(entry.name) ? [absolute] : []
	})
}

test("frontend uses the canonical device API only", () => {
	for (const file of sourceFiles(sourceRoot)) {
		if (file === fileURLToPath(import.meta.url)) continue
		const source = readFileSync(file, "utf8")
		for (const prefix of forbiddenAPIPrefixes) {
			assert.equal(source.includes(prefix), false, `${path.relative(sourceRoot, file)} still references ${prefix}`)
		}
	}
})
