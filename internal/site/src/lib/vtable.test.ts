import assert from "node:assert/strict"
import test from "node:test"
import { calculateFilterPopoverPosition } from "./vtable.ts"

test("positions a filter below its header when there is room", () => {
	assert.deepEqual(
		calculateFilterPopoverPosition({ x: 220, y: 40 }, { width: 200, height: 240 }, { width: 800, height: 600 }),
		{ left: 40, top: 50 }
	)
})

test("keeps the filter inside the right viewport edge", () => {
	assert.deepEqual(
		calculateFilterPopoverPosition({ x: 798, y: 40 }, { width: 240, height: 240 }, { width: 800, height: 600 }),
		{ left: 552, top: 50 }
	)
})

test("moves the filter above a header near the bottom edge", () => {
	assert.deepEqual(
		calculateFilterPopoverPosition({ x: 220, y: 590 }, { width: 200, height: 240 }, { width: 800, height: 600 }),
		{ left: 40, top: 340 }
	)
})

test("clamps an oversized filter to the viewport gutter", () => {
	assert.deepEqual(
		calculateFilterPopoverPosition({ x: 4, y: 4 }, { width: 400, height: 500 }, { width: 320, height: 420 }),
		{ left: 8, top: 8 }
	)
})
