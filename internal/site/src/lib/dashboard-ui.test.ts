import assert from "node:assert/strict"
import test from "node:test"
import {
	addDashboardPanel,
	moveDashboardPanel,
	normalizeDashboardLayout,
	referenceMap,
	removeDashboardPanel,
} from "./dashboard-ui.ts"

test("dashboard layout keeps compatible fields and normalizes panels", () => {
	assert.deepEqual(
		normalizeDashboardLayout({
			revision: 2,
			panels: [{ graph_id: " graph-a ", title: "Core", span: 2 }, { title: "bad" }],
		}),
		{
			revision: 2,
			panels: [{ id: "panel-0-graph-a", graph_id: "graph-a", title: "Core", span: 2 }],
		}
	)
})

test("dashboard panel operations preserve order and stable ids", () => {
	const a = { id: "a", graph_id: "g-a" }
	const b = { id: "b", graph_id: "g-b" }
	assert.deepEqual(moveDashboardPanel([a, b], 1, -1), [b, a])
	assert.deepEqual(moveDashboardPanel([a, b], 0, -1), [a, b])
	assert.deepEqual(addDashboardPanel([a], a), [a])
	assert.deepEqual(addDashboardPanel([a], b), [a, b])
	assert.deepEqual(removeDashboardPanel([a, b], "a"), [b])
})

test("dashboard preview references are keyed by stable graph id", () => {
	const references = referenceMap([
		{ graph_id: "g-a", exists: true, graph: { ID: "g-a", Name: "A" } },
		{ graph_id: "g-missing", exists: false },
	])
	assert.equal(references.get("g-a")?.graph?.Name, "A")
	assert.equal(references.get("g-missing")?.exists, false)
})
