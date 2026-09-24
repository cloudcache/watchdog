import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const flowAttribution = readFileSync(new URL("../components/routes/flow-attribution.tsx", import.meta.url), "utf8")
const flowCustomerBoundaries = readFileSync(
	new URL("../components/routes/flow-customer-boundaries.tsx", import.meta.url),
	"utf8"
)
const flowWorkerDeployments = readFileSync(
	new URL("../components/routes/flow-worker-deployments.tsx", import.meta.url),
	"utf8"
)

test("Flow attribution separates draft editing from signed worker deployments", () => {
	assert.match(flowAttribution, /<FlowCustomerBoundaries\s*\/>/)
	assert.match(flowAttribution, /<FlowWorkerDeployments\s*\/>/)
	assert.doesNotMatch(flowAttribution, /FlowActivationGuide|FlowEnrichmentPublications/)
	assert.doesNotMatch(flowCustomerBoundaries, /\/api\/v1\/flow\/enrichment-publications/)
	assert.doesNotMatch(flowCustomerBoundaries, /Flow processing worker|selectedWorkerID/)
	assert.match(flowWorkerDeployments, /\/api\/v1\/flow\/deployments\/validate/)
	assert.match(flowWorkerDeployments, /\/api\/v1\/flow\/deployments/)
	assert.match(flowWorkerDeployments, /\/api\/v1\/operation-jobs/)
	assert.match(flowWorkerDeployments, /ack_state/)
})
