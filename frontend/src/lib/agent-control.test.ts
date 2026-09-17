import assert from "node:assert/strict"
import test from "node:test"
import {
	agentActivationCommand,
	agentCapabilities,
	agentServiceName,
	compatibleDeviceKind,
	defaultAgentPlan,
	registryArguments,
} from "./agent-control.ts"

test("agent capabilities match the production process contracts", () => {
	assert.deepEqual(agentCapabilities("snmp"), ["snmp.poll/v2"])
	assert.deepEqual(agentCapabilities("flow_worker"), ["flow.write.clickhouse/v1"])
	assert.equal(compatibleDeviceKind("system"), "host")
	assert.equal(compatibleDeviceKind("snmp"), "network")
})

test("registry arguments keep lifecycle and domain configuration separate", () => {
	const args = registryArguments("flow_worker", "flow-worker-a", "http://127.0.0.1:8091/")
	assert.deepEqual(args.slice(0, 4), ["-control-plane-url", "http://127.0.0.1:8091", "-worker-id", "flow-worker-a"])
	assert.ok(args.includes("-agent-enrollment-token-file"))
	assert.ok(args.includes("/var/lib/watchdog/agents/flow-worker-a/enrollment"))
	assert.ok(!args.includes("-bootstrap-plan"))
	assert.equal(agentServiceName("flow_worker"), "watchdog-flow-worker")
})

test("activation command delegates process ownership to the host service manager", () => {
	assert.equal(
		agentActivationCommand("snmp", "snmp-main", "http://127.0.0.1:8091/"),
		"sudo /opt/watchdog/current/deploy/systemd/activate-agent.sh snmp snmp-main /path/to/enrollment-token /path/to/agent-plan.pub http://127.0.0.1:8091"
	)
	assert.equal(agentActivationCommand("probe", "probe-main", "http://127.0.0.1:8091"), "")
})

test("typed plan defaults use only fields implemented by each process", () => {
	assert.deepEqual(defaultAgentPlan("snmp"), { interval_seconds: 60, poll_limit: 500 })
	assert.deepEqual(defaultAgentPlan("flow_collect"), {
		sockets: 1,
		receive_buffer_bytes: 33554432,
		max_datagram_bytes: 65535,
	})
})
