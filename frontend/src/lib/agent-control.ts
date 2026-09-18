export type AgentKind = "system" | "snmp" | "flow_collect" | "flow_worker" | "probe"

const capabilities: Record<AgentKind, string[]> = {
	system: ["system.samples/v1"],
	snmp: ["snmp.poll/v2"],
	flow_collect: ["flow.receive.sflow/v1", "flow.receive.netflow/v1"],
	flow_worker: ["flow.write.clickhouse/v1"],
	probe: ["probe.execute/v1"],
}

const serviceNames: Partial<Record<AgentKind, string>> = {
	system: "watchdog-system-agent",
	snmp: "watchdog-snmp-collector",
	flow_collect: "watchdog-flow-collect",
	flow_worker: "watchdog-flow-worker",
}

export function agentCapabilities(kind: string) {
	return capabilities[kind as AgentKind] ?? []
}

export function compatibleDeviceKind(agentKind: string) {
	if (agentKind === "system") return "host"
	if (agentKind === "snmp") return "network"
	return ""
}

export function defaultAgentID(kind: string) {
	const suffix = crypto.getRandomValues(new Uint32Array(1))[0].toString(16).padStart(8, "0")
	return `${kind.replaceAll("_", "-")}-${suffix}`.slice(0, 26)
}

export function registryArguments(kind: string, agentID: string, apiURL: string) {
	const root = `/var/lib/watchdog/agents/${agentID}`
	const controlFlag =
		kind === "system" ? "-hub-url" : kind === "flow_worker" ? "-agent-control-plane-url" : "-control-plane-url"
	const identityFlag = kind === "flow_worker" ? "-worker-id" : "-agent-id"
	return [
		controlFlag,
		apiURL.replace(/\/+$/, ""),
		identityFlag,
		agentID,
		"-agent-token-file",
		`${root}/credential`,
		"-agent-enrollment-token-file",
		`${root}/enrollment`,
		"-agent-plan-public-key",
		"/etc/watchdog/agents/agent-plan.pub",
		"-agent-plan-lkg",
		`${root}/plan.lkg`,
	]
}

export function agentActivationCommand(kind: string, agentID: string, publicKey: string) {
	const serviceName = agentServiceName(kind)
	if (!serviceName) return ""
	const normalizedPublicKey = publicKey.trim()
	if (!/^[A-Za-z0-9+/]{43}=$/.test(normalizedPublicKey)) return ""
	return [
		"sudo",
		"/opt/watchdog/current/deploy/systemd/activate-agent.sh",
		kind,
		agentID,
		"--plan-public-key",
		normalizedPublicKey,
	].join(" ")
}

export function registryArgumentsText(kind: string, agentID: string, apiURL: string) {
	return registryArguments(kind, agentID, apiURL).join(" ")
}

export function agentServiceName(kind: string) {
	return serviceNames[kind as AgentKind] ?? ""
}

export function defaultAgentPlan(kind: string): Record<string, string | number> {
	switch (kind) {
		case "system":
			return { interval_seconds: 60, root_path: "/" }
		case "snmp":
			return { interval_seconds: 60, poll_limit: 500 }
		case "flow_collect":
			return {
				sockets: 1,
				receive_buffer_bytes: 33554432,
				max_datagram_bytes: 65535,
			}
		case "flow_worker":
			return {
				kafka_fetch_min_bytes: 1000000,
				kafka_fetch_max_wait_ms: 1000,
				clickhouse_block_max_rows: 50000,
				clickhouse_block_max_bytes: 67108864,
			}
		default:
			return {}
	}
}
