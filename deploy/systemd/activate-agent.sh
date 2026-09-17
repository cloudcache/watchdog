#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat >&2 <<'EOF'
usage: activate-agent.sh KIND AGENT_ID ENROLLMENT_TOKEN_FILE PUBLIC_KEY_FILE [CONTROL_PLANE_URL]

KIND is one of: system, snmp, flow_collect, flow_worker.
The enrollment token and public key must be copied from the administrator UI
to owner-readable files before invoking this command. Secrets are never passed
as command-line values.
EOF
	exit 2
}

[[ $# -ge 4 && $# -le 5 ]] || usage
[[ ${EUID} -eq 0 ]] || { echo "activate-agent.sh must run as root" >&2; exit 1; }

kind=$1
agent_id=$2
enrollment_source=$3
public_key_source=$4
control_plane_url=${5:-http://127.0.0.1:8091}

[[ $agent_id =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,25}$ ]] || {
	echo "agent id must contain 1..26 safe characters" >&2
	exit 2
}
[[ $control_plane_url =~ ^https?://[^[:space:]]+$ ]] || {
	echo "control-plane URL must be an http(s) URL without whitespace" >&2
	exit 2
}
[[ -s $enrollment_source ]] || { echo "enrollment token file is empty or missing" >&2; exit 2; }
[[ -s $public_key_source ]] || { echo "agent plan public key file is empty or missing" >&2; exit 2; }

case $kind in
	system)
		service=watchdog-system-agent
		control_flag=-hub-url
		identity_flag=-agent-id
		;;
	snmp)
		service=watchdog-snmp-collector
		control_flag=-control-plane-url
		identity_flag=-agent-id
		;;
	flow_collect)
		service=watchdog-flow-collect
		control_flag=-control-plane-url
		identity_flag=-agent-id
		;;
	flow_worker)
		service=watchdog-flow-worker
		control_flag=-control-plane-url
		identity_flag=-worker-id
		[[ -s /etc/watchdog/flow/worker.env ]] || {
			echo "flow worker remains disabled: /etc/watchdog/flow/worker.env is missing" >&2
			exit 3
		}
		;;
	*) usage ;;
esac

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
unit_source="$script_dir/$service.service"
[[ -s $unit_source ]] || { echo "missing packaged unit: $unit_source" >&2; exit 2; }
getent passwd watchdog >/dev/null || { echo "watchdog user does not exist" >&2; exit 2; }

state_dir="/var/lib/watchdog/agents/$agent_id"
config_dir=/etc/watchdog/agents
enrollment_target="$state_dir/enrollment"
credential_target="$state_dir/credential"
lkg_target="$state_dir/plan.lkg"
env_target="$config_dir/$service.env"

install -d -m 0750 -o watchdog -g watchdog "$state_dir"
install -d -m 0755 -o root -g root "$config_dir"
install -m 0600 -o watchdog -g watchdog "$enrollment_source" "$enrollment_target"
install -m 0644 -o root -g root "$public_key_source" "$config_dir/agent-plan.pub"

env_tmp=$(mktemp "$config_dir/.${service}.env.XXXXXX")
trap 'rm -f "$env_tmp"' EXIT
printf 'WATCHDOG_AGENT_ARGS="%s %s %s %s -agent-token-file %s -agent-enrollment-token-file %s -agent-plan-public-key %s -agent-plan-lkg %s"\n' \
	"$control_flag" "$control_plane_url" "$identity_flag" "$agent_id" "$credential_target" "$enrollment_target" \
	"$config_dir/agent-plan.pub" "$lkg_target" >"$env_tmp"
chmod 0644 "$env_tmp"
chown root:root "$env_tmp"
mv -f "$env_tmp" "$env_target"
trap - EXIT

install -m 0644 -o root -g root "$unit_source" "/etc/systemd/system/$service.service"
systemctl daemon-reload
systemctl enable "$service.service"
# An older deployment may already have the service active with a static
# ExecStart. Restart (rather than only enable --now) so the registry bootstrap
# environment is always loaded by the running process.
systemctl restart "$service.service"
systemctl --no-pager --full status "$service.service" | sed -n '1,12p'
