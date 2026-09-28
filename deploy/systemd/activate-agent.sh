#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat >&2 <<'EOF'
usage:
  activate-agent.sh KIND AGENT_ID [CONTROL_PLANE_URL]
  activate-agent.sh KIND AGENT_ID --plan-public-key BASE64 [CONTROL_PLANE_URL]
  activate-agent.sh KIND AGENT_ID SHARED_TOKEN_FILE PUBLIC_KEY_FILE [CONTROL_PLANE_URL]

KIND is one of: system, snmp, flow_collect, flow_worker.
The short form prompts for the installation-wide shared agent token without
echoing it. It reuses an installed Watchdog plan public key, or prompts for the
public key on a new host. The long form is retained for non-interactive
automation. Secrets are never passed as command-line values.
EOF
	exit 2
}

[[ ${EUID} -eq 0 ]] || { echo "activate-agent.sh must run as root" >&2; exit 1; }

temp_root=
env_tmp=
cleanup() {
	[[ -z $env_tmp ]] || rm -f "$env_tmp"
	[[ -z $temp_root ]] || rm -rf "$temp_root"
}
trap cleanup EXIT

interactive_bootstrap() {
	control_plane_url=$1
	public_key=${2:-}
	temp_root=$(mktemp -d)
	token_source="$temp_root/token"
	public_key_source="$temp_root/agent-plan.pub"
	IFS= read -r -s -p "Installation shared agent token: " shared_token
	printf '\n'
	[[ -n $shared_token ]] || { echo "shared token is empty" >&2; exit 2; }
	printf '%s\n' "$shared_token" >"$token_source"
	unset shared_token
	if [[ -n $public_key ]]; then
		printf '%s\n' "$public_key" >"$public_key_source"
		unset public_key
	elif [[ -s /etc/watchdog/agents/agent-plan.pub ]]; then
		public_key_source=/etc/watchdog/agents/agent-plan.pub
	else
		IFS= read -r -p "Agent plan public key: " public_key
		[[ -n $public_key ]] || { echo "agent plan public key is empty" >&2; exit 2; }
		printf '%s\n' "$public_key" >"$public_key_source"
		unset public_key
	fi
}

case $# in
	2|3)
		kind=$1
		agent_id=$2
		control_plane_url=${3:-http://127.0.0.1:8091}
		interactive_bootstrap "$control_plane_url"
		;;
	4|5)
		kind=$1
		agent_id=$2
		if [[ $3 == --plan-public-key ]]; then
			control_plane_url=${5:-http://127.0.0.1:8091}
			interactive_bootstrap "$control_plane_url" "$4"
		else
			token_source=$3
			public_key_source=$4
			control_plane_url=${5:-http://127.0.0.1:8091}
		fi
		;;
	*) usage ;;
esac

[[ $agent_id =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,25}$ ]] || {
	echo "agent id must contain 1..26 safe characters" >&2
	exit 2
}
[[ $control_plane_url =~ ^https?://[^[:space:]]+$ ]] || {
	echo "control-plane URL must be an http(s) URL without whitespace" >&2
	exit 2
}
[[ -s $token_source ]] || { echo "shared token file is empty or missing" >&2; exit 2; }
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
		# Agent lifecycle and Flow enrichment publication delivery are separate
		# contracts. Do not switch a working bootstrap worker into remote
		# enrichment mode merely by registering it in the Agent registry.
		control_flag=-agent-control-plane-url
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
token_target="$state_dir/token"
lkg_target="$state_dir/plan.lkg"
env_target="$config_dir/$service.env"

install -d -m 0750 -o watchdog -g watchdog "$state_dir"
install -d -m 0755 -o root -g root "$config_dir"
install -m 0600 -o watchdog -g watchdog "$token_source" "$token_target"
if [[ $(readlink -f "$public_key_source") != $(readlink -f "$config_dir/agent-plan.pub" 2>/dev/null || true) ]]; then
	install -m 0644 -o root -g root "$public_key_source" "$config_dir/agent-plan.pub"
fi

env_tmp=$(mktemp "$config_dir/.${service}.env.XXXXXX")
printf 'WATCHDOG_AGENT_ARGS="%s %s %s %s -agent-token-file %s -agent-plan-public-key %s -agent-plan-lkg %s"\n' \
	"$control_flag" "$control_plane_url" "$identity_flag" "$agent_id" "$token_target" \
	"$config_dir/agent-plan.pub" "$lkg_target" >"$env_tmp"
chmod 0644 "$env_tmp"
chown root:root "$env_tmp"
mv -f "$env_tmp" "$env_target"
env_tmp=

install -m 0644 -o root -g root "$unit_source" "/etc/systemd/system/$service.service"
systemctl daemon-reload
systemctl enable "$service.service"
# An older deployment may already have the service active with a static
# ExecStart. Restart (rather than only enable --now) so the registry bootstrap
# environment is always loaded by the running process.
systemctl restart "$service.service"
systemctl --no-pager --full status "$service.service" | sed -n '1,12p'
