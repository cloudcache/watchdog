#!/bin/sh

is_alpine() {
  [ -f /etc/alpine-release ]
}

is_openwrt() {
  grep -qi "OpenWrt" /etc/os-release
}

is_freebsd() {
  [ "$(uname -s)" = "FreeBSD" ]
}

is_opnsense() {
  [ -f /usr/local/sbin/opnsense-version ] || [ -f /usr/local/etc/opnsense-version ] || [ -f /etc/opnsense-release ]
}

is_glibc() {
  # Prefer glibc-enabled agent (NVML via purego) on linux/amd64 glibc systems.
  # Check common dynamic loader paths first (fast + reliable).
  for p in \
    /lib64/ld-linux-x86-64.so.2 \
    /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 \
    /lib/ld-linux-x86-64.so.2; do
    [ -e "$p" ] && return 0
  done

  # Fallback to ldd output if available.
  if command -v ldd >/dev/null 2>&1; then
    ldd --version 2>&1 | grep -qiE 'gnu libc|glibc' && return 0
  fi

  return 1
}


# If SELinux is enabled, set the context of the binary
set_selinux_context() {
  # Check if SELinux is enabled and in enforcing or permissive mode
  if command -v getenforce >/dev/null 2>&1; then
    SELINUX_MODE=$(getenforce)
    if [ "$SELINUX_MODE" != "Disabled" ]; then
      echo "SELinux is enabled (${SELINUX_MODE} mode). Setting appropriate context..."

      # First try to set persistent context if semanage is available
      if command -v semanage >/dev/null 2>&1; then
        echo "Attempting to set persistent SELinux context..."
        if semanage fcontext -a -t bin_t "$BIN_PATH" >/dev/null 2>&1; then
          restorecon -v "$BIN_PATH" >/dev/null 2>&1
        else
          echo "Warning: Failed to set persistent context, falling back to temporary context."
        fi
      fi

      # Fall back to chcon if semanage failed or isn't available
      if command -v chcon >/dev/null 2>&1; then
        # Set context for both the directory and binary
        chcon -t bin_t "$BIN_PATH" || echo "Warning: Failed to set SELinux context for binary."
        chcon -R -t bin_t "$AGENT_DIR" || echo "Warning: Failed to set SELinux context for directory."
      else
        if [ "$SELINUX_MODE" = "Enforcing" ]; then
          echo "Warning: SELinux is in enforcing mode but chcon command not found. The service may fail to start."
          echo "Consider installing the policycoreutils package or temporarily setting SELinux to permissive mode."
        else
          echo "Warning: SELinux is in permissive mode but chcon command not found."
        fi
      fi
    fi
  fi
}

# Clean up SELinux contexts if they were set
cleanup_selinux_context() {
  if command -v getenforce >/dev/null 2>&1 && [ "$(getenforce)" != "Disabled" ]; then
    echo "Cleaning up SELinux contexts..."
    # Remove persistent context if semanage is available
    if command -v semanage >/dev/null 2>&1; then
      semanage fcontext -d "$BIN_PATH" 2>/dev/null || true
    fi
  fi
}

# Ensure the proxy URL ends with a /
ensure_trailing_slash() {
  if [ -n "$1" ]; then
    case "$1" in
    */) echo "$1" ;;
    *) echo "$1/" ;;
    esac
  else
    echo "$1"
  fi
}

# Generate FreeBSD rc service content
generate_freebsd_rc_service() {
  cat <<'EOF'
#!/bin/sh

# PROVIDE: watchdog_agent
# REQUIRE: DAEMON NETWORKING
# BEFORE: LOGIN
# KEYWORD: shutdown

# Add the following lines to /etc/rc.conf to configure Watchdog Agent:
#
# watchdog_agent_enable (bool):   Set to YES to enable Watchdog Agent
#                               Default: YES
# watchdog_agent_env_file (str):  Watchdog Agent env configuration file
#                               Default: /usr/local/etc/watchdog-agent/env
# watchdog_agent_user (str):      Watchdog Agent daemon user
#                               Default: watchdog
# watchdog_agent_bin (str):       Path to the watchdog-agent binary
#                               Default: /usr/local/sbin/watchdog-agent
# watchdog_agent_flags (str):     Extra flags passed to watchdog-agent command invocation
#                               Default:

. /etc/rc.subr

name="watchdog_agent"
rcvar=watchdog_agent_enable

load_rc_config $name
: ${watchdog_agent_enable:="YES"}
: ${watchdog_agent_user:="watchdog"}
: ${watchdog_agent_flags:=""}
: ${watchdog_agent_env_file:="/usr/local/etc/watchdog-agent/env"}
: ${watchdog_agent_bin:="/usr/local/sbin/watchdog-agent"}

logfile="/var/log/${name}.log"
pidfile="/var/run/${name}.pid"

procname="/usr/sbin/daemon"
start_precmd="${name}_prestart"
start_cmd="${name}_start"
stop_cmd="${name}_stop"

extra_commands="upgrade"
upgrade_cmd="watchdog_agent_upgrade"

watchdog_agent_prestart()
{
    if [ ! -f "${watchdog_agent_env_file}" ]; then
        echo WARNING: missing "${watchdog_agent_env_file}" env file. Start aborted.
        exit 1
    fi
}

watchdog_agent_start()
{
    echo "Starting ${name}"
    /usr/sbin/daemon -fc \
            -P "${pidfile}" \
            -o "${logfile}" \
            -u "${watchdog_agent_user}" \
            "${watchdog_agent_bin}" ${watchdog_agent_flags}
}

watchdog_agent_stop()
{
    pid="$(check_pidfile "${pidfile}" "${procname}")"
    if [ -n "${pid}" ]; then
        echo "Stopping ${name} (pid=${pid})"
        kill -- "-${pid}"
        wait_for_pids "${pid}"
    else
        echo "${name} isn't running"
    fi
}

watchdog_agent_upgrade()
{
    echo "Upgrading ${name}"
    if command -v sudo >/dev/null; then
        sudo -u "${watchdog_agent_user}" -- "${watchdog_agent_bin}" update
    else
        su -m "${watchdog_agent_user}" -c "${watchdog_agent_bin} update"
    fi
}

run_rc_command "$1"
EOF
}

# Detect system architecture
detect_architecture() {
  local arch=$(uname -m)

  if [ "$arch" = "mips" ]; then
    detect_mips_endianness
    return $?
  fi

  case "$arch" in
    x86_64)
      arch="amd64"
      ;;
    armv6l|armv7l)
      arch="arm"
      ;;
    aarch64)
      arch="arm64"
      ;;
  esac

  echo "$arch"
}

# Detect MIPS endianness using ELF header
detect_mips_endianness() {
  local bins="/bin/sh /bin/ls /usr/bin/env"
  local bin_to_check endian
  
  for bin_to_check in $bins; do
    if [ -f "$bin_to_check" ]; then
      # The 6th byte in ELF header: 01 = little, 02 = big
      endian=$(hexdump -n 1 -s 5 -e '1/1 "%02x"' "$bin_to_check" 2>/dev/null)
      if [ "$endian" = "01" ]; then
        echo "mipsle"
        return
      elif [ "$endian" = "02" ]; then
        echo "mips" 
        return
      fi
    fi
  done
  
  # Final fallback
  echo "mips"
}

# Default values
PORT=45876
UNINSTALL=false
GITHUB_URL="https://github.com"
KEY=""
TOKEN=""
HUB_URL=""
AUTO_UPDATE_FLAG="" # empty string means prompt, "true" means auto-enable, "false" means skip
VERSION="latest"

# Check for help flag
case "$1" in
-h | --help)
  printf "Watchdog Agent installation script\n\n"
  printf "Usage: ./install-agent.sh [options]\n\n"
  printf "Options: \n"
  printf "  -k                    : SSH key (required, or interactive if not provided)\n"
  printf "  -p                    : Port (default: $PORT)\n"
  printf "  -t                    : Token (optional for backwards compatibility)\n"
  printf "  -url                  : Hub URL (optional for backwards compatibility)\n"
  printf "  -v, --version         : Version to install (default: latest)\n"
  printf "  -u                    : Uninstall Watchdog Agent\n"
  printf "  --auto-update [VALUE] : Control automatic daily updates\n"
  printf "                          VALUE can be true (enable) or false (disable). If not specified, will prompt.\n"
  printf "  --mirror URL          : Prefix GitHub downloads with an explicit proxy URL\n"
  printf "  -h, --help            : Display this help message\n"
  exit 0
  ;;
esac

# Build sudo args by properly quoting everything
build_sudo_args() {
  QUOTED_ARGS=""
  while [ $# -gt 0 ]; do
    if [ -n "$QUOTED_ARGS" ]; then
      QUOTED_ARGS="$QUOTED_ARGS "
    fi
    QUOTED_ARGS="$QUOTED_ARGS'$(echo "$1" | sed "s/'/'\\\\''/g")'"
    shift
  done
  echo "$QUOTED_ARGS"
}

# Check if running as root and re-execute with sudo if needed
if [ "$(id -u)" != "0" ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO_ARGS=$(build_sudo_args "$@")
    eval "exec sudo $0 $SUDO_ARGS"
  else
    echo "This script must be run as root. Please either:"
    echo "1. Run this script as root (su root)"
    echo "2. Install sudo and run with sudo"
    exit 1
  fi
fi

# Parse arguments
while [ $# -gt 0 ]; do
  case "$1" in
  -k)
    shift
    KEY="$1"
    ;;
  -p)
    shift
    PORT="$1"
    ;;
  -t)
    shift
    TOKEN="$1"
    ;;
  -url)
    shift
    HUB_URL="$1"
    ;;
  -v | --version)
    shift
    VERSION="$1"
    ;;
  -u)
    UNINSTALL=true
    ;;
  --mirror=* | --china-mirrors=*)
    CUSTOM_PROXY=${1#*=}
    if [ -z "$CUSTOM_PROXY" ]; then
      echo "--mirror requires a proxy URL" >&2
      exit 1
    fi
    GITHUB_URL="$(ensure_trailing_slash "$CUSTOM_PROXY")https://github.com"
    ;;
  --mirror | --china-mirrors)
    if [ -z "$2" ] || echo "$2" | grep -q '^-'; then
      echo "--mirror requires a proxy URL" >&2
      exit 1
    fi
    GITHUB_URL="$(ensure_trailing_slash "$2")https://github.com"
    shift
    ;;
  --auto-update*)
    # Check if there's a value after the = sign
    if echo "$1" | grep -q "="; then
      # Extract the value after =
      AUTO_UPDATE_VALUE=$(echo "$1" | cut -d'=' -f2)
      if [ "$AUTO_UPDATE_VALUE" = "true" ]; then
        AUTO_UPDATE_FLAG="true"
      elif [ "$AUTO_UPDATE_VALUE" = "false" ]; then
        AUTO_UPDATE_FLAG="false"
      else
        echo "Invalid value for --auto-update flag: $AUTO_UPDATE_VALUE. Using default (prompt)."
      fi
    elif [ "$2" = "true" ] || [ "$2" = "false" ]; then
      # Value provided as next argument
      AUTO_UPDATE_FLAG="$2"
      shift
    else
      # No value specified, use true
      AUTO_UPDATE_FLAG="true"
    fi
    ;;
  *)
    echo "Invalid option: $1" >&2
    exit 1
    ;;
  esac
  shift
done

# Set paths based on operating system
if is_freebsd; then
  AGENT_DIR="/usr/local/etc/watchdog-agent"
  BIN_DIR="/usr/local/sbin"
  BIN_PATH="/usr/local/sbin/watchdog-agent"
else
  AGENT_DIR="/opt/watchdog-agent"
  BIN_DIR="/opt/watchdog-agent"
  BIN_PATH="/opt/watchdog-agent/watchdog-agent"
fi

# Stop existing service if it exists (for upgrades)
if [ "$UNINSTALL" != true ] && [ -f "$BIN_PATH" ]; then
  echo "Existing installation detected. Stopping service for upgrade..."
  if is_alpine; then
    rc-service watchdog-agent stop 2>/dev/null || true
  elif is_openwrt; then
    /etc/init.d/watchdog-agent stop 2>/dev/null || true
  elif is_freebsd; then
    service watchdog-agent stop 2>/dev/null || true
  else
    systemctl stop watchdog-agent.service 2>/dev/null || true
  fi
fi

# Uninstall process
if [ "$UNINSTALL" = true ]; then
  # Clean up SELinux contexts before removing files
  cleanup_selinux_context

  if is_alpine; then
    echo "Stopping and disabling the agent service..."
    rc-service watchdog-agent stop
    rc-update del watchdog-agent default

    echo "Removing the OpenRC service files..."
    rm -f /etc/init.d/watchdog-agent

    # Remove the daily update cron job if it exists
    echo "Removing the daily update cron job..."
    if crontab -u root -l 2>/dev/null | grep -q "watchdog-agent.*update"; then
      crontab -u root -l 2>/dev/null | grep -v "watchdog-agent.*update" | crontab -u root -
    fi

    # Remove log files
    echo "Removing log files..."
    rm -f /var/log/watchdog-agent.log /var/log/watchdog-agent.err
  elif is_openwrt; then
    echo "Stopping and disabling the agent service..."
    /etc/init.d/watchdog-agent stop
    /etc/init.d/watchdog-agent disable

    echo "Removing the OpenWRT service files..."
    rm -f /etc/init.d/watchdog-agent

    # Remove the update service if it exists
    echo "Removing the daily update service..."
    # Remove legacy watchdog account based crontab file
    rm -f /etc/crontabs/watchdog
    # Install root crontab job
    if crontab -u root -l 2>/dev/null | grep -q "watchdog-agent.*update"; then
      crontab -u root -l 2>/dev/null | grep -v "watchdog-agent.*update" | crontab -u root -
    fi

  elif is_freebsd; then
    echo "Stopping and disabling the agent service..."
    service watchdog-agent stop
    sysrc watchdog_agent_enable="NO"

    echo "Removing the FreeBSD service files..."
    rm -f /usr/local/etc/rc.d/watchdog-agent

    # Remove the daily update cron job if it exists
    echo "Removing the daily update cron job..."
    rm -f /etc/cron.d/watchdog-agent

    # Remove log files
    echo "Removing log files..."
    rm -f /var/log/watchdog-agent.log

    # Remove env file and directories
    echo "Removing environment configuration file..."
    rm -f "$AGENT_DIR/env"
    rm -f "$BIN_PATH"
    rmdir "$AGENT_DIR" 2>/dev/null || true

  else
    echo "Stopping and disabling the agent service..."
    systemctl stop watchdog-agent.service
    systemctl disable watchdog-agent.service >/dev/null 2>&1

    echo "Removing the systemd service file..."
    rm /etc/systemd/system/watchdog-agent.service

    # Remove the update timer and service if they exist
    echo "Removing the daily update service and timer..."
    systemctl stop watchdog-agent-update.timer 2>/dev/null
    systemctl disable watchdog-agent-update.timer >/dev/null 2>&1
    rm -f /etc/systemd/system/watchdog-agent-update.service
    rm -f /etc/systemd/system/watchdog-agent-update.timer

    systemctl daemon-reload
  fi

  echo "Removing the Watchdog Agent directory..."
  rm -rf "$AGENT_DIR"

  echo "Removing the dedicated user for the agent service..."
  killall watchdog-agent 2>/dev/null
  if is_alpine || is_openwrt; then
    deluser watchdog 2>/dev/null
  elif is_freebsd; then
    pw user del watchdog 2>/dev/null
  else
    userdel watchdog 2>/dev/null
  fi

  echo "Watchdog Agent has been uninstalled successfully!"
  exit 0
fi

# Check if a package is installed
package_installed() {
  command -v "$1" >/dev/null 2>&1
}

# Check for package manager and install necessary packages if not installed
if package_installed apk; then
  if ! package_installed tar || ! package_installed curl || ! package_installed sha256sum; then
    apk update
    apk add tar curl coreutils shadow
  fi
elif package_installed opkg; then
  if ! package_installed tar || ! package_installed curl || ! package_installed sha256sum; then
    opkg update
    opkg install tar curl coreutils
  fi
elif package_installed pkg && is_freebsd; then
  if ! package_installed tar || ! package_installed curl || ! package_installed sha256sum; then
    pkg update
    pkg install -y gtar curl coreutils
  fi
elif package_installed apt-get; then
  if ! package_installed tar || ! package_installed curl || ! package_installed sha256sum; then
    apt-get update
    apt-get install -y tar curl coreutils
  fi
elif package_installed yum; then
  if ! package_installed tar || ! package_installed curl || ! package_installed sha256sum; then
    yum install -y tar curl coreutils
  fi
elif package_installed pacman; then
  if ! package_installed tar || ! package_installed curl || ! package_installed sha256sum; then
    pacman -Sy --noconfirm tar curl coreutils
  fi
else
  echo "Warning: Please ensure 'tar' and 'curl' and 'sha256sum (coreutils)' are installed."
fi

# If no SSH key is provided, ask for the SSH key interactively (skip if upgrading)
if [ -z "$KEY" ]; then
  if [ -f "$BIN_PATH" ]; then
    echo "Upgrading existing installation. Using existing service configuration."
  else
    printf "Enter your SSH key: "
    read KEY
  fi
fi

# Remove newlines from KEY
KEY=$(echo "$KEY" | tr -d '\n')

# TOKEN and HUB_URL are optional for backwards compatibility - no interactive prompts
# They will be set as empty environment variables if not provided

# Verify checksum
if command -v sha256sum >/dev/null; then
  CHECK_CMD="sha256sum"
elif command -v sha256 >/dev/null; then
  # FreeBSD uses 'sha256' instead of 'sha256sum', with different output format
  CHECK_CMD="sha256 -q"
else
  echo "No SHA256 checksum utility found"
  exit 1
fi

# Create a dedicated user for the service if it doesn't exist
AGENT_USER="watchdog"
echo "Configuring the dedicated user for the Watchdog Agent service..."
if is_alpine; then
  if ! id -u watchdog >/dev/null 2>&1; then
    addgroup watchdog
    adduser -S -D -H -s /sbin/nologin -G watchdog watchdog
  fi
  # Add the user to the docker group to allow access to the Docker socket if group docker exists
  if getent group docker >/dev/null 2>&1; then
    echo "Adding watchdog to docker group"
    addgroup watchdog docker
  fi
  
elif is_openwrt; then
  # Create watchdog group first if it doesn't exist (check /etc/group directly)
  if ! grep -q "^watchdog:" /etc/group >/dev/null 2>&1; then
    echo "watchdog:x:999:" >> /etc/group
  fi
  
  # Create watchdog user if it doesn't exist (double-check to prevent duplicates)
  if ! id -u watchdog >/dev/null 2>&1 && ! grep -q "^watchdog:" /etc/passwd >/dev/null 2>&1; then
    echo "watchdog:x:999:999::/nonexistent:/bin/false" >> /etc/passwd
  fi
  
  # Add the user to the docker group if docker group exists and user is not already in it
  if grep -q "^docker:" /etc/group >/dev/null 2>&1; then
    echo "Adding watchdog to docker group"
    # Check if watchdog is already in docker group
    if ! grep "^docker:" /etc/group | grep -q "watchdog"; then
      # Add watchdog to docker group by modifying /etc/group
      # Handle both cases: group with existing members and group without members
      if grep "^docker:" /etc/group | grep -q ":.*:.*$"; then
        # Group has existing members, append with comma
        sed -i 's/^docker:\([^:]*:[^:]*:\)\(.*\)$/docker:\1\2,watchdog/' /etc/group
      else
        # Group has no members, just append
        sed -i 's/^docker:\([^:]*:[^:]*:\)$/docker:\1watchdog/' /etc/group
      fi
    fi
  fi

elif is_freebsd; then
  if is_opnsense; then
    echo "OPNsense detected: skipping user creation (using daemon user instead)"
    AGENT_USER="daemon"
  else
    if ! id -u watchdog >/dev/null 2>&1; then
      pw user add watchdog -d /nonexistent -s /usr/sbin/nologin -c "watchdog user"
    fi
    # Add the user to the wheel group to allow self-updates
    if pw group show wheel >/dev/null 2>&1; then
      echo "Adding watchdog to wheel group for self-updates"
      pw group mod wheel -m watchdog
    fi
  fi

else
  if ! id -u watchdog >/dev/null 2>&1; then
    useradd --system --home-dir /nonexistent --shell /bin/false watchdog
  fi
  # Add the user to the docker group to allow access to the Docker socket if group docker exists
  if getent group docker >/dev/null 2>&1; then
    echo "Adding watchdog to docker group"
    usermod -aG docker watchdog
  fi
  # Add the user to the disk group to allow access to disk devices if group disk exists
  if getent group disk >/dev/null 2>&1; then
    echo "Adding watchdog to disk group"
    usermod -aG disk watchdog
  fi
fi

# Create the directory for the Watchdog Agent

if [ ! -d "$AGENT_DIR" ]; then
  echo "Creating the directory for the Watchdog Agent..."
  mkdir -p "$AGENT_DIR"
  chown "${AGENT_USER}:${AGENT_USER}" "$AGENT_DIR"
  chmod 755 "$AGENT_DIR"
fi

if [ ! -d "$BIN_DIR" ]; then
  mkdir -p "$BIN_DIR"
fi

# Download and install the Watchdog Agent

OS=$(uname -s | sed -e 'y/ABCDEFGHIJKLMNOPQRSTUVWXYZ/abcdefghijklmnopqrstuvwxyz/')
ARCH=$(detect_architecture)
FILE_NAME="watchdog-agent_${OS}_${ARCH}.tar.gz"
if [ "$OS" = "linux" ] && [ "$ARCH" = "amd64" ] && is_glibc; then
  FILE_NAME="watchdog-agent_${OS}_${ARCH}_glibc.tar.gz"
fi

# Determine version to install
if [ "$VERSION" = "latest" ]; then
  API_RELEASE_URL="https://api.github.com/repos/cloudcache/watchdog/releases/latest"
  INSTALL_VERSION=$(curl -fsSL "$API_RELEASE_URL" | grep -o '"tag_name": "v[^"]*"' | cut -d'"' -f4 | tr -d 'v')
  if [ -z "$INSTALL_VERSION" ]; then
    echo "Failed to get latest version"
    exit 1
  fi
else
  INSTALL_VERSION="$VERSION"
  # Remove 'v' prefix if present
  INSTALL_VERSION=$(echo "$INSTALL_VERSION" | sed 's/^v//')
fi

echo "Downloading watchdog-agent v${INSTALL_VERSION}..."

# Download checksums file
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR" || exit 1
CHECKSUM=$(curl -fsSL "$GITHUB_URL/cloudcache/watchdog/releases/download/v${INSTALL_VERSION}/watchdog_${INSTALL_VERSION}_checksums.txt" | grep "$FILE_NAME" | cut -d' ' -f1)
if [ -z "$CHECKSUM" ] || ! echo "$CHECKSUM" | grep -qE "^[a-fA-F0-9]{64}$"; then
  echo "Failed to get checksum or invalid checksum format"
  echo "Try again with --mirror (or --mirror <url>) if GitHub is not reachable."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if ! curl -fL# --retry 3 --retry-delay 2 --connect-timeout 10 "$GITHUB_URL/cloudcache/watchdog/releases/download/v${INSTALL_VERSION}/$FILE_NAME" -o "$FILE_NAME"; then
  echo "Failed to download the agent from $GITHUB_URL/cloudcache/watchdog/releases/download/v${INSTALL_VERSION}/$FILE_NAME"
  echo "Try again with --mirror (or --mirror <url>) if GitHub is not reachable."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if ! tar -tzf "$FILE_NAME" >/dev/null 2>&1; then
  echo "Downloaded archive is invalid or incomplete (possible network/proxy issue)."
  echo "Try again with --mirror (or --mirror <url>) if the download path is unstable."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if [ "$($CHECK_CMD "$FILE_NAME" | cut -d' ' -f1)" != "$CHECKSUM" ]; then
  echo "Checksum verification failed: $($CHECK_CMD "$FILE_NAME" | cut -d' ' -f1) & $CHECKSUM"
  rm -rf "$TEMP_DIR"
  exit 1
fi

if ! tar -xzf "$FILE_NAME" watchdog-agent; then
  echo "Failed to extract the agent"
  rm -rf "$TEMP_DIR"
  exit 1
fi

if [ ! -s "$TEMP_DIR/watchdog-agent" ]; then
  echo "Downloaded binary is missing or empty."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if [ -f "$BIN_PATH" ]; then
  echo "Backing up existing binary..."
  cp "$BIN_PATH" "$BIN_PATH.bak"
fi

mv watchdog-agent "$BIN_PATH"
chown watchdog:watchdog "$BIN_PATH"
chmod 755 "$BIN_PATH"

# Set SELinux context if needed
set_selinux_context

# Cleanup
rm -rf "$TEMP_DIR"

# Make sure /etc/machine-id exists for persistent fingerprint
if [ ! -f /etc/machine-id ]; then
  cat /proc/sys/kernel/random/uuid | tr -d '-' > /etc/machine-id
fi

# Check for NVIDIA GPUs and grant device permissions for systemd service
detect_nvidia_devices() {
  local devices=""
  for i in /dev/nvidia*; do
    if [ -e "$i" ]; then
      devices="${devices}DeviceAllow=$i rw\n"
    fi
  done
  echo "$devices"
}

# Modify service installation part, add Alpine check before systemd service creation
if is_alpine; then
  if [ ! -f /etc/init.d/watchdog-agent ]; then
    echo "Creating OpenRC service for Alpine Linux..."
    cat >/etc/init.d/watchdog-agent <<EOF
#!/sbin/openrc-run

name="watchdog-agent"
description="Watchdog Agent Service"
command="$BIN_PATH"
command_user="watchdog"
command_background="yes"
pidfile="/run/\${RC_SVCNAME}.pid"
output_log="/var/log/watchdog-agent.log"
error_log="/var/log/watchdog-agent.err"

start_pre() {
    checkpath -f -m 0644 -o watchdog:watchdog "\$output_log" "\$error_log"
}

export PORT="$PORT"
export KEY="$KEY"
export TOKEN="$TOKEN"
export HUB_URL="$HUB_URL"

depend() {
    need net
    after firewall
}
EOF
    chmod +x /etc/init.d/watchdog-agent
    rc-update add watchdog-agent default
  else
    echo "Alpine OpenRC service file already exists. Skipping creation."
  fi

  # Create log files with proper permissions
  touch /var/log/watchdog-agent.log /var/log/watchdog-agent.err
  chown watchdog:watchdog /var/log/watchdog-agent.log /var/log/watchdog-agent.err

  # Start the service
  rc-service watchdog-agent restart

  # Check if service started successfully
  sleep 2
  if ! rc-service watchdog-agent status | grep -q "started"; then
    echo "Error: The Watchdog Agent service failed to start. Checking logs..."
    tail -n 20 /var/log/watchdog-agent.err
    exit 1
  fi

  # Auto-update service for Alpine
  if [ "$AUTO_UPDATE_FLAG" = "true" ]; then
    AUTO_UPDATE="y"
  elif [ "$AUTO_UPDATE_FLAG" = "false" ]; then
    AUTO_UPDATE="n"
  else
    printf "\nEnable automatic daily updates for watchdog-agent? (y/n): "
    read AUTO_UPDATE
  fi
  case "$AUTO_UPDATE" in
  [Yy]*)
    echo "Setting up daily automatic updates for watchdog-agent..."

    # Create cron job to run watchdog-agent update command daily at midnight
    if ! crontab -u root -l 2>/dev/null | grep -q "watchdog-agent.*update"; then
      (crontab -u root -l 2>/dev/null; echo "12 0 * * * $BIN_PATH update >/dev/null 2>&1") | crontab -u root -
    fi

    printf "\nDaily updates have been enabled via cron job.\n"
    ;;
  esac

  # Check service status
  if ! rc-service watchdog-agent status >/dev/null 2>&1; then
    echo "Error: The Watchdog Agent service is not running."
    rc-service watchdog-agent status
    exit 1
  fi

elif is_openwrt; then
  if [ ! -f /etc/init.d/watchdog-agent ]; then
    echo "Creating procd init script service for OpenWRT..."
    cat >/etc/init.d/watchdog-agent <<EOF
#!/bin/sh /etc/rc.common

USE_PROCD=1
START=99

start_service() {
    procd_open_instance
    procd_set_param command $BIN_PATH
    procd_set_param user watchdog
    procd_set_param pidfile /var/run/watchdog-agent.pid
    procd_set_param env PORT="$PORT" KEY="$KEY" TOKEN="$TOKEN" HUB_URL="$HUB_URL"
    procd_set_param respawn
    procd_set_param stdout 1
    procd_set_param stderr 1
    procd_close_instance
}

# Extra command to trigger agent update
EXTRA_COMMANDS="update restart"
EXTRA_HELP="        update          Update the Watchdog agent
        restart         Restart the Watchdog agent"

update() {
    $BIN_PATH update
}

EOF
    # Enable the service
    chmod +x /etc/init.d/watchdog-agent
    /etc/init.d/watchdog-agent enable
  else
    echo "OpenWRT init script already exists. Skipping creation."
  fi

  # Start the service
  /etc/init.d/watchdog-agent restart

  # Auto-update service for OpenWRT using a crontab job
  if [ "$AUTO_UPDATE_FLAG" = "true" ]; then
    AUTO_UPDATE="y"
    sleep 1 # give time for the service to start
  elif [ "$AUTO_UPDATE_FLAG" = "false" ]; then
    AUTO_UPDATE="n"
    sleep 1 # give time for the service to start
  else
    printf "\nEnable automatic daily updates for watchdog-agent? (y/n): "
    read AUTO_UPDATE
  fi
  case "$AUTO_UPDATE" in
  [Yy]*)
    echo "Setting up daily automatic updates for watchdog-agent..."

    if ! crontab -u root -l 2>/dev/null | grep -q "watchdog-agent.*update"; then
      (crontab -u root -l 2>/dev/null; echo "12 0 * * * /etc/init.d/watchdog-agent update") | crontab -u root -
    fi

    /etc/init.d/cron restart

    printf "\nDaily updates have been enabled.\n"
    ;;
  esac

  # Check service status
  if ! /etc/init.d/watchdog-agent running >/dev/null 2>&1; then
    echo "Error: The Watchdog Agent service is not running."
    /etc/init.d/watchdog-agent status
    exit 1
  fi

elif is_freebsd; then
  echo "Checking for existing FreeBSD service configuration..."
  # Ensure rc.d directory exists on minimal FreeBSD installs
  mkdir -p /usr/local/etc/rc.d
  
  # Create environment configuration file with proper permissions if it doesn't exist
  if [ ! -f "$AGENT_DIR/env" ]; then
    echo "Creating environment configuration file..."
    cat >"$AGENT_DIR/env" <<EOF
LISTEN=$PORT
KEY="$KEY"
TOKEN=$TOKEN
HUB_URL=$HUB_URL
EOF
    chmod 640 "$AGENT_DIR/env"
    chown "root:${AGENT_USER}" "$AGENT_DIR/env"
  else
    echo "FreeBSD environment file already exists. Skipping creation."
  fi
  
  # Create the rc service file if it doesn't exist
  if [ ! -f /usr/local/etc/rc.d/watchdog-agent ]; then
    echo "Creating FreeBSD rc service..."
    generate_freebsd_rc_service > /usr/local/etc/rc.d/watchdog-agent
    # Set proper permissions for the rc script
    chmod 755 /usr/local/etc/rc.d/watchdog-agent
  else
    echo "FreeBSD rc service file already exists. Skipping creation."
  fi

  # Enable and start the service
  echo "Enabling and starting the agent service..."
  sysrc watchdog_agent_enable="YES"
  sysrc watchdog_agent_user="${AGENT_USER}"
  service watchdog-agent restart
  
  # Check if service started successfully
  sleep 2
  if ! service watchdog-agent status | grep -q "is running"; then
    echo "Error: The Watchdog Agent service failed to start. Checking logs..."
    tail -n 20 /var/log/watchdog_agent.log
    exit 1
  fi

  # Auto-update service for FreeBSD
  if [ "$AUTO_UPDATE_FLAG" = "true" ]; then
    AUTO_UPDATE="y"
  elif [ "$AUTO_UPDATE_FLAG" = "false" ]; then
    AUTO_UPDATE="n"
  else
    printf "\nEnable automatic daily updates for watchdog-agent? (y/n): "
    read AUTO_UPDATE
  fi
  case "$AUTO_UPDATE" in
  [Yy]*)
    echo "Setting up daily automatic updates for watchdog-agent..."

    # Create cron job in /etc/cron.d 
    cat >/etc/cron.d/watchdog-agent <<EOF
# Watchdog Agent daily update job
12 0 * * * root $BIN_PATH update >/dev/null 2>&1
EOF
    chmod 644 /etc/cron.d/watchdog-agent
    printf "\nDaily updates have been enabled via /etc/cron.d.\n"
    ;;
  esac

  # Check service status
  if ! service watchdog-agent status >/dev/null 2>&1; then
    echo "Error: The Watchdog Agent service is not running."
    service watchdog-agent status
    exit 1
  fi

else
  # Original systemd service installation code
  if [ ! -f /etc/systemd/system/watchdog-agent.service ]; then
    echo "Creating the systemd service for the agent..."

    # Detect NVIDIA devices and grant device permissions
    NVIDIA_DEVICES=$(detect_nvidia_devices)

    cat >/etc/systemd/system/watchdog-agent.service <<EOF
[Unit]
Description=Watchdog Agent Service
Wants=network-online.target
After=network-online.target

[Service]
Environment="PORT=$PORT"
Environment="KEY=$KEY"
Environment="TOKEN=$TOKEN"
Environment="HUB_URL=$HUB_URL"
# Environment="EXTRA_FILESYSTEMS=sdb"
ExecStart=$BIN_PATH
User=watchdog
Restart=on-failure
RestartSec=5
StateDirectory=watchdog-agent

# Security/sandboxing settings
KeyringMode=private
LockPersonality=yes
ProtectClock=yes
ProtectHome=read-only
ProtectHostname=yes
ProtectKernelLogs=yes
ProtectSystem=strict
RemoveIPC=yes
RestrictSUIDSGID=true

$(if [ -n "$NVIDIA_DEVICES" ]; then printf "%b" "# NVIDIA device permissions\n${NVIDIA_DEVICES}"; fi)

[Install]
WantedBy=multi-user.target
EOF
  else
    echo "Systemd service file already exists. Skipping creation."
  fi

  # Load and start the service
  printf "\nLoading and starting the agent service...\n"
  systemctl daemon-reload
  systemctl enable watchdog-agent.service >/dev/null 2>&1
  systemctl restart watchdog-agent.service



  # Prompt for auto-update setup
  if [ "$AUTO_UPDATE_FLAG" = "true" ]; then
    AUTO_UPDATE="y"
    sleep 1 # give time for the service to start
  elif [ "$AUTO_UPDATE_FLAG" = "false" ]; then
    AUTO_UPDATE="n"
    sleep 1 # give time for the service to start
  else
    printf "\nEnable automatic daily updates for watchdog-agent? (y/n): "
    read AUTO_UPDATE
  fi
  case "$AUTO_UPDATE" in
  [Yy]*)
    echo "Setting up daily automatic updates for watchdog-agent..."

    # Create systemd service for the daily update
    cat >/etc/systemd/system/watchdog-agent-update.service <<EOF
[Unit]
Description=Update watchdog-agent if needed
Wants=watchdog-agent.service

[Service]
Type=oneshot
ExecStart=$BIN_PATH update
EOF

    # Create systemd timer for the daily update
    cat >/etc/systemd/system/watchdog-agent-update.timer <<EOF
[Unit]
Description=Run watchdog-agent update daily

[Timer]
OnCalendar=daily
Persistent=true
RandomizedDelaySec=4h

[Install]
WantedBy=timers.target
EOF

    systemctl daemon-reload
    systemctl enable --now watchdog-agent-update.timer >/dev/null 2>&1

    printf "\nDaily updates have been enabled.\n"
    ;;
  esac

  # Wait for the service to start or fail
  if [ "$(systemctl is-active watchdog-agent.service)" != "active" ]; then
    echo "Error: The Watchdog Agent service is not running."
    echo "$(systemctl status watchdog-agent.service)"
    exit 1
  fi
fi

printf "\n\033[32mWatchdog Agent has been installed successfully! It is now running on $PORT.\033[0m\n"
