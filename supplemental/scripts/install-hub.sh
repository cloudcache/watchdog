#!/bin/sh

is_freebsd() {
  [ "$(uname -s)" = "FreeBSD" ]
}

# Function to ensure the proxy URL ends with a /
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

# PROVIDE: watchdog_hub
# REQUIRE: DAEMON NETWORKING
# BEFORE: LOGIN
# KEYWORD: shutdown

# Add the following lines to /etc/rc.conf to configure Watchdog Hub:
#
# watchdog_hub_enable (bool):   Set to YES to enable Watchdog Hub
#                             Default: YES
# watchdog_hub_port (str):      Port to listen on
#                             Default: 8090
# watchdog_hub_user (str):      Watchdog Hub daemon user
#                             Default: watchdog
# watchdog_hub_bin (str):       Path to the watchdog binary
#                             Default: /usr/local/sbin/watchdog
# watchdog_hub_data (str):      Path to the watchdog data directory
#                             Default: /usr/local/etc/watchdog/watchdog_data
# watchdog_hub_flags (str):     Extra flags passed to watchdog command invocation
#                             Default:

. /etc/rc.subr

name="watchdog_hub"
rcvar=watchdog_hub_enable

load_rc_config $name
: ${watchdog_hub_enable:="YES"}
: ${watchdog_hub_port:="8090"}
: ${watchdog_hub_user:="watchdog"}
: ${watchdog_hub_flags:=""}
: ${watchdog_hub_bin:="/usr/local/sbin/watchdog"}
: ${watchdog_hub_data:="/usr/local/etc/watchdog/watchdog_data"}

logfile="/var/log/${name}.log"
pidfile="/var/run/${name}.pid"

procname="/usr/sbin/daemon"
start_precmd="${name}_prestart"
start_cmd="${name}_start"
stop_cmd="${name}_stop"

extra_commands="upgrade"
upgrade_cmd="watchdog_hub_upgrade"

watchdog_hub_prestart()
{
    if [ ! -d "${watchdog_hub_data}" ]; then
        echo "Creating data directory ${watchdog_hub_data}"
        mkdir -p "${watchdog_hub_data}"
        chown "${watchdog_hub_user}:${watchdog_hub_user}" "${watchdog_hub_data}"
    fi
}

watchdog_hub_start()
{
    echo "Starting ${name}"
    cd "$(dirname "${watchdog_hub_data}")" || exit 1
    /usr/sbin/daemon -f \
            -P "${pidfile}" \
            -o "${logfile}" \
            -u "${watchdog_hub_user}" \
            "${watchdog_hub_bin}" serve --http "0.0.0.0:${watchdog_hub_port}" ${watchdog_hub_flags}
}

watchdog_hub_stop()
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

watchdog_hub_upgrade()
{
    echo "Upgrading ${name}"
    if command -v sudo >/dev/null; then
        sudo -u "${watchdog_hub_user}" -- "${watchdog_hub_bin}" update
    else
        su -m "${watchdog_hub_user}" -c "${watchdog_hub_bin} update"
    fi
}

run_rc_command "$1"
EOF
}

# Detect system architecture
detect_architecture() {
  arch=$(uname -m)
  case "$arch" in
    x86_64)
      arch="amd64"
      ;;
    armv7l)
      arch="arm"
      ;;
    aarch64)
      arch="arm64"
      ;;
  esac
  echo "$arch"
}

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

# Define default values
PORT=8090
GITHUB_URL="https://github.com"
AUTO_UPDATE_FLAG="false"
UNINSTALL=false

# Parse command line arguments
while [ $# -gt 0 ]; do
  case "$1" in
    -u)
      UNINSTALL=true
      shift
      ;;
    -h|--help)
      printf "Watchdog Hub installation script\n\n"
      printf "Usage: ./install-hub.sh [options]\n\n"
      printf "Options: \n"
      printf "  -u           : Uninstall the Watchdog Hub\n"
      printf "  -p <port>    : Specify a port number (default: 8090)\n"
      printf "  -c, --mirror URL : Prefix GitHub downloads with an explicit proxy URL\n"
      printf "  --auto-update : Enable automatic daily updates (disabled by default)\n"
      printf "  -h, --help   : Display this help message\n"
      exit 0
      ;;
    -p)
      shift
      PORT="$1"
      shift
      ;;
    -c | --mirror)
      shift
      if [ -n "$1" ] && ! echo "$1" | grep -q '^-'; then
        GITHUB_URL="$(ensure_trailing_slash "$1")https://github.com"
        shift
      else
        echo "--mirror requires a proxy URL" >&2
        exit 1
      fi
      ;;
    --auto-update)
      AUTO_UPDATE_FLAG="true"
      shift
      ;;
    *)
      echo "Invalid option: $1" >&2
      exit 1
      ;;
  esac
done

# Set paths based on operating system
if is_freebsd; then
  HUB_DIR="/usr/local/etc/watchdog"
  BIN_PATH="/usr/local/sbin/watchdog"
else
  HUB_DIR="/opt/watchdog"
  BIN_PATH="/opt/watchdog/watchdog"
fi

# Uninstall process
if [ "$UNINSTALL" = true ]; then
  if is_freebsd; then
    echo "Stopping and disabling the Watchdog Hub service..."
    service watchdog-hub stop 2>/dev/null
    sysrc watchdog_hub_enable="NO" 2>/dev/null

    echo "Removing the FreeBSD service files..."
    rm -f /usr/local/etc/rc.d/watchdog-hub

    echo "Removing the daily update cron job..."
    rm -f /etc/cron.d/watchdog-hub

    echo "Removing log files..."
    rm -f /var/log/watchdog_hub.log

    echo "Removing the Watchdog Hub binary and data..."
    rm -f "$BIN_PATH"
    rm -rf "$HUB_DIR"

    echo "Removing the dedicated user..."
    pw user del watchdog 2>/dev/null

    echo "The Watchdog Hub has been uninstalled successfully!"
    exit 0
  else
    # Stop and disable the Watchdog Hub service
    echo "Stopping and disabling the Watchdog Hub service..."
    systemctl stop watchdog-hub.service
    systemctl disable watchdog-hub.service

    # Remove the systemd service file
    echo "Removing the systemd service file..."
    rm -f /etc/systemd/system/watchdog-hub.service

    # Remove the update timer and service if they exist
    echo "Removing the daily update service and timer..."
    systemctl stop watchdog-hub-update.timer 2>/dev/null
    systemctl disable watchdog-hub-update.timer 2>/dev/null
    rm -f /etc/systemd/system/watchdog-hub-update.service
    rm -f /etc/systemd/system/watchdog-hub-update.timer

    # Reload the systemd daemon
    echo "Reloading the systemd daemon..."
    systemctl daemon-reload

    # Remove the Watchdog Hub binary and data
    echo "Removing the Watchdog Hub binary and data..."
    rm -rf "$HUB_DIR"

    # Remove the dedicated user
    echo "Removing the dedicated user..."
    userdel watchdog 2>/dev/null

    echo "The Watchdog Hub has been uninstalled successfully!"
    exit 0
  fi
fi

# Function to check if a package is installed
package_installed() {
  command -v "$1" >/dev/null 2>&1
}

# Check for package manager and install necessary packages if not installed
if package_installed pkg && is_freebsd; then
  if ! package_installed tar || ! package_installed curl; then
    pkg update
    pkg install -y gtar curl
  fi
elif package_installed apt-get; then
  if ! package_installed tar || ! package_installed curl; then
    apt-get update
    apt-get install -y tar curl
  fi
elif package_installed yum; then
  if ! package_installed tar || ! package_installed curl; then
    yum install -y tar curl
  fi
elif package_installed pacman; then
  if ! package_installed tar || ! package_installed curl; then
    pacman -Sy --noconfirm tar curl
  fi
else
  echo "Warning: Please ensure 'tar' and 'curl' are installed."
fi

# Create a dedicated user for the service if it doesn't exist
echo "Creating a dedicated user for the Watchdog Hub service..."
if is_freebsd; then
  if ! id -u watchdog >/dev/null 2>&1; then
    pw user add watchdog -d /nonexistent -s /usr/sbin/nologin -c "watchdog user"
  fi
else
  if ! id -u watchdog >/dev/null 2>&1; then
    useradd -M -s /bin/false watchdog
  fi
fi

# Create the directory for the Watchdog Hub
echo "Creating the directory for the Watchdog Hub..."
mkdir -p "$HUB_DIR/watchdog_data"
chown -R watchdog:watchdog "$HUB_DIR"
chmod 755 "$HUB_DIR"

# Download and install the Watchdog Hub
echo "Downloading and installing the Watchdog Hub..."

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(detect_architecture)
FILE_NAME="watchdog_${OS}_${ARCH}.tar.gz"

TEMP_DIR=$(mktemp -d)
ARCHIVE_PATH="$TEMP_DIR/$FILE_NAME"
DOWNLOAD_URL="$GITHUB_URL/cloudcache/watchdog/releases/latest/download/$FILE_NAME"

if ! curl -fL# --retry 3 --retry-delay 2 --connect-timeout 10 "$DOWNLOAD_URL" -o "$ARCHIVE_PATH"; then
  echo "Failed to download the Watchdog Hub from:"
  echo "$DOWNLOAD_URL"
  echo "Try again with --mirror (or --mirror <url>) if GitHub is not reachable."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if ! tar -tzf "$ARCHIVE_PATH" >/dev/null 2>&1; then
  echo "Downloaded archive is invalid or incomplete (possible network/proxy issue)."
  echo "Try again with --mirror (or --mirror <url>) if the download path is unstable."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if ! tar -xzf "$ARCHIVE_PATH" -C "$TEMP_DIR" watchdog; then
  echo "Failed to extract watchdog from archive."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if [ ! -s "$TEMP_DIR/watchdog" ]; then
  echo "Downloaded binary is missing or empty."
  rm -rf "$TEMP_DIR"
  exit 1
fi

chmod +x "$TEMP_DIR/watchdog"
mv "$TEMP_DIR/watchdog" "$BIN_PATH"
chown watchdog:watchdog "$BIN_PATH"
rm -rf "$TEMP_DIR"

if is_freebsd; then
  echo "Creating FreeBSD rc service..."

  # Create the rc service file
  generate_freebsd_rc_service > /usr/local/etc/rc.d/watchdog-hub

  # Set proper permissions for the rc script
  chmod 755 /usr/local/etc/rc.d/watchdog-hub

  # Configure the port
  sysrc watchdog_hub_port="$PORT"

  # Enable and start the service
  echo "Enabling and starting the Watchdog Hub service..."
  sysrc watchdog_hub_enable="YES"
  service watchdog-hub restart

  # Check if service started successfully
  sleep 2
  if ! service watchdog-hub status | grep -q "is running"; then
    echo "Error: The Watchdog Hub service failed to start. Checking logs..."
    tail -n 20 /var/log/watchdog_hub.log
    exit 1
  fi

  # Auto-update service for FreeBSD
  if [ "$AUTO_UPDATE_FLAG" = "true" ]; then
    echo "Setting up daily automatic updates for watchdog-hub..."

    # Create cron job in /etc/cron.d
    cat >/etc/cron.d/watchdog-hub <<EOF
# Watchdog Hub daily update job
12 8 * * * root $BIN_PATH update >/dev/null 2>&1
EOF
    chmod 644 /etc/cron.d/watchdog-hub
    printf "\nDaily updates have been enabled via /etc/cron.d.\n"
  fi

  # Check service status
  if ! service watchdog-hub status >/dev/null 2>&1; then
    echo "Error: The Watchdog Hub service is not running."
    service watchdog-hub status
    exit 1
  fi

else
  # Original systemd service installation code
  printf "Creating the systemd service for the Watchdog Hub...\n"
  cat >/etc/systemd/system/watchdog-hub.service <<EOF
[Unit]
Description=Watchdog Hub Service
After=network.target

[Service]
ExecStart=$BIN_PATH serve --http "0.0.0.0:$PORT"
WorkingDirectory=$HUB_DIR
User=watchdog
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  # Load and start the service
  printf "Loading and starting the Watchdog Hub service...\n"
  systemctl daemon-reload
  systemctl enable --quiet watchdog-hub.service
  systemctl start --quiet watchdog-hub.service

  # Wait for the service to start or fail
  sleep 2

  # Check if the service is running
  if [ "$(systemctl is-active watchdog-hub.service)" != "active" ]; then
    echo "Error: The Watchdog Hub service is not running."
    echo "$(systemctl status watchdog-hub.service)"
    exit 1
  fi

  # Enable auto-update if flag is set to true
  if [ "$AUTO_UPDATE_FLAG" = "true" ]; then
    echo "Setting up daily automatic updates for watchdog-hub..."

    # Create systemd service for the daily update
    cat >/etc/systemd/system/watchdog-hub-update.service <<EOF
[Unit]
Description=Update watchdog-hub if needed
Wants=watchdog-hub.service

[Service]
Type=oneshot
ExecStart=$BIN_PATH update
EOF

    # Create systemd timer for the daily update
    cat >/etc/systemd/system/watchdog-hub-update.timer <<EOF
[Unit]
Description=Run watchdog-hub update daily

[Timer]
OnCalendar=daily
Persistent=true
RandomizedDelaySec=4h

[Install]
WantedBy=timers.target
EOF

    systemctl daemon-reload
    systemctl enable --now watchdog-hub-update.timer

    printf "\nDaily updates have been enabled.\n"
  fi
fi

printf "\n\033[32mWatchdog Hub has been installed successfully! It is now accessible on port $PORT.\033[0m\n"
