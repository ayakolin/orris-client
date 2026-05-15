#!/bin/bash

# Orris Forward Agent Installer
# Usage:
#   Install:   curl -fsSL URL | sudo bash -s -- -s URL -t TOKEN
#   Uninstall: curl -fsSL URL | sudo bash -s -- uninstall

set -e

# Configuration
BINARY_NAME="orris-client"
SERVICE_NAME="orris-forward-agent"
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/orris"
CONFIG_FILE="${CONFIG_DIR}/client.env"
LOG_FILE="/var/log/${SERVICE_NAME}.log"
SYSTEMD_SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
OPENRC_SERVICE_FILE="/etc/init.d/${SERVICE_NAME}"
DOWNLOAD_URL="${DOWNLOAD_URL:-https://github.com/orris-inc/orris-client/releases/latest/download}"
DOWNLOAD_TIMEOUT=120
CONNECT_TIMEOUT=10
MAX_RETRIES=3
INIT_SYSTEM=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Print functions
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
check_root() {
    if [ "$(id -u)" != "0" ]; then
        print_error "This script must be run as root"
        exit 1
    fi
}

# Usage help
usage() {
    echo "Orris Forward Agent Installer"
    echo ""
    echo "Usage:"
    echo "  Install:"
    echo "    sudo bash $0 -s <url> -t <token> [-W <port>] [-T <port>] [-l <level>] [--version <ver>]"
    echo ""
    echo "  Uninstall:"
    echo "    sudo bash $0 uninstall"
    echo ""
    echo "Options:"
    echo "  -s, --server URL      Server URL (required)"
    echo "  -t, --token TOKEN     Agent token (required)"
    echo "  -W, --ws-port PORT    WebSocket listen port (0 = random)"
    echo "  -T, --tls-port PORT   TLS listen port (0 = random)"
    echo "  -l, --loglevel LEVEL  Log level (debug, info, warn, error)"
    echo "      --version VER     Specific version (default: latest)"
    echo "  -h, --help            Show this help"
    echo ""
    echo "Examples:"
    echo "  sudo bash $0 -s https://api.example.com -t fwd_xxx"
    echo "  sudo bash $0 --server https://api.example.com --token fwd_xxx -W 8080"
}

# Detect platform
detect_platform() {
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')
    local arch=$(uname -m)

    if [ "$os" != "linux" ]; then
        print_error "Unsupported operating system: $os. Only Linux is supported."
        exit 1
    fi

    case "$arch" in
        x86_64|amd64)
            ARCH="amd64"
            ;;
        aarch64|arm64)
            ARCH="arm64"
            ;;
        *)
            print_error "Unsupported architecture: $arch. Only amd64 and arm64 are supported."
            exit 1
            ;;
    esac

    OS="$os"
    print_info "Detected platform: ${OS}-${ARCH}"
}

# Detect init system (systemd or openrc)
detect_init_system() {
    if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
        INIT_SYSTEM="systemd"
    elif command -v rc-service >/dev/null 2>&1; then
        INIT_SYSTEM="openrc"
    elif [ -f /etc/alpine-release ]; then
        INIT_SYSTEM="openrc"
    else
        print_error "Unsupported init system. Only systemd and OpenRC are supported."
        exit 1
    fi
    print_info "Detected init system: $INIT_SYSTEM"
}

# Check if mount namespaces are supported (some containers do not allow them)
check_namespace_support() {
    if command -v systemd-detect-virt >/dev/null 2>&1; then
        local virt_type
        virt_type=$(systemd-detect-virt --container 2>/dev/null || true)
        case "$virt_type" in
            openvz|lxc|lxc-libvirt)
                return 1
                ;;
        esac
    fi

    if command -v unshare >/dev/null 2>&1 && unshare --mount true 2>/dev/null; then
        return 0
    fi

    return 1
}

# Download binary with retry
download_binary() {
    local version="$1"
    local binary_filename="${BINARY_NAME}-${OS}-${ARCH}"
    local download_url
    local temp_file="/tmp/${binary_filename}"

    if [ "$version" = "latest" ]; then
        download_url="${DOWNLOAD_URL}/${binary_filename}"
    else
        # Replace "latest/download" with "download/<version>" without bash-specific substitution
        local base_url
        base_url=$(echo "$DOWNLOAD_URL" | sed "s|latest/download|download/${version}|")
        download_url="${base_url}/${binary_filename}"
    fi

    print_info "Downloading ${BINARY_NAME} ${version} for ${OS}-${ARCH}..."

    local attempt=0
    while [ $attempt -lt $MAX_RETRIES ]; do
        attempt=$((attempt + 1))
        print_info "Download attempt $attempt of $MAX_RETRIES..."

        if curl -fL --connect-timeout ${CONNECT_TIMEOUT} --max-time ${DOWNLOAD_TIMEOUT} \
            -o "$temp_file" "$download_url"; then
            chmod +x "$temp_file"
            mv "$temp_file" "${INSTALL_DIR}/${BINARY_NAME}"
            print_info "Binary downloaded successfully"
            return 0
        fi

        print_warn "Download attempt $attempt failed"
        rm -f "$temp_file"
        sleep 2
    done

    print_error "Failed to download binary after $MAX_RETRIES attempts"
    return 1
}

# Create configuration file
create_config() {
    local server="$1"
    local token="$2"
    local ws_port="$3"
    local tls_port="$4"
    local loglevel="$5"

    print_info "Creating configuration directory..."
    mkdir -p "$CONFIG_DIR"

    print_info "Creating configuration file..."
    cat > "$CONFIG_FILE" <<EOF
# Orris Client Configuration
ORRIS_SERVER_URL=${server}
ORRIS_TOKEN=${token}
EOF

    [ -n "$ws_port" ]  && echo "ORRIS_WS_LISTEN_PORT=${ws_port}"  >> "$CONFIG_FILE"
    [ -n "$tls_port" ] && echo "ORRIS_TLS_LISTEN_PORT=${tls_port}" >> "$CONFIG_FILE"
    [ -n "$loglevel" ] && echo "ORRIS_LOG_LEVEL=${loglevel}"       >> "$CONFIG_FILE"

    chmod 600 "$CONFIG_FILE"
    print_info "Configuration file created at $CONFIG_FILE"
}

# Create systemd service file
create_systemd_service() {
    print_info "Creating systemd service..."

    local security_opts=""
    if check_namespace_support; then
        security_opts="# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=${INSTALL_DIR}"
        print_info "Namespace support detected, enabling security hardening"
    else
        security_opts="# Security hardening (limited: mount namespaces not available)
NoNewPrivileges=true"
        print_warn "Mount namespaces not supported, skipping ProtectSystem/ProtectHome/PrivateTmp"
    fi

    cat > "$SYSTEMD_SERVICE_FILE" <<EOF
[Unit]
Description=Orris Forward Agent
Documentation=https://github.com/orris-inc/orris-client
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${CONFIG_FILE}
ExecStart=${INSTALL_DIR}/${BINARY_NAME}
Restart=always
RestartSec=5
StartLimitInterval=60
StartLimitBurst=3

${security_opts}

# Resource limits
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    print_info "Systemd service created"
}

# Create OpenRC service file
create_openrc_service() {
    print_info "Creating OpenRC service..."

    cat > "$OPENRC_SERVICE_FILE" <<EOF
#!/sbin/openrc-run
# Orris Forward Agent OpenRC init script

name="Orris Forward Agent"
description="Orris Forward Agent"

command="${INSTALL_DIR}/${BINARY_NAME}"
command_background=true
pidfile="/run/\${RC_SVCNAME}.pid"
output_log="${LOG_FILE}"
error_log="${LOG_FILE}"

supervisor="supervise-daemon"
respawn_delay=5
respawn_max=3
respawn_period=60

rc_ulimit="-n 65535"

depend() {
    need net
    after firewall
}

start_pre() {
    if [ -f "${CONFIG_FILE}" ]; then
        set -a
        # shellcheck disable=SC1090
        . "${CONFIG_FILE}"
        set +a
    fi
    checkpath --file --mode 0644 "${LOG_FILE}"
}
EOF

    chmod +x "$OPENRC_SERVICE_FILE"
    print_info "OpenRC service created"
}

# Create service (dispatcher)
create_service() {
    case "$INIT_SYSTEM" in
        systemd) create_systemd_service ;;
        openrc)  create_openrc_service ;;
    esac
}

# Start service
start_service() {
    print_info "Enabling and starting service..."
    case "$INIT_SYSTEM" in
        systemd)
            systemctl enable "${SERVICE_NAME}" >/dev/null 2>&1
            systemctl start "${SERVICE_NAME}"
            ;;
        openrc)
            rc-update add "${SERVICE_NAME}" default >/dev/null 2>&1
            rc-service "${SERVICE_NAME}" start
            ;;
    esac
}

# Stop service if running
stop_service() {
    case "$INIT_SYSTEM" in
        systemd)
            if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
                print_info "Stopping service..."
                systemctl stop "${SERVICE_NAME}" || true
            fi
            ;;
        openrc)
            if rc-service "${SERVICE_NAME}" status >/dev/null 2>&1; then
                print_info "Stopping service..."
                rc-service "${SERVICE_NAME}" stop || true
            fi
            ;;
    esac
}

# Check if service is active
is_service_active() {
    case "$INIT_SYSTEM" in
        systemd) systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null ;;
        openrc)  rc-service "${SERVICE_NAME}" status 2>/dev/null | grep -q "started" ;;
    esac
}

# Kill leftover process by name with timeout (defensive cleanup)
kill_process() {
    if ! command -v pgrep >/dev/null 2>&1; then
        return 0
    fi

    if ! pgrep -x "${BINARY_NAME}" >/dev/null 2>&1; then
        return 0
    fi

    print_info "Sending SIGTERM to leftover ${BINARY_NAME} processes..."
    pkill -x "${BINARY_NAME}" 2>/dev/null || true

    local count=0
    while [ $count -lt 10 ]; do
        if ! pgrep -x "${BINARY_NAME}" >/dev/null 2>&1; then
            print_info "Process terminated gracefully"
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done

    if pgrep -x "${BINARY_NAME}" >/dev/null 2>&1; then
        print_warn "Process did not terminate gracefully, sending SIGKILL..."
        pkill -9 -x "${BINARY_NAME}" 2>/dev/null || true
        sleep 1
    fi
}

# Install function
install() {
    local server="$1"
    local token="$2"
    local version="$3"
    local ws_port="$4"
    local tls_port="$5"
    local loglevel="$6"

    print_info "Starting Orris Forward Agent installation..."

    detect_platform
    detect_init_system

    stop_service

    # Backup existing binary if present
    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        print_info "Backing up existing binary..."
        mv "${INSTALL_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}.bak"
    fi

    if ! download_binary "$version"; then
        if [ -f "${INSTALL_DIR}/${BINARY_NAME}.bak" ]; then
            print_warn "Restoring backup binary..."
            mv "${INSTALL_DIR}/${BINARY_NAME}.bak" "${INSTALL_DIR}/${BINARY_NAME}"
        fi
        exit 1
    fi

    rm -f "${INSTALL_DIR}/${BINARY_NAME}.bak"

    create_config "$server" "$token" "$ws_port" "$tls_port" "$loglevel"
    create_service
    start_service

    sleep 2
    if is_service_active; then
        print_info "Installation completed successfully!"
    else
        print_warn "Service may not be running properly. Check logs."
    fi

    # Show installed version
    local installed_version
    if installed_version=$("${INSTALL_DIR}/${BINARY_NAME}" --version 2>/dev/null); then
        print_info "Version: $installed_version"
    fi

    echo ""
    echo "Useful commands:"
    case "$INIT_SYSTEM" in
        systemd)
            echo "  Check status:  systemctl status ${SERVICE_NAME}"
            echo "  View logs:     journalctl -u ${SERVICE_NAME} -f"
            echo "  Restart:       systemctl restart ${SERVICE_NAME}"
            echo "  Stop:          systemctl stop ${SERVICE_NAME}"
            ;;
        openrc)
            echo "  Check status:  rc-service ${SERVICE_NAME} status"
            echo "  View logs:     tail -f ${LOG_FILE}"
            echo "  Restart:       rc-service ${SERVICE_NAME} restart"
            echo "  Stop:          rc-service ${SERVICE_NAME} stop"
            ;;
    esac
    echo "  Edit config:   ${CONFIG_FILE}"
    echo ""
}

# Uninstall function
uninstall() {
    print_info "Starting Orris Forward Agent uninstallation..."

    detect_init_system

    # Stop and disable service based on init system
    case "$INIT_SYSTEM" in
        systemd)
            if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
                print_info "Stopping service..."
                systemctl stop "${SERVICE_NAME}" || true
                sleep 2
            fi
            if systemctl is-enabled --quiet "${SERVICE_NAME}" 2>/dev/null; then
                print_info "Disabling service..."
                systemctl disable "${SERVICE_NAME}" || true
            fi
            ;;
        openrc)
            if rc-service "${SERVICE_NAME}" status >/dev/null 2>&1; then
                print_info "Stopping service..."
                rc-service "${SERVICE_NAME}" stop || true
                sleep 2
            fi
            if rc-update show default 2>/dev/null | grep -q "${SERVICE_NAME}"; then
                print_info "Disabling service..."
                rc-update del "${SERVICE_NAME}" default || true
            fi
            ;;
    esac

    # Defensive cleanup for non-service starts
    kill_process

    # Remove service file
    case "$INIT_SYSTEM" in
        systemd)
            if [ -f "$SYSTEMD_SERVICE_FILE" ]; then
                print_info "Removing service file..."
                rm -f "$SYSTEMD_SERVICE_FILE"
                systemctl daemon-reload
            fi
            ;;
        openrc)
            if [ -f "$OPENRC_SERVICE_FILE" ]; then
                print_info "Removing service file..."
                rm -f "$OPENRC_SERVICE_FILE"
            fi
            ;;
    esac

    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        print_info "Removing binary..."
        rm -f "${INSTALL_DIR}/${BINARY_NAME}"
    fi
    rm -f "${INSTALL_DIR}/${BINARY_NAME}.bak"

    if [ -d "$CONFIG_DIR" ]; then
        print_info "Removing configuration directory..."
        rm -rf "$CONFIG_DIR"
    fi

    if [ -f "$LOG_FILE" ]; then
        print_info "Removing log file..."
        rm -f "$LOG_FILE"
    fi

    print_info "Uninstallation completed successfully!"
}

# Main
main() {
    if [ $# -eq 0 ]; then
        usage
        exit 1
    fi

    # Handle subcommands and help first (uninstall does not require args)
    case "$1" in
        uninstall)
            check_root
            uninstall
            exit 0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
    esac

    check_root

    local server="" token="" version="latest"
    local ws_port="" tls_port="" loglevel=""

    while [ $# -gt 0 ]; do
        case "$1" in
            -s|--server)   server="$2";   shift 2 ;;
            -t|--token)    token="$2";    shift 2 ;;
            -W|--ws-port)  ws_port="$2";  shift 2 ;;
            -T|--tls-port) tls_port="$2"; shift 2 ;;
            -l|--loglevel) loglevel="$2"; shift 2 ;;
            --version)     version="$2";  shift 2 ;;
            -h|--help)     usage; exit 0 ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    if [ -z "$server" ]; then
        print_error "Missing required parameter: -s/--server"
        usage
        exit 1
    fi

    if [ -z "$token" ]; then
        print_error "Missing required parameter: -t/--token"
        usage
        exit 1
    fi

    install "$server" "$token" "$version" "$ws_port" "$tls_port" "$loglevel"
}

main "$@"
