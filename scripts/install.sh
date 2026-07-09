#!/bin/bash

# Orris Forward Agent Installer (multi-instance)
# Usage:
#   Install:           curl -fsSL URL | sudo bash -s -- -s URL -t TOKEN [-n INSTANCE]
#   Uninstall one:     curl -fsSL URL | sudo bash -s -- uninstall [-n INSTANCE]
#   Uninstall all:     curl -fsSL URL | sudo bash -s -- uninstall --all
#   List instances:    curl -fsSL URL | sudo bash -s -- list

set -e

# Static configuration
BINARY_NAME="orris-client"
BASE_SERVICE_NAME="orris-forward-agent"
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/orris"
DOWNLOAD_URL="${DOWNLOAD_URL:-https://github.com/ayakolin/orris-client/releases/latest/download}"
DOWNLOAD_TIMEOUT=120
CONNECT_TIMEOUT=10
MAX_RETRIES=3

# Instance-specific (populated by derive_paths after INSTANCE is known)
INSTANCE=""
SERVICE_NAME=""
CONFIG_FILE=""
LOG_FILE=""
SYSTEMD_SERVICE_FILE=""
OPENRC_SERVICE_FILE=""
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
    echo "Orris Forward Agent Installer (multi-instance)"
    echo ""
    echo "Usage:"
    echo "  Install:"
    echo "    sudo bash $0 -s <url> -t <token> [-n <instance>] [-W <port>] [-T <port>] [-l <level>] [--version <ver>]"
    echo ""
    echo "  Uninstall a single instance:"
    echo "    sudo bash $0 uninstall [-n <instance>]"
    echo ""
    echo "  Uninstall every instance and remove the binary:"
    echo "    sudo bash $0 uninstall --all"
    echo ""
    echo "  List installed instances:"
    echo "    sudo bash $0 list"
    echo ""
    echo "Options:"
    echo "  -s, --server URL      Server URL (required for install)"
    echo "  -t, --token TOKEN     Agent token (required for install)"
    echo "  -n, --name NAME       Instance name (default: 'default'; allowed: [a-zA-Z0-9_-], max 32 chars)"
    echo "  -W, --ws-port PORT    WebSocket listen port (0 = random; required to differ per instance if not 0)"
    echo "  -T, --tls-port PORT   TLS listen port (0 = random; required to differ per instance if not 0)"
    echo "  -l, --loglevel LEVEL  Log level (debug, info, warn, error)"
    echo "      --version VER     Specific binary version (default: latest)"
    echo "      --all             (uninstall only) remove every instance and the shared binary"
    echo "  -h, --help            Show this help"
    echo ""
    echo "Notes:"
    echo "  - All instances share the binary at ${INSTALL_DIR}/${BINARY_NAME}."
    echo "  - The 'default' instance keeps the legacy paths (${BASE_SERVICE_NAME}.service,"
    echo "    ${CONFIG_DIR}/client.env) for backward compatibility."
    echo "  - Other instances use suffixed names: ${BASE_SERVICE_NAME}-<NAME>.service, ${CONFIG_DIR}/client-<NAME>.env."
    echo "  - When running multiple instances on one host, set distinct -W/-T ports (or 0 for random) to avoid conflicts."
    echo ""
    echo "Examples:"
    echo "  sudo bash $0 -s https://api.example.com -t fwd_xxx"
    echo "  sudo bash $0 -s https://api.example.com -t fwd_yyy -n agent-b -W 0 -T 0"
}

# Validate instance name (whitelisted characters, prevents path injection)
validate_instance_name() {
    local name="$1"
    if ! echo "$name" | grep -qE '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$'; then
        print_error "Invalid instance name: '$name'"
        print_error "Must start with alphanumeric, contain only [a-zA-Z0-9_-], and be at most 32 chars"
        exit 1
    fi
}

# Derive all instance-specific paths from INSTANCE.
# The 'default' instance keeps legacy filenames to preserve backward compatibility
# with hosts already running the single-instance installer.
derive_paths() {
    if [ -z "$INSTANCE" ] || [ "$INSTANCE" = "default" ]; then
        SERVICE_NAME="${BASE_SERVICE_NAME}"
        CONFIG_FILE="${CONFIG_DIR}/client.env"
    else
        SERVICE_NAME="${BASE_SERVICE_NAME}-${INSTANCE}"
        CONFIG_FILE="${CONFIG_DIR}/client-${INSTANCE}.env"
    fi
    LOG_FILE="/var/log/${SERVICE_NAME}.log"
    SYSTEMD_SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
    OPENRC_SERVICE_FILE="/etc/init.d/${SERVICE_NAME}"
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
    local temp_file="/tmp/${binary_filename}.$$"

    if [ "$version" = "latest" ]; then
        download_url="${DOWNLOAD_URL}/${binary_filename}"
    else
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

    print_info "Creating configuration file at $CONFIG_FILE ..."
    cat > "$CONFIG_FILE" <<EOF
# Orris Client Configuration (instance: ${INSTANCE:-default})
ORRIS_SERVER_URL=${server}
ORRIS_TOKEN=${token}
EOF

    [ -n "$ws_port" ]  && echo "ORRIS_WS_LISTEN_PORT=${ws_port}"  >> "$CONFIG_FILE"
    [ -n "$tls_port" ] && echo "ORRIS_TLS_LISTEN_PORT=${tls_port}" >> "$CONFIG_FILE"
    [ -n "$loglevel" ] && echo "ORRIS_LOG_LEVEL=${loglevel}"       >> "$CONFIG_FILE"

    chmod 600 "$CONFIG_FILE"
}

# Create systemd service file
create_systemd_service() {
    print_info "Creating systemd service ${SERVICE_NAME}..."

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
Description=Orris Forward Agent (${INSTANCE:-default})
Documentation=https://github.com/ayakolin/orris-client
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
}

# Create OpenRC service file
create_openrc_service() {
    print_info "Creating OpenRC service ${SERVICE_NAME}..."

    cat > "$OPENRC_SERVICE_FILE" <<EOF
#!/sbin/openrc-run
# Orris Forward Agent OpenRC init script (instance: ${INSTANCE:-default})

name="Orris Forward Agent (${INSTANCE:-default})"
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
    print_info "Enabling and starting ${SERVICE_NAME}..."
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

# Stop service if running (scoped to current SERVICE_NAME, never touches siblings)
stop_service() {
    case "$INIT_SYSTEM" in
        systemd)
            if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
                print_info "Stopping ${SERVICE_NAME}..."
                systemctl stop "${SERVICE_NAME}" || true
            fi
            ;;
        openrc)
            if rc-service "${SERVICE_NAME}" status >/dev/null 2>&1; then
                print_info "Stopping ${SERVICE_NAME}..."
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

# Enumerate installed instances by scanning service files.
# Prints one instance name per line.
list_installed_instances() {
    case "$INIT_SYSTEM" in
        systemd)
            for f in "/etc/systemd/system/${BASE_SERVICE_NAME}.service" \
                     "/etc/systemd/system/${BASE_SERVICE_NAME}-"*.service; do
                [ -f "$f" ] || continue
                local base
                base=$(basename "$f" .service)
                if [ "$base" = "$BASE_SERVICE_NAME" ]; then
                    echo "default"
                else
                    echo "${base#${BASE_SERVICE_NAME}-}"
                fi
            done
            ;;
        openrc)
            for f in "/etc/init.d/${BASE_SERVICE_NAME}" \
                     "/etc/init.d/${BASE_SERVICE_NAME}-"*; do
                [ -f "$f" ] || continue
                local base
                base=$(basename "$f")
                if [ "$base" = "$BASE_SERVICE_NAME" ]; then
                    echo "default"
                else
                    echo "${base#${BASE_SERVICE_NAME}-}"
                fi
            done
            ;;
    esac
}

# Install function (single instance)
install() {
    local server="$1"
    local token="$2"
    local version="$3"
    local ws_port="$4"
    local tls_port="$5"
    local loglevel="$6"

    print_info "Installing Orris Forward Agent (instance: ${INSTANCE:-default})..."

    detect_platform
    detect_init_system
    derive_paths

    # Warn if other instances exist and the user did not pick distinct ports
    local other_count=0
    while IFS= read -r existing; do
        [ -z "$existing" ] && continue
        if [ "$existing" != "${INSTANCE:-default}" ]; then
            other_count=$((other_count + 1))
        fi
    done <<< "$(list_installed_instances)"

    if [ "$other_count" -gt 0 ]; then
        if [ -n "$ws_port" ] && [ "$ws_port" != "0" ]; then
            print_warn "Other instances are present. Ensure WS port $ws_port does not collide with them."
        fi
        if [ -n "$tls_port" ] && [ "$tls_port" != "0" ]; then
            print_warn "Other instances are present. Ensure TLS port $tls_port does not collide with them."
        fi
        if [ -z "$ws_port" ] && [ -z "$tls_port" ]; then
            print_warn "Other instances are present and no -W/-T given. Consider -W 0 -T 0 to avoid port conflicts."
        fi
    fi

    # Stop only the current instance's service, so siblings keep running
    stop_service

    # Backup existing binary if present (binary is shared across all instances)
    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        print_info "Backing up existing binary..."
        cp "${INSTALL_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}.bak"
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
        print_info "Instance '${INSTANCE:-default}' installed and running."
    else
        print_warn "Service may not be running properly. Check logs."
    fi

    # Show installed binary version
    local installed_version
    if installed_version=$("${INSTALL_DIR}/${BINARY_NAME}" --version 2>/dev/null); then
        print_info "Version: $installed_version"
    fi

    # If other instances exist and the binary was just upgraded, they will pick up
    # the new binary on their next restart. Surface this so the user is not surprised.
    if [ "$other_count" -gt 0 ]; then
        print_warn "Binary at ${INSTALL_DIR}/${BINARY_NAME} is shared. Other instances will use the new binary after they restart."
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

# Remove a single instance's service unit, config, and log (does NOT touch the shared binary)
uninstall_one_resources() {
    case "$INIT_SYSTEM" in
        systemd)
            if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
                print_info "Stopping ${SERVICE_NAME}..."
                systemctl stop "${SERVICE_NAME}" || true
                sleep 2
            fi
            if systemctl is-enabled --quiet "${SERVICE_NAME}" 2>/dev/null; then
                print_info "Disabling ${SERVICE_NAME}..."
                systemctl disable "${SERVICE_NAME}" || true
            fi
            if [ -f "$SYSTEMD_SERVICE_FILE" ]; then
                print_info "Removing service file ${SYSTEMD_SERVICE_FILE}..."
                rm -f "$SYSTEMD_SERVICE_FILE"
                systemctl daemon-reload || true
            fi
            ;;
        openrc)
            if rc-service "${SERVICE_NAME}" status >/dev/null 2>&1; then
                print_info "Stopping ${SERVICE_NAME}..."
                rc-service "${SERVICE_NAME}" stop || true
                sleep 2
            fi
            if rc-update show default 2>/dev/null | grep -q "${SERVICE_NAME}"; then
                print_info "Disabling ${SERVICE_NAME}..."
                rc-update del "${SERVICE_NAME}" default || true
            fi
            if [ -f "$OPENRC_SERVICE_FILE" ]; then
                print_info "Removing service file ${OPENRC_SERVICE_FILE}..."
                rm -f "$OPENRC_SERVICE_FILE"
            fi
            ;;
    esac

    if [ -f "$CONFIG_FILE" ]; then
        print_info "Removing config ${CONFIG_FILE}..."
        rm -f "$CONFIG_FILE"
    fi
    # ".rules_cache.json" must stay in sync with cacheSuffix in internal/rulecache/rulecache.go
    if [ -f "${CONFIG_FILE}.rules_cache.json" ]; then
        print_info "Removing rule cache ${CONFIG_FILE}.rules_cache.json..."
        rm -f "${CONFIG_FILE}.rules_cache.json"
    fi
    if [ -f "$LOG_FILE" ]; then
        print_info "Removing log ${LOG_FILE}..."
        rm -f "$LOG_FILE"
    fi
}

# Uninstall a single instance (binary is preserved because other instances may use it)
uninstall_one() {
    detect_init_system
    derive_paths

    local exists=0
    [ -f "$SYSTEMD_SERVICE_FILE" ] && exists=1
    [ -f "$OPENRC_SERVICE_FILE" ] && exists=1
    [ -f "$CONFIG_FILE" ] && exists=1

    if [ $exists -eq 0 ]; then
        print_warn "Instance '${INSTANCE:-default}' not found (no service or config file). Nothing to do."
        exit 0
    fi

    print_info "Uninstalling instance: ${INSTANCE:-default}"
    uninstall_one_resources

    # Report remaining instances so the operator knows the binary is intentionally kept
    local remaining=0
    while IFS= read -r inst; do
        [ -z "$inst" ] && continue
        remaining=$((remaining + 1))
    done <<< "$(list_installed_instances)"

    if [ "$remaining" -gt 0 ]; then
        print_info "Instance removed. $remaining other instance(s) still present; binary at ${INSTALL_DIR}/${BINARY_NAME} is preserved."
    else
        print_info "Instance removed. No other instances present; use 'uninstall --all' to also remove the binary."
    fi
}

# Uninstall every instance and remove the shared binary
uninstall_all() {
    detect_init_system

    local instances=()
    while IFS= read -r inst; do
        [ -z "$inst" ] && continue
        instances+=("$inst")
    done <<< "$(list_installed_instances)"

    if [ ${#instances[@]} -eq 0 ]; then
        print_info "No instances installed."
    else
        print_info "Removing ${#instances[@]} instance(s): ${instances[*]}"
        for inst in "${instances[@]}"; do
            INSTANCE="$inst"
            derive_paths
            print_info "--- Removing instance: $inst ---"
            uninstall_one_resources
        done
    fi

    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        print_info "Removing binary ${INSTALL_DIR}/${BINARY_NAME}..."
        rm -f "${INSTALL_DIR}/${BINARY_NAME}"
    fi
    rm -f "${INSTALL_DIR}/${BINARY_NAME}.bak"

    if [ -d "$CONFIG_DIR" ]; then
        if [ -z "$(ls -A "$CONFIG_DIR" 2>/dev/null)" ]; then
            rmdir "$CONFIG_DIR"
            print_info "Removed empty config directory $CONFIG_DIR"
        else
            print_warn "Config directory $CONFIG_DIR is not empty, leaving it in place"
        fi
    fi

    print_info "Uninstallation completed successfully!"
}

# List installed instances with their status
list_instances() {
    detect_init_system

    local instances=()
    while IFS= read -r inst; do
        [ -z "$inst" ] && continue
        instances+=("$inst")
    done <<< "$(list_installed_instances)"

    if [ ${#instances[@]} -eq 0 ]; then
        echo "No Orris Forward Agent instances installed."
        return 0
    fi

    printf "%-20s %-10s %s\n" "INSTANCE" "STATUS" "CONFIG"
    printf "%-20s %-10s %s\n" "--------" "------" "------"
    for inst in "${instances[@]}"; do
        INSTANCE="$inst"
        derive_paths
        local status="inactive"
        if is_service_active; then
            status="active"
        fi
        printf "%-20s %-10s %s\n" "$inst" "$status" "$CONFIG_FILE"
    done
}

# Main
main() {
    if [ $# -eq 0 ]; then
        usage
        exit 1
    fi

    # Handle subcommands and help first
    case "$1" in
        uninstall)
            shift
            check_root

            local uninstall_all_flag=0
            local instance_arg=""

            while [ $# -gt 0 ]; do
                case "$1" in
                    --all)         uninstall_all_flag=1; shift ;;
                    -n|--name)     instance_arg="$2";    shift 2 ;;
                    -h|--help)     usage; exit 0 ;;
                    *)
                        print_error "Unknown option for uninstall: $1"
                        usage
                        exit 1
                        ;;
                esac
            done

            if [ $uninstall_all_flag -eq 1 ]; then
                if [ -n "$instance_arg" ]; then
                    print_error "--all and -n/--name are mutually exclusive"
                    exit 1
                fi
                uninstall_all
            else
                if [ -n "$instance_arg" ]; then
                    validate_instance_name "$instance_arg"
                    INSTANCE="$instance_arg"
                fi
                uninstall_one
            fi
            exit 0
            ;;
        list)
            check_root
            list_instances
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
    local instance_arg=""

    while [ $# -gt 0 ]; do
        case "$1" in
            -s|--server)   server="$2";       shift 2 ;;
            -t|--token)    token="$2";        shift 2 ;;
            -n|--name)     instance_arg="$2"; shift 2 ;;
            -W|--ws-port)  ws_port="$2";      shift 2 ;;
            -T|--tls-port) tls_port="$2";     shift 2 ;;
            -l|--loglevel) loglevel="$2";     shift 2 ;;
            --version)     version="$2";      shift 2 ;;
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

    if [ -n "$instance_arg" ]; then
        validate_instance_name "$instance_arg"
        INSTANCE="$instance_arg"
    fi

    install "$server" "$token" "$version" "$ws_port" "$tls_port" "$loglevel"
}

main "$@"
