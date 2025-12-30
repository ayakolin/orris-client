#!/bin/bash
set -euo pipefail

# =============================================================================
# Orris Forward Agent Installer
# Usage: curl -fsSL URL | sudo bash -s -- --server URL --token TOKEN
# Uninstall: curl -fsSL URL | sudo bash -s -- uninstall
# =============================================================================

readonly SERVICE="orris-forward-agent"
readonly BINARY="orris-client"
readonly INSTALL_DIR="/usr/local/bin"
readonly CONFIG_DIR="/etc/orris"
readonly CONFIG_FILE="${CONFIG_DIR}/client.env"
readonly SERVICE_FILE="/etc/systemd/system/${SERVICE}.service"
readonly DOWNLOAD_URL="${DOWNLOAD_URL:-https://github.com/orris-inc/orris-client/releases/latest/download}"

info()  { echo -e "\033[32m[INFO]\033[0m $*"; }
warn()  { echo -e "\033[33m[WARN]\033[0m $*"; }
error() { echo -e "\033[31m[ERROR]\033[0m $*" >&2; exit 1; }

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [COMMAND]

Commands:
    uninstall    Uninstall the service

Options:
    --server URL    Server URL (required)
    --token TOKEN   Agent token (required)
    --version VER   Specific version (default: latest)
    -h, --help      Show this help

Examples:
    Install:   $0 --server https://api.example.com --token fwd_xxx
    Uninstall: $0 uninstall
EOF
    exit 0
}

check_root() {
    [[ $EUID -eq 0 ]] || error "Please run as root: sudo bash"
}

detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    case "$ARCH" in
        x86_64|amd64)  ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *) error "Unsupported architecture: $ARCH" ;;
    esac
    [[ "$OS" == "linux" ]] || error "Unsupported OS: $OS"
}

download_binary() {
    local url="$1"
    local output="$2"

    info "Downloading from: $url"
    if ! curl -fsSL \
        --connect-timeout 10 \
        --max-time 120 \
        --retry 3 \
        --retry-delay 2 \
        -o "$output" \
        "$url"; then
        error "Download failed"
    fi
}

uninstall() {
    info "Uninstalling ${SERVICE}..."

    systemctl stop "$SERVICE" 2>/dev/null || true
    systemctl disable "$SERVICE" 2>/dev/null || true
    rm -f "$SERVICE_FILE"
    rm -f "${INSTALL_DIR}/${BINARY}"
    rm -rf "$CONFIG_DIR"
    systemctl daemon-reload 2>/dev/null || true

    info "Uninstalled successfully"
    exit 0
}

create_config() {
    local server="$1"
    local token="$2"

    mkdir -p "$CONFIG_DIR"
    cat > "$CONFIG_FILE" <<EOF
# Orris Client Configuration
ORRIS_SERVER_URL=${server}
ORRIS_TOKEN=${token}
EOF
    chmod 600 "$CONFIG_FILE"
}

create_service() {
    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Orris Forward Agent
Documentation=https://github.com/orris-inc/orris-client
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${CONFIG_FILE}
ExecStart=${INSTALL_DIR}/${BINARY}
Restart=always
RestartSec=5
StartLimitInterval=60
StartLimitBurst=3

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

# Resource limits
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
}

install() {
    local server="$1"
    local token="$2"
    local version="${3:-latest}"

    detect_platform
    info "Installing... (OS: $OS, Arch: $ARCH)"

    # Stop existing service
    if systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
        warn "Stopping existing service..."
        systemctl stop "$SERVICE"
    fi

    # Backup old binary
    [[ -f "${INSTALL_DIR}/${BINARY}" ]] && mv "${INSTALL_DIR}/${BINARY}" "${INSTALL_DIR}/${BINARY}.bak"

    # Download binary
    local binary_url
    if [[ "$version" == "latest" ]]; then
        binary_url="${DOWNLOAD_URL}/${BINARY}-${OS}-${ARCH}"
    else
        binary_url="${DOWNLOAD_URL/latest\/download/download\/${version}}/${BINARY}-${OS}-${ARCH}"
    fi

    if ! download_binary "$binary_url" "${INSTALL_DIR}/${BINARY}"; then
        [[ -f "${INSTALL_DIR}/${BINARY}.bak" ]] && mv "${INSTALL_DIR}/${BINARY}.bak" "${INSTALL_DIR}/${BINARY}"
        error "Installation failed"
    fi

    chmod +x "${INSTALL_DIR}/${BINARY}"
    rm -f "${INSTALL_DIR}/${BINARY}.bak"

    # Create config and service
    create_config "$server" "$token"
    create_service

    # Start service
    systemctl daemon-reload
    systemctl enable "$SERVICE"
    systemctl start "$SERVICE"

    sleep 2
    if systemctl is-active --quiet "$SERVICE"; then
        info "Installed successfully!"
    else
        warn "Service may not be running properly"
    fi

    # Show installed version
    local installed_version
    if installed_version=$("${INSTALL_DIR}/${BINARY}" --version 2>/dev/null); then
        info "Version: $installed_version"
    fi

    echo
    echo "Commands:"
    echo "  Status: systemctl status $SERVICE"
    echo "  Logs:   journalctl -u $SERVICE -f"
    echo "  Stop:   systemctl stop $SERVICE"
    echo
}

# =============================================================================
# Main
# =============================================================================

main() {
    local server="" token="" version="latest"

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --server)  server="$2"; shift 2 ;;
            --token)   token="$2"; shift 2 ;;
            --version) version="$2"; shift 2 ;;
            -h|--help) usage ;;
            uninstall) check_root; uninstall ;;
            *) shift ;;
        esac
    done

    check_root

    [[ -z "$server" ]] && error "Missing --server URL"
    [[ -z "$token" ]]  && error "Missing --token TOKEN"

    install "$server" "$token" "$version"
}

main "$@"
