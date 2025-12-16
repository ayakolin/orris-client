# Makefile for Orris Agent

# Variables
BINARY_NAME=orris-client
VERSION?=dev
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}"

# Default target
.PHONY: all
all: build

# Build
.PHONY: build
build:
	@echo "Building ${BINARY_NAME}..."
	@go build ${LDFLAGS} -o bin/${BINARY_NAME} ./cmd/orris-client
	@echo "Build complete: bin/${BINARY_NAME}"

# Clean
.PHONY: clean
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@echo "Clean complete"

# Run (requires ORRIS_TOKEN env or --token flag)
.PHONY: run
run: build
	@echo "Running ${BINARY_NAME}..."
	@./bin/${BINARY_NAME}

# Test
.PHONY: test
test:
	@echo "Running tests..."
	@go test -v ./...

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# Lint
.PHONY: lint
lint:
	@echo "Running linter..."
	@golangci-lint run

# Download dependencies
.PHONY: deps
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

# UPX flags (macOS requires --force-macos)
UPX_FLAGS=--best --lzma
ifeq ($(shell uname),Darwin)
	UPX_FLAGS+=--force-macos
endif

# Build with UPX compression
.PHONY: build-upx
build-upx:
	@echo "Building ${BINARY_NAME} with UPX compression..."
	@go build ${LDFLAGS} -o bin/${BINARY_NAME} ./cmd/orris-client
	@echo "Compressing with UPX..."
	@upx ${UPX_FLAGS} bin/${BINARY_NAME}
	@echo "Build complete: bin/${BINARY_NAME} (compressed)"

# Build release with stripped symbols and UPX
.PHONY: release
release:
	@echo "Building release ${BINARY_NAME}..."
	@go build -ldflags "-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" -o bin/${BINARY_NAME} ./cmd/orris-client
	@echo "Compressing with UPX..."
	@upx ${UPX_FLAGS} bin/${BINARY_NAME}
	@echo "Release build complete: bin/${BINARY_NAME}"

# Build for Linux amd64
.PHONY: build-linux
build-linux:
	@echo "Building ${BINARY_NAME} for Linux amd64..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o bin/${BINARY_NAME}-linux-amd64 ./cmd/orris-client
	@echo "Build complete: bin/${BINARY_NAME}-linux-amd64"

# Build for Linux arm64
.PHONY: build-linux-arm64
build-linux-arm64:
	@echo "Building ${BINARY_NAME} for Linux arm64..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ${LDFLAGS} -o bin/${BINARY_NAME}-linux-arm64 ./cmd/orris-client
	@echo "Build complete: bin/${BINARY_NAME}-linux-arm64"

# Build Linux with UPX compression
.PHONY: build-linux-upx
build-linux-upx:
	@echo "Building ${BINARY_NAME} for Linux amd64 with UPX..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" -o bin/${BINARY_NAME}-linux-amd64 ./cmd/orris-client
	@echo "Compressing with UPX..."
	@upx --best --lzma bin/${BINARY_NAME}-linux-amd64
	@echo "Build complete: bin/${BINARY_NAME}-linux-amd64 (compressed)"

# Build Linux arm64 with UPX compression
.PHONY: build-linux-arm64-upx
build-linux-arm64-upx:
	@echo "Building ${BINARY_NAME} for Linux arm64 with UPX..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" -o bin/${BINARY_NAME}-linux-arm64 ./cmd/orris-client
	@echo "Compressing with UPX..."
	@upx --best --lzma bin/${BINARY_NAME}-linux-arm64
	@echo "Build complete: bin/${BINARY_NAME}-linux-arm64 (compressed)"

# Build all Linux targets
.PHONY: build-linux-all
build-linux-all: build-linux build-linux-arm64
	@echo "All Linux builds complete"

# Build universal installer (auto-detect arch)
.PHONY: build-linux-universal
build-linux-universal: build-linux build-linux-arm64
	@echo "Creating universal installer..."
	@echo '#!/bin/bash' > bin/${BINARY_NAME}-linux
	@echo 'set -e' >> bin/${BINARY_NAME}-linux
	@echo 'ARCH=$$(uname -m)' >> bin/${BINARY_NAME}-linux
	@echo 'case $$ARCH in' >> bin/${BINARY_NAME}-linux
	@echo '  x86_64|amd64) SUFFIX=amd64 ;;' >> bin/${BINARY_NAME}-linux
	@echo '  aarch64|arm64) SUFFIX=arm64 ;;' >> bin/${BINARY_NAME}-linux
	@echo '  *) echo "Unsupported: $$ARCH"; exit 1 ;;' >> bin/${BINARY_NAME}-linux
	@echo 'esac' >> bin/${BINARY_NAME}-linux
	@echo 'SCRIPT_DIR=$$(cd "$$(dirname "$$0")" && pwd)' >> bin/${BINARY_NAME}-linux
	@echo 'exec "$$SCRIPT_DIR/${BINARY_NAME}-linux-$$SUFFIX" "$$@"' >> bin/${BINARY_NAME}-linux
	@chmod +x bin/${BINARY_NAME}-linux
	@echo "Build complete: bin/${BINARY_NAME}-linux (universal wrapper)"

# Build all Linux targets with UPX
.PHONY: build-linux-all-upx
build-linux-all-upx: build-linux-upx build-linux-arm64-upx
	@echo "All Linux UPX builds complete"

# Alias for build-linux-all-upx
.PHONY: release-linux
release-linux: build-linux-all-upx
	@echo "All Linux release builds complete"

# Install (simple copy to /usr/local/bin)
.PHONY: install
install: build
	@echo "Installing ${BINARY_NAME}..."
	@cp bin/${BINARY_NAME} /usr/local/bin/
	@echo "Installation complete"

# Install with systemd service
.PHONY: install-systemd
install-systemd: build
	@echo "Installing ${BINARY_NAME} with systemd..."
	@sudo LOCAL_BINARY=bin/${BINARY_NAME} ./scripts/install.sh

# Uninstall systemd service
.PHONY: uninstall
uninstall:
	@echo "Uninstalling ${BINARY_NAME}..."
	@sudo ./scripts/install.sh uninstall

# Help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make build              - Build project"
	@echo "  make build-upx          - Build with UPX compression"
	@echo "  make release            - Build release (stripped + UPX)"
	@echo "  make build-linux        - Build for Linux amd64"
	@echo "  make build-linux-arm64  - Build for Linux arm64"
	@echo "  make build-linux-upx    - Build Linux amd64 with UPX"
	@echo "  make build-linux-arm64-upx - Build Linux arm64 with UPX"
	@echo "  make build-linux-all-upx - Build both Linux amd64 and arm64 with UPX"
	@echo "  make release-linux      - Build all Linux releases with UPX (alias)"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make run                - Build and run (needs ORRIS_TOKEN)"
	@echo "  make test               - Run tests"
	@echo "  make fmt                - Format code"
	@echo "  make lint               - Run linter"
	@echo "  make deps               - Download dependencies"
	@echo "  make install            - Install to /usr/local/bin"
	@echo "  make install-systemd    - Install with systemd service"
	@echo "  make uninstall          - Uninstall systemd service"
	@echo "  make help               - Show this help"
