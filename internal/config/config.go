package config

import (
	"bufio"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServerURL          string
	Token              string
	WsListenPort       uint16 // WebSocket listen port for tunnel connections (exit agent)
	TlsListenPort      uint16 // TLS listen port for tunnel connections (exit agent)
	SyncInterval       time.Duration
	TrafficInterval    time.Duration
	StatusInterval     time.Duration // WebSocket mode status interval
	StatusIntervalRest time.Duration // REST mode status interval (fallback)
	HTTPTimeout        time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		ServerURL:          "http://localhost:8080",
		Token:              "",
		SyncInterval:       30 * time.Second,
		TrafficInterval:    1 * time.Second,
		StatusInterval:     1 * time.Second,
		StatusIntervalRest: 30 * time.Second,
		HTTPTimeout:        10 * time.Second,
	}
}

func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	// Load from config file first (lowest priority)
	loadFromFile(cfg)

	// Environment variables override config file
	if v := os.Getenv("ORRIS_SERVER_URL"); v != "" {
		cfg.ServerURL = v
	}
	if v := os.Getenv("ORRIS_TOKEN"); v != "" {
		cfg.Token = v
	}
	if v := os.Getenv("ORRIS_WS_LISTEN_PORT"); v != "" {
		if port, err := strconv.ParseUint(v, 10, 16); err == nil {
			cfg.WsListenPort = uint16(port)
		}
	}
	if v := os.Getenv("ORRIS_TLS_LISTEN_PORT"); v != "" {
		if port, err := strconv.ParseUint(v, 10, 16); err == nil {
			cfg.TlsListenPort = uint16(port)
		}
	}

	return cfg
}

// ConfigFilePath returns the path to the config file.
// Default: /etc/orris/client.env (same as install script)
// Override: ORRIS_CONFIG_FILE environment variable
func ConfigFilePath() string {
	if v := os.Getenv("ORRIS_CONFIG_FILE"); v != "" {
		return v
	}
	return "/etc/orris/client.env"
}

// loadFromFile loads configuration from the config file.
// File format: environment variable style (ORRIS_SERVER_URL=xxx)
func loadFromFile(cfg *Config) {
	path := ConfigFilePath()
	file, err := os.Open(path)
	if err != nil {
		return // File doesn't exist or can't be read, skip
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "ORRIS_SERVER_URL":
			cfg.ServerURL = value
		case "ORRIS_TOKEN":
			cfg.Token = value
		case "ORRIS_WS_LISTEN_PORT":
			if port, err := strconv.ParseUint(value, 10, 16); err == nil {
				cfg.WsListenPort = uint16(port)
			}
		case "ORRIS_TLS_LISTEN_PORT":
			if port, err := strconv.ParseUint(value, 10, 16); err == nil {
				cfg.TlsListenPort = uint16(port)
			}
		}
	}
}

// ErrInvalidServerURL is returned when the server URL is invalid.
var ErrInvalidServerURL = errors.New("invalid server URL")

// ErrSymlinkNotAllowed is returned when the config file is a symlink.
var ErrSymlinkNotAllowed = errors.New("config file cannot be a symlink")

// RedactURL returns a URL string with credentials redacted for safe logging.
// Example: "https://user:pass@example.com" -> "https://[REDACTED]@example.com"
func RedactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "[invalid URL]"
	}
	if u.User != nil {
		u.User = url.User("[REDACTED]")
	}
	return u.String()
}

// ValidateServerURL validates that the given URL is a valid server URL.
// Returns nil if valid, error otherwise.
//
// Validation rules:
//   - Must be a valid URL format
//   - Must use http or https scheme
//   - Must have a valid host
//   - Must not contain newlines or control characters (prevents config injection)
//   - Must not be a localhost/internal/metadata address (prevents SSRF)
func ValidateServerURL(serverURL string) error {
	if serverURL == "" {
		return ErrInvalidServerURL
	}

	// Check for newlines and control characters (config injection prevention)
	for _, c := range serverURL {
		if c == '\n' || c == '\r' || c < 32 {
			return ErrInvalidServerURL
		}
	}

	// Parse and validate URL
	u, err := url.Parse(serverURL)
	if err != nil {
		return ErrInvalidServerURL
	}

	// Must be http or https
	if u.Scheme != "http" && u.Scheme != "https" {
		return ErrInvalidServerURL
	}

	// Must have a valid host
	if u.Host == "" {
		return ErrInvalidServerURL
	}

	// Extract hostname (without port)
	hostname := u.Hostname()

	// Block localhost and internal addresses
	if isInternalHost(hostname) {
		return ErrInvalidServerURL
	}

	return nil
}

// isInternalHost checks if the hostname is a localhost, internal, or metadata address.
func isInternalHost(hostname string) bool {
	// Normalize to lowercase
	hostname = strings.ToLower(hostname)

	// Block localhost variants
	if hostname == "localhost" ||
		hostname == "localhost.localdomain" ||
		strings.HasSuffix(hostname, ".localhost") {
		return true
	}

	// Parse as IP address
	ip := net.ParseIP(hostname)
	if ip == nil {
		// Not an IP address, allow (could be a valid domain)
		return false
	}

	// Block loopback addresses (127.0.0.0/8, ::1)
	if ip.IsLoopback() {
		return true
	}

	// Block private addresses (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
	if ip.IsPrivate() {
		return true
	}

	// Block link-local addresses (169.254.0.0/16, fe80::/10)
	// This includes AWS/cloud metadata endpoint 169.254.169.254
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	// Block unspecified addresses (0.0.0.0, ::)
	if ip.IsUnspecified() {
		return true
	}

	return false
}

// SaveServerURL saves the server URL to the config file.
// This is used when the server notifies a URL change.
// Preserves existing config entries and comments.
// Returns ErrInvalidServerURL if the URL is invalid.
// Returns ErrSymlinkNotAllowed if the config file is a symlink.
func SaveServerURL(serverURL string) error {
	// Validate URL before saving
	if err := ValidateServerURL(serverURL); err != nil {
		return err
	}

	path := ConfigFilePath()

	// Security check: reject symlinks to prevent symlink attacks
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return ErrSymlinkNotAllowed
		}
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Read existing file content, preserving order and comments
	var lines []string
	found := false

	if file, err := os.Open(path); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			trimmed := strings.TrimSpace(line)

			// Check if this is the ORRIS_SERVER_URL line
			if strings.HasPrefix(trimmed, "ORRIS_SERVER_URL=") {
				lines = append(lines, "ORRIS_SERVER_URL="+serverURL)
				found = true
			} else {
				lines = append(lines, line)
			}
		}
		file.Close()
	}

	// If ORRIS_SERVER_URL was not found, add it
	if !found {
		lines = append(lines, "ORRIS_SERVER_URL="+serverURL)
	}

	// Write back
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, line := range lines {
		if _, err := file.WriteString(line + "\n"); err != nil {
			return err
		}
	}

	return nil
}
