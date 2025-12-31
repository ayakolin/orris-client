// Package updater provides self-update functionality for the agent binary.
package updater

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/version"
)

// updateMu prevents concurrent updates.
var updateMu sync.Mutex

// UpdatePayload represents the update command payload from server.
type UpdatePayload struct {
	DownloadURL string `json:"download_url"`
	Version     string `json:"version"`
	Checksum    string `json:"checksum"` // SHA256 hex string
}

// ParsePayload parses update payload from command data.
func ParsePayload(data any) (*UpdatePayload, error) {
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid payload type: %T", data)
	}

	payload := &UpdatePayload{}
	if v, ok := m["download_url"].(string); ok {
		payload.DownloadURL = v
	}
	if v, ok := m["version"].(string); ok {
		payload.Version = v
	}
	if v, ok := m["checksum"].(string); ok {
		payload.Checksum = v
	}

	if payload.DownloadURL == "" {
		return nil, fmt.Errorf("missing download_url")
	}
	if payload.Version == "" {
		return nil, fmt.Errorf("missing version")
	}

	return payload, nil
}

// Update performs the self-update process.
// Returns true if update was applied and restart is needed.
func Update(payload *UpdatePayload) (bool, error) {
	// Prevent concurrent updates
	if !updateMu.TryLock() {
		return false, fmt.Errorf("update already in progress")
	}
	defer updateMu.Unlock()

	// Check if already at target version
	if payload.Version == version.Version {
		logger.Info("already at target version, skipping update", "version", payload.Version)
		return false, nil
	}

	logger.Info("starting update",
		"current_version", version.Version,
		"target_version", payload.Version,
		"download_url", payload.DownloadURL)

	// Get current executable path
	execPath, err := os.Executable()
	if err != nil {
		return false, fmt.Errorf("get executable path: %w", err)
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return false, fmt.Errorf("resolve symlinks: %w", err)
	}

	// Check if we have write permission to the directory
	execDir := filepath.Dir(execPath)
	if err := checkWritePermission(execDir); err != nil {
		return false, fmt.Errorf("no write permission to %s: %w", execDir, err)
	}

	// Create temp file for download
	tmpFile, err := os.CreateTemp("", "orris-client-update-*")
	if err != nil {
		return false, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			os.Remove(tmpPath)
		}
	}()

	// Download new binary
	logger.Info("downloading update", "url", payload.DownloadURL)
	if err := downloadFile(tmpFile, payload.DownloadURL); err != nil {
		tmpFile.Close()
		return false, fmt.Errorf("download: %w", err)
	}
	tmpFile.Close()

	// Verify checksum if provided
	if payload.Checksum != "" {
		logger.Debug("verifying checksum", "expected", payload.Checksum)
		if err := verifyChecksum(tmpPath, payload.Checksum); err != nil {
			return false, fmt.Errorf("checksum verification: %w", err)
		}
		logger.Debug("checksum verified")
	}

	// Make downloaded file executable
	if err := os.Chmod(tmpPath, 0755); err != nil {
		return false, fmt.Errorf("chmod: %w", err)
	}

	// Backup current executable
	backupPath := execPath + ".bak"
	if err := os.Rename(execPath, backupPath); err != nil {
		return false, fmt.Errorf("backup current binary: %w", err)
	}

	// Move new binary to target location
	if err := moveFile(tmpPath, execPath); err != nil {
		// Try to restore backup
		os.Rename(backupPath, execPath)
		return false, fmt.Errorf("replace binary: %w", err)
	}

	// Update successful, don't clean up tmp (it was moved)
	cleanupTmp = false

	// Remove backup after successful update
	os.Remove(backupPath)

	logger.Info("update completed successfully",
		"old_version", version.Version,
		"new_version", payload.Version)

	return true, nil
}

// downloadFile downloads a file from URL to the given writer.
func downloadFile(w io.Writer, url string) error {
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}

	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	_, err = io.Copy(w, resp.Body)
	return err
}

// verifyChecksum verifies the SHA256 checksum of a file.
func verifyChecksum(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}

	actual := hex.EncodeToString(h.Sum(nil))
	if actual != expected {
		return fmt.Errorf("checksum mismatch: got %s, want %s", actual, expected)
	}

	return nil
}

// moveFile moves a file from src to dst, handling cross-device moves.
func moveFile(src, dst string) error {
	// Try rename first (fast, same filesystem)
	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	// Fall back to copy + remove (cross-filesystem)
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

// checkWritePermission checks if we can write to the given directory.
func checkWritePermission(dir string) error {
	testFile := filepath.Join(dir, ".orris-update-test")
	f, err := os.Create(testFile)
	if err != nil {
		return err
	}
	f.Close()
	return os.Remove(testFile)
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}
