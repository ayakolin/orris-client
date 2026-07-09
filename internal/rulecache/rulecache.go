// Package rulecache persists the agent's last successfully synced rule set to
// local disk, so the agent can keep serving previously known rules when it
// cannot reach the control server (e.g. on startup after a restart).
package rulecache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/forward"
)

// cacheSuffix is appended to the agent's config file path to derive the rule
// cache file path. Suffixing (rather than a fixed filename in the config
// directory) keeps multi-instance installs from colliding: each instance has
// its own config file (client.env, client-<name>.env, ...) under the same
// directory (see scripts/install.sh), so each instance gets its own cache
// file too.
const cacheSuffix = ".rules_cache.json"

// Snapshot is the persisted view of an agent's last successfully synced state.
type Snapshot struct {
	Rules            []forward.Rule                  `json:"rules"`
	ClientToken      string                          `json:"client_token,omitempty"`
	BlockedProtocols []string                        `json:"blocked_protocols,omitempty"`
	Endpoints        map[string]forward.ExitEndpoint `json:"endpoints,omitempty"`
	SavedAt          int64                           `json:"saved_at"`
}

// ErrSymlinkNotAllowed is returned when the cache file path is a symlink.
var ErrSymlinkNotAllowed = errors.New("rule cache file cannot be a symlink")

// FilePath returns the path to this agent's local rule cache file: the
// agent's config file path (config.ConfigFilePath()) with cacheSuffix
// appended.
func FilePath() string {
	return config.ConfigFilePath() + cacheSuffix
}

// Load reads and decodes the rule cache file. It returns an error if the file
// does not exist, cannot be read, cannot be parsed, or contains no rules.
func Load() (*Snapshot, error) {
	path := FilePath()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rule cache: %w", err)
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("parse rule cache: %w", err)
	}

	if len(snap.Rules) == 0 {
		return nil, fmt.Errorf("rule cache is empty")
	}

	return &snap, nil
}

// Save atomically writes the snapshot to the rule cache file (temp file +
// rename). The file is written with 0600 permissions since it may contain a
// client token.
//
// Note: Save() is not concurrent-safe. The caller must ensure that only one
// goroutine calls Save() at a time (e.g., via a single-goroutine debounce
// routine).
func Save(snap *Snapshot) error {
	path := FilePath()

	// Reject symlinks to prevent symlink attacks (mirrors config.SaveServerURL).
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return ErrSymlinkNotAllowed
		}
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal rule cache: %w", err)
	}

	tmpPath := path + ".tmp"
	// Use O_EXCL to prevent following symlinks that may have been placed at tmpPath
	// by an attacker. If tmpPath exists (including as a symlink), this will fail
	// with EEXIST rather than following the link.
	tmpFile, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		// If the temp file already exists, check if it's a symlink (security issue)
		// or just a leftover from a previous failed Save().
		if os.IsExist(err) {
			info, statErr := os.Lstat(tmpPath)
			if statErr == nil && info.Mode()&os.ModeSymlink != 0 {
				// It's a symlink at the temp path, which is a security issue. Reject it.
				return fmt.Errorf("create temp rule cache: %w", err)
			}
			// It's not a symlink, so clean it up and retry once.
			os.Remove(tmpPath)
			tmpFile, err = os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		}
		if err != nil {
			return fmt.Errorf("create temp rule cache: %w", err)
		}
	}

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write temp rule cache: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp rule cache: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename rule cache: %w", err)
	}

	return nil
}
