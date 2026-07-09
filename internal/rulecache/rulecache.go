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
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("write temp rule cache: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename rule cache: %w", err)
	}

	return nil
}
