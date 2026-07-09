package rulecache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/orris-inc/orris-client/internal/forward"
)

// setConfigFile points ORRIS_CONFIG_FILE at a fresh temp file for the
// duration of the test, so FilePath() resolves to an isolated location.
func setConfigFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "client.env")
	t.Setenv("ORRIS_CONFIG_FILE", configPath)
	return configPath
}

func TestFilePathIsSuffixedConfigPath(t *testing.T) {
	configPath := setConfigFile(t)

	want := configPath + cacheSuffix
	if got := FilePath(); got != want {
		t.Fatalf("FilePath() = %q, want %q", got, want)
	}
}

func TestFilePathDoesNotCollideAcrossInstances(t *testing.T) {
	dir := t.TempDir()

	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client.env"))
	primary := FilePath()

	t.Setenv("ORRIS_CONFIG_FILE", filepath.Join(dir, "client-second.env"))
	secondary := FilePath()

	if primary == secondary {
		t.Fatalf("FilePath() collided across instances: %q == %q", primary, secondary)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	setConfigFile(t)

	snap := &Snapshot{
		Rules:            []forward.Rule{{ID: "fr_1", RuleType: forward.RuleTypeDirect, Protocol: "tcp"}},
		ClientToken:      "fwd_abc",
		BlockedProtocols: []string{"udp"},
		Endpoints:        map[string]forward.ExitEndpoint{"fa_1": {Address: "1.2.3.4", WsPort: 9000}},
		SavedAt:          1234567890,
	}

	if err := Save(snap); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if len(got.Rules) != 1 || got.Rules[0].ID != "fr_1" {
		t.Fatalf("Rules = %+v, want 1 rule with ID fr_1", got.Rules)
	}
	if got.ClientToken != "fwd_abc" {
		t.Errorf("ClientToken = %q, want fwd_abc", got.ClientToken)
	}
	if len(got.BlockedProtocols) != 1 || got.BlockedProtocols[0] != "udp" {
		t.Errorf("BlockedProtocols = %v, want [udp]", got.BlockedProtocols)
	}
	ep, ok := got.Endpoints["fa_1"]
	if !ok || ep.Address != "1.2.3.4" || ep.WsPort != 9000 {
		t.Errorf("Endpoints[fa_1] = %+v, want {1.2.3.4 9000 0}", ep)
	}
}

func TestLoadMissingFile(t *testing.T) {
	setConfigFile(t)

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for missing file")
	}
}

func TestLoadCorruptedFile(t *testing.T) {
	setConfigFile(t)

	if err := os.WriteFile(FilePath(), []byte("not json"), 0600); err != nil {
		t.Fatalf("setup write: %v", err)
	}

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for corrupted file")
	}
}

func TestLoadEmptyRules(t *testing.T) {
	setConfigFile(t)

	if err := Save(&Snapshot{Rules: nil, SavedAt: 1}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for empty rule set")
	}
}

func TestSaveRejectsSymlink(t *testing.T) {
	setConfigFile(t)

	target := FilePath() + ".real"
	if err := os.WriteFile(target, []byte("{}"), 0600); err != nil {
		t.Fatalf("setup write: %v", err)
	}
	if err := os.Symlink(target, FilePath()); err != nil {
		t.Fatalf("setup symlink: %v", err)
	}

	err := Save(&Snapshot{Rules: []forward.Rule{{ID: "fr_1"}}, SavedAt: 1})
	if !errors.Is(err, ErrSymlinkNotAllowed) {
		t.Fatalf("Save() error = %v, want ErrSymlinkNotAllowed", err)
	}
}

func TestSaveFilePermissions(t *testing.T) {
	setConfigFile(t)

	if err := Save(&Snapshot{Rules: []forward.Rule{{ID: "fr_1"}}, SavedAt: 1}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	info, err := os.Stat(FilePath())
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("file permissions = %o, want 0600", perm)
	}
}
