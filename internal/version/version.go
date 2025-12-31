package version

import "runtime"

// Build-time variables, set via ldflags.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

// Platform returns the OS platform (linux, darwin, windows).
func Platform() string {
	return runtime.GOOS
}

// Arch returns the CPU architecture (amd64, arm64, arm, 386).
func Arch() string {
	return runtime.GOARCH
}
