// Package version holds build metadata for the rsk / rskd binaries.
package version

// Version is the released version of the toolchain. The remote daemon and
// local client must agree on it (see proto.APIVersion for the wire protocol).
var Version = "0.1.0"

// Commit and BuildTime are injected at build time via -ldflags.
var (
	Commit    = "dev"
	BuildTime = "unknown"
)
