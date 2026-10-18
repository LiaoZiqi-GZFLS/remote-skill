//go:build !windows

package daemon

import "context"

// nativePTY reports whether sessions are real ptys running a login shell.
// Windows remotes use a "virtual session" instead: every command runs in a
// fresh PowerShell process whose working directory and environment are
// snapshotted by the keeper, which keeps `cd` and `$env:X` persistent without
// needing a pty.
const nativePTY = true

// The native (Windows) entry points below exist so session.go compiles on every
// platform; with nativePTY constant-folded to true they are unreachable here.

func (s *Session) startNative() error { return ErrSessionClosed }

func (s *Session) runNative(_ context.Context, _ ExecRequest, _ Sink) (*ExecResult, error) {
	return nil, ErrSessionClosed
}

func (s *Session) signalNative(string) error { return ErrSessionClosed }

func (s *Session) closeNative() error { return nil }

func (s *Session) attachSupported() bool { return true }
