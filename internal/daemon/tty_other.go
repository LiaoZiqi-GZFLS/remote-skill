//go:build !windows

package daemon

import "remote-skill/internal/proto"

// handleExecTTYWindows is unreachable on unix (nativePTY is true).
func (c *conn) handleExecTTYWindows(_ *request, env *proto.Envelope, _ TTYParams) {
	c.fail(env.ID, proto.Errorf(proto.CodeInternal, "windows tty path not available"))
}
