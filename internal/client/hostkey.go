package client

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os/exec"
	"strings"
)

// HostKeyFingerprint asks ssh-keyscan for the host key and returns the
// SHA256 fingerprint in the form plink's -hostkey expects.
func HostKeyFingerprint(host string, port int) (string, error) {
	if port <= 0 {
		port = 22
	}
	out, err := exec.Command("ssh-keyscan", "-p", fmt.Sprint(port), "-T", "10", "-t", "ed25519", host).Output()
	if err != nil {
		return "", fmt.Errorf("ssh-keyscan %s: %v", host, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		blob, err := base64.StdEncoding.DecodeString(fields[2])
		if err != nil {
			continue
		}
		sum := sha256.Sum256(blob)
		return "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:]), nil
	}
	return "", fmt.Errorf("no ed25519 host key offered by %s", host)
}
