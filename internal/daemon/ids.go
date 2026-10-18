package daemon

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// newRequestID returns a short, unique, shell safe identifier.
func newRequestID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format("150405.000000")))
	}
	return hex.EncodeToString(b[:])
}
