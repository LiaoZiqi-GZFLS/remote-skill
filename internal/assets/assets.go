// Package assets carries the prebuilt rskilld binaries that are uploaded to
// remote hosts. The binaries are produced by tools/pack and embedded into the
// local `rsk` executable so no toolchain is needed on either side.
package assets

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed all:bin
var binFS embed.FS

// Binary returns the embedded rskilld build for a platform.
func Binary(goos, goarch string) (data []byte, name string, ok bool) {
	base := fmt.Sprintf("rskilld_%s_%s", goos, goarch)
	for _, candidate := range []string{base, base + ".exe"} {
		if b, err := binFS.ReadFile("bin/" + candidate); err == nil {
			return b, candidate, true
		}
	}
	return nil, "", false
}

// Platforms lists the platforms with an embedded build.
func Platforms() []string {
	entries, err := binFS.ReadDir("bin")
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "rskilld_") {
			continue
		}
		name = strings.TrimSuffix(name, ".exe")
		out = append(out, strings.TrimPrefix(name, "rskilld_"))
	}
	sort.Strings(out)
	return out
}

// Has reports whether a platform build exists.
func Has(goos, goarch string) bool {
	_, _, ok := Binary(goos, goarch)
	return ok
}
