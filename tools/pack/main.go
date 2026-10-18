// Command pack cross compiles rskilld for every supported remote platform and
// drops the results into internal/assets/bin so `go build ./cmd/rsk` embeds
// them.
//
//	go run ./tools/pack
//	go run ./tools/pack -platforms linux/amd64
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

var defaultPlatforms = []string{
	"linux/amd64",
	"linux/arm64",
	"linux/arm",
	"linux/386",
	"darwin/amd64",
	"darwin/arm64",
	"freebsd/amd64",
	"windows/amd64",
}

func main() {
	platforms := flag.String("platforms", "", "comma separated GOOS/GOARCH list")
	keepDebug := flag.Bool("debug", false, "keep debug info")
	flag.Parse()

	list := defaultPlatforms
	if *platforms != "" {
		list = strings.Split(*platforms, ",")
	}
	root, err := moduleRoot()
	if err != nil {
		fatal(err)
	}
	version, err := moduleVersion(filepath.Join(root, "internal", "version", "version.go"))
	if err != nil {
		fatal(err)
	}
	outDir := filepath.Join(root, "internal", "assets", "bin")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fatal(err)
	}
	ldflags := "-X remote-skill/internal/version.Version=" + version
	if !*keepDebug {
		ldflags = "-s -w " + ldflags
	}
	for _, p := range list {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			fatal(fmt.Errorf("invalid platform %q", p))
		}
		goos, goarch := parts[0], parts[1]
		name := fmt.Sprintf("rskilld_%s_%s", goos, goarch)
		if goos == "windows" {
			name += ".exe"
		}
		out := filepath.Join(outDir, name)
		cmd := exec.Command("go", "build", "-trimpath", "-ldflags", ldflags, "-o", out, "./cmd/rskilld")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		start := fmt.Sprintf("%s/%s", goos, goarch)
		if err := cmd.Run(); err != nil {
			fatal(fmt.Errorf("build %s: %v\n%s", start, err, stderr.String()))
		}
		st, err := os.Stat(out)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("packed %-16s %8.2f MiB  %s\n", start, float64(st.Size())/(1<<20), filepath.Base(out))
	}
	fmt.Printf("done: %d build(s) in %s (host %s/%s)\n", len(list), outDir, runtime.GOOS, runtime.GOARCH)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "pack: %v\n", err)
	os.Exit(1)
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("go.mod not found above %s", dir)
}

var versionRe = regexp.MustCompile(`(?m)^var Version = "([^"]+)"`)

func moduleVersion(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	m := versionRe.FindSubmatch(data)
	if len(m) < 2 {
		return "", fmt.Errorf("version not found in %s", path)
	}
	return string(m[1]), nil
}
