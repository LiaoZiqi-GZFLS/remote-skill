package client

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Host is a configured remote target.
type Host struct {
	Name        string            `json:"name"`
	Target      string            `json:"target"`
	User        string            `json:"user,omitempty"`
	Port        int               `json:"port,omitempty"`
	Identity    string            `json:"identity,omitempty"`
	SSHArgs     []string          `json:"ssh_args,omitempty"`
	Session     string            `json:"session,omitempty"`
	Shell       string            `json:"shell,omitempty"`
	Workspace   string            `json:"workspace,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	NoProfile   bool              `json:"no_profile,omitempty"`
	Inline      bool              `json:"inline_sessions,omitempty"`
	ControlMux  *bool             `json:"control_master,omitempty"`
	RemoteBin   string            `json:"remote_bin,omitempty"`
	RemoteHome  string            `json:"remote_home,omitempty"`
	RemoteUser  string            `json:"remote_user,omitempty"`
	RemoteOS    string            `json:"remote_os,omitempty"`
	RemoteArch  string            `json:"remote_arch,omitempty"`
	RemoteShell string            `json:"remote_shell,omitempty"`
	RemoteSHA   string            `json:"remote_sha,omitempty"`
	HasSha256   bool              `json:"has_sha256,omitempty"`
	Version     string            `json:"version,omitempty"`
	LastSeen    time.Time         `json:"last_seen,omitempty"`
}

// Spec renders the ssh destination, e.g. user@host.
func (h *Host) Spec() string {
	if h.User != "" {
		return h.User + "@" + h.Target
	}
	return h.Target
}

// SessionName is the session used when none is given.
func (h *Host) SessionName() string {
	if h.Session == "" {
		return "main"
	}
	return h.Session
}

// SocketPath is the daemon socket on the remote host.
func (h *Host) SocketPath() string {
	home := h.RemoteHome
	if home == "" {
		home = "~"
	}
	return strings.TrimSuffix(home, "/") + "/.rsk/run/rskd.sock"
}

// LogPath is the daemon log on the remote host.
func (h *Host) LogPath() string {
	home := h.RemoteHome
	if home == "" {
		home = "~"
	}
	return strings.TrimSuffix(home, "/") + "/.rsk/log/rskd.log"
}

// RemoteDir is where uploaded binaries live.
func (h *Host) RemoteDir() string {
	home := h.RemoteHome
	if home == "" {
		home = "~"
	}
	return strings.TrimSuffix(home, "/") + "/.rsk/bin"
}

// Config is the local registry of remote hosts.
type Config struct {
	path  string
	Hosts map[string]*Host `json:"hosts"`
}

// ConfigPath returns the config file location.
func ConfigPath() string {
	if p := os.Getenv("RSK_CONFIG"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".rsk-config.json"
	}
	if dir := os.Getenv("RSK_HOME"); dir != "" {
		return filepath.Join(dir, "config.json")
	}
	return filepath.Join(home, ".rsk", "config.json")
}

// RuntimeDir returns the local runtime directory (control sockets, logs).
func RuntimeDir() string {
	if dir := os.Getenv("RSK_HOME"); dir != "" {
		return filepath.Join(dir, "run")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".rsk-run"
	}
	return filepath.Join(home, ".rsk", "run")
}

// LoadConfig reads the host registry, returning an empty config when absent.
func LoadConfig() (*Config, error) {
	path := ConfigPath()
	cfg := &Config{path: path, Hosts: map[string]*Host{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
	}
	cfg.path = path
	if cfg.Hosts == nil {
		cfg.Hosts = map[string]*Host{}
	}
	return cfg, nil
}

// Save writes the registry atomically.
func (c *Config) Save() error {
	if c.path == "" {
		c.path = ConfigPath()
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}

// Get looks up a host by name.
func (c *Config) Get(name string) (*Host, bool) {
	h, ok := c.Hosts[name]
	return h, ok
}

// Put stores a host, keyed by name.
func (c *Config) Put(h *Host) {
	if c.Hosts == nil {
		c.Hosts = map[string]*Host{}
	}
	h.Name = NormalizeName(h)
	c.Hosts[h.Name] = h
}

// Delete removes a host.
func (c *Config) Delete(name string) bool {
	if _, ok := c.Hosts[name]; !ok {
		return false
	}
	delete(c.Hosts, name)
	return true
}

// Names lists configured hosts sorted by name.
func (c *Config) Names() []string {
	out := make([]string, 0, len(c.Hosts))
	for name := range c.Hosts {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// NormalizeName derives the registry key for a host.
func NormalizeName(h *Host) string {
	if h.Name != "" {
		return h.Name
	}
	name := h.Target
	if h.User != "" {
		name = h.User + "@" + h.Target
	}
	return name
}

// ParseTarget splits [user@]host[:port] into its parts. IPv6 literals in
// brackets are supported.
func ParseTarget(spec string) (user, host string, port int) {
	rest := strings.TrimSpace(spec)
	if i := strings.LastIndex(rest, "@"); i >= 0 {
		user, rest = rest[:i], rest[i+1:]
	}
	if strings.HasPrefix(rest, "[") {
		if end := strings.Index(rest, "]"); end > 0 {
			host = rest[1:end]
			if tail := rest[end+1:]; strings.HasPrefix(tail, ":") {
				port, _ = strconv.Atoi(tail[1:])
			}
			return user, host, port
		}
	}
	if i := strings.LastIndex(rest, ":"); i >= 0 && strings.Count(rest, ":") == 1 {
		if p, err := strconv.Atoi(rest[i+1:]); err == nil {
			return user, rest[:i], p
		}
	}
	return user, rest, 0
}

// Describe renders a one line summary for `rsk hosts`.
func (h *Host) Describe() string {
	parts := []string{h.Spec()}
	if h.Port > 0 {
		parts = append(parts, "port="+strconv.Itoa(h.Port))
	}
	if h.RemoteArch != "" {
		parts = append(parts, h.RemoteOS+"/"+h.RemoteArch)
	}
	if h.Version != "" {
		parts = append(parts, "rskilld "+h.Version)
	}
	if !h.LastSeen.IsZero() {
		parts = append(parts, "last seen "+h.LastSeen.Format(time.RFC3339))
	}
	return strings.Join(parts, "  ")
}
