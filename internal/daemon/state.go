package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SessionState is what survives a machine reboot: the parts of a session that
// can be recreated without keeping a process alive. The keeper writes it
// periodically and on exit; a keeper started later seeds itself from it.
type SessionState struct {
	Name       string       `json:"name"`
	Shell      string       `json:"shell,omitempty"`
	Cwd        string       `json:"cwd,omitempty"`
	StartedAt  time.Time    `json:"started_at,omitempty"`
	LastActive time.Time    `json:"last_active,omitempty"`
	SavedAt    time.Time    `json:"saved_at"`
	Commands   []ExecResult `json:"commands,omitempty"`
	Scrollback []byte       `json:"scrollback,omitempty"`
}

// Persisted scrollback cap: enough to explain what happened before a reboot
// without writing megabytes on every save.
const (
	maxStateCommands   = 100
	maxStateScrollback = 256 << 10
)

// StatePath is the JSON state file of a session (the environment snapshot is
// stored separately as <name>.env).
func StatePath(home, name string) string {
	return filepath.Join(home, ".rsk", "state", sanitizeName(name)+".json")
}

// HistoryPath is the shell history file of a session.
func HistoryPath(home, name string) string {
	return filepath.Join(home, ".rsk", "history", sanitizeName(name))
}

// LoadSessionState reads a persisted session state, returning false when there
// is nothing usable to restore.
func LoadSessionState(home, name string) (*SessionState, bool) {
	path := StatePath(home, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var st SessionState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, false
	}
	if st.Name == "" {
		st.Name = name
	}
	return &st, true
}

// SaveSessionState writes the state atomically so a crash never leaves a half
// written file behind.
func SaveSessionState(home string, st SessionState) error {
	path := StatePath(home, st.Name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	st.SavedAt = time.Now()
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// RemoveSessionState deletes both the JSON state and the environment snapshot.
func RemoveSessionState(home, name string) {
	if home == "" {
		return
	}
	_ = os.Remove(StatePath(home, name))
	_ = os.Remove(filepath.Join(home, ".rsk", "state", sanitizeName(name)+".env"))
}

// WriteHistoryFile seeds the shell history file with previously seen commands
// so `history` still shows what ran before a reboot.
func WriteHistoryFile(home, name string, commands []ExecResult) error {
	if len(commands) == 0 {
		return nil
	}
	path := HistoryPath(home, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	existing := ""
	if data, err := os.ReadFile(path); err == nil {
		existing = string(data)
	}
	out := existing
	for _, cmd := range commands {
		line := cmd.Cmd
		if line == "" {
			continue
		}
		// keep history one command per line
		line = replaceNewlines(line)
		out += line + "\n"
	}
	return os.WriteFile(path, []byte(out), 0o600)
}

func replaceNewlines(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch r {
		case '\n', '\r':
			out = append(out, ' ')
		default:
			out = append(out, r)
		}
	}
	return string(out)
}

// stateSummary is a compact description used in logs and status output.
func (s SessionState) String() string {
	cwd := s.Cwd
	if cwd == "" {
		cwd = "?"
	}
	return fmt.Sprintf("saved %s, cwd %s, %d command(s), %d scrollback byte(s)",
		s.SavedAt.Format(time.RFC3339), cwd, len(s.Commands), len(s.Scrollback))
}
