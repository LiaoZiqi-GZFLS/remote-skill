package client

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Credentials are stored per host, encrypted with the Windows DPAPI user key
// (ConvertFrom-SecureString). The plaintext password never touches the config
// file and is only handed to plink at connection time.

func credentialDir() string {
	if dir := os.Getenv("RSK_HOME"); dir != "" {
		return filepath.Join(dir, "credentials")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".rsk", "credentials")
	}
	return filepath.Join(home, ".rsk", "credentials")
}

// CredentialPath is where the encrypted password of a host lives.
func CredentialPath(host string) string {
	return filepath.Join(credentialDir(), sanitizeHost(host)+".dpapi")
}

func sanitizeHost(host string) string {
	out := make([]rune, 0, len(host))
	for _, r := range host {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.', r == '@':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "default"
	}
	return string(out)
}

// HasCredential reports whether a password is stored for the host.
func HasCredential(host string) bool {
	_, err := os.Stat(CredentialPath(host))
	return err == nil
}

// SaveCredential encrypts the password with DPAPI and writes it with 0600.
func SaveCredential(host, password string) error {
	if runtime.GOOS != "windows" {
		return fmt.Errorf("saved passwords are currently supported on windows clients only")
	}
	script := `$s = ConvertTo-SecureString -String $env:RSK_SECRET -AsPlainText -Force; $s | ConvertFrom-SecureString`
	cmd := exec.Command(powershellExe(), "-NoProfile", "-NonInteractive", "-Command", script)
	cmd.Env = append(os.Environ(), "RSK_SECRET="+password)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("encrypt credential: %v", err)
	}
	if err := os.MkdirAll(credentialDir(), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(CredentialPath(host), []byte(strings.TrimSpace(string(out))), 0o600); err != nil {
		return err
	}
	return nil
}

// LoadCredential decrypts the stored password for the host.
func LoadCredential(host string) (string, bool, error) {
	if runtime.GOOS != "windows" {
		return "", false, nil
	}
	path := CredentialPath(host)
	if _, err := os.Stat(path); err != nil {
		return "", false, nil
	}
	script := `$sec = ConvertTo-SecureString (Get-Content -Raw -LiteralPath $env:RSK_CRED); ` +
		`$b = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec); ` +
		`try { [Runtime.InteropServices.Marshal]::PtrToStringAuto($b) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($b) }`
	cmd := exec.Command(powershellExe(), "-NoProfile", "-NonInteractive", "-Command", script)
	cmd.Env = append(os.Environ(), "RSK_CRED="+path)
	out, err := cmd.Output()
	if err != nil {
		return "", true, fmt.Errorf("decrypt credential: %v", err)
	}
	pw := strings.TrimRight(string(out), "\r\n")
	if pw == "" {
		return "", true, fmt.Errorf("stored credential for %s is empty", host)
	}
	return pw, true, nil
}

// DeleteCredential forgets the stored password of a host.
func DeleteCredential(host string) error {
	err := os.Remove(CredentialPath(host))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func powershellExe() string {
	for _, name := range []string{"pwsh.exe", "powershell.exe"} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	return "powershell.exe"
}
