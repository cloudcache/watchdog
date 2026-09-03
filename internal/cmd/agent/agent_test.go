package main

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"

	"github.com/cloudcache/watchdog/agent"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

func TestGetAddress(t *testing.T) {
	tests := []struct {
		name     string
		opts     cmdOptions
		envVars  map[string]string
		expected string
	}{
		{
			name:     "default port when no config",
			opts:     cmdOptions{},
			expected: ":45876",
		},
		{
			name: "use address from flag",
			opts: cmdOptions{
				listen: "8080",
			},
			expected: ":8080",
		},
		{
			name: "use unix socket from flag",
			opts: cmdOptions{
				listen: "/tmp/watchdog.sock",
			},
			expected: "/tmp/watchdog.sock",
		},
		{
			name: "use LISTEN env var",
			opts: cmdOptions{},
			envVars: map[string]string{
				"LISTEN": "1.2.3.4:9090",
			},
			expected: "1.2.3.4:9090",
		},
		{
			name: "use legacy PORT env var",
			opts: cmdOptions{},
			envVars: map[string]string{
				"PORT": "7070",
			},
			expected: ":7070",
		},
		{
			name: "use unix socket from env var",
			opts: cmdOptions{
				listen: "",
			},
			envVars: map[string]string{
				"LISTEN": "/tmp/watchdog.sock",
			},
			expected: "/tmp/watchdog.sock",
		},
		{
			name: "flag takes precedence over env vars",
			opts: cmdOptions{
				listen: ":8080",
			},
			envVars: map[string]string{
				"LISTEN": ":9090",
				"PORT":   "7070",
			},
			expected: ":8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup environment
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			addr := tt.opts.getAddress()
			assert.Equal(t, tt.expected, addr)
		})
	}
}

func TestLoadPublicKeys(t *testing.T) {
	// Generate a test key
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	pubKey := ssh.MarshalAuthorizedKey(signer.PublicKey())

	tests := []struct {
		name        string
		opts        cmdOptions
		envVars     map[string]string
		setupFiles  map[string][]byte
		wantErr     bool
		errContains string
	}{
		{
			name: "load key from flag",
			opts: cmdOptions{
				key: string(pubKey),
			},
		},
		{
			name: "load key from env var",
			envVars: map[string]string{
				"KEY": string(pubKey),
			},
		},
		{
			name: "load key from file",
			envVars: map[string]string{
				"KEY_FILE": "testkey.pub",
			},
			setupFiles: map[string][]byte{
				"testkey.pub": pubKey,
			},
		},
		{
			name:        "error when no key provided",
			wantErr:     true,
			errContains: "no key provided",
		},
		{
			name: "error on invalid key file",
			envVars: map[string]string{
				"KEY_FILE": "nonexistent.pub",
			},
			wantErr:     true,
			errContains: "failed to read key file",
		},
		{
			name: "error on invalid key data",
			opts: cmdOptions{
				key: "invalid-key-data",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary directory for test files
			if len(tt.setupFiles) > 0 {
				tmpDir := t.TempDir()
				for name, content := range tt.setupFiles {
					path := filepath.Join(tmpDir, name)
					err := os.WriteFile(path, content, 0600)
					require.NoError(t, err)
					if tt.envVars != nil {
						tt.envVars["KEY_FILE"] = path
					}
				}
			}

			// Set up environment
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			keys, err := tt.opts.loadPublicKeys()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Len(t, keys, 1)
			assert.Equal(t, signer.PublicKey().Type(), keys[0].Type())
		})
	}
}

func TestGetNetwork(t *testing.T) {
	tests := []struct {
		name     string
		opts     cmdOptions
		envVars  map[string]string
		expected string
	}{
		{
			name: "NETWORK env var",
			envVars: map[string]string{
				"NETWORK": "tcp4",
			},
			expected: "tcp4",
		},
		{
			name:     "only port",
			opts:     cmdOptions{listen: "8080"},
			expected: "tcp",
		},
		{
			name:     "ipv4 address",
			opts:     cmdOptions{listen: "1.2.3.4:8080"},
			expected: "tcp",
		},
		{
			name:     "ipv6 address",
			opts:     cmdOptions{listen: "[2001:db8::1]:8080"},
			expected: "tcp",
		},
		{
			name:     "unix network",
			opts:     cmdOptions{listen: "/tmp/watchdog.sock"},
			expected: "unix",
		},
		{
			name:     "env var network",
			opts:     cmdOptions{listen: ":8080"},
			envVars:  map[string]string{"NETWORK": "tcp4"},
			expected: "tcp4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup environment
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}
			network := agent.GetNetwork(tt.opts.listen)
			assert.Equal(t, tt.expected, network)
		})
	}
}

func TestParseFlags(t *testing.T) {
	// Save original command line arguments and restore after test
	oldArgs := os.Args
	defer func() {
		os.Args = oldArgs
		pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	}()

	tests := []struct {
		name     string
		args     []string
		expected cmdOptions
	}{
		{
			name: "no flags",
			args: []string{"cmd"},
			expected: cmdOptions{
				key:    "",
				listen: "",
			},
		},
		{
			name: "key flag only",
			args: []string{"cmd", "-key", "testkey"},
			expected: cmdOptions{
				key:    "testkey",
				listen: "",
			},
		},
		{
			name: "key flag double dash",
			args: []string{"cmd", "--key", "testkey"},
			expected: cmdOptions{
				key:    "testkey",
				listen: "",
			},
		},
		{
			name: "key flag short",
			args: []string{"cmd", "-k", "testkey"},
			expected: cmdOptions{
				key:    "testkey",
				listen: "",
			},
		},
		{
			name: "addr flag only",
			args: []string{"cmd", "-listen", ":8080"},
			expected: cmdOptions{
				key:    "",
				listen: ":8080",
			},
		},
		{
			name: "addr flag double dash",
			args: []string{"cmd", "--listen", ":8080"},
			expected: cmdOptions{
				key:    "",
				listen: ":8080",
			},
		},
		{
			name: "addr flag short",
			args: []string{"cmd", "-l", ":8080"},
			expected: cmdOptions{
				key:    "",
				listen: ":8080",
			},
		},
		{
			name: "both flags",
			args: []string{"cmd", "-key", "testkey", "-listen", ":8080"},
			expected: cmdOptions{
				key:    "testkey",
				listen: ":8080",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset flags for each test
			pflag.CommandLine = pflag.NewFlagSet(tt.args[0], pflag.ExitOnError)
			os.Args = tt.args

			var opts cmdOptions
			opts.parse()
			pflag.Parse()

			assert.Equal(t, tt.expected, opts)
		})
	}
}

func TestCLIClientFlagsOverridePrefixedEnvironment(t *testing.T) {
	oldArgs := os.Args
	oldFlags := pflag.CommandLine
	defer func() {
		os.Args = oldArgs
		pflag.CommandLine = oldFlags
	}()

	t.Setenv("WATCHDOG_AGENT_HUB_URL", "https://env.example.com")
	t.Setenv("WATCHDOG_AGENT_TOKEN", "env-token")
	t.Setenv("HUB_URL", "https://legacy-env.example.com")
	t.Setenv("TOKEN", "legacy-env-token")
	os.Args = []string{"cmd", "--url", " https://cli.example.com/ ", "--token", "cli-token"}
	pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	var opts cmdOptions
	if opts.parse() {
		t.Fatal("client flags unexpectedly handled as a subcommand")
	}
	if got := os.Getenv("WATCHDOG_AGENT_HUB_URL"); got != "https://cli.example.com/" {
		t.Fatalf("hub URL = %q", got)
	}
	if got := os.Getenv("WATCHDOG_AGENT_TOKEN"); got != "cli-token" {
		t.Fatalf("token = %q", got)
	}
	if got := os.Getenv("HUB_URL"); got != "https://cli.example.com/" {
		t.Fatalf("legacy hub URL = %q", got)
	}
	if got := os.Getenv("TOKEN"); got != "cli-token" {
		t.Fatalf("legacy token = %q", got)
	}
}
