// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clickhousemigration "github.com/cloudcache/watchdog/deploy/migration/clickhouse"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
)

type options struct {
	command                               string
	address, user, passwordFile           string
	tlsEnabled                            bool
	caFile, certFile, keyFile, serverName string
	dialTimeout, readTimeout              time.Duration
	lockOwner                             string
}

type migrationService interface {
	Inspect(context.Context, []flowch.Migration, bool) (flowch.MigrationInspection, error)
	Apply(context.Context, []flowch.Migration, flowch.MigrationApplyOptions) (flowch.MigrationRun, error)
	Unlock(context.Context, string) error
}

type commandOutput struct {
	Command    string                      `json:"command"`
	Inspection *flowch.MigrationInspection `json:"inspection,omitempty"`
	Run        *flowch.MigrationRun        `json:"run,omitempty"`
	LockOwner  string                      `json:"lock_owner,omitempty"`
	Unlocked   bool                        `json:"unlocked,omitempty"`
}

func main() {
	var opt options
	flag.StringVar(&opt.command, "command", "inspect", "inspect, apply, resume, or unlock")
	flag.StringVar(&opt.address, "clickhouse-address", "127.0.0.1:9000", "ClickHouse native TCP address")
	flag.StringVar(&opt.user, "clickhouse-user", "default", "ClickHouse user")
	flag.StringVar(&opt.passwordFile, "clickhouse-password-file", "", "file containing the ClickHouse password")
	flag.DurationVar(&opt.dialTimeout, "clickhouse-dial-timeout", 3*time.Second, "ClickHouse connection timeout")
	flag.DurationVar(&opt.readTimeout, "clickhouse-read-timeout", 30*time.Second, "ClickHouse packet read timeout")
	flag.BoolVar(&opt.tlsEnabled, "clickhouse-tls", false, "enable ClickHouse TLS")
	flag.StringVar(&opt.caFile, "clickhouse-tls-ca", "", "ClickHouse TLS CA file")
	flag.StringVar(&opt.certFile, "clickhouse-tls-cert", "", "ClickHouse TLS client certificate file")
	flag.StringVar(&opt.keyFile, "clickhouse-tls-key", "", "ClickHouse TLS client key file")
	flag.StringVar(&opt.serverName, "clickhouse-tls-server-name", "", "ClickHouse TLS server name")
	flag.StringVar(&opt.lockOwner, "lock-owner", "", "exact lock token; required for unlock and optional for apply/resume")
	flag.Parse()
	if err := run(opt, os.Stdout, os.Stderr); err != nil {
		log.Fatal(err)
	}
}

func run(opt options, stdout, stderr io.Writer) error {
	if stdout == nil || stderr == nil {
		return errors.New("migration output streams are required")
	}
	opt.command = strings.ToLower(strings.TrimSpace(opt.command))
	switch opt.command {
	case "inspect", "apply", "resume", "unlock":
	default:
		return fmt.Errorf("unsupported migration command %q", opt.command)
	}
	if opt.command == "unlock" && strings.TrimSpace(opt.lockOwner) == "" {
		return errors.New("lock-owner is required for unlock")
	}
	if (opt.command == "apply" || opt.command == "resume") && strings.TrimSpace(opt.lockOwner) == "" {
		owner, err := newLockOwner()
		if err != nil {
			return err
		}
		opt.lockOwner = owner
	}
	if opt.lockOwner != "" && opt.command != "inspect" {
		if _, err := fmt.Fprintf(stderr, "ClickHouse migration lock owner: %s\n", opt.lockOwner); err != nil {
			return fmt.Errorf("write migration lock owner: %w", err)
		}
	}
	config, err := buildNativeConfig(opt)
	if err != nil {
		return err
	}
	migrations, err := flowch.LoadMigrations(clickhousemigration.Files, ".")
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	native, err := flowch.NewNativeInserter(ctx, config)
	if err != nil {
		return err
	}
	defer native.Close()
	migrator, err := flowch.NewMigrator(native)
	if err != nil {
		return err
	}
	return executeCommand(ctx, opt, migrator, migrations, stdout)
}

func executeCommand(ctx context.Context, opt options, service migrationService, migrations []flowch.Migration, output io.Writer) error {
	if service == nil || output == nil {
		return errors.New("migration service and output are required")
	}
	result := commandOutput{Command: opt.command}
	var commandErr error
	switch opt.command {
	case "inspect":
		inspection, err := service.Inspect(ctx, migrations, true)
		result.Inspection = &inspection
		commandErr = err
	case "apply", "resume":
		run, err := service.Apply(ctx, migrations, flowch.MigrationApplyOptions{Resume: opt.command == "resume", LockOwner: opt.lockOwner})
		result.Run = &run
		result.LockOwner = opt.lockOwner
		commandErr = err
	case "unlock":
		result.LockOwner = opt.lockOwner
		commandErr = service.Unlock(ctx, opt.lockOwner)
		result.Unlocked = commandErr == nil
	default:
		return fmt.Errorf("unsupported migration command %q", opt.command)
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("encode migration result: %w", err)
	}
	return commandErr
}

func buildNativeConfig(opt options) (flowch.NativeConfig, error) {
	if strings.TrimSpace(opt.address) == "" || strings.TrimSpace(opt.user) == "" {
		return flowch.NativeConfig{}, errors.New("ClickHouse address and user are required")
	}
	if opt.dialTimeout <= 0 || opt.readTimeout <= 0 {
		return flowch.NativeConfig{}, errors.New("ClickHouse timeouts must be positive")
	}
	password := ""
	var err error
	if opt.passwordFile != "" {
		password, err = flowstream.ReadSecretFile(opt.passwordFile)
		if err != nil {
			return flowch.NativeConfig{}, fmt.Errorf("load ClickHouse password: %w", err)
		}
	}
	tlsConfig, err := (flowstream.TLSConfig{
		Enabled: opt.tlsEnabled, CAFile: opt.caFile, CertFile: opt.certFile,
		KeyFile: opt.keyFile, ServerName: opt.serverName,
	}).ClientConfig()
	if err != nil {
		return flowch.NativeConfig{}, fmt.Errorf("build ClickHouse TLS configuration: %w", err)
	}
	return flowch.NativeConfig{
		Address: strings.TrimSpace(opt.address), Database: "default", User: strings.TrimSpace(opt.user), Password: password,
		ClientName: "watchdog-flow-migrate", DialTimeout: opt.dialTimeout, ReadTimeout: opt.readTimeout,
		MaxConns: 1, MinConns: 1, TLS: tlsConfig,
	}, nil
}

func newLockOwner() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate migration lock owner: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}
