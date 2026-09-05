// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousemigration "github.com/cloudcache/watchdog/deploy/migration/clickhouse"
	"github.com/cloudcache/watchdog/internal/flowch"
)

type fakeMigrationService struct {
	inspectResume bool
	applyOptions  flowch.MigrationApplyOptions
	unlockOwner   string
	err           error
}

func (f *fakeMigrationService) Inspect(_ context.Context, _ []flowch.Migration, resume bool) (flowch.MigrationInspection, error) {
	f.inspectResume = resume
	return flowch.MigrationInspection{Plan: flowch.MigrationPlan{
		Resume:  true,
		Pending: []flowch.Migration{{Version: 1, Name: "001_test.sql", Checksum: strings.Repeat("a", 64), Statements: []string{"SECRET SQL"}}},
	}}, f.err
}

func (f *fakeMigrationService) Apply(_ context.Context, _ []flowch.Migration, options flowch.MigrationApplyOptions) (flowch.MigrationRun, error) {
	f.applyOptions = options
	return flowch.MigrationRun{LockOwner: options.LockOwner}, f.err
}

func (f *fakeMigrationService) Unlock(_ context.Context, owner string) error {
	f.unlockOwner = owner
	return f.err
}

func TestExecuteCommandMapsExplicitLifecycleOperations(t *testing.T) {
	owner := strings.Repeat("a", 32)
	for _, test := range []struct {
		command string
		check   func(*testing.T, *fakeMigrationService, commandOutput)
	}{
		{command: "inspect", check: func(t *testing.T, service *fakeMigrationService, output commandOutput) {
			if !service.inspectResume || output.Inspection == nil {
				t.Fatalf("inspect output=%+v", output)
			}
		}},
		{command: "apply", check: func(t *testing.T, service *fakeMigrationService, output commandOutput) {
			if service.applyOptions.Resume || service.applyOptions.LockOwner != owner || output.Run == nil {
				t.Fatalf("apply output=%+v options=%+v", output, service.applyOptions)
			}
		}},
		{command: "resume", check: func(t *testing.T, service *fakeMigrationService, output commandOutput) {
			if !service.applyOptions.Resume || service.applyOptions.LockOwner != owner || output.Run == nil {
				t.Fatalf("resume output=%+v options=%+v", output, service.applyOptions)
			}
		}},
		{command: "unlock", check: func(t *testing.T, service *fakeMigrationService, output commandOutput) {
			if service.unlockOwner != owner || !output.Unlocked {
				t.Fatalf("unlock output=%+v owner=%q", output, service.unlockOwner)
			}
		}},
	} {
		t.Run(test.command, func(t *testing.T) {
			service := &fakeMigrationService{}
			var encoded bytes.Buffer
			if err := executeCommand(context.Background(), options{command: test.command, lockOwner: owner}, service, []flowch.Migration{{Version: 1}}, &encoded); err != nil {
				t.Fatal(err)
			}
			var output commandOutput
			if err := json.Unmarshal(encoded.Bytes(), &output); err != nil {
				t.Fatal(err)
			}
			test.check(t, service, output)
		})
	}
}

func TestExecuteCommandPreservesDiagnosticOutputOnFailure(t *testing.T) {
	service := &fakeMigrationService{err: errors.New("dirty")}
	var encoded bytes.Buffer
	err := executeCommand(context.Background(), options{command: "inspect"}, service, nil, &encoded)
	if err == nil || encoded.Len() == 0 || strings.Contains(encoded.String(), "SECRET SQL") {
		t.Fatalf("error=%v output=%q", err, encoded.String())
	}
}

func TestBuildNativeConfigUsesDefaultDatabaseAndSecretFile(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "clickhouse-password")
	if err := os.WriteFile(secret, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := buildNativeConfig(options{
		address: "127.0.0.1:9000", user: "default", passwordFile: secret,
		dialTimeout: time.Second, readTimeout: 2 * time.Second, operationTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Database != "default" || config.Password != "secret" || config.ClientName != "watchdog-flow-migrate" || config.MaxConns != 1 || config.OperationTimeout != 3*time.Second {
		t.Fatalf("config=%+v", config)
	}
	hiddenTLS := options{address: "127.0.0.1:9000", user: "default", caFile: "ca.pem", dialTimeout: time.Second, readTimeout: time.Second, operationTimeout: time.Second}
	if _, err := buildNativeConfig(hiddenTLS); err == nil {
		t.Fatal("hidden TLS file was accepted")
	}
}

func TestEmbeddedMigrationSetAndGeneratedOwnerAreValid(t *testing.T) {
	migrations, err := flowch.LoadMigrations(clickhousemigration.Files, ".")
	if err != nil || len(migrations) != 5 {
		t.Fatalf("migrations=%d error=%v", len(migrations), err)
	}
	owner, err := newLockOwner()
	if err != nil || len(owner) != 32 {
		t.Fatalf("owner=%q error=%v", owner, err)
	}
}
