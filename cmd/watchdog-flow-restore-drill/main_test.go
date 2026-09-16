// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

func validOptions() options {
	return options{
		address: "127.0.0.1:9000", user: "default", dialTimeout: time.Second,
		readTimeout: time.Second, operationTimeout: time.Minute,
		sourceDatabase: "watchdog_flow", restoreDatabase: "watchdog_restore_test",
		backupDisk: "flow_backups", backupName: "raw/2026-09-16",
		backupManifestFile: "/backup/.backup", sourceDate: "2026-09-16",
	}
}

func TestBuildRestoreDrillInput(t *testing.T) {
	config, request, err := buildRestoreDrillInput(validOptions())
	if err != nil {
		t.Fatal(err)
	}
	if config.Address != "127.0.0.1:9000" || config.MaxConns != 1 || request.SourceDate.Format(time.DateOnly) != "2026-09-16" || request.BackupName != "raw/2026-09-16" {
		t.Fatalf("config=%+v request=%+v", config, request)
	}
	invalid := validOptions()
	invalid.sourceDate = "today"
	if _, _, err := buildRestoreDrillInput(invalid); err == nil {
		t.Fatal("invalid source date accepted")
	}
}

func TestRunEmitsEvidenceWhenDrillFails(t *testing.T) {
	wantErr := errors.New("evidence mismatch")
	service := func(_ context.Context, _ flowch.NativeConfig, request flowch.RestoreDrillRequest) (flowch.RestoreDrillResult, error) {
		return flowch.RestoreDrillResult{BackupRef: request.BackupName, Matched: false}, wantErr
	}
	var output bytes.Buffer
	err := run(context.Background(), validOptions(), service, &output)
	if !errors.Is(err, wantErr) || !strings.Contains(output.String(), `"matched": false`) || !strings.Contains(output.String(), `"backup_ref": "raw/2026-09-16"`) {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
}
