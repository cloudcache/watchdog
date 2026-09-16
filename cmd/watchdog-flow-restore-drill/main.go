// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
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

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
)

type options struct {
	address, user, passwordFile                string
	tlsEnabled                                 bool
	caFile, certFile, keyFile, serverName      string
	dialTimeout, readTimeout, operationTimeout time.Duration
	sourceDatabase, restoreDatabase            string
	backupDisk, backupName, backupManifestFile string
	sourceDate                                 string
	keepRestoreDatabase                        bool
}

type restoreDrillService func(context.Context, flowch.NativeConfig, flowch.RestoreDrillRequest) (flowch.RestoreDrillResult, error)

func main() {
	var opt options
	flag.StringVar(&opt.address, "clickhouse-address", "127.0.0.1:9000", "ClickHouse native TCP address")
	flag.StringVar(&opt.user, "clickhouse-user", "default", "ClickHouse user")
	flag.StringVar(&opt.passwordFile, "clickhouse-password-file", "", "file containing the ClickHouse password")
	flag.DurationVar(&opt.dialTimeout, "clickhouse-dial-timeout", 3*time.Second, "ClickHouse connection timeout")
	flag.DurationVar(&opt.readTimeout, "clickhouse-read-timeout", 30*time.Second, "ClickHouse packet polling interval")
	flag.DurationVar(&opt.operationTimeout, "clickhouse-operation-timeout", time.Hour, "maximum duration of the restore operation")
	flag.BoolVar(&opt.tlsEnabled, "clickhouse-tls", false, "enable ClickHouse TLS")
	flag.StringVar(&opt.caFile, "clickhouse-tls-ca", "", "ClickHouse TLS CA file")
	flag.StringVar(&opt.certFile, "clickhouse-tls-cert", "", "ClickHouse TLS client certificate file")
	flag.StringVar(&opt.keyFile, "clickhouse-tls-key", "", "ClickHouse TLS client key file")
	flag.StringVar(&opt.serverName, "clickhouse-tls-server-name", "", "ClickHouse TLS server name")
	flag.StringVar(&opt.sourceDatabase, "source-database", "watchdog_flow", "database name stored in the backup")
	flag.StringVar(&opt.restoreDatabase, "restore-database", "", "new isolated database name (must start with watchdog_restore_)")
	flag.StringVar(&opt.backupDisk, "backup-disk", "flow_backups", "ClickHouse named backup disk")
	flag.StringVar(&opt.backupName, "backup-name", "", "relative backup object name on the named disk")
	flag.StringVar(&opt.backupManifestFile, "backup-manifest-file", "", "local path to the backup's .backup manifest")
	flag.StringVar(&opt.sourceDate, "source-date", "", "UTC raw partition date (YYYY-MM-DD)")
	flag.BoolVar(&opt.keepRestoreDatabase, "keep-restore-database", false, "retain the isolated restore database after verification")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opt, flowch.RunRestoreDrill, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, opt options, service restoreDrillService, output io.Writer) error {
	if ctx == nil || service == nil || output == nil {
		return errors.New("restore drill context, service, and output are required")
	}
	config, request, err := buildRestoreDrillInput(opt)
	if err != nil {
		return err
	}
	result, drillErr := service(ctx, config, request)
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("encode restore drill result: %w", err)
	}
	return drillErr
}

func buildRestoreDrillInput(opt options) (flowch.NativeConfig, flowch.RestoreDrillRequest, error) {
	if strings.TrimSpace(opt.address) == "" || strings.TrimSpace(opt.user) == "" ||
		opt.dialTimeout <= 0 || opt.readTimeout <= 0 || opt.operationTimeout <= 0 {
		return flowch.NativeConfig{}, flowch.RestoreDrillRequest{}, errors.New("valid ClickHouse connection settings are required")
	}
	day, err := time.Parse(time.DateOnly, strings.TrimSpace(opt.sourceDate))
	if err != nil {
		return flowch.NativeConfig{}, flowch.RestoreDrillRequest{}, errors.New("source-date must use YYYY-MM-DD")
	}
	password := ""
	if strings.TrimSpace(opt.passwordFile) != "" {
		password, err = flowstream.ReadSecretFile(strings.TrimSpace(opt.passwordFile))
		if err != nil {
			return flowch.NativeConfig{}, flowch.RestoreDrillRequest{}, fmt.Errorf("load ClickHouse password: %w", err)
		}
	}
	tlsConfig, err := (flowstream.TLSConfig{
		Enabled: opt.tlsEnabled, CAFile: opt.caFile, CertFile: opt.certFile,
		KeyFile: opt.keyFile, ServerName: opt.serverName,
	}).ClientConfig()
	if err != nil {
		return flowch.NativeConfig{}, flowch.RestoreDrillRequest{}, fmt.Errorf("build ClickHouse TLS configuration: %w", err)
	}
	config := flowch.NativeConfig{
		Address: strings.TrimSpace(opt.address), User: strings.TrimSpace(opt.user), Password: password,
		DialTimeout: opt.dialTimeout, ReadTimeout: opt.readTimeout, OperationTimeout: opt.operationTimeout,
		MaxConns: 1, MinConns: 1, TLS: tlsConfig,
	}
	request := flowch.RestoreDrillRequest{
		SourceDatabase: strings.TrimSpace(opt.sourceDatabase), RestoreDatabase: strings.TrimSpace(opt.restoreDatabase),
		BackupDisk: strings.TrimSpace(opt.backupDisk), BackupName: strings.TrimSpace(opt.backupName),
		BackupManifestFile: strings.TrimSpace(opt.backupManifestFile), SourceDate: day,
		KeepRestoreDatabase: opt.keepRestoreDatabase,
	}
	return config, request, nil
}
