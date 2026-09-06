package watchdog

import (
	"context"
	"fmt"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
)

func newFlowClickHouseNative(ctx context.Context, config FlowRollupConfig) (*flowch.NativeInserter, error) {
	tlsConfig, err := (flowstream.TLSConfig{
		Enabled: config.ClickHouseTLS, CAFile: config.ClickHouseCAFile,
		CertFile: config.ClickHouseCertFile, KeyFile: config.ClickHouseKeyFile,
		ServerName: config.ClickHouseServerName,
	}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("configure Flow ClickHouse TLS: %w", err)
	}
	password := ""
	if config.ClickHousePasswordFile != "" {
		password, err = flowstream.ReadSecretFile(config.ClickHousePasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read Flow ClickHouse password: %w", err)
		}
	}
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: config.ClickHouseAddress, Database: config.ClickHouseDatabase,
		User: config.ClickHouseUser, Password: password, ClientName: "watchdog-flow-hub",
		DialTimeout: config.ClickHouseDialTimeout, ReadTimeout: config.ClickHouseReadTimeout, OperationTimeout: config.ClickHouseOperationTimeout,
		MaxConns: int32(config.ClickHouseMaxConns), MinConns: int32(config.ClickHouseMinConns), TLS: tlsConfig,
	})
	if err != nil {
		return nil, err
	}
	return native, nil
}

func newFlowRollupRuntime(store *MySQLStore, config FlowRollupConfig, native *flowch.NativeInserter) (*flowch.RollupRunner, *FlowRollupService, error) {
	runner, err := flowch.NewRollupRunner(native)
	if err != nil {
		return nil, nil, err
	}
	scheduler, err := NewFlowRollupScheduler(store, FlowRollupScheduleConfig{
		LateArrivalWindow: config.LateArrivalWindow, BootstrapLookback: config.BootstrapLookback,
		MaxBucketsPerSeriesScan: config.MaxBucketsPerSeriesScan, MaxBucketsPerScan: config.MaxBucketsPerScan,
	})
	if err != nil {
		return nil, nil, err
	}
	service := &FlowRollupService{
		Scheduler: scheduler, Tenants: store, Interval: config.ScanInterval,
		MaxTenantsPerScan: config.MaxTenantsPerScan,
	}
	return runner, service, nil
}
