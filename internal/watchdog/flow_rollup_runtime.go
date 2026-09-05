package watchdog

import (
	"context"
	"fmt"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowstream"
)

func newFlowRollupRuntime(ctx context.Context, store *MySQLStore, config FlowRollupConfig) (*flowch.NativeInserter, *flowch.RollupRunner, *FlowRollupService, error) {
	tlsConfig, err := (flowstream.TLSConfig{
		Enabled: config.ClickHouseTLS, CAFile: config.ClickHouseCAFile,
		CertFile: config.ClickHouseCertFile, KeyFile: config.ClickHouseKeyFile,
		ServerName: config.ClickHouseServerName,
	}).ClientConfig()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("configure flow rollup ClickHouse TLS: %w", err)
	}
	password := ""
	if config.ClickHousePasswordFile != "" {
		password, err = flowstream.ReadSecretFile(config.ClickHousePasswordFile)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read flow rollup ClickHouse password: %w", err)
		}
	}
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: config.ClickHouseAddress, Database: config.ClickHouseDatabase,
		User: config.ClickHouseUser, Password: password, ClientName: "watchdog-flow-rollup",
		DialTimeout: config.ClickHouseDialTimeout, ReadTimeout: config.ClickHouseReadTimeout, OperationTimeout: config.ClickHouseOperationTimeout,
		MaxConns: int32(config.ClickHouseMaxConns), MinConns: int32(config.ClickHouseMinConns), TLS: tlsConfig,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	runner, err := flowch.NewRollupRunner(native)
	if err != nil {
		native.Close()
		return nil, nil, nil, err
	}
	scheduler, err := NewFlowRollupScheduler(store, FlowRollupScheduleConfig{
		LateArrivalWindow: config.LateArrivalWindow, BootstrapLookback: config.BootstrapLookback,
		MaxBucketsPerSeriesScan: config.MaxBucketsPerSeriesScan, MaxBucketsPerScan: config.MaxBucketsPerScan,
	})
	if err != nil {
		native.Close()
		return nil, nil, nil, err
	}
	service := &FlowRollupService{
		Scheduler: scheduler, Tenants: store, Interval: config.ScanInterval,
		MaxTenantsPerScan: config.MaxTenantsPerScan,
	}
	return native, runner, service, nil
}
