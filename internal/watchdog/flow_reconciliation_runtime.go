// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowmetrics"
	"github.com/cloudcache/watchdog/internal/flowstream"
)

func newFlowReconciliationRuntime(ctx context.Context, config FlowReconciliationConfig, native *flowch.NativeInserter) (*flowstream.CommittedOffsetReader, *flowch.ReconciliationScanner, *flowmetrics.Reconciliation, error) {
	password := ""
	var err error
	if config.KafkaSASLPasswordFile != "" {
		password, err = flowstream.ReadSecretFile(config.KafkaSASLPasswordFile)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read Flow reconciliation Kafka SASL password: %w", err)
		}
	}
	reader, err := flowstream.NewCommittedOffsetReader(flowstream.KafkaConfig{
		Brokers: config.KafkaBrokers, Topic: config.KafkaTopic, ClientID: config.KafkaClientID,
		TLS: flowstream.TLSConfig{Enabled: config.KafkaTLS, CAFile: config.KafkaCAFile, CertFile: config.KafkaCertFile,
			KeyFile: config.KafkaKeyFile, ServerName: config.KafkaServerName},
		SASL: flowstream.SASLConfig{Mechanism: flowstream.SASLMechanism(config.KafkaSASLMechanism),
			Username: config.KafkaSASLUsername, Password: password},
	})
	if err != nil {
		return nil, nil, nil, err
	}
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = reader.Ready(readyCtx)
	cancel()
	if err != nil {
		reader.Close()
		return nil, nil, nil, fmt.Errorf("connect Flow reconciliation Kafka: %w", err)
	}
	scanner, err := flowch.NewReconciliationScanner(native)
	if err != nil {
		reader.Close()
		return nil, nil, nil, err
	}
	return reader, scanner, flowmetrics.NewReconciliation(), nil
}

func flowReconciliationRuntimeJobConfig(config FlowReconciliationConfig) flowReconciliationJobConfig {
	bootstrap := make(map[int32]uint64, len(config.BootstrapOffsets))
	for partition, offset := range config.BootstrapOffsets {
		bootstrap[partition] = offset
	}
	return flowReconciliationJobConfig{
		SourceStreamID: config.SourceStreamID, KafkaTopic: config.KafkaTopic, ConsumerGroup: config.KafkaConsumerGroup,
		BootstrapOffset: bootstrap, MaxBatches: config.MaxBatches, MaxFactRows: config.MaxFactRows, MaxReadBytes: config.MaxReadBytes,
	}
}

func ensureFlowReconciliationSchedule(ctx context.Context, store *MySQLStore, config FlowReconciliationConfig) error {
	payload, err := encodeFlowReconciliationJobPayload(flowReconciliationRuntimeJobConfig(config))
	if err != nil {
		return err
	}
	schedule, err := store.EnsureSystemOperationJobSchedule(ctx, OperationJobSchedule{
		ScopeType: OperationJobScopeSystem, Name: "Flow Kafka→ClickHouse reconciliation",
		JobType: FlowReconciliationJobType, PartitionKey: config.SourceStreamID,
		CronExpression: config.ScheduleCron, Timezone: "UTC", PayloadJSON: payload,
		Enabled: true, MaxInflight: 1,
	})
	if err != nil {
		return err
	}
	return store.DisableSystemOperationJobSchedulesExcept(ctx, FlowReconciliationJobType, schedule.ID)
}
