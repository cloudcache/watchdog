package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/opjob"
)

const flowReconciliationClientID = "watchdog-flow-reconciliation"

func flowReconciliationPayload(cfg Config) (flowlifecycle.ReconciliationConfig, error) {
	reconciliation := cfg.Flow.Reconciliation
	payload := flowlifecycle.ReconciliationConfig{
		SourceStreamID:  strings.TrimSpace(reconciliation.SourceStreamID),
		KafkaTopic:      strings.TrimSpace(cfg.Kafka.Topic),
		ConsumerGroup:   strings.TrimSpace(cfg.Kafka.ConsumerGroup),
		BootstrapOffset: reconciliation.BootstrapOffsets,
		MaxBatches:      reconciliation.MaxBatches,
		MaxFactRows:     reconciliation.MaxFactRows,
		MaxReadBytes:    reconciliation.MaxReadBytes,
	}
	if err := payload.Validate(); err != nil {
		return flowlifecycle.ReconciliationConfig{}, err
	}
	return payload, nil
}

func flowReconciliationOperationJob(cfg Config, now time.Time) (opjob.Job, error) {
	payloadConfig, err := flowReconciliationPayload(cfg)
	if err != nil {
		return opjob.Job{}, err
	}
	interval := cfg.Flow.Reconciliation.Interval
	if interval < time.Minute || now.IsZero() {
		return opjob.Job{}, errors.New("Flow reconciliation interval and schedule time are invalid")
	}
	payload, err := flowlifecycle.EncodeReconciliationPayload(payloadConfig)
	if err != nil {
		return opjob.Job{}, err
	}
	requestHash := sha256hex(string(payload))
	bucket := now.UTC().Unix() / int64(interval/time.Second)
	return opjob.Job{
		JobType:        flowlifecycle.ReconciliationJobType,
		IdempotencyKey: fmt.Sprintf("flow-reconcile:%s:%d", requestHash[:16], bucket),
		RequestHash:    requestHash,
		CheckpointJSON: payload,
	}, nil
}

func (s *Server) startFlowReconciliation() error {
	if !s.cfg.Flow.Reconciliation.Enabled {
		return nil
	}
	if s.jobs == nil || s.db == nil {
		return errors.New("operation job store is not initialized")
	}
	if s.clickHouseBatch == nil {
		return errors.New("ClickHouse is unavailable")
	}

	payloadConfig, err := flowReconciliationPayload(s.cfg)
	if err != nil {
		return err
	}
	password := ""
	if path := strings.TrimSpace(s.cfg.Kafka.SASLPasswordFile); path != "" {
		password, err = flowstream.ReadSecretFile(path)
		if err != nil {
			return fmt.Errorf("read Kafka SASL password: %w", err)
		}
	}
	reader, err := flowstream.NewCommittedOffsetReader(flowstream.KafkaConfig{
		Brokers:  s.cfg.Kafka.Brokers,
		Topic:    payloadConfig.KafkaTopic,
		ClientID: flowReconciliationClientID,
		SASL: flowstream.SASLConfig{
			Mechanism: flowstream.SASLMechanism(strings.TrimSpace(s.cfg.Kafka.SASLMechanism)),
			Username:  strings.TrimSpace(s.cfg.Kafka.SASLUsername),
			Password:  password,
		},
	})
	if err != nil {
		return err
	}
	readyContext, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = reader.Ready(readyContext)
	readyCancel()
	if err != nil {
		reader.Close()
		return err
	}
	scanner, err := flowch.NewReconciliationScanner(s.clickHouseBatch)
	if err != nil {
		reader.Close()
		return err
	}

	workerContext, cancel := context.WithCancel(context.Background())
	s.flowReconciliationCancel = func() {
		cancel()
		reader.Close()
	}
	worker := &opjob.Worker{
		Repo: s.jobs, JobType: flowlifecycle.ReconciliationJobType,
		Owner: "watchdog-server/flow-reconciliation",
		Handler: flowlifecycle.NewReconciliationHandler(
			reader, scanner, flowlifecycle.NewStore(s.db),
		),
		Logf: log.Printf,
	}
	go worker.Run(workerContext)
	go s.scheduleFlowReconciliation(workerContext)
	return nil
}

func (s *Server) scheduleFlowReconciliation(ctx context.Context) {
	interval := s.cfg.Flow.Reconciliation.Interval
	enqueue := func(at time.Time) {
		job, err := flowReconciliationOperationJob(s.cfg, at)
		if err == nil {
			_, err = s.jobs.Enqueue(ctx, job)
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("schedule Flow reconciliation: %v", err)
		}
	}
	enqueue(time.Now())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-ticker.C:
			enqueue(at)
		}
	}
}
