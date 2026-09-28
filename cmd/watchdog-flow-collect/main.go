// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/flowmetrics"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/google/uuid"
)

type options struct {
	planFile, planPublicKey                       string
	brokers, topic, clientID, compression         string
	kafkaCAFile, kafkaCertFile, kafkaKeyFile      string
	kafkaServerName, saslMechanism, saslUsername  string
	saslPasswordFile, sflowListen, netflowListen  string
	metricsListen                                 string
	agentControlURL, agentID, agentTokenFile      string
	agentPublicKey, agentLKG string
	queueSize, sockets, receiveBuffer, maxUDP     int
	kafkaTLS, check, agentPlanCheck               bool
}

func main() {
	var opt options
	flag.StringVar(&opt.planFile, "plan", "", "signed collector plan file")
	flag.StringVar(&opt.planPublicKey, "plan-public-key", "", "Ed25519 plan public key file")
	flag.StringVar(&opt.brokers, "kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	flag.StringVar(&opt.topic, "kafka-topic", "watchdog.flow.raw", "RawFlow Kafka topic base; schema suffix is automatic")
	flag.StringVar(&opt.clientID, "kafka-client-id", "watchdog-flow-collect", "Kafka client ID")
	flag.IntVar(&opt.queueSize, "kafka-queue-size", 65536, "maximum buffered Kafka records")
	flag.StringVar(&opt.compression, "kafka-compression", "lz4", "none, gzip, snappy, lz4 or zstd")
	flag.BoolVar(&opt.kafkaTLS, "kafka-tls", false, "enable Kafka TLS")
	flag.StringVar(&opt.kafkaCAFile, "kafka-tls-ca", "", "Kafka TLS CA file")
	flag.StringVar(&opt.kafkaCertFile, "kafka-tls-cert", "", "Kafka TLS client certificate file")
	flag.StringVar(&opt.kafkaKeyFile, "kafka-tls-key", "", "Kafka TLS client key file")
	flag.StringVar(&opt.kafkaServerName, "kafka-tls-server-name", "", "Kafka TLS server name")
	flag.StringVar(&opt.saslMechanism, "kafka-sasl-mechanism", "none", "none, plain, scram-sha-256 or scram-sha-512")
	flag.StringVar(&opt.saslUsername, "kafka-sasl-username", "", "Kafka SASL username")
	flag.StringVar(&opt.saslPasswordFile, "kafka-sasl-password-file", "", "file containing the Kafka SASL password")
	flag.StringVar(&opt.sflowListen, "sflow-listen", ":6343", "sFlow UDP listen address; empty disables it")
	flag.StringVar(&opt.netflowListen, "netflow-listen", ":2055", "NetFlow/IPFIX UDP listen address; empty disables it")
	flag.StringVar(&opt.metricsListen, "metrics-listen", "127.0.0.1:9090", "Prometheus metrics listen address; empty disables it")
	flag.IntVar(&opt.sockets, "sockets", 1, "SO_REUSEPORT sockets per enabled listener")
	flag.IntVar(&opt.receiveBuffer, "receive-buffer-bytes", 32<<20, "UDP socket receive buffer")
	flag.IntVar(&opt.maxUDP, "max-datagram-bytes", 65535, "maximum accepted UDP datagram size")
	flag.StringVar(&opt.agentControlURL, "control-plane-url", "", "Watchdog API base URL for agent management")
	flag.StringVar(&opt.agentID, "agent-id", "watchdog-flow-collect", "registered flow_collect agent ID")
	flag.StringVar(&opt.agentTokenFile, "agent-token-file", "", "file containing the flow_collect machine token")
	flag.StringVar(&opt.agentPublicKey, "agent-plan-public-key", "", "agent plan Ed25519 public key file")
	flag.StringVar(&opt.agentLKG, "agent-plan-lkg", "", "durable agent plan LKG file")
	flag.BoolVar(&opt.agentPlanCheck, "agent-plan-check", false, "register/sync/apply the agent plan, then exit")
	flag.BoolVar(&opt.check, "check", false, "validate configuration and signed plan, then exit")
	flag.Parse()

	if err := run(opt); err != nil {
		log.Fatal(err)
	}
}

func run(opt options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	agentLifecycle := make(chan error, 1)
	var agentToken string
	var agentPlanVersion uint64
	runtimeConfig := flowCollectAgentRuntime(opt)
	if runtimeConfig.Enabled() {
		result, token, err := runtimeConfig.Sync(ctx, func(_ context.Context, spec agentplan.Spec) error {
			return applyFlowCollectAgentPlan(&opt, spec)
		})
		if errors.Is(err, agentplan.ErrUnauthorized) && !opt.agentPlanCheck {
			log.Printf("flow-collect agent credential is revoked; stopping without restart")
			return nil
		}
		if err != nil && (!errors.Is(err, agentplan.ErrNoPlan) || opt.agentPlanCheck) {
			return err
		}
		appliedPlanVersion := result.Envelope.Metadata.PlanVersion
		if errors.Is(err, agentplan.ErrNoPlan) {
			log.Printf("flow-collect registered without a desired agent plan; using bootstrap values until a plan is published")
		} else {
			log.Printf("flow-collect agent plan applied: version=%d source=%s", appliedPlanVersion, result.Source)
			if result.AckError != nil {
				return result.AckError
			}
			if opt.agentPlanCheck {
				return nil
			}
		}
		agentToken = token
		agentPlanVersion = appliedPlanVersion
		go runtimeConfig.RunHeartbeats(ctx, token, 30*time.Second, appliedPlanVersion, func(err error) {
			log.Printf("flow-collect agent heartbeat: %v", err)
			if errors.Is(err, agentplan.ErrUnauthorized) || errors.Is(err, agentplan.ErrPlanChanged) {
				select {
				case agentLifecycle <- err:
				default:
				}
				stop()
			}
		})
	} else if opt.agentPlanCheck {
		return errors.New("-control-plane-url is required with -agent-plan-check")
	}
	if opt.planFile == "" || opt.planPublicKey == "" {
		return errors.New("-plan and -plan-public-key are required")
	}
	if opt.sockets < 1 || opt.sockets > 128 || opt.receiveBuffer < 1 || opt.maxUDP < 512 || opt.maxUDP > 65535 {
		return errors.New("sockets must be 1..128, receive buffer positive, and max datagram 512..65535")
	}
	if strings.TrimSpace(opt.sflowListen) == "" && strings.TrimSpace(opt.netflowListen) == "" {
		return errors.New("at least one flow listener is required")
	}
	if err := flowmetrics.ValidateListenAddress(opt.metricsListen); err != nil {
		return err
	}
	registry, err := flowplan.LoadSignedPlan(opt.planFile, opt.planPublicKey, time.Now().UTC())
	if err != nil {
		return err
	}
	producerConfig, err := buildProducerConfig(opt)
	if err != nil {
		return err
	}
	plan := registry.Plan()
	planLoadedAt := time.Now().UTC()
	if opt.check {
		log.Printf("flow-collect configuration valid: collector=%s plan_revision=%d sources=%d", plan.CollectorID, plan.Revision, len(plan.Sources))
		return nil
	}

	producer, err := flowstream.NewProducer(producerConfig)
	if err != nil {
		return err
	}
	// closeProducer flushes and closes the Kafka producer exactly once. It backs
	// both the early-error defer below and the concurrent close in the shutdown
	// path; sync.Once makes the two callers safe.
	var closeProducerOnce sync.Once
	closeProducer := func() {
		closeProducerOnce.Do(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := producer.Close(closeCtx); err != nil {
				log.Printf("flush RawFlow Kafka producer: %v", err)
			}
		})
	}
	defer closeProducer()
	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	err = producer.Ping(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("connect RawFlow Kafka: %w", err)
	}
	verifyCtx, cancelVerify := context.WithTimeout(ctx, 10*time.Second)
	err = producer.VerifyTopic(verifyCtx)
	cancelVerify()
	if err != nil {
		return fmt.Errorf("verify RawFlow Kafka topic: %w", err)
	}
	inlet, err := flowstream.NewInlet(producer)
	if err != nil {
		return err
	}
	runtimeMetrics, err := flowmetrics.NewCollector(plan.Revision, planLoadedAt, producer.Stats)
	if err != nil {
		return err
	}
	metricsServer, err := startMetricsServer(opt.metricsListen, runtimeMetrics.Handler())
	if err != nil {
		return fmt.Errorf("start flow-collect metrics: %w", err)
	}
	defer shutdownMetricsServer(metricsServer, "flow-collect")

	type listener struct {
		id        string
		address   string
		decoder   flowpb.RawFlow_Decoder
		protocols []flowplan.Protocol
	}
	listeners := []listener{
		{id: "sflow", address: strings.TrimSpace(opt.sflowListen), decoder: flowpb.RawFlow_DECODER_SFLOW, protocols: []flowplan.Protocol{flowplan.ProtocolSFlow5}},
		{id: "netflow", address: strings.TrimSpace(opt.netflowListen), decoder: flowpb.RawFlow_DECODER_NETFLOW, protocols: []flowplan.Protocol{flowplan.ProtocolNetFlow5, flowplan.ProtocolNetFlow9, flowplan.ProtocolIPFIX}},
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var metricsErrCh <-chan error
	if metricsServer != nil {
		result := make(chan error, 1)
		go func() { result <- metricsServer.Wait() }()
		metricsErrCh = result
		log.Printf("flow-collect metrics listening: address=%s", metricsServer.Address())
	}
	errCh := make(chan error, len(listeners)*opt.sockets)
	running := 0
	for _, spec := range listeners {
		if spec.address == "" {
			continue
		}
		for socket := 0; socket < opt.sockets; socket++ {
			receiver := &flowstream.Receiver{
				ListenAddr: spec.address, CollectorID: plan.CollectorID,
				ListenerID: spec.id, RegistryVersion: plan.Revision, Decoder: spec.decoder,
				ReceiveBufferBytes: opt.receiveBuffer, MaxDatagramBytes: opt.maxUDP,
				ReusePort: opt.sockets > 1, Sender: inlet,
				Admit: func(protocols []flowplan.Protocol) func(netip.AddrPort) bool {
					return func(source netip.AddrPort) bool { return registry.AdmitSourceFamily(protocols, source.Addr()) }
				}(spec.protocols),
				OnReceived: runtimeMetrics.ObserveReceived,
				OnRejected: runtimeMetrics.ObserveRejected,
				OnInvalid:  runtimeMetrics.ObserveInvalid,
				OnOversize: runtimeMetrics.ObserveOversize,
				OnKernelDrops: func(count uint64) {
					runtimeMetrics.ObserveKernelDrops(count)
				},
				OnReceiveBuffer: func(requested, effective int) {
					if effective < requested {
						log.Printf("flow-collect socket receive buffer clamped: listener=%s requested=%d effective=%d; raise net.core.rmem_max (see deploy/sysctl.d/90-watchdog-flow.conf)", spec.id, requested, effective)
					}
				},
			}
			running++
			go func() { errCh <- receiver.Run(runCtx) }()
		}
	}
	log.Printf("flow-collect started: collector=%s revision=%d listeners=%d sockets_per_listener=%d topic=%s-v1", plan.CollectorID, plan.Revision, running/opt.sockets, opt.sockets, producerConfig.Kafka.Topic)
	if runtimeConfig.Enabled() && agentToken != "" {
		previous := runtimeMetrics.Snapshot()
		windowStarted := time.Now()
		go runtimeConfig.RunReports(runCtx, agentToken, time.Minute, func() agentplan.RunReport {
			snap := runtimeMetrics.Snapshot()
			now := time.Now()
			report := agentplan.RunReport{
				StartedAt:  windowStarted,
				EndedAt:    now,
				DurationMS: uint64(now.Sub(windowStarted).Milliseconds()),
				Summary: map[string]any{
					"plan_version":     agentPlanVersion,
					"plan_revision":    plan.Revision,
					"sflow_received":   flowCounterDelta(snap.SFlowReceived, previous.SFlowReceived),
					"netflow_received": flowCounterDelta(snap.NetFlowReceived, previous.NetFlowReceived),
					"rejected":         flowCounterDelta(snap.Rejected, previous.Rejected),
					"invalid":          flowCounterDelta(snap.Invalid, previous.Invalid),
					"oversize":         flowCounterDelta(snap.Oversize, previous.Oversize),
					"kernel_drops":     flowCounterDelta(snap.KernelDrops, previous.KernelDrops),
					"kafka_records":    flowCounterDelta(snap.KafkaRecords, previous.KafkaRecords),
					"kafka_bytes":      flowCounterDelta(snap.KafkaBytes, previous.KafkaBytes),
					"kafka_errors":     flowCounterDelta(snap.KafkaErrors, previous.KafkaErrors),
					"kafka_buffered":   snap.BufferedRecords,
				},
			}
			previous = snap
			windowStarted = now
			return report
		}, func(err error) {
			log.Printf("flow-collect agent run report: %v", err)
		})
	}
	var first error
	receiverResults := 0
	select {
	case first = <-errCh:
		receiverResults = 1
	case metricsErr := <-metricsErrCh:
		first = fmt.Errorf("flow-collect metrics server: %w", metricsErr)
	case <-ctx.Done():
	}
	cancelRun()
	// A receiver can be parked in a full-buffer Produce, which uses a background
	// context and only unblocks when the producer client is closed. Close the
	// producer concurrently with the receiver join — not via the deferred close,
	// which runs only after this function returns. Otherwise the join waits on a
	// receiver that waits on a close that never happens: a shutdown deadlock
	// until SIGKILL, which drops the whole buffer instead of flushing it. The
	// Flush inside Close drains the buffer and releases the parked receivers; we
	// wait for both the join and the close before returning so the flush is not
	// truncated by process exit.
	closeDone := make(chan struct{})
	go func() {
		closeProducer()
		close(closeDone)
	}()
	for count := receiverResults; count < running; count++ {
		first = errors.Join(first, <-errCh)
	}
	<-closeDone
	select {
	case lifecycleErr := <-agentLifecycle:
		if errors.Is(lifecycleErr, agentplan.ErrPlanChanged) {
			return errors.Join(first, lifecycleErr)
		}
		return first
	default:
	}
	return first
}

func flowCounterDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

type flowCollectPlanConfig struct {
	SFlowListen        *string `json:"sflow_listen"`
	NetFlowListen      *string `json:"netflow_listen"`
	Sockets            *int    `json:"sockets"`
	ReceiveBufferBytes *int    `json:"receive_buffer_bytes"`
	MaxDatagramBytes   *int    `json:"max_datagram_bytes"`
}

func applyFlowCollectAgentPlan(opt *options, spec agentplan.Spec) error {
	var config flowCollectPlanConfig
	if err := agentplan.DecodeConfig(spec, &config); err != nil {
		return err
	}
	if config.SFlowListen != nil {
		opt.sflowListen = strings.TrimSpace(*config.SFlowListen)
	}
	if config.NetFlowListen != nil {
		opt.netflowListen = strings.TrimSpace(*config.NetFlowListen)
	}
	if config.Sockets != nil {
		opt.sockets = *config.Sockets
	}
	if config.ReceiveBufferBytes != nil {
		opt.receiveBuffer = *config.ReceiveBufferBytes
	}
	if config.MaxDatagramBytes != nil {
		opt.maxUDP = *config.MaxDatagramBytes
	}
	if opt.sockets < 1 || opt.sockets > 128 || opt.receiveBuffer < 1 || opt.maxUDP < 512 || opt.maxUDP > 65535 {
		return errors.New("agent plan flow-collect socket limits are invalid")
	}
	if opt.sflowListen == "" && opt.netflowListen == "" {
		return errors.New("agent plan must leave at least one flow listener enabled")
	}
	return nil
}

func flowCollectAgentRuntime(opt options) agentplan.RuntimeConfig {
	return agentplan.RuntimeConfig{
		BaseURL: opt.agentControlURL, AgentID: strings.TrimSpace(opt.agentID), Name: strings.TrimSpace(opt.agentID),
		Kind: "flow_collect", Role: "flow_collect", Mode: "push", SoftwareVersion: "watchdog-flow-collect-v1",
		APIVersion: "v1", Capabilities: []string{"flow.receive.netflow/v1", "flow.receive.sflow/v1"},
		TokenFile:     opt.agentTokenFile,
		PublicKeyFile: opt.agentPublicKey, LKGFile: opt.agentLKG, BootID: uuid.NewString(),
	}
}

func startMetricsServer(address string, handler http.Handler) (*flowmetrics.Server, error) {
	if strings.TrimSpace(address) == "" {
		return nil, nil
	}
	return flowmetrics.StartServer(address, handler)
}

func shutdownMetricsServer(server *flowmetrics.Server, processName string) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown %s metrics: %v", processName, err)
	}
}

func buildProducerConfig(opt options) (flowstream.ProducerConfig, error) {
	config := flowstream.DefaultProducerConfig()
	config.Kafka.Brokers = splitNonEmpty(opt.brokers)
	config.Kafka.Topic = strings.TrimSpace(opt.topic)
	config.Kafka.ClientID = strings.TrimSpace(opt.clientID)
	config.QueueSize = opt.queueSize
	config.Compression = strings.TrimSpace(opt.compression)
	config.Kafka.TLS = flowstream.TLSConfig{
		Enabled: opt.kafkaTLS, CAFile: strings.TrimSpace(opt.kafkaCAFile),
		CertFile: strings.TrimSpace(opt.kafkaCertFile), KeyFile: strings.TrimSpace(opt.kafkaKeyFile),
		ServerName: strings.TrimSpace(opt.kafkaServerName),
	}
	config.Kafka.SASL = flowstream.SASLConfig{
		Mechanism: flowstream.SASLMechanism(strings.TrimSpace(opt.saslMechanism)),
		Username:  strings.TrimSpace(opt.saslUsername),
	}
	if opt.saslPasswordFile != "" {
		password, err := flowstream.ReadSecretFile(opt.saslPasswordFile)
		if err != nil {
			return flowstream.ProducerConfig{}, err
		}
		config.Kafka.SASL.Password = password
	}
	if err := config.Validate(); err != nil {
		return flowstream.ProducerConfig{}, err
	}
	return config, nil
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}
