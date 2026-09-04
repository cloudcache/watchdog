package flowcollect

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrPlanRevisionUnavailable = errors.New("WAL record plan revision is not available")
)

type Runner struct {
	Config       Config
	Registry     *Registry
	Plans        *PlanHistory
	WAL          *WAL
	Decoder      *Decoder
	State        *CollectStateStore
	Attempts     *AttemptStore
	Publisher    Publisher
	OnError      func(error)
	Metrics      *Metrics
	Quality      *QualityTracker
	QualityState *QualityStateStore
	Runtime      *RuntimeState

	inflight        sync.Map
	retryNeeded     atomic.Bool
	metricsOnce     sync.Once
	qualityOnce     sync.Once
	runtimeOnce     sync.Once
	qualityBarrier  sync.RWMutex
	reportMu        sync.Mutex
	lastErrorReport time.Time
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Registry == nil || r.WAL == nil || r.Decoder == nil || r.State == nil || r.Attempts == nil || r.Publisher == nil || r.QualityState == nil {
		return errors.New("flow runner dependencies are required")
	}
	if r.Config.SocketCount <= 0 || r.Config.DecodeWorkers <= 0 || r.Config.DecodeQueueDatagrams <= 0 || r.Config.Diagnostics.DecodeMaxAttempts <= 0 || r.Config.Diagnostics.RetryInitial <= 0 || r.Config.Diagnostics.RetryMax < r.Config.Diagnostics.RetryInitial || r.Config.Diagnostics.AttemptJournalFsync <= 0 || r.Config.Diagnostics.AttemptCheckpointEvery <= 0 || r.Config.Diagnostics.AttemptJournalMaxBytes < attemptHeaderSize+attemptRecordSize || r.Config.Diagnostics.QuarantineQueueEvents <= 0 || r.Config.Diagnostics.QuarantineMaxEventsPerSecond <= 0 || r.Config.Diagnostics.QuarantineMaxEventsPerSourceSecond <= 0 || r.Config.Quality.StateTTL <= 0 || r.Config.Quality.AnomalyWindow <= 0 || r.Config.Quality.JournalFsync <= 0 || r.Config.Quality.CheckpointEvery <= 0 || r.Config.Quality.JournalMaxBytes <= qualityFrameHeaderSize || r.Config.Quality.MaxExporters <= 0 || r.Config.Quality.MaxDataSources <= 0 {
		return errors.New("flow runner socket, worker, queue, and retry limits must be positive")
	}
	metrics := r.metrics()
	r.WAL.SetMetrics(metrics)
	metrics.ReceiveQueueDepth.Store(0)
	metrics.DecodeQueueDepth.Store(0)
	metrics.QuarantineQueueDepth.Store(0)
	runtime := r.runtimeState()
	runtime.start(time.Now())
	defer runtime.stop()
	quality := r.quality()
	if r.QualityState.tracker != quality || r.QualityState.wal != r.WAL {
		return errors.New("flow runner quality-state store does not match its tracker or WAL")
	}
	if r.Attempts.wal != r.WAL || r.Attempts.collectorID != r.Registry.Plan().CollectorID {
		return errors.New("flow runner attempt store does not match its registry or WAL")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ingress := make(chan Datagram, r.Config.DecodeQueueDatagrams)
	quarantineQueue := make(chan *flowpb.QuarantineEvent, r.Config.Diagnostics.QuarantineQueueEvents)
	metrics.ReceiveQueueCapacity.Store(int64(cap(ingress)))
	metrics.QuarantineQueueCapacity.Store(int64(cap(quarantineQueue)))
	quarantineRate := newQuarantineLimiter(r.Config.Diagnostics.QuarantineMaxEventsPerSecond, r.Config.Diagnostics.QuarantineMaxEventsPerSourceSecond)
	decodeQueues := make([]chan WALRecord, r.Config.DecodeWorkers)
	queueCapacity := (r.Config.DecodeQueueDatagrams + r.Config.DecodeWorkers - 1) / r.Config.DecodeWorkers
	metrics.DecodeQueueCapacity.Store(int64(queueCapacity * len(decodeQueues)))
	fatal := make(chan error, 1)

	var workerWG sync.WaitGroup
	for index := range decodeQueues {
		decodeQueues[index] = make(chan WALRecord, queueCapacity)
		workerWG.Add(1)
		go func(queue <-chan WALRecord) { defer workerWG.Done(); r.decodeLoop(ctx, queue) }(decodeQueues[index])
	}
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.ingestLoop(ctx, ingress, quarantineQueue, quarantineRate, fatal) }()
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.quarantineLoop(ctx, quarantineQueue) }()
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.replayLoop(ctx, decodeQueues) }()
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.reclaimLoop(ctx) }()
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.qualityCheckpointLoop(ctx) }()
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.attemptCheckpointLoop(ctx) }()

	var receiverWG sync.WaitGroup
	for socket := 0; socket < r.Config.SocketCount; socket++ {
		for _, endpoint := range []struct {
			protocol Protocol
			address  string
			listener string
		}{{ProtocolSFlow5, r.Config.SFlowListen, "sflow"}, {0, r.Config.NetFlowListen, "netflow"}} {
			receiver := &Receiver{
				Protocol: endpoint.protocol, ListenAddr: endpoint.address, ReceiveBufferBytes: r.Config.ReceiveBufferBytes,
				MaxDatagramBytes: r.Config.MaxDatagramBytes, Queue: ingress, ReusePort: r.Config.SocketCount > 1, Listener: endpoint.listener,
				OnReceived: func(protocol Protocol) { metrics.recordReceived(protocol) },
				OnQueueFull: func(Protocol) {
					metrics.ReceiveQueueDrops.Add(1)
					if endpoint.listener == "sflow" {
						metrics.UDPQueueDropsSFlow.Add(1)
					} else {
						metrics.UDPQueueDropsNetFlow.Add(1)
					}
				},
				OnQueueDepth: func(depth int) { metrics.ReceiveQueueDepth.Store(int64(depth)) },
				OnInvalid: func(protocol Protocol) {
					metrics.InvalidDatagrams.Add(1)
					metrics.recordDecodeError(protocol, "invalid_datagram")
				},
				OnKernelDrops: func(listener string, drops uint64) {
					if listener == "sflow" {
						metrics.UDPKernelDropsSFlow.Add(drops)
					} else {
						metrics.UDPKernelDropsNetFlow.Add(drops)
					}
				},
			}
			receiverWG.Add(1)
			go func() {
				defer receiverWG.Done()
				if err := receiver.Run(ctx); err != nil {
					sendFatal(fatal, err)
				}
			}()
		}
	}

	var result error
	select {
	case <-ctx.Done():
	case result = <-fatal:
		cancel()
	}
	receiverWG.Wait()
	cancel()
	workerWG.Wait()
	return result
}

func (r *Runner) ingestLoop(ctx context.Context, ingress <-chan Datagram, quarantineQueue chan<- *flowpb.QuarantineEvent, quarantineRate *quarantineLimiter, fatal chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case datagram := <-ingress:
			r.metrics().ReceiveQueueDepth.Store(int64(len(ingress)))
			protocol, domain, err := InspectDatagram(datagram.Payload)
			if err != nil {
				datagram.Release()
				r.metrics().InvalidDatagrams.Add(1)
				r.metrics().recordDecodeError(datagram.Protocol, "invalid_datagram")
				continue
			}
			binding, admitted := r.Registry.Admit(protocol, datagram.Source.Addr(), domain)
			if !admitted {
				if quarantineRate.Allow(datagram.Source.Addr(), datagram.ReceivedAt) {
					event := BuildQuarantineEvent(datagram, r.Registry.Plan().CollectorID, protocol, domain)
					select {
					case quarantineQueue <- event:
						r.metrics().QuarantineQueueDepth.Store(int64(len(quarantineQueue)))
					default:
						r.metrics().QuarantineQueueDrops.Add(1)
					}
				} else {
					r.metrics().QuarantineRateLimited.Add(1)
				}
				datagram.Release()
				r.metrics().QuarantinedDatagrams.Add(1)
				continue
			}
			_, err = r.WAL.Append(WALInput{Protocol: protocol, ReceivedAt: datagram.ReceivedAt, Source: datagram.Source, ObservationDomainID: domain, RegistryVersion: r.Registry.plan.Revision, TenantID: binding.TenantID, ExporterID: binding.ExporterID, TargetID: binding.TargetID, DeviceID: binding.DeviceID, Payload: datagram.Payload})
			datagram.Release()
			if err != nil {
				if errors.Is(err, ErrWALHardLimit) {
					r.metrics().WALHardStops.Add(1)
				}
				sendFatal(fatal, err)
				return
			}
			r.metrics().WALAppended.Add(1)
		}
	}
}

func (r *Runner) decodeLoop(ctx context.Context, records <-chan WALRecord) {
	for {
		select {
		case <-ctx.Done():
			return
		case record := <-records:
			r.metrics().DecodeQueueDepth.Add(-1)
			for ctx.Err() == nil {
				generation, generationErr := r.Attempts.Generation(record.DatagramID)
				if generationErr != nil {
					r.runtimeState().observeAttemptJournal(generationErr, time.Now())
					r.report(generationErr)
					r.retryNeeded.Store(true)
					break
				}
				if generation > 0 {
					r.metrics().ReplayAttempts.Add(1)
				}
				err := r.processRecord(ctx, record, generation)
				if err == nil {
					r.metrics().DecodedDatagrams.Add(1)
					break
				}
				r.report(err)
				if generation == ^uint32(0) {
					r.report(errors.New("attempt generation exhausted uint32"))
					r.retryNeeded.Store(true)
					break
				}
				attempts := generation + 1
				attemptErr := r.Attempts.Advance(ctx, record.DatagramID, attempts)
				r.runtimeState().observeAttemptJournal(attemptErr, time.Now())
				if attemptErr != nil {
					r.report(attemptErr)
					r.retryNeeded.Store(true)
					break
				}
				if code, cause, permanent := permanentFailureDetails(err); permanent && attempts >= uint32(r.Config.Diagnostics.DecodeMaxAttempts) {
					if dlqErr := r.deadLetter(ctx, record, code, cause, attempts); dlqErr == nil {
						break
					} else {
						r.report(dlqErr)
					}
				}
				if errors.Is(err, ErrTemplatePending) {
					r.retryNeeded.Store(true)
					break
				}
				timer := time.NewTimer(retryBackoff(r.Config.Diagnostics, attempts))
				select {
				case <-ctx.Done():
					timer.Stop()
				case <-timer.C:
				}
			}
			r.inflight.Delete(record.DatagramID)
		}
	}
}

func (r *Runner) processRecord(ctx context.Context, record WALRecord, replayGeneration uint32) error {
	if err := r.WAL.WaitDurable(ctx, record); err != nil {
		return err
	}
	registry, available := r.registryForRevision(record.RegistryVersion)
	if !available {
		return fmt.Errorf("%w: revision %d", ErrPlanRevisionUnavailable, record.RegistryVersion)
	}
	plan := registry.Plan()
	if (!plan.NotBefore.IsZero() && record.ReceivedAt.Before(plan.NotBefore)) || !record.ReceivedAt.Before(plan.ExpiresAt) {
		return fmt.Errorf("%w: revision %d was not valid at receive time", ErrPlanRevisionUnavailable, record.RegistryVersion)
	}
	binding, admitted := registry.Admit(record.Protocol, record.Source.Addr(), record.ObservationDomainID)
	if !admitted || binding.TenantID != record.TenantID || binding.ExporterID != record.ExporterID {
		return errors.New("WAL source binding no longer matches its signed plan")
	}
	decoded, err := r.Decoder.Decode(record)
	if err != nil {
		if decoded.CollectStateChanged {
			if stateErr := r.checkpointCollectState(ctx, record, decoded, binding, plan, false, 0); stateErr != nil {
				return stateErr
			}
		}
		if errors.Is(err, ErrTemplatePending) {
			r.metrics().TemplatePending.Add(1)
			r.metrics().recordDecodeError(record.Protocol, "template_pending")
		} else {
			r.metrics().DecodeFailures.Add(1)
			r.metrics().recordDecodeError(record.Protocol, "decode_rejected")
		}
		if !errors.Is(err, ErrTemplatePending) {
			return permanentProcessingFailure(decodeRejectedCode, err)
		}
		return err
	}
	r.metrics().recordDecodedRecords(record.Protocol, len(decoded.Records))
	decoded, err = r.applyQuality(record, decoded, binding)
	if err != nil {
		return err
	}
	qualityComplete := false
	defer func() {
		if qualityComplete {
			r.quality().Forget(record.DatagramID)
			if r.QualityState != nil {
				r.QualityState.Complete(record.DatagramID)
			}
		} else {
			r.quality().Remember(record.DatagramID, decoded)
		}
	}()
	batches, err := BuildNormalizedBatches(record, decoded, binding, plan.CollectorID, plan, r.Config.NormalizedBatch, replayGeneration)
	if err != nil {
		r.metrics().NormalizeFailures.Add(1)
		r.metrics().recordDecodeError(record.Protocol, "normalize_rejected")
		r.metrics().recordNormalized(record.Protocol, len(decoded.Records), err)
		if errors.Is(err, ErrSamplingRateMissing) {
			r.metrics().recordMissingSamplingRate(record.Protocol)
		}
		if decoded.CollectStateChanged {
			if stateErr := r.checkpointCollectState(ctx, record, decoded, binding, plan, false, 0); stateErr != nil {
				return stateErr
			}
		}
		return permanentProcessingFailure(normalizeRejectedCode, err)
	}
	r.metrics().recordNormalized(record.Protocol, len(decoded.Records), nil)
	if len(batches) == 0 && !decoded.CollectStateChanged {
		if err := r.waitQualityDurable(ctx, record.DatagramID); err != nil {
			return err
		}
		if err := r.WAL.Acknowledge(record.DatagramID); err != nil {
			return err
		}
		qualityComplete = true
		return nil
	}
	childCount := uint32(len(batches))
	dataChildOffset := uint32(0)
	if decoded.CollectStateChanged {
		childCount++
		dataChildOffset = 1
		if childCount == 1 {
			if err := r.waitQualityDurable(ctx, record.DatagramID); err != nil {
				return err
			}
		}
		if err := r.checkpointCollectState(ctx, record, decoded, binding, plan, true, childCount); err != nil {
			return err
		}
	}
	for index, batch := range batches {
		childIndex := uint32(index) + dataChildOffset
		if r.WAL.ChildAcknowledged(record.DatagramID, childIndex, childCount) {
			continue
		}
		startedAt := time.Now()
		r.metrics().NormalizedBatchRecords.Store(int64(len(batch.Records)))
		r.metrics().NormalizedBatchBytes.Store(int64(proto.Size(batch)))
		err = r.Publisher.Publish(ctx, batch)
		r.observePublish(kafkaTopicNormalized, err, time.Since(startedAt))
		if err != nil {
			r.metrics().PublishFailures.Add(1)
			return err
		}
		r.metrics().PublishedBatches.Add(1)
		if childIndex+1 == childCount {
			if err := r.waitQualityDurable(ctx, record.DatagramID); err != nil {
				return err
			}
		}
		if err := r.WAL.AcknowledgeChild(record.DatagramID, childIndex, childCount); err != nil {
			return err
		}
	}
	qualityComplete = true
	return nil
}

func (r *Runner) registryForRevision(revision uint64) (*Registry, bool) {
	if r.Plans != nil {
		return r.Plans.Resolve(revision)
	}
	if r.Registry == nil || r.Registry.Plan().Revision != revision {
		return nil, false
	}
	return r.Registry, true
}

func (r *Runner) applyQuality(record WALRecord, decoded DecodedDatagram, binding SourceBinding) (DecodedDatagram, error) {
	tracker := r.quality()
	r.qualityBarrier.RLock()
	defer r.qualityBarrier.RUnlock()
	decoded, _ = tracker.Observe(record, decoded)
	if r.QualityState == nil {
		return decoded, nil
	}
	journal, err := tracker.BuildJournalRecord(record, decoded, binding, r.QualityState.collectorID)
	if err == nil {
		err = r.QualityState.Append(journal)
	}
	if err != nil {
		r.runtimeState().observeQualityJournal(err, time.Now())
		tracker.Remember(record.DatagramID, decoded)
		return decoded, fmt.Errorf("persist quality state: %w", err)
	}
	r.runtimeState().observeQualityJournal(nil, time.Now())
	return decoded, nil
}

func (r *Runner) waitQualityDurable(ctx context.Context, id DatagramID) error {
	if r.QualityState == nil {
		return nil
	}
	if err := r.QualityState.WaitDatagram(ctx, id); err != nil {
		r.runtimeState().observeQualityJournal(err, time.Now())
		return fmt.Errorf("wait for durable quality state: %w", err)
	}
	r.runtimeState().observeQualityJournal(nil, time.Now())
	return nil
}

func (r *Runner) deadLetter(ctx context.Context, record WALRecord, code string, cause error, attempts uint32) error {
	failure := BuildDecodeFailure(record, r.Registry.Plan().CollectorID, code, cause, attempts, r.Config.Diagnostics.DLQPayloadMaxBytes)
	startedAt := time.Now()
	err := r.Publisher.PublishDecodeFailure(ctx, failure)
	r.observePublish(kafkaTopicDecodeDLQ, err, time.Since(startedAt))
	if err != nil {
		r.metrics().DLQPublishFailures.Add(1)
		return err
	}
	if err := r.waitQualityDurable(ctx, record.DatagramID); err != nil {
		return err
	}
	if err := r.WAL.Acknowledge(record.DatagramID); err != nil {
		return err
	}
	r.quality().Forget(record.DatagramID)
	if r.QualityState != nil {
		r.QualityState.Complete(record.DatagramID)
	}
	r.metrics().DLQDatagrams.Add(1)
	return nil
}

func (r *Runner) quarantineLoop(ctx context.Context, events <-chan *flowpb.QuarantineEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			r.metrics().QuarantineQueueDepth.Store(int64(len(events)))
			startedAt := time.Now()
			err := r.Publisher.PublishQuarantine(ctx, event)
			r.observePublish(kafkaTopicQuarantine, err, time.Since(startedAt))
			if err != nil {
				r.metrics().QuarantinePublishFailures.Add(1)
				r.report(err)
				continue
			}
			r.metrics().PublishedQuarantine.Add(1)
		}
	}
}

func (r *Runner) checkpointCollectState(ctx context.Context, record WALRecord, decoded DecodedDatagram, binding SourceBinding, plan Plan, acknowledge bool, childCount uint32) error {
	state, err := BuildCollectState(record, decoded, binding, plan.CollectorID, r.Decoder)
	if err != nil {
		return err
	}
	if acknowledge && r.WAL.ChildAcknowledged(record.DatagramID, 0, childCount) {
		return nil
	}
	if err := r.State.Persist(state); err != nil {
		r.runtimeState().observeCollect(err, time.Now())
		r.metrics().CollectStateFailures.Add(1)
		return err
	}
	r.runtimeState().observeCollect(nil, time.Now())
	r.metrics().CollectStateCheckpoints.Add(1)
	startedAt := time.Now()
	err = r.Publisher.PublishCollectState(ctx, state)
	r.observePublish(kafkaTopicCollectState, err, time.Since(startedAt))
	if err != nil {
		r.metrics().CollectStateFailures.Add(1)
		return err
	}
	r.metrics().PublishedCollectStates.Add(1)
	if acknowledge {
		return r.WAL.AcknowledgeChild(record.DatagramID, 0, childCount)
	}
	return nil
}

func (r *Runner) metrics() *Metrics {
	r.metricsOnce.Do(func() {
		if r.Metrics == nil {
			r.Metrics = &Metrics{}
		}
	})
	return r.Metrics
}

func (r *Runner) quality() *QualityTracker {
	r.qualityOnce.Do(func() {
		if r.Quality == nil {
			config := r.Config.Quality
			if config.StateTTL <= 0 || config.AnomalyWindow <= 0 || config.MaxExporters <= 0 || config.MaxDataSources <= 0 {
				config = DefaultConfig().Quality
			}
			r.Quality = NewQualityTracker(config, r.metrics())
		}
	})
	return r.Quality
}

func (r *Runner) runtimeState() *RuntimeState {
	r.runtimeOnce.Do(func() {
		if r.Runtime == nil {
			r.Runtime = NewRuntimeState()
		}
	})
	return r.Runtime
}

func (r *Runner) observePublish(topic kafkaTopic, err error, duration time.Duration) {
	r.metrics().observeKafka(topic, err, duration)
	r.runtimeState().observeKafka(topic, err, time.Now())
}

func (r *Runner) replayLoop(ctx context.Context, decodeQueues []chan WALRecord) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	cursor := ReplayCursor{}
	for {
		next, reachedEnd, err := r.WAL.ReplayFrom(cursor, func(record WALRecord) bool { return r.submit(record, decodeQueues) })
		if err != nil && ctx.Err() == nil {
			r.report(err)
		}
		cursor = next
		if reachedEnd && r.retryNeeded.Swap(false) {
			cursor = ReplayCursor{}
			ticker.Reset(time.Second)
		} else {
			ticker.Reset(100 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runner) reclaimLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.WAL.Reclaim(); err != nil {
				r.report(err)
			}
		}
	}
}

func (r *Runner) qualityCheckpointLoop(ctx context.Context) {
	ticker := time.NewTicker(r.Config.Quality.CheckpointEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.qualityBarrier.Lock()
			err := r.QualityState.Compact(now)
			r.qualityBarrier.Unlock()
			if err != nil {
				r.metrics().QualityCheckpointFailures.Add(1)
				r.runtimeState().observeQualityCheckpoint(err, now)
				r.report(fmt.Errorf("compact quality state: %w", err))
			} else {
				r.runtimeState().observeQualityCheckpoint(nil, now)
			}
		}
	}
}

func (r *Runner) attemptCheckpointLoop(ctx context.Context) {
	ticker := time.NewTicker(r.Config.Diagnostics.AttemptCheckpointEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			err := r.Attempts.Compact()
			r.runtimeState().observeAttemptCheckpoint(err, now)
			if err != nil {
				r.metrics().AttemptCheckpointFailures.Add(1)
				r.report(fmt.Errorf("compact attempt state: %w", err))
			}
		}
	}
}

func (r *Runner) submit(record WALRecord, queues []chan WALRecord) bool {
	if _, loaded := r.inflight.LoadOrStore(record.DatagramID, struct{}{}); loaded {
		return true
	}
	queue := queues[decodeWorkerIndex(record, len(queues))]
	r.metrics().DecodeQueueDepth.Add(1)
	select {
	case queue <- record:
		return true
	default:
		r.metrics().DecodeQueueDepth.Add(-1)
		r.inflight.Delete(record.DatagramID)
		return false
	}
}

func decodeWorkerIndex(record WALRecord, workers int) int {
	if workers <= 1 {
		return 0
	}
	hash := xxhash.New()
	_, _ = hash.Write([]byte{byte(record.Protocol)})
	if source := record.Source.Addr(); source.IsValid() {
		value := source.As16()
		_, _ = hash.Write(value[:])
	}
	var domain [8]byte
	binary.BigEndian.PutUint64(domain[:], record.ObservationDomainID)
	_, _ = hash.Write(domain[:])
	return int(hash.Sum64() % uint64(workers))
}

func (r *Runner) report(err error) {
	if err == nil || r.OnError == nil {
		return
	}
	r.reportMu.Lock()
	defer r.reportMu.Unlock()
	if time.Since(r.lastErrorReport) < time.Second {
		return
	}
	r.lastErrorReport = time.Now()
	r.OnError(err)
}

func sendFatal(channel chan<- error, err error) {
	select {
	case channel <- err:
	default:
	}
}
