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
)

var (
	ErrPlanRevisionUnavailable = errors.New("WAL record plan revision is not available")
)

type Runner struct {
	Config    Config
	Registry  *Registry
	WAL       *WAL
	Decoder   *Decoder
	State     *CollectStateStore
	Publisher Publisher
	OnError   func(error)
	Metrics   *Metrics

	inflight        sync.Map
	attempts        sync.Map
	retryNeeded     atomic.Bool
	metricsOnce     sync.Once
	reportMu        sync.Mutex
	lastErrorReport time.Time
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Registry == nil || r.WAL == nil || r.Decoder == nil || r.State == nil || r.Publisher == nil {
		return errors.New("flow runner dependencies are required")
	}
	if r.Config.SocketCount <= 0 || r.Config.DecodeWorkers <= 0 || r.Config.DecodeQueueDatagrams <= 0 {
		return errors.New("flow runner socket, worker, and queue counts must be positive")
	}
	r.metrics()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ingress := make(chan Datagram, r.Config.DecodeQueueDatagrams)
	decodeQueues := make([]chan WALRecord, r.Config.DecodeWorkers)
	queueCapacity := (r.Config.DecodeQueueDatagrams + r.Config.DecodeWorkers - 1) / r.Config.DecodeWorkers
	fatal := make(chan error, 1)

	var workerWG sync.WaitGroup
	for index := range decodeQueues {
		decodeQueues[index] = make(chan WALRecord, queueCapacity)
		workerWG.Add(1)
		go func(queue <-chan WALRecord) { defer workerWG.Done(); r.decodeLoop(ctx, queue) }(decodeQueues[index])
	}
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.ingestLoop(ctx, ingress, fatal) }()
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.replayLoop(ctx, decodeQueues) }()
	workerWG.Add(1)
	go func() { defer workerWG.Done(); r.reclaimLoop(ctx) }()

	var receiverWG sync.WaitGroup
	for socket := 0; socket < r.Config.SocketCount; socket++ {
		for _, endpoint := range []struct {
			protocol Protocol
			address  string
		}{{ProtocolSFlow5, r.Config.SFlowListen}, {0, r.Config.NetFlowListen}} {
			receiver := &Receiver{Protocol: endpoint.protocol, ListenAddr: endpoint.address, ReceiveBufferBytes: r.Config.ReceiveBufferBytes, MaxDatagramBytes: r.Config.MaxDatagramBytes, Queue: ingress, ReusePort: r.Config.SocketCount > 1, OnQueueFull: func(Protocol) { r.metrics().ReceiveQueueDrops.Add(1) }, OnInvalid: func() { r.metrics().InvalidDatagrams.Add(1) }}
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

func (r *Runner) ingestLoop(ctx context.Context, ingress <-chan Datagram, fatal chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case datagram := <-ingress:
			r.metrics().ReceivedDatagrams.Add(1)
			protocol, domain, err := InspectDatagram(datagram.Payload)
			if err != nil {
				datagram.Release()
				r.metrics().InvalidDatagrams.Add(1)
				continue
			}
			binding, admitted := r.Registry.Admit(protocol, datagram.Source.Addr(), domain)
			if !admitted {
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
			for ctx.Err() == nil {
				generation := r.nextReplayGeneration(record.DatagramID)
				if generation > 0 {
					r.metrics().ReplayAttempts.Add(1)
				}
				err := r.processRecord(ctx, record, generation)
				if err == nil {
					r.metrics().DecodedDatagrams.Add(1)
					r.attempts.Delete(record.DatagramID)
					break
				}
				r.report(err)
				if errors.Is(err, ErrTemplatePending) {
					r.retryNeeded.Store(true)
					break
				}
				timer := time.NewTimer(time.Second)
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
	plan := r.Registry.Plan()
	if record.RegistryVersion != plan.Revision {
		return fmt.Errorf("%w: have %d need %d", ErrPlanRevisionUnavailable, plan.Revision, record.RegistryVersion)
	}
	binding, admitted := r.Registry.Admit(record.Protocol, record.Source.Addr(), record.ObservationDomainID)
	if !admitted || binding.TenantID != record.TenantID || binding.ExporterID != record.ExporterID {
		return errors.New("WAL source binding no longer matches its signed plan")
	}
	decoded, err := r.Decoder.Decode(record)
	if err != nil {
		if errors.Is(err, ErrTemplatePending) && decoded.CollectStateChanged {
			if stateErr := r.checkpointCollectState(ctx, record, decoded, binding, plan, false, 0); stateErr != nil {
				return errors.Join(err, stateErr)
			}
		}
		if errors.Is(err, ErrTemplatePending) {
			r.metrics().TemplatePending.Add(1)
		} else {
			r.metrics().DecodeFailures.Add(1)
		}
		return err
	}
	batches, err := BuildNormalizedBatches(record, decoded, binding, plan.CollectorID, plan, r.Config.NormalizedBatch, replayGeneration)
	if err != nil {
		r.metrics().NormalizeFailures.Add(1)
		return err
	}
	if len(batches) == 0 && !decoded.CollectStateChanged {
		return r.WAL.Acknowledge(record.DatagramID)
	}
	childCount := uint32(len(batches))
	dataChildOffset := uint32(0)
	if decoded.CollectStateChanged {
		childCount++
		dataChildOffset = 1
		if err := r.checkpointCollectState(ctx, record, decoded, binding, plan, true, childCount); err != nil {
			return err
		}
	}
	for index, batch := range batches {
		childIndex := uint32(index) + dataChildOffset
		if r.WAL.ChildAcknowledged(record.DatagramID, childIndex, childCount) {
			continue
		}
		if err := r.Publisher.Publish(ctx, batch); err != nil {
			r.metrics().PublishFailures.Add(1)
			return err
		}
		r.metrics().PublishedBatches.Add(1)
		if err := r.WAL.AcknowledgeChild(record.DatagramID, childIndex, childCount); err != nil {
			return err
		}
	}
	return nil
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
		r.metrics().CollectStateFailures.Add(1)
		return err
	}
	r.metrics().CollectStateCheckpoints.Add(1)
	if err := r.Publisher.PublishCollectState(ctx, state); err != nil {
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

func (r *Runner) submit(record WALRecord, queues []chan WALRecord) bool {
	if _, loaded := r.inflight.LoadOrStore(record.DatagramID, struct{}{}); loaded {
		return true
	}
	queue := queues[decodeWorkerIndex(record, len(queues))]
	select {
	case queue <- record:
		return true
	default:
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

func (r *Runner) nextReplayGeneration(id DatagramID) uint32 {
	value, _ := r.attempts.LoadOrStore(id, &atomic.Uint32{})
	return value.(*atomic.Uint32).Add(1) - 1
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
