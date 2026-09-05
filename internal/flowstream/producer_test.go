// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowstream

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProducerClient struct {
	record          *kgo.Record
	produceContext  context.Context
	promise         func(*kgo.Record, error)
	result          error
	flushErr        error
	flushed         bool
	closed          bool
	buffered        int64
	deferCompletion bool
}

func (f *fakeProducerClient) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	copyRecord := *record
	copyRecord.Key = append([]byte(nil), record.Key...)
	copyRecord.Value = append([]byte(nil), record.Value...)
	f.record = &copyRecord
	f.produceContext = ctx
	f.promise = promise
	if !f.deferCompletion {
		promise(record, f.result)
	}
}

func (f *fakeProducerClient) Flush(context.Context) error {
	f.flushed = true
	return f.flushErr
}

func (f *fakeProducerClient) Ping(context.Context) error    { return nil }
func (f *fakeProducerClient) Close()                        { f.closed = true }
func (f *fakeProducerClient) BufferedProduceRecords() int64 { return f.buffered }

func TestProducerSendCompletesAsynchronouslyOwnedPayload(t *testing.T) {
	client := &fakeProducerClient{buffered: 3}
	producer := newProducerWithClient(client, "watchdog.flow.raw-v1")
	released := 0
	completed := 0
	if err := producer.Send(context.Background(), []byte("exporter"), []byte("payload"), func() { released++ }, func(err error) {
		if err != nil {
			t.Errorf("unexpected completion error: %v", err)
		}
		completed++
	}); err != nil {
		t.Fatal(err)
	}
	if released != 1 || completed != 1 {
		t.Fatalf("release=%d completion=%d", released, completed)
	}
	if client.record.Topic != "watchdog.flow.raw-v1" || string(client.record.Key) != "exporter" || string(client.record.Value) != "payload" {
		t.Fatalf("unexpected Kafka record: %+v", client.record)
	}
	if got := producer.Stats(); got.Records != 1 || got.Bytes != 7 || got.Errors != 0 || got.BufferedRecords != 3 {
		t.Fatalf("unexpected stats: %+v", got)
	}
}

func TestProducerSendFailureIsObservableAndReleasesPayload(t *testing.T) {
	client := &fakeProducerClient{result: errors.New("broker unavailable")}
	producer := newProducerWithClient(client, "watchdog.flow.raw-v1")
	released := 0
	var completionErr error
	if err := producer.Send(context.Background(), []byte("exporter"), []byte("payload"), func() { released++ }, func(err error) { completionErr = err }); err != nil {
		t.Fatal(err)
	}
	if released != 1 || completionErr == nil {
		t.Fatalf("release=%d completion=%v", released, completionErr)
	}
	if got := producer.Stats(); got.Records != 0 || got.Errors != 1 {
		t.Fatalf("unexpected stats: %+v", got)
	}
}

func TestProducerSendTransfersLifecycleBeforeReceiverCancellation(t *testing.T) {
	client := &fakeProducerClient{deferCompletion: true}
	producer := newProducerWithClient(client, "watchdog.flow.raw-v1")
	ctx, cancel := context.WithCancel(context.Background())
	released := 0
	completed := 0
	if err := producer.Send(ctx, []byte("exporter"), []byte("payload"), func() { released++ }, func(err error) {
		if err != nil {
			t.Errorf("unexpected completion error: %v", err)
		}
		completed++
	}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if client.produceContext == nil || client.produceContext.Err() != nil || client.produceContext.Done() != nil {
		t.Fatalf("Kafka delivery retained receiver context: %v", client.produceContext)
	}
	if released != 0 || completed != 0 {
		t.Fatalf("release=%d completion=%d before delivery", released, completed)
	}
	client.promise(client.record, nil)
	if released != 1 || completed != 1 || producer.Stats().Records != 1 {
		t.Fatalf("release=%d completion=%d stats=%+v", released, completed, producer.Stats())
	}
}

func TestProducerSendRejectsAlreadyCanceledContext(t *testing.T) {
	client := &fakeProducerClient{}
	producer := newProducerWithClient(client, "watchdog.flow.raw-v1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	released := 0
	completed := 0
	err := producer.Send(ctx, []byte("exporter"), []byte("payload"), func() { released++ }, func(error) { completed++ })
	if !errors.Is(err, context.Canceled) || client.record != nil || released != 1 || completed != 1 {
		t.Fatalf("error=%v record=%v release=%d completion=%d", err, client.record, released, completed)
	}
}

func TestProducerRejectsInvalidRecordAndClosesOnce(t *testing.T) {
	client := &fakeProducerClient{}
	producer := newProducerWithClient(client, "watchdog.flow.raw-v1")
	released := 0
	if err := producer.Send(context.Background(), nil, []byte("payload"), func() { released++ }, nil); err == nil || released != 1 {
		t.Fatalf("invalid record err=%v release=%d", err, released)
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !client.flushed || !client.closed {
		t.Fatalf("producer close did not flush and close: %+v", client)
	}
	if err := producer.Send(context.Background(), []byte("key"), []byte("payload"), func() { released++ }, nil); err == nil || released != 2 {
		t.Fatalf("closed producer err=%v release=%d", err, released)
	}
}
