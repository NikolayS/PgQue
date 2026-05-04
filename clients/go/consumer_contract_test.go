// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque-go"
)

// contractBackend is a minimal Consumer backend that delivers one batch
// of messages on the first Receive and nothing afterwards. Ack and Nack
// are recorded with configurable error returns so a test can lock a
// specific runtime contract.
//
// It is intentionally separate from stubBackend in
// consumer_nackfail_test.go: the contract tests assert across multi-
// message batches and across all four handler outcomes (success, error,
// missing handler, panic), which the single-message stubBackend does
// not cover.
type contractBackend struct {
	mu sync.Mutex

	delivered bool
	msgs      []pgque.Message

	nackErr error

	nackCount int32
	ackCount  int32

	nackedMsgIDs []int64
	ackedBatches []int64
}

func (b *contractBackend) Receive(_ context.Context, _, _ string, _ int) ([]pgque.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.delivered {
		return nil, nil
	}
	b.delivered = true
	out := make([]pgque.Message, len(b.msgs))
	copy(out, b.msgs)
	return out, nil
}

func (b *contractBackend) Ack(_ context.Context, batchID int64) error {
	b.mu.Lock()
	b.ackedBatches = append(b.ackedBatches, batchID)
	b.mu.Unlock()
	atomic.AddInt32(&b.ackCount, 1)
	return nil
}

func (b *contractBackend) Nack(_ context.Context, _ int64, msg pgque.Message, _ ...pgque.NackOption) error {
	b.mu.Lock()
	b.nackedMsgIDs = append(b.nackedMsgIDs, msg.MsgID)
	b.mu.Unlock()
	atomic.AddInt32(&b.nackCount, 1)
	return b.nackErr
}

// runConsumer wires the stub backend into a fresh Consumer, runs Start
// long enough for the single delivered batch to be processed, and
// returns once the consumer has cancelled.
func runConsumer(t *testing.T, stub *contractBackend, opts ...pgque.ConsumerOption) *pgque.Consumer {
	t.Helper()
	var client *pgque.Client
	defaultOpts := []pgque.ConsumerOption{pgque.WithPollInterval(20 * time.Millisecond)}
	c := client.NewConsumer("dummy_queue", "dummy_consumer", append(defaultOpts, opts...)...)
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)
	return c
}

// TestConsumerContract_HandlerSuccess_AcksBatch locks the documented
// contract: when every message in a batch is dispatched to a registered
// handler that returns nil, the batch is Ack'd exactly once and no Nack
// is issued.
func TestConsumerContract_HandlerSuccess_AcksBatch(t *testing.T) {
	stub := &contractBackend{
		msgs: []pgque.Message{
			{MsgID: 1, BatchID: 100, Type: "ok", Payload: `{}`},
			{MsgID: 2, BatchID: 100, Type: "ok", Payload: `{}`},
		},
	}

	var client *pgque.Client
	c := client.NewConsumer("q", "c",
		pgque.WithPollInterval(20*time.Millisecond))
	c.Handle("ok", func(ctx context.Context, m pgque.Message) error { return nil })
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.nackCount); got != 0 {
		t.Fatalf("handler success must not Nack, got %d", got)
	}
	if got := atomic.LoadInt32(&stub.ackCount); got == 0 {
		t.Fatal("handler success must Ack the batch")
	}
}

// TestConsumerContract_HandlerError_NacksMessageThenAcksBatch locks the
// documented contract: when a handler returns a non-nil error, the
// individual message is Nack'd; the batch is Ack'd because the Nack
// succeeded. Other messages in the same batch still reach their
// handlers.
//
// Note: the related question of what should happen when Nack itself
// fails is covered by TestConsumer_NackFailure_DoesNotAck in
// consumer_nackfail_test.go and is intentionally not re-asserted here.
func TestConsumerContract_HandlerError_NacksMessageThenAcksBatch(t *testing.T) {
	stub := &contractBackend{
		msgs: []pgque.Message{
			{MsgID: 10, BatchID: 200, Type: "fail", Payload: `{}`},
			{MsgID: 11, BatchID: 200, Type: "ok", Payload: `{}`},
		},
	}

	var client *pgque.Client
	c := client.NewConsumer("q", "c",
		pgque.WithPollInterval(20*time.Millisecond))

	var okSeen int32
	c.Handle("fail", func(ctx context.Context, m pgque.Message) error {
		return errors.New("boom")
	})
	c.Handle("ok", func(ctx context.Context, m pgque.Message) error {
		atomic.AddInt32(&okSeen, 1)
		return nil
	})
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.nackCount); got != 1 {
		t.Fatalf("handler error must Nack exactly once, got %d", got)
	}
	if got := atomic.LoadInt32(&stub.ackCount); got != 1 {
		t.Fatalf("batch must be Ack'd after successful per-message Nack, got %d", got)
	}
	if got := atomic.LoadInt32(&okSeen); got != 1 {
		t.Fatalf("handler error must not stop dispatch of remaining messages; got okSeen = %d, want 1", got)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.nackedMsgIDs) != 1 || stub.nackedMsgIDs[0] != 10 {
		t.Fatalf("Nack must target the failed message (id 10), got %v", stub.nackedMsgIDs)
	}
}

// TestConsumerContract_MissingHandler_DefaultPolicyNacks locks the
// documented default UnknownHandlerPolicy contract: a message whose
// Type has no registered handler is Nack'd, and the batch is Ack'd
// because the Nack succeeded.
func TestConsumerContract_MissingHandler_DefaultPolicyNacks(t *testing.T) {
	stub := &contractBackend{
		msgs: []pgque.Message{
			{MsgID: 20, BatchID: 300, Type: "no.handler", Payload: `{}`},
		},
	}

	c := runConsumer(t, stub)
	_ = c

	if got := atomic.LoadInt32(&stub.nackCount); got != 1 {
		t.Fatalf("default policy must Nack unknown types exactly once, got %d", got)
	}
	if got := atomic.LoadInt32(&stub.ackCount); got != 1 {
		t.Fatalf("default policy must Ack the batch after successful Nack, got %d", got)
	}
}

// TestConsumerContract_HandlerPanic_NacksMessageThenAcksBatch locks the
// documented panic contract: a handler panic is caught by
// dispatchWithRecover and treated identically to a handler error —
// per-message Nack, then batch Ack.
func TestConsumerContract_HandlerPanic_NacksMessageThenAcksBatch(t *testing.T) {
	stub := &contractBackend{
		msgs: []pgque.Message{
			{MsgID: 30, BatchID: 400, Type: "panic", Payload: `{}`},
		},
	}

	var client *pgque.Client
	c := client.NewConsumer("q", "c",
		pgque.WithPollInterval(20*time.Millisecond))
	c.Handle("panic", func(ctx context.Context, m pgque.Message) error {
		panic("kaboom")
	})
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.nackCount); got != 1 {
		t.Fatalf("handler panic must Nack exactly once (same as handler error), got %d", got)
	}
	if got := atomic.LoadInt32(&stub.ackCount); got != 1 {
		t.Fatalf("batch must be Ack'd after successful per-message Nack, got %d", got)
	}
}
