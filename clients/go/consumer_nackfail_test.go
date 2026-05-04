// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque-go"
)

// stubBackend is a Consumer backend that returns a single message on the
// first Receive, then nothing. Nack always fails. Ack is recorded.
// It is intentionally minimal — just enough to drive the Consumer's
// per-batch ack/nack accounting.
type stubBackend struct {
	mu sync.Mutex

	delivered bool
	msg       pgque.Message

	nackCount int32
	ackCount  int32

	nackErr error
	lastMax int32
}

func (s *stubBackend) Receive(_ context.Context, _, _ string, maxMessages int) ([]pgque.Message, error) {
	atomic.StoreInt32(&s.lastMax, int32(maxMessages))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered {
		return nil, nil
	}
	s.delivered = true
	return []pgque.Message{s.msg}, nil
}

func (s *stubBackend) Ack(_ context.Context, _ int64) (int64, error) {
	atomic.AddInt32(&s.ackCount, 1)
	return 1, nil
}

func (s *stubBackend) Nack(_ context.Context, _ int64, _ pgque.Message, _ ...pgque.NackOption) error {
	atomic.AddInt32(&s.nackCount, 1)
	return s.nackErr
}

// TestConsumer_NackFailure_DoesNotAck is the red/green guard for the
// data-loss bug where the Consumer logged a Nack error and acked the
// batch anyway, causing PgQue to advance past a message whose failure
// was never recorded. With the fix in place, a Nack error must leave
// the batch unacked so that PgQue redelivers it on the next Receive.
func TestConsumer_NackFailure_DoesNotAck(t *testing.T) {
	var client *pgque.Client

	stub := &stubBackend{
		msg: pgque.Message{
			MsgID:   1,
			BatchID: 42,
			Type:    "no.handler.registered",
			Payload: `{"x":1}`,
		},
		nackErr: errors.New("simulated nack failure"),
	}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		pgque.WithPollInterval(50*time.Millisecond))
	pgque.SetConsumerBackend(c, stub)
	// No Handle() call — the message will hit the unknown-type path,
	// which Nacks under the default policy.

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.nackCount); got == 0 {
		t.Fatalf("expected Nack to be attempted, got 0 calls")
	}
	if got := atomic.LoadInt32(&stub.ackCount); got != 0 {
		t.Fatalf("Nack failed but Ack was still called %d times — data-loss bug", got)
	}
}

// TestConsumer_NackSuccess_StillAcks confirms the green case: when the
// per-message Nack succeeds, the batch is acked exactly once.
func TestConsumer_NackSuccess_StillAcks(t *testing.T) {
	var client *pgque.Client

	stub := &stubBackend{
		msg: pgque.Message{
			MsgID:   1,
			BatchID: 7,
			Type:    "still.unknown",
			Payload: `{}`,
		},
		nackErr: nil,
	}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		pgque.WithPollInterval(50*time.Millisecond))
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.nackCount); got == 0 {
		t.Fatal("expected at least one Nack call")
	}
	if got := atomic.LoadInt32(&stub.ackCount); got == 0 {
		t.Fatal("expected Ack after successful Nack, got 0")
	}
}

// TestConsumer_AckUnknownPolicy_SkipsNack verifies that
// WithUnknownHandlerPolicy(AckUnknown) suppresses the per-message Nack
// for unhandled types: the batch is acked, the unknown message is
// effectively dropped (consumer-side ignored).
func TestConsumer_AckUnknownPolicy_SkipsNack(t *testing.T) {
	var client *pgque.Client

	stub := &stubBackend{
		msg: pgque.Message{
			MsgID:   1,
			BatchID: 99,
			Type:    "ignored.type",
			Payload: `{}`,
		},
	}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		pgque.WithPollInterval(50*time.Millisecond),
		pgque.WithUnknownHandlerPolicy(pgque.AckUnknown))
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.nackCount); got != 0 {
		t.Fatalf("AckUnknown policy must not Nack, got %d Nacks", got)
	}
	if got := atomic.LoadInt32(&stub.ackCount); got == 0 {
		t.Fatal("AckUnknown policy must still Ack the batch, got 0")
	}
}

func TestConsumer_DefaultMaxMessagesRequestsWholeBatch(t *testing.T) {
	var client *pgque.Client
	stub := &stubBackend{}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		pgque.WithPollInterval(10*time.Millisecond))
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.lastMax); got != math.MaxInt32 {
		t.Fatalf("default maxMessages = %d, want math.MaxInt32", got)
	}
}

func TestConsumer_WithMaxMessagesPassesReceiveLimit(t *testing.T) {
	var client *pgque.Client
	stub := &stubBackend{}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		pgque.WithPollInterval(10*time.Millisecond),
		pgque.WithMaxMessages(123))
	pgque.SetConsumerBackend(c, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.lastMax); got != 123 {
		t.Fatalf("configured maxMessages = %d, want 123", got)
	}
}
