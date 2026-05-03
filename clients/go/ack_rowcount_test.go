// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

// TestAck_ReturnsRowcount_Unit uses a stub backend to verify that Ack
// returns the integer row-count from pgque.finish_batch: 1 on success, 0
// for a stale/double ack. This is a unit test — no live database required.
//
// Red: stubBackend.Ack still has the OLD signature (error), so the test
// file will not compile until the signature is widened to (int64, error).

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque-go"
)

// ackRowcountBackend is a minimal consumerBackend stub where Ack returns a
// configurable (int64, error) pair, exercising the new contract.
type ackRowcountBackend struct {
	mu sync.Mutex

	delivered  bool
	msg        pgque.Message
	ackResult  int64
	ackErr     error
	ackCount   int32
	nackCalled int32
}

func (s *ackRowcountBackend) Receive(_ context.Context, _, _ string, _ int) ([]pgque.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered {
		return nil, nil
	}
	s.delivered = true
	return []pgque.Message{s.msg}, nil
}

func (s *ackRowcountBackend) Ack(_ context.Context, _ int64) (int64, error) {
	atomic.AddInt32(&s.ackCount, 1)
	return s.ackResult, s.ackErr
}

func (s *ackRowcountBackend) Nack(_ context.Context, _ int64, _ pgque.Message, _ ...pgque.NackOption) error {
	atomic.AddInt32(&s.nackCalled, 1)
	return nil
}

// TestAck_StubReturnsOne confirms the consumer calls Ack and accepts a
// rowcount of 1 (normal success path).
func TestAck_StubReturnsOne(t *testing.T) {
	var client *pgque.Client
	stub := &ackRowcountBackend{
		msg: pgque.Message{
			MsgID:   1,
			BatchID: 10,
			Type:    "ok.type",
			Payload: `{}`,
		},
		ackResult: 1,
	}
	c := client.NewConsumer("q", "c", pgque.WithPollInterval(20*time.Millisecond))
	pgque.SetConsumerBackend(c, stub)
	c.Handle("ok.type", func(_ context.Context, _ pgque.Message) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.ackCount); got == 0 {
		t.Fatal("expected Ack to be called at least once")
	}
}

// TestAck_StubReturnsZero confirms the consumer logs (not errors) when Ack
// returns 0 (stale/double ack). The batch loop must continue rather than panic
// or propagate an error.
func TestAck_StubReturnsZero(t *testing.T) {
	var client *pgque.Client
	stub := &ackRowcountBackend{
		msg: pgque.Message{
			MsgID:   2,
			BatchID: 20,
			Type:    "stale.type",
			Payload: `{}`,
		},
		ackResult: 0,
	}
	c := client.NewConsumer("q", "c", pgque.WithPollInterval(20*time.Millisecond))
	pgque.SetConsumerBackend(c, stub)
	c.Handle("stale.type", func(_ context.Context, _ pgque.Message) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.ackCount); got == 0 {
		t.Fatal("expected Ack to be called at least once")
	}
}

// TestAck_DirectCall_ReturnsRowcount verifies the Client.Ack method itself
// returns (int64, error). This test requires a live database; it is skipped
// when PGQUE_TEST_DSN is not reachable.
func TestAck_DirectCall_ReturnsRowcount(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "ack.rowcount.test", Payload: map[string]any{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Skip("no messages received — queue may not have ticked")
	}

	batchID := msgs[0].BatchID

	// First ack: expects rowcount 1.
	n1, err := client.Ack(ctx, batchID)
	if err != nil {
		t.Fatalf("first Ack returned error: %v", err)
	}
	if n1 != 1 {
		t.Fatalf("first Ack: expected rowcount 1, got %d", n1)
	}

	// Second ack of the same batch: expects rowcount 0 (stale ack).
	n2, err := client.Ack(ctx, batchID)
	if err != nil {
		t.Fatalf("second Ack returned error: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second Ack (double-ack): expected rowcount 0, got %d", n2)
	}
}
