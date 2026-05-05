// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ackRowcountBackend is a minimal consumerBackend stub where Ack returns a
// configurable (int64, error) pair, exercising the new contract.
type ackRowcountBackend struct {
	mu sync.Mutex

	delivered  bool
	msg        Message
	ackResult  int64
	ackErr     error
	ackCount   int32
	nackCalled int32
}

func (s *ackRowcountBackend) Receive(_ context.Context, _, _ string, _ int) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered {
		return nil, nil
	}
	s.delivered = true
	return []Message{s.msg}, nil
}

func (s *ackRowcountBackend) Ack(_ context.Context, _ int64) (int64, error) {
	atomic.AddInt32(&s.ackCount, 1)
	return s.ackResult, s.ackErr
}

func (s *ackRowcountBackend) Nack(_ context.Context, _ int64, _ Message, _ NackOptions) error {
	atomic.AddInt32(&s.nackCalled, 1)
	return nil
}

// TestAck_StubReturnsOne confirms the consumer calls Ack and accepts a
// rowcount of 1 (normal success path).
func TestAck_StubReturnsOne(t *testing.T) {
	client := &Client{}
	stub := &ackRowcountBackend{
		msg: Message{
			MsgID:   1,
			BatchID: 10,
			Type:    "ok.type",
			Payload: `{}`,
		},
		ackResult: 1,
	}
	c := client.NewConsumer("q", "c", WithPollInterval(20*time.Millisecond))
	c.backend = stub
	c.Handle("ok.type", func(_ context.Context, _ Message) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.ackCount); got == 0 {
		t.Fatal("expected Ack to be called at least once")
	}
	if got := atomic.LoadInt32(&stub.nackCalled); got != 0 {
		t.Fatalf("Nack must not be called on the success path; got %d", got)
	}
}

// TestAck_StubReturnsZero confirms the consumer logs (not errors) when Ack
// returns 0 (stale/double ack). The batch loop must continue rather than panic
// or propagate an error.
func TestAck_StubReturnsZero(t *testing.T) {
	client := &Client{}
	stub := &ackRowcountBackend{
		msg: Message{
			MsgID:   2,
			BatchID: 20,
			Type:    "stale.type",
			Payload: `{}`,
		},
		ackResult: 0,
	}
	c := client.NewConsumer("q", "c", WithPollInterval(20*time.Millisecond))
	c.backend = stub
	c.Handle("stale.type", func(_ context.Context, _ Message) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.ackCount); got == 0 {
		t.Fatal("expected Ack to be called at least once")
	}
	if got := atomic.LoadInt32(&stub.nackCalled); got != 0 {
		t.Fatalf("Nack must not be called on the success path (rowcount 0 is informational); got %d", got)
	}
}
