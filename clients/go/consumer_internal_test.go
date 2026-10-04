// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// stubBackend is a Consumer backend that returns a single message on the
// first Receive, then nothing. Nack always fails. Ack is recorded.
// It is intentionally minimal — just enough to drive the Consumer's
// per-batch ack/nack accounting.
type stubBackend struct {
	mu sync.Mutex

	delivered bool
	msg       Message

	nackCount int32
	ackCount  int32

	nackErr         error
	lastMax         int32
	lastNackOptions NackOptions
}

type receiveErrorBackend struct {
	receiveErr error
	coopErr    error

	receiveCount int32
	coopCount    int32
	handlerCount int32
	nackCount    int32
	ackCount     int32
}

func (s *receiveErrorBackend) Receive(_ context.Context, _, _ string, _ int) ([]Message, error) {
	atomic.AddInt32(&s.receiveCount, 1)
	return nil, s.receiveErr
}

func (s *receiveErrorBackend) ReceiveCoop(_ context.Context, _, _, _ string, _ ...ReceiveCoopOption) ([]Message, error) {
	atomic.AddInt32(&s.coopCount, 1)
	return nil, s.coopErr
}

func (s *receiveErrorBackend) Ack(_ context.Context, _ int64) (int64, error) {
	atomic.AddInt32(&s.ackCount, 1)
	return 1, nil
}

func (s *receiveErrorBackend) Nack(_ context.Context, _ int64, _ Message, _ NackOptions) error {
	atomic.AddInt32(&s.nackCount, 1)
	return nil
}

func testOverflowError(op string, ceiling int) error {
	function := op
	if op == "receive coop" {
		function = "receive_coop"
	}
	return wrapReceiveError(op, ceiling, &pgconn.PgError{
		Code:    "54000",
		Message: fmt.Sprintf("pgque.%s: batch exceeds max_return of %d", function, ceiling),
		Hint:    "Retry with a larger resource-safe max_return to receive the complete batch. Do not acknowledge after this error.",
	})
}

func TestConsumer_ReceiveOverflowFailsFast(t *testing.T) {
	for _, tc := range []struct {
		name string
		coop bool
		op   string
	}{
		{"plain", false, "receive"},
		{"coop", true, "receive coop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &receiveErrorBackend{}
			if tc.coop {
				backend.coopErr = testOverflowError(tc.op, 10)
			} else {
				backend.receiveErr = testOverflowError(tc.op, 10)
			}
			client := &Client{}
			opts := []ConsumerOption{WithMaxMessages(10), WithPollInterval(time.Hour)}
			if tc.coop {
				opts = append(opts, WithSubconsumer("worker"))
			}
			consumer := client.NewConsumer("queue", "consumer", opts...)
			consumer.backend = backend
			consumer.Handle("event", func(context.Context, Message) error {
				atomic.AddInt32(&backend.handlerCount, 1)
				return nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := consumer.Start(ctx)
			if !errors.Is(err, ErrReceiveOverflow) {
				t.Fatalf("expected immediate overflow, got %v", err)
			}
			if ctx.Err() != nil {
				t.Fatalf("overflow waited for context cancellation: %v", ctx.Err())
			}
			if got := atomic.LoadInt32(&backend.receiveCount) + atomic.LoadInt32(&backend.coopCount); got != 1 {
				t.Fatalf("receive calls = %d, want 1", got)
			}
			if got := atomic.LoadInt32(&backend.handlerCount); got != 0 {
				t.Fatalf("handler calls = %d, want 0", got)
			}
			if got := atomic.LoadInt32(&backend.ackCount); got != 0 {
				t.Fatalf("ack calls = %d, want 0", got)
			}
			if got := atomic.LoadInt32(&backend.nackCount); got != 0 {
				t.Fatalf("nack calls = %d, want 0", got)
			}
		})
	}
}

func TestConsumer_TransientReceiveErrorStillRetriesUntilCancellation(t *testing.T) {
	backend := &receiveErrorBackend{receiveErr: errors.New("temporary receive failure")}
	client := &Client{}
	consumer := client.NewConsumer("queue", "consumer", WithPollInterval(10*time.Millisecond))
	consumer.backend = backend

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Millisecond)
	defer cancel()
	err := consumer.Start(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cancellation cleanup, got %v", err)
	}
	if got := atomic.LoadInt32(&backend.receiveCount); got < 2 {
		t.Fatalf("transient error was not retried: %d receives", got)
	}
}

func (s *stubBackend) Receive(_ context.Context, _, _ string, maxMessages int) ([]Message, error) {
	atomic.StoreInt32(&s.lastMax, int32(maxMessages))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered {
		return nil, nil
	}
	s.delivered = true
	return []Message{s.msg}, nil
}

func (s *stubBackend) Ack(_ context.Context, _ int64) (int64, error) {
	atomic.AddInt32(&s.ackCount, 1)
	return 1, nil
}

func (s *stubBackend) Nack(_ context.Context, _ int64, _ Message, opts NackOptions) error {
	s.mu.Lock()
	s.lastNackOptions = opts
	s.mu.Unlock()
	atomic.AddInt32(&s.nackCount, 1)
	return s.nackErr
}

// TestConsumer_NackFailure_DoesNotAck is the red/green guard for the
// data-loss bug where the Consumer logged a Nack error and acked the
// batch anyway, causing PgQue to advance past a message whose failure
// was never recorded. With the fix in place, a Nack error must leave
// the batch unacked so that PgQue redelivers it on the next Receive.
func TestConsumer_NackFailure_DoesNotAck(t *testing.T) {
	client := &Client{}

	stub := &stubBackend{
		msg: Message{
			MsgID:   1,
			BatchID: 42,
			Type:    "no.handler.registered",
			Payload: `{"x":1}`,
		},
		nackErr: errors.New("simulated nack failure"),
	}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(50*time.Millisecond))
	c.backend = stub
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
	client := &Client{}

	stub := &stubBackend{
		msg: Message{
			MsgID:   1,
			BatchID: 7,
			Type:    "still.unknown",
			Payload: `{}`,
		},
		nackErr: nil,
	}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(50*time.Millisecond))
	c.backend = stub

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
	client := &Client{}

	stub := &stubBackend{
		msg: Message{
			MsgID:   1,
			BatchID: 99,
			Type:    "ignored.type",
			Payload: `{}`,
		},
	}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(50*time.Millisecond),
		WithUnknownHandlerPolicy(AckUnknown))
	c.backend = stub

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
	client := &Client{}
	stub := &stubBackend{}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(10*time.Millisecond))
	c.backend = stub

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.lastMax); got != math.MaxInt32 {
		t.Fatalf("default maxMessages = %d, want math.MaxInt32", got)
	}
}

func TestConsumer_WithMaxMessagesPassesReceiveLimit(t *testing.T) {
	client := &Client{}
	stub := &stubBackend{}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(10*time.Millisecond),
		WithMaxMessages(123))
	c.backend = stub

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.lastMax); got != 123 {
		t.Fatalf("configured maxMessages = %d, want 123", got)
	}
}

func TestConsumer_WithRetryAfterPassesNackOption(t *testing.T) {
	client := &Client{}
	retryAfter := 7 * time.Second
	stub := &stubBackend{
		msg: Message{
			MsgID:   1,
			BatchID: 42,
			Type:    "no.handler.registered",
			Payload: `{}`,
		},
	}

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(50*time.Millisecond),
		WithRetryAfter(retryAfter))
	c.backend = stub

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.nackCount); got == 0 {
		t.Fatal("expected Nack to be attempted")
	}
	stub.mu.Lock()
	got := stub.lastNackOptions.RetryAfter
	stub.mu.Unlock()
	if got == nil || *got != retryAfter {
		t.Fatalf("RetryAfter = %v, want %s", got, retryAfter)
	}
}

func TestWithRetryAfterPanicsOnNegative(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	WithRetryAfter(-time.Second)
}
