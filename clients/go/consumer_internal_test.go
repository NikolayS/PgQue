// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestConsumer_NullTypeAlwaysUsesUnknownPolicy(t *testing.T) {
	client := &Client{}
	stub := &stubBackend{
		msg: Message{MsgID: 1, BatchID: 100, Type: "", Payload: ""},
	}
	var handlerCount int32

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(10*time.Millisecond))
	c.backend = stub
	c.Handle("", func(_ context.Context, _ Message) error {
		atomic.AddInt32(&handlerCount, 1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&handlerCount); got != 0 {
		t.Fatalf("NULL/empty type invoked an empty-string handler %d times", got)
	}
	if got := atomic.LoadInt32(&stub.nackCount); got != 1 {
		t.Fatalf("NULL/empty type must follow NackUnknown, got %d Nacks", got)
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

// redeliverStubBackend simulates a backend where the batch is never
// finished: Receive keeps returning the same message (as pgque.next_batch
// does for an unfinished batch), and Nack/Ack fail with the configured
// errors. It counts Receive calls so tests can detect a tight re-poll
// loop that skips the poll-interval backoff.
type redeliverStubBackend struct {
	msg     Message
	nackErr error
	ackErr  error

	receiveCount int32
	nackCount    int32
	ackCount     int32
}

func (s *redeliverStubBackend) Receive(_ context.Context, _, _ string, _ int) ([]Message, error) {
	atomic.AddInt32(&s.receiveCount, 1)
	return []Message{s.msg}, nil
}

func (s *redeliverStubBackend) Ack(_ context.Context, _ int64) (int64, error) {
	atomic.AddInt32(&s.ackCount, 1)
	if s.ackErr != nil {
		return 0, s.ackErr
	}
	return 1, nil
}

func (s *redeliverStubBackend) Nack(_ context.Context, _ int64, _ Message, _ NackOptions) error {
	atomic.AddInt32(&s.nackCount, 1)
	return s.nackErr
}

// maxPollsWithin returns a generous upper bound on how many Receive
// calls a well-behaved poll loop can make in window when it sleeps
// pollInterval between attempts: window/pollInterval plus slack for
// scheduling jitter and the initial poll.
func maxPollsWithin(window, pollInterval time.Duration) int32 {
	return int32(window/pollInterval) + 3
}

// TestConsumer_NackFailure_BacksOffBeforeRepoll guards against the
// tight-loop bug: when a Nack fails, the batch is left unfinished and
// the next Receive returns the same batch immediately. Without a
// poll-interval sleep on the nack-failure path the loop re-receives
// and re-runs every handler at full speed. The Consumer must wait
// pollInterval before re-polling.
func TestConsumer_NackFailure_BacksOffBeforeRepoll(t *testing.T) {
	client := &Client{}

	stub := &redeliverStubBackend{
		msg: Message{
			MsgID:   1,
			BatchID: 42,
			Type:    "no.handler.registered",
			Payload: `{}`,
		},
		nackErr: errors.New("simulated persistent nack failure"),
	}

	pollInterval := 50 * time.Millisecond
	window := 400 * time.Millisecond

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(pollInterval))
	c.backend = stub
	// No Handle() call — the unknown-type path Nacks under the default
	// policy, the Nack fails, and the batch is redelivered forever.

	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	_ = c.Start(ctx)

	got := atomic.LoadInt32(&stub.receiveCount)
	if max := maxPollsWithin(window, pollInterval); got > max {
		t.Fatalf("Receive called %d times in %v with pollInterval %v — "+
			"tight re-poll loop on nack failure (want <= %d)",
			got, window, pollInterval, max)
	}
}

// TestConsumer_AckFailure_BacksOffBeforeRepoll is the same guard for
// the Ack-error path: an Ack failure leaves the batch unfinished, so
// re-polling without a pollInterval sleep re-executes every handler in
// the batch immediately (duplicate side effects at full speed).
func TestConsumer_AckFailure_BacksOffBeforeRepoll(t *testing.T) {
	client := &Client{}

	stub := &redeliverStubBackend{
		msg: Message{
			MsgID:   1,
			BatchID: 43,
			Type:    "ok.type",
			Payload: `{}`,
		},
		ackErr: errors.New("simulated persistent ack failure"),
	}

	pollInterval := 50 * time.Millisecond
	window := 400 * time.Millisecond

	c := client.NewConsumer("dummy_queue", "dummy_consumer",
		WithPollInterval(pollInterval))
	c.backend = stub
	c.Handle("ok.type", func(_ context.Context, _ Message) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	_ = c.Start(ctx)

	got := atomic.LoadInt32(&stub.receiveCount)
	if max := maxPollsWithin(window, pollInterval); got > max {
		t.Fatalf("Receive called %d times in %v with pollInterval %v — "+
			"tight re-poll loop on ack failure (want <= %d)",
			got, window, pollInterval, max)
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
