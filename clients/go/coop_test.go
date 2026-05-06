// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

// White-box tests for the cooperative consumer option-threading layer.
// Lives in package pgque so it can read unexported Consumer fields.

package pgque

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWithSubconsumer_SetsConsumerField asserts that WithSubconsumer
// sets the subconsumer name and toggles coop mode on.
func TestWithSubconsumer_SetsConsumerField(t *testing.T) {
	client := &Client{}

	c := client.NewConsumer("q", "logical_consumer",
		WithSubconsumer("worker-1"))

	if c.subconsumer != "worker-1" {
		t.Fatalf("subconsumer = %q, want %q", c.subconsumer, "worker-1")
	}
	if !c.coopMode() {
		t.Fatalf("coopMode() = false, want true after WithSubconsumer")
	}
}

// TestWithDeadInterval_SetsConsumerField asserts that WithDeadInterval
// stores the duration on the Consumer.
func TestWithDeadInterval_SetsConsumerField(t *testing.T) {
	client := &Client{}
	d := 90 * time.Second
	c := client.NewConsumer("q", "logical_consumer",
		WithSubconsumer("worker-1"),
		WithDeadInterval(d))

	if c.deadInterval != d {
		t.Fatalf("deadInterval = %v, want %v", c.deadInterval, d)
	}
}

// TestNewConsumer_DefaultsCoopOff is the baseline: a Consumer built
// without WithSubconsumer must have coop mode disabled and an empty
// subconsumer name. Catches accidental defaults that would force every
// existing consumer onto the cooperative path.
func TestNewConsumer_DefaultsCoopOff(t *testing.T) {
	client := &Client{}
	c := client.NewConsumer("q", "logical_consumer")

	if c.subconsumer != "" {
		t.Fatalf("default subconsumer = %q, want empty", c.subconsumer)
	}
	if c.coopMode() {
		t.Fatal("default coopMode() = true, want false")
	}
	if c.deadInterval != 0 {
		t.Fatalf("default deadInterval = %v, want 0", c.deadInterval)
	}
}

// coopStubBackend records calls to ReceiveCoop / Receive and lets a
// test inspect which path the Consumer used. It returns no messages.
type coopStubBackend struct {
	mu sync.Mutex

	receiveCalls     int32
	receiveCoopCalls int32

	lastQueue       string
	lastConsumer    string
	lastSubconsumer string
	lastMaxMessages int32
	lastDeadAccess  time.Duration
}

func (s *coopStubBackend) Receive(_ context.Context, queue, consumer string, maxMessages int) ([]Message, error) {
	atomic.AddInt32(&s.receiveCalls, 1)
	s.mu.Lock()
	s.lastQueue = queue
	s.lastConsumer = consumer
	s.lastMaxMessages = int32(maxMessages)
	s.mu.Unlock()
	return nil, nil
}

func (s *coopStubBackend) Ack(_ context.Context, _ int64) (int64, error) {
	return 1, nil
}

func (s *coopStubBackend) Nack(_ context.Context, _ int64, _ Message, _ NackOptions) error {
	return nil
}

func (s *coopStubBackend) ReceiveCoop(_ context.Context, queue, consumer, subconsumer string, opts ...ReceiveCoopOption) ([]Message, error) {
	atomic.AddInt32(&s.receiveCoopCalls, 1)

	cfg := newReceiveCoopConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	s.mu.Lock()
	s.lastQueue = queue
	s.lastConsumer = consumer
	s.lastSubconsumer = subconsumer
	s.lastMaxMessages = int32(cfg.maxMessages)
	s.lastDeadAccess = cfg.deadInterval
	s.mu.Unlock()
	return nil, nil
}

// TestConsumer_NormalModeUsesReceive: without WithSubconsumer, the
// Consumer poll loop must call Receive (not ReceiveCoop), preserving
// today's behavior for existing users.
func TestConsumer_NormalModeUsesReceive(t *testing.T) {
	client := &Client{}
	stub := &coopStubBackend{}

	c := client.NewConsumer("q", "logical_consumer",
		WithPollInterval(10*time.Millisecond))
	c.backend = stub

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.receiveCalls); got == 0 {
		t.Fatal("expected Receive to be called in normal mode")
	}
	if got := atomic.LoadInt32(&stub.receiveCoopCalls); got != 0 {
		t.Fatalf("ReceiveCoop called %d times in normal mode, want 0", got)
	}
}

// TestConsumer_CoopModeUsesReceiveCoop: with WithSubconsumer, the
// poll loop must call ReceiveCoop and pass the subconsumer name plus
// the configured dead interval.
func TestConsumer_CoopModeUsesReceiveCoop(t *testing.T) {
	client := &Client{}
	stub := &coopStubBackend{}

	dead := 45 * time.Second
	c := client.NewConsumer("q", "logical_consumer",
		WithPollInterval(10*time.Millisecond),
		WithSubconsumer("worker-1"),
		WithDeadInterval(dead))
	c.backend = stub

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	if got := atomic.LoadInt32(&stub.receiveCoopCalls); got == 0 {
		t.Fatal("expected ReceiveCoop to be called in coop mode")
	}
	if got := atomic.LoadInt32(&stub.receiveCalls); got != 0 {
		t.Fatalf("Receive called %d times in coop mode, want 0", got)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.lastSubconsumer != "worker-1" {
		t.Fatalf("ReceiveCoop got subconsumer %q, want %q", stub.lastSubconsumer, "worker-1")
	}
	if stub.lastDeadAccess != dead {
		t.Fatalf("ReceiveCoop got dead interval %v, want %v", stub.lastDeadAccess, dead)
	}
}

// TestReceiveCoopConfig_Defaults locks in the default max messages
// (100, matching the SQL default) and zero dead interval (no takeover).
func TestReceiveCoopConfig_Defaults(t *testing.T) {
	cfg := newReceiveCoopConfig()
	if cfg.maxMessages != 100 {
		t.Fatalf("default maxMessages = %d, want 100", cfg.maxMessages)
	}
	if cfg.deadInterval != 0 {
		t.Fatalf("default deadInterval = %v, want 0", cfg.deadInterval)
	}
}

// TestWithCoopMaxMessages_Sets confirms the option threads through.
func TestWithCoopMaxMessages_Sets(t *testing.T) {
	cfg := newReceiveCoopConfig()
	WithCoopMaxMessages(50)(cfg)
	if cfg.maxMessages != 50 {
		t.Fatalf("maxMessages = %d, want 50", cfg.maxMessages)
	}
}

// TestWithCoopMaxMessages_PanicsOnZero matches the WithMaxMessages
// contract: a non-positive limit is a programmer error.
func TestWithCoopMaxMessages_PanicsOnZero(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for n=0")
		}
	}()
	WithCoopMaxMessages(0)
}

// TestWithCoopDeadInterval_Sets confirms the dead-interval option
// threads through.
func TestWithCoopDeadInterval_Sets(t *testing.T) {
	cfg := newReceiveCoopConfig()
	WithCoopDeadInterval(30 * time.Second)(cfg)
	if cfg.deadInterval != 30*time.Second {
		t.Fatalf("deadInterval = %v, want 30s", cfg.deadInterval)
	}
}

// TestUnsubscribeSubconsumerConfig_Default locks the default
// batch-handling value (0 = refuse if active batch exists).
func TestUnsubscribeSubconsumerConfig_Default(t *testing.T) {
	cfg := newUnsubscribeSubconsumerConfig()
	if cfg.batchHandling != 0 {
		t.Fatalf("default batchHandling = %d, want 0", cfg.batchHandling)
	}
}

// TestWithBatchHandlingRetry_SetsOne asserts the option flips
// batch_handling to 1 (route active batch through retry/DLQ).
func TestWithBatchHandlingRetry_SetsOne(t *testing.T) {
	cfg := newUnsubscribeSubconsumerConfig()
	WithBatchHandlingRetry()(cfg)
	if cfg.batchHandling != 1 {
		t.Fatalf("batchHandling = %d, want 1", cfg.batchHandling)
	}
}
