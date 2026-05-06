// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

// Integration tests for the experimental cooperative-consumer client
// API. Each test gates on a reachable PGQUE_TEST_DSN via connectOrSkip.

package pgque_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque-go"
)

// setupFreshCoopGroup creates a queue and a logical consumer name with
// a random suffix; subconsumers are auto-registered by the SQL layer
// on first receive_coop call, but a default cleanup is registered so
// drop_queue runs even on test failure.
func setupFreshCoopGroup(t *testing.T, client *pgque.Client) (queue, consumer string) {
	t.Helper()
	ctx := context.Background()
	suffix := randSuffix(t)
	queue = "gotest_coop_q_" + suffix
	consumer = "gotest_coop_c_" + suffix

	if _, err := client.Pool().Exec(ctx,
		"select pgque.create_queue($1)", queue); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		client.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
	})
	return queue, consumer
}

// TestSubscribeSubconsumer_Idempotent: first call returns 1, second
// returns 0. Locks the row-count contract documented on the SQL side.
func TestSubscribeSubconsumer_Idempotent(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	n, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("first SubscribeSubconsumer = %d, want 1", n)
	}

	n2, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("second SubscribeSubconsumer = %d, want 0", n2)
	}
}

// TestReceiveCoop_BasicRoundTrip: send N events, ReceiveCoop drains
// them, Ack finishes the batch, and a second ReceiveCoop returns
// empty.
func TestReceiveCoop_BasicRoundTrip(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1"); err != nil {
		t.Fatal(err)
	}

	const n = 3
	for i := 0; i < n; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "coop.test", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client, queue)

	msgs, err := client.ReceiveCoop(ctx, queue, consumer, "worker-1",
		pgque.WithCoopMaxMessages(100))
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != n {
		t.Fatalf("ReceiveCoop returned %d messages, want %d", len(msgs), n)
	}
	if _, err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatal(err)
	}

	// Need a tick for the next batch even though the queue is empty;
	// receive_coop auto-finishes empty windows internally.
	tick(t, client, queue)
	msgs2, err := client.ReceiveCoop(ctx, queue, consumer, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs2) != 0 {
		t.Fatalf("second ReceiveCoop returned %d messages, want 0", len(msgs2))
	}
}

// TestReceiveCoop_TwoSubconsumersDistinctBatches: two subconsumers
// under one logical consumer must each receive a distinct batch_id
// across two ticks. Verifies the SQL allocation hands a tick to one
// member at a time and that the Go wrapper preserves that.
func TestReceiveCoop_TwoSubconsumersDistinctBatches(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	for _, name := range []string{"worker-a", "worker-b"} {
		if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, name); err != nil {
			t.Fatal(err)
		}
	}

	// Tick 1
	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "coop.split", Payload: map[string]any{"i": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)
	a1, err := client.ReceiveCoop(ctx, queue, consumer, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(a1) != 1 {
		t.Fatalf("worker-a tick 1 got %d messages, want 1", len(a1))
	}
	// worker-b on the same tick window must see nothing — tick 1 is
	// owned by worker-a until acked.
	b1, err := client.ReceiveCoop(ctx, queue, consumer, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(b1) != 0 {
		t.Fatalf("worker-b tick 1 saw %d messages, want 0 (tick still owned by worker-a)", len(b1))
	}
	if _, err := client.Ack(ctx, a1[0].BatchID); err != nil {
		t.Fatal(err)
	}

	// Tick 2: worker-b should get the new batch.
	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "coop.split", Payload: map[string]any{"i": 2},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)
	b2, err := client.ReceiveCoop(ctx, queue, consumer, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(b2) != 1 {
		t.Fatalf("worker-b tick 2 got %d messages, want 1", len(b2))
	}
	if a1[0].BatchID == b2[0].BatchID {
		t.Fatalf("worker-a and worker-b received same batch_id %d", a1[0].BatchID)
	}
	if _, err := client.Ack(ctx, b2[0].BatchID); err != nil {
		t.Fatal(err)
	}
}

// TestUnsubscribeSubconsumer_IdleSucceeds: unsubscribing a subconsumer
// with no active batch returns 1 (one row removed) and no error.
func TestUnsubscribeSubconsumer_IdleSucceeds(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1"); err != nil {
		t.Fatal(err)
	}

	n, err := client.UnsubscribeSubconsumer(ctx, queue, consumer, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("UnsubscribeSubconsumer = %d, want 1", n)
	}
}

// TestUnsubscribeSubconsumer_ActiveBatchDefaultErrors: an active batch
// must block default unsubscribe; passing WithBatchHandlingRetry()
// routes the messages through retry/DLQ and removes the row.
func TestUnsubscribeSubconsumer_ActiveBatchDefaultErrors(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "coop.unsub", Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	msgs, err := client.ReceiveCoop(ctx, queue, consumer, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message before unsubscribe, got %d", len(msgs))
	}

	// Default options must error out — active batch present.
	_, err = client.UnsubscribeSubconsumer(ctx, queue, consumer, "worker-1")
	if err == nil {
		t.Fatal("expected error when unsubscribing with active batch (default batch_handling=0)")
	}
	// Should be a SQL error (not a typed sentinel).
	var sqlErr *pgque.SQLError
	if !errors.As(err, &sqlErr) {
		t.Logf("note: expected *SQLError, got %T (still acceptable as long as it surfaces)", err)
	}

	// WithBatchHandlingRetry must succeed.
	n, err := client.UnsubscribeSubconsumer(ctx, queue, consumer, "worker-1",
		pgque.WithBatchHandlingRetry())
	if err != nil {
		t.Fatalf("WithBatchHandlingRetry: unexpected error %v", err)
	}
	if n != 1 {
		t.Fatalf("WithBatchHandlingRetry returned %d, want 1", n)
	}
	// The active batch's messages should be in retry_queue.
	if got := retryQueueCount(t, client, queue); got == 0 {
		t.Fatalf("expected retry_queue to hold the routed message, got 0")
	}
}

// TestTouchSubconsumer_RegisteredReturnsOne verifies the heartbeat
// updates exactly one row when called against a registered idle
// subconsumer.
func TestTouchSubconsumer_RegisteredReturnsOne(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1"); err != nil {
		t.Fatal(err)
	}

	n, err := client.TouchSubconsumer(ctx, queue, consumer, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("TouchSubconsumer = %d, want 1", n)
	}
}

// TestConsumer_WithSubconsumer_DispatchesAndAcks: the high-level
// Consumer with WithSubconsumer must drive the coop receive path,
// dispatch handlers, and ack normally.
func TestConsumer_WithSubconsumer_DispatchesAndAcks(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	// Register the subconsumer BEFORE producing so its cursor predates
	// the events; without this, the first auto-registration happens at
	// the current tick and the events would be invisible.
	if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1"); err != nil {
		t.Fatal(err)
	}

	const n = 4
	for i := 0; i < n; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "coop.high", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client, queue)

	var seen int32
	c := client.NewConsumer(queue, consumer,
		pgque.WithPollInterval(50*time.Millisecond),
		pgque.WithSubconsumer("worker-1"))
	c.Handle("coop.high", func(ctx context.Context, m pgque.Message) error {
		atomic.AddInt32(&seen, 1)
		return nil
	})

	consumerCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	go c.Start(consumerCtx)

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&seen) >= n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&seen); got != n {
		t.Fatalf("expected handler called %d times, got %d", n, got)
	}
}

// TestConsumer_WithoutSubconsumer_UnchangedSmoke is a smoke test:
// without WithSubconsumer the high-level Consumer behaves as before
// (same path used in pre-coop tests). The queue + consumer are set up
// with the legacy register_consumer helper.
func TestConsumer_WithoutSubconsumer_UnchangedSmoke(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "non.coop", Payload: map[string]any{"i": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	var seen int32
	c := client.NewConsumer(queue, consumer,
		pgque.WithPollInterval(50*time.Millisecond))
	c.Handle("non.coop", func(ctx context.Context, m pgque.Message) error {
		atomic.AddInt32(&seen, 1)
		return nil
	})

	consumerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go c.Start(consumerCtx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&seen) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&seen); got != 1 {
		t.Fatalf("expected handler called 1 time, got %d", got)
	}
}

// TestConsumer_TwoSubconsumers_DisjointDelivery is the full
// two-worker demo encoded as a test: two goroutines under one logical
// consumer must observe disjoint message IDs and the union must equal
// every published message.
func TestConsumer_TwoSubconsumers_DisjointDelivery(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	// Subscribe both subconsumers before publishing so their cursors
	// predate the events.
	for _, name := range []string{"worker-a", "worker-b"} {
		if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, name); err != nil {
			t.Fatal(err)
		}
	}

	// Publish events across multiple ticks so different batches have
	// different owners. One event per tick is the simplest way to
	// guarantee both workers see at least one batch each in this
	// allocation model.
	const ticks = 4
	expectTotal := int32(ticks)
	for i := 0; i < ticks; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "coop.disjoint", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
		tick(t, client, queue)
	}

	var (
		mu      sync.Mutex
		seenByA []int64
		seenByB []int64
		total   int32
	)

	makeConsumer := func(name string, dst *[]int64) *pgque.Consumer {
		c := client.NewConsumer(queue, consumer,
			pgque.WithPollInterval(50*time.Millisecond),
			pgque.WithSubconsumer(name))
		c.Handle("coop.disjoint", func(ctx context.Context, m pgque.Message) error {
			mu.Lock()
			*dst = append(*dst, m.MsgID)
			mu.Unlock()
			atomic.AddInt32(&total, 1)
			return nil
		})
		return c
	}

	cA := makeConsumer("worker-a", &seenByA)
	cB := makeConsumer("worker-b", &seenByB)

	consumerCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	go cA.Start(consumerCtx)
	go cB.Start(consumerCtx)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&total) >= expectTotal {
			break
		}
		// Force additional ticks so the SQL allocator hands the next
		// tick to whichever worker is idle.
		tick(t, client, queue)
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	got := int32(len(seenByA) + len(seenByB))
	if got < expectTotal {
		t.Fatalf("total delivered = %d, want >= %d (a=%d b=%d)",
			got, expectTotal, len(seenByA), len(seenByB))
	}
	// Disjoint check across both workers.
	seen := make(map[int64]string, len(seenByA)+len(seenByB))
	for _, id := range seenByA {
		if owner, dup := seen[id]; dup {
			t.Fatalf("msg %d delivered twice (first to %s, then to worker-a)", id, owner)
		}
		seen[id] = "worker-a"
	}
	for _, id := range seenByB {
		if owner, dup := seen[id]; dup {
			t.Fatalf("msg %d delivered twice (first to %s, then to worker-b)", id, owner)
		}
		seen[id] = "worker-b"
	}
}

// TestReceive_OnCoopMain_RaisesGuard: once a logical consumer has
// subconsumers, a normal Receive on that name must surface a SQL
// error directing the caller to the cooperative form. Acts as a
// regression guard so the typed-error layer never silently maps this
// to an empty result.
func TestReceive_OnCoopMain_RaisesGuard(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1"); err != nil {
		t.Fatal(err)
	}

	_, err := client.Receive(ctx, queue, consumer, 10)
	if err == nil {
		t.Fatal("expected error from Receive on cooperative main, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "coop") &&
		!strings.Contains(strings.ToLower(err.Error()), "subconsumer") {
		t.Fatalf("error should mention cooperative form; got %q", err.Error())
	}
}
