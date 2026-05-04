// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque-go"
)

// TestRace_ConcurrentSend: many goroutines call Send concurrently; no race,
// all events are persisted. Run under -race to catch shared-state issues.
func TestRace_ConcurrentSend(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	const goroutines = 10
	const perGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if _, err := client.Send(ctx, queue, pgque.Event{
					Type:    "race.send",
					Payload: map[string]any{"g": g, "i": i},
				}); err != nil {
					t.Errorf("send goroutine %d msg %d: %v", g, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	tick(t, client, queue)

	expected := goroutines * perGoroutine
	// pgque.receive truncates yielded rows at i_max_return, but Ack
	// finishes the entire batch — events past the cap are not yielded
	// again without a fresh tick. Size the cap above the total so the
	// whole tick window flows out in one call.
	total := 0
	for {
		msgs, err := client.Receive(ctx, queue, consumer, 2*expected)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			break
		}
		total += len(msgs)
		if _, err := client.Ack(ctx, msgs[0].BatchID); err != nil {
			t.Fatal(err)
		}
	}
	if total != expected {
		t.Fatalf("expected %d messages received, got %d", expected, total)
	}
}

// TestRace_SendReceiveLoop runs producers and consumers in parallel for a
// short window. Designed to be run under `go test -race`.
func TestRace_SendReceiveLoop(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var sent, received int64

	// Two producer goroutines.
	var producers sync.WaitGroup
	producers.Add(2)
	for p := 0; p < 2; p++ {
		go func() {
			defer producers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if _, err := client.Send(ctx, queue, pgque.Event{
					Type: "loop.test", Payload: map[string]any{"t": time.Now().UnixNano()},
				}); err != nil {
					if ctx.Err() != nil {
						return
					}
					t.Errorf("send: %v", err)
					return
				}
				atomic.AddInt64(&sent, 1)
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	// Single consumer goroutine.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, err := client.Pool().Exec(ctx, "select pgque.ticker($1)", queue); err != nil {
				if ctx.Err() != nil {
					return
				}
			}
			msgs, err := client.Receive(ctx, queue, consumer, 100)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if len(msgs) > 0 {
				atomic.AddInt64(&received, int64(len(msgs)))
				_, _ = client.Ack(ctx, msgs[0].BatchID)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	producers.Wait()
	<-consumerDone

	if atomic.LoadInt64(&sent) == 0 {
		t.Fatal("no messages sent")
	}
	t.Logf("sent=%d received=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&received))
}

// TestRace_HandlerNackUnderLoad: producers + a consumer whose handler
// randomly errors. Verifies retry_queue accumulates the failed messages
// without races or deadlocks.
func TestRace_HandlerNackUnderLoad(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const total = 30
	for i := 0; i < total; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "rand.test", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client, queue)

	rng := rand.New(rand.NewSource(1))
	var rngMu sync.Mutex

	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(50*time.Millisecond))
	c.Handle("rand.test", func(ctx context.Context, m pgque.Message) error {
		rngMu.Lock()
		fail := rng.Intn(3) == 0
		rngMu.Unlock()
		if fail {
			return errors.New("simulated random failure")
		}
		return nil
	})

	go c.Start(ctx)

	<-ctx.Done()

	// The retry_queue must have accumulated at least one failed message.
	// With a fixed RNG seed (source=1) and 30 messages at ~1/3 failure
	// rate, at least one nack is guaranteed deterministically.
	failed := retryQueueCount(t, client, queue)
	if failed == 0 {
		t.Errorf("expected retry_queue to contain failed messages, got 0")
	}
}

// TestConcurrent_TwoConsumersDistinctNames: two independent consumers
// (different registration names) on the same queue each receive a
// full copy of every message. PgQ delivers one batch per consumer name,
// so each consumer must see all total messages exactly once.
func TestConcurrent_TwoConsumersDistinctNames(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumerA := setupFreshQueue(t, client)
	ctx := context.Background()

	// Register a second independent consumer on the same queue.
	consumerB := "gotest_c_" + randSuffix(t)
	if _, err := client.Pool().Exec(ctx, "select pgque.register_consumer($1, $2)", queue, consumerB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Pool().Exec(context.Background(), "select pgque.unregister_consumer($1, $2)", queue, consumerB)
	})

	const total = 10
	for i := 0; i < total; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "twin.test", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client, queue)

	var countA, countB int64

	consumerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	mkConsumer := func(name string, counter *int64) *pgque.Consumer {
		c := client.NewConsumer(queue, name, pgque.WithPollInterval(50*time.Millisecond))
		c.Handle("twin.test", func(ctx context.Context, m pgque.Message) error {
			atomic.AddInt64(counter, 1)
			return nil
		})
		return c
	}

	go mkConsumer(consumerA, &countA).Start(consumerCtx)
	go mkConsumer(consumerB, &countB).Start(consumerCtx)

	<-consumerCtx.Done()

	gotA := atomic.LoadInt64(&countA)
	gotB := atomic.LoadInt64(&countB)
	if gotA != total {
		t.Errorf("consumer A: expected %d messages, got %d", total, gotA)
	}
	if gotB != total {
		t.Errorf("consumer B: expected %d messages, got %d", total, gotB)
	}
}

// TestConsumer_WithMaxMessages_FetchesEntireBatch: prior to WithMaxMessages
// the Consumer hardcoded Receive(..., 100), capping every poll at 100 rows
// and stranding any extras in the same tick window. Send 105 messages,
// configure WithMaxMessages(500), and verify the handler is invoked for
// every one in a single Receive cycle.
func TestConsumer_WithMaxMessages_FetchesEntireBatch(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	const total = 105
	payloads := make([]any, total)
	for i := 0; i < total; i++ {
		payloads[i] = map[string]any{"i": i}
	}
	if _, err := client.SendBatch(ctx, queue, "wide.batch", payloads); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	var seen int32
	c := client.NewConsumer(queue, consumer,
		pgque.WithPollInterval(50*time.Millisecond),
		pgque.WithMaxMessages(500))
	c.Handle("wide.batch", func(ctx context.Context, m pgque.Message) error {
		atomic.AddInt32(&seen, 1)
		return nil
	})

	consumerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go c.Start(consumerCtx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&seen) >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&seen); got != total {
		t.Fatalf("expected %d messages dispatched, got %d (max_messages plumbing broken?)", total, got)
	}
}

// TestConsumer_DoubleStart_DoesNotPanic: calling Start twice concurrently on
// the same Consumer instance must not panic. The semantics of concurrent
// Start calls are otherwise undefined; this test only asserts absence of panic.
func TestConsumer_DoubleStart_DoesNotPanic(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)

	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(50*time.Millisecond))
	c.Handle("twice", func(ctx context.Context, m pgque.Message) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Start panicked: %v", r)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			c.Start(ctx)
		}()
	}
	wg.Wait()
}
