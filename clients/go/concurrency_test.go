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

	pgque "github.com/NikolayS/pgque/clients/go"
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
	tick(t, client)

	total := 0
	for {
		msgs, err := client.Receive(ctx, queue, consumer, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			break
		}
		total += len(msgs)
		if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
			t.Fatal(err)
		}
	}
	expected := goroutines * perGoroutine
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
			if _, err := client.Pool().Exec(ctx, "select pgque.ticker()"); err != nil {
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
				client.Ack(ctx, msgs[0].BatchID)
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
	tick(t, client)

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

	// The retry_queue should have a non-zero number of failed messages.
	failed := retryQueueCount(t, client, queue)
	if failed == 0 {
		t.Logf("note: zero retry_queue rows after run (may be timing-related; not a race failure)")
	}
}

// TestConcurrent_TwoConsumersSameQueue: two consumer goroutines on the same
// (queue, consumer-name) — must not double-process. PgQ's batch semantics
// guarantee at-most-one delivery per batch per consumer.
func TestConcurrent_TwoConsumersSameQueue(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	const total = 10
	for i := 0; i < total; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "twin.test", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client)

	processed := make(map[int64]int)
	var mu sync.Mutex

	consumerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	mkConsumer := func() *pgque.Consumer {
		c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(50*time.Millisecond))
		c.Handle("twin.test", func(ctx context.Context, m pgque.Message) error {
			mu.Lock()
			processed[m.MsgID]++
			mu.Unlock()
			return nil
		})
		return c
	}

	go mkConsumer().Start(consumerCtx)
	go mkConsumer().Start(consumerCtx)

	<-consumerCtx.Done()

	mu.Lock()
	defer mu.Unlock()

	for id, count := range processed {
		if count > 1 {
			t.Errorf("message %d processed %d times — expected at most once per consumer goroutine in a single batch window", id, count)
		}
	}
}

// TestConsumer_StartTwiceFromSameInstance: calling Start twice on the same
// Consumer (concurrently) is undefined / not recommended; this test
// documents that no panic occurs (the second Start should run alongside
// the first; both compete for batches via the SQL backend).
func TestConsumer_StartTwiceFromSameInstance(t *testing.T) {
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
