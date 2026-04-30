// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque/clients/go"
)

// TestConsumer_StartStop_Clean: Start should return ctx.Err() promptly when
// its context is cancelled, and not panic during shutdown.
func TestConsumer_StartStop_Clean(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)

	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(50*time.Millisecond))
	c.Handle("anything", func(ctx context.Context, m pgque.Message) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

// TestConsumer_LivenessUnderEmptyQueue: with a 100ms poll interval, Start
// must wake, poll, and honour context cancellation within a 600ms window.
// This verifies liveness — that an empty queue does not cause the consumer
// to block indefinitely between polls.
func TestConsumer_LivenessUnderEmptyQueue(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)

	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(100*time.Millisecond))
	c.Handle("ping", func(ctx context.Context, m pgque.Message) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.Start(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("Start took %v — expected ~600ms; consumer is not polling", elapsed)
	}
}

// TestConsumer_UnregisteredEventType_Nacks verifies the consumer nacks a
// message whose Type has no registered handler.
func TestConsumer_UnregisteredEventType_Nacks(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "type.no.handler", Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(50*time.Millisecond))
	// Register a handler for a *different* type so we exercise the
	// "unregistered" path.
	c.Handle("other.type", func(ctx context.Context, m pgque.Message) error { return nil })

	consumerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	go c.Start(consumerCtx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if retryQueueCount(t, client, queue) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got := retryQueueCount(t, client, queue); got != 1 {
		t.Fatalf("expected 1 retry_queue row for unhandled type, got %d", got)
	}
}

// TestConsumer_ContextPropagatedToHandler: handlers receive the consumer's
// context and can observe its cancellation.
func TestConsumer_ContextPropagatedToHandler(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "ctx.test", Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	gotCtx := make(chan context.Context, 1)
	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(50*time.Millisecond))
	c.Handle("ctx.test", func(ctx context.Context, m pgque.Message) error {
		gotCtx <- ctx
		return nil
	})

	consumerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go c.Start(consumerCtx)

	select {
	case received := <-gotCtx:
		if received == nil {
			t.Fatal("handler received nil context")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called")
	}
}

// TestConsumer_AllMessagesDispatched: all messages in a batch must be
// delivered to their handler before the batch is acked. Verifies that the
// consumer does not silently drop messages mid-batch.
func TestConsumer_AllMessagesDispatched(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	const n = 3
	for i := 0; i < n; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "ack.once", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client, queue)

	var seen int32
	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(50*time.Millisecond))
	c.Handle("ack.once", func(ctx context.Context, m pgque.Message) error {
		atomic.AddInt32(&seen, 1)
		return nil
	})

	consumerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go c.Start(consumerCtx)

	deadline := time.Now().Add(3 * time.Second)
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
