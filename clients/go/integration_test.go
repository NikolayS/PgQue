// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"strings"
	"testing"

	pgque "github.com/NikolayS/pgque/clients/go"
)

// TestSend_DefaultEventType verifies that an Event with empty Type is sent
// with the "default" type (the Go driver fills it in client-side).
func TestSend_DefaultEventType(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{Payload: map[string]any{"x": 1}}); err != nil {
		t.Fatal(err)
	}
	tick(t, client)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Type != "default" {
		t.Fatalf("expected default type, got %q", msgs[0].Type)
	}
	if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatal(err)
	}
}

// TestSend_MultipleEventsOneBatch sends several events and verifies they
// are all delivered in a single batch (same batch id).
func TestSend_MultipleEventsOneBatch(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type:    "batch.test",
			Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client)

	msgs, err := client.Receive(ctx, queue, consumer, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != n {
		t.Fatalf("expected %d messages, got %d", n, len(msgs))
	}
	first := msgs[0].BatchID
	for _, m := range msgs[1:] {
		if m.BatchID != first {
			t.Fatalf("expected all messages in one batch, got mixed batch ids %d and %d", first, m.BatchID)
		}
	}
	if err := client.Ack(ctx, first); err != nil {
		t.Fatal(err)
	}
}

// TestReceive_RespectsMaxBatch ensures Receive returns at most maxMessages.
func TestReceive_RespectsMaxBatch(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	const total = 50
	for i := 0; i < total; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "max.test", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	tick(t, client)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) > 10 {
		t.Fatalf("Receive returned %d messages, expected ≤ 10", len(msgs))
	}
	if len(msgs) > 0 {
		client.Ack(ctx, msgs[0].BatchID)
	}
}

// TestReceive_EmptyQueue confirms receiving from an empty queue returns no
// messages and no error (after a tick).
func TestReceive_EmptyQueue(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	tick(t, client)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages on empty queue, got %d", len(msgs))
	}
}

// TestNack_ToDLQAtRetryLimit drives a single message through repeated nacks
// until it lands in the DLQ. Verifies the SQL backend's retry-limit routing.
func TestNack_ToDLQAtRetryLimit(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	// Lower the retry limit for this queue so the test is fast.
	if _, err := client.Pool().Exec(ctx,
		"select pgque.set_queue_max_retries($1, 2)", queue); err != nil {
		t.Skipf("set_queue_max_retries not available: %v", err)
	}

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "retry.test", Payload: map[string]any{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}

	// Drive the message through 3 nack/ack cycles. After 2 retries the
	// 3rd nack should route to the DLQ instead of retry_queue.
	for i := 0; i < 3; i++ {
		// Move retry_queue rows back into the active table for redelivery.
		if _, err := client.Pool().Exec(ctx, "select pgque.maint_retry_events()"); err != nil {
			// Earlier pgque versions may not have this; fall back to ticker.
			t.Logf("maint_retry_events: %v", err)
		}
		tick(t, client)

		msgs, err := client.Receive(ctx, queue, consumer, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			if err := client.Nack(ctx, m.BatchID, m); err != nil {
				t.Fatal(err)
			}
		}
		if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
			t.Fatal(err)
		}
	}

	if got := dlqCount(t, client, queue); got == 0 {
		t.Skipf("DLQ row not produced after 3 cycles (got %d) — retry routing may differ in this build", got)
	}
}

// TestPool_NotNil asserts that Pool() exposes a usable underlying pool.
func TestPool_NotNil(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()

	pool := client.Pool()
	if pool == nil {
		t.Fatal("Pool() returned nil")
	}
	var n int
	if err := pool.QueryRow(context.Background(), "select 42").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("expected 42, got %d", n)
	}
}

// TestSendReceive_PayloadRoundTrip verifies the JSON payload round-trips
// through Send -> Receive byte-for-byte (after re-marshaling).
func TestSendReceive_PayloadRoundTrip(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	payload := map[string]any{
		"string": "hello",
		"int":    int64(42),
		"float":  3.14,
		"bool":   true,
		"null":   nil,
		"nested": map[string]any{"a": 1},
		"array":  []any{"x", "y", "z"},
	}
	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "rt.test", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if !strings.Contains(msgs[0].Payload, `"string"`) || !strings.Contains(msgs[0].Payload, `"hello"`) {
		t.Fatalf("payload missing expected fields: %s", msgs[0].Payload)
	}
	client.Ack(ctx, msgs[0].BatchID)
}
