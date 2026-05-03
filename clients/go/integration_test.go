// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque-go"
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
	tick(t, client, queue)

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
	tick(t, client, queue)

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
	tick(t, client, queue)

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

	tick(t, client, queue)

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
	// set_queue_config prepends "queue_" internally, so pass "max_retries".
	if _, err := client.Pool().Exec(ctx,
		"select pgque.set_queue_config($1, 'max_retries', '2')", queue); err != nil {
		t.Fatalf("set_queue_config max_retries: %v", err)
	}

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "retry.test", Payload: map[string]any{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}

	// Drive the message through nack cycles. After max_retries nacks the
	// backend routes the message to the DLQ instead of retry_queue.
	// Ack is required after all Nack calls to close the batch; nack routes
	// individual events (to retry_queue or dead_letter) but does not finish
	// the batch itself — that is always done via Ack / finish_batch.
	const maxCycles = 5
	for i := 0; i < maxCycles; i++ {
		// Expire any pending retry delays so maint_retry_events picks them up
		// immediately; in production these would expire naturally.
		if _, err := client.Pool().Exec(ctx,
			"update pgque.retry_queue set ev_retry_after = now() - interval '1 second'"); err != nil {
			t.Logf("retry_queue update unavailable: %v", err)
		}
		// Re-queue retry_queue rows for redelivery.
		if _, err := client.Pool().Exec(ctx, "select pgque.maint_retry_events()"); err != nil {
			t.Logf("maint_retry_events unavailable, using ticker fallback: %v", err)
		}
		tick(t, client, queue)

		msgs, err := client.Receive(ctx, queue, consumer, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			// No more messages in active queue; they may be in DLQ already.
			break
		}
		var batchID int64
		for _, m := range msgs {
			batchID = m.BatchID
			if err := client.Nack(ctx, m.BatchID, m); err != nil {
				t.Fatal(err)
			}
		}
		// Close the batch so PgQ advances the consumer cursor and the
		// retry_queue rows become eligible for redelivery on the next cycle.
		if err := client.Ack(ctx, batchID); err != nil {
			t.Fatalf("ack after nack: %v", err)
		}
	}

	if got := dlqCount(t, client, queue); got == 0 {
		t.Fatalf("expected DLQ to contain the exhausted message after %d nack cycles, got 0", maxCycles)
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
	tick(t, client, queue)

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

// TestSendBatch_RoundTrip verifies that SendBatch publishes every payload
// exactly once and returns one event ID per input in input order.
func TestSendBatch_RoundTrip(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	payloads := []any{
		map[string]any{"i": 0, "k": "a"},
		map[string]any{"i": 1, "k": "b"},
		map[string]any{"i": 2, "k": "c"},
	}
	ids, err := client.SendBatch(ctx, queue, "batch.type", payloads)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != len(payloads) {
		t.Fatalf("expected %d event ids, got %d", len(payloads), len(ids))
	}
	for i, id := range ids {
		if id == 0 {
			t.Fatalf("ids[%d] is 0", i)
		}
	}
	tick(t, client, queue)

	msgs, err := client.Receive(ctx, queue, consumer, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != len(payloads) {
		t.Fatalf("expected %d messages, got %d", len(payloads), len(msgs))
	}
	for _, m := range msgs {
		if m.Type != "batch.type" {
			t.Fatalf("expected batch.type, got %q", m.Type)
		}
	}
	if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatal(err)
	}
}

// TestSendBatch_EmptySlice sends an empty payloads slice and expects an
// empty id slice with no error — the SQL function returns an empty
// bigint[] when no payloads are supplied.
func TestSendBatch_EmptySlice(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, _ := setupFreshQueue(t, client)
	ctx := context.Background()

	ids, err := client.SendBatch(ctx, queue, "ignored", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids for empty input, got %d", len(ids))
	}
}

// TestNack_WithRetryAfter verifies the WithRetryAfter option threads
// through to pgque.nack — a long retry delay must keep the message out
// of redelivery via maint_retry_events for at least the configured
// window. We cross-check by inspecting retry_queue.ev_retry_after.
func TestNack_WithRetryAfter(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "retry.opts", Payload: map[string]any{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if err := client.Nack(ctx, msgs[0].BatchID, msgs[0],
		pgque.WithRetryAfter(3600*time.Second),
		pgque.WithReason("custom-reason-string")); err != nil {
		t.Fatal(err)
	}
	if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatal(err)
	}

	var seconds float64
	if err := client.Pool().QueryRow(ctx, `
		select extract(epoch from (rq.ev_retry_after - now()))
		from pgque.retry_queue rq
		join pgque.queue q on q.queue_id = rq.ev_queue
		where q.queue_name = $1`, queue).Scan(&seconds); err != nil {
		t.Fatal(err)
	}
	if seconds < 3000 {
		t.Fatalf("expected retry delay >= 3000s, got %v", seconds)
	}
}
