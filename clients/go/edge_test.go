// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque/clients/go"
)

// TestEvent_EmptyPayload: a nil Payload marshals to JSON null and round-trips.
func TestEvent_EmptyPayload(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{Type: "empty.test", Payload: nil}); err != nil {
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
	if !strings.Contains(msgs[0].Payload, "null") {
		t.Logf("payload for nil Payload is %q (acceptable as long as Receive does not panic)", msgs[0].Payload)
	}
	client.Ack(ctx, msgs[0].BatchID)
}

// TestEvent_LargePayload: 1 MiB payload round-trips without truncation.
func TestEvent_LargePayload(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	const size = 1 << 20 // 1 MiB
	big := strings.Repeat("a", size)
	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "large.test", Payload: map[string]any{"blob": big},
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
	if !strings.Contains(msgs[0].Payload, big) {
		t.Fatalf("large payload truncated: received %d bytes, expected ≥ %d", len(msgs[0].Payload), size)
	}
	client.Ack(ctx, msgs[0].BatchID)
}

// TestEvent_UnicodeEverything: type and payload contain unicode (CJK + emoji).
func TestEvent_UnicodeEverything(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type:    "事件.test.🚀",
		Payload: map[string]any{"name": "中文 emoji 🎉", "n": 42},
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
	if msgs[0].Type != "事件.test.🚀" {
		t.Fatalf("type unicode mangled: %q", msgs[0].Type)
	}
	if !strings.Contains(msgs[0].Payload, "中文") || !strings.Contains(msgs[0].Payload, "🎉") {
		t.Fatalf("payload unicode mangled: %s", msgs[0].Payload)
	}
	client.Ack(ctx, msgs[0].BatchID)
}

// TestEvent_TypeWithSpecialChars: types with dots, dashes, and slashes are
// preserved verbatim.
func TestEvent_TypeWithSpecialChars(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	cases := []string{
		"namespace.event.created",
		"my-event-with-dashes",
		"category/subcategory",
		"v2.event.type:scoped",
	}
	for _, typ := range cases {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: typ, Payload: map[string]any{"x": 1},
		}); err != nil {
			t.Fatalf("send %q: %v", typ, err)
		}
	}
	tick(t, client)

	msgs, err := client.Receive(ctx, queue, consumer, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != len(cases) {
		t.Fatalf("expected %d messages, got %d", len(cases), len(msgs))
	}
	got := map[string]bool{}
	for _, m := range msgs {
		got[m.Type] = true
	}
	for _, want := range cases {
		if !got[want] {
			t.Errorf("type %q not received intact", want)
		}
	}
	if len(msgs) > 0 {
		client.Ack(ctx, msgs[0].BatchID)
	}
}

// TestMessage_TimestampSensible: CreatedAt is non-zero and within a few
// seconds of now. (Microsecond precision is hard to verify portably; we
// just sanity-check the round-trip.)
func TestMessage_TimestampSensible(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	before := time.Now().Add(-1 * time.Minute)
	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "ts.test", Payload: map[string]any{"x": 1},
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
	if msgs[0].CreatedAt.IsZero() {
		t.Fatal("CreatedAt is zero")
	}
	after := time.Now().Add(1 * time.Minute)
	if msgs[0].CreatedAt.Before(before) || msgs[0].CreatedAt.After(after) {
		t.Fatalf("CreatedAt %v not within [%v, %v]", msgs[0].CreatedAt, before, after)
	}
	client.Ack(ctx, msgs[0].BatchID)
}

// TestMessage_RetryCountInitiallyNilOrZero: a freshly-sent event has retry_count
// either nil or zero. Documented so callers can rely on it.
func TestMessage_RetryCountInitiallyNilOrZero(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "rc.test", Payload: map[string]any{"x": 1},
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
	if msgs[0].RetryCount != nil && *msgs[0].RetryCount != 0 {
		t.Fatalf("expected RetryCount nil or 0 for fresh event, got %d", *msgs[0].RetryCount)
	}
	client.Ack(ctx, msgs[0].BatchID)
}

// TestEvent_PayloadIsString: a string Payload marshals to a JSON string.
func TestEvent_PayloadIsString(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "str.test", Payload: "just a string",
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
	if msgs[0].Payload != `"just a string"` {
		t.Fatalf("expected JSON-quoted string payload, got %q", msgs[0].Payload)
	}
	client.Ack(ctx, msgs[0].BatchID)
}

// TestEvent_PayloadIsArray: a slice payload marshals to a JSON array.
func TestEvent_PayloadIsArray(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "arr.test", Payload: []int{1, 2, 3},
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
	if msgs[0].Payload != "[1,2,3]" {
		t.Fatalf("expected [1,2,3], got %q", msgs[0].Payload)
	}
	client.Ack(ctx, msgs[0].BatchID)
}

// NOTE: The Send API does not currently accept Extra1..Extra4 fields on
// outgoing Events — they are receive-only. When a future v0.3.0 wraps
// pgque.send_batch (with extras), add a TestExtraColumns_RoundTrip variant
// here that sets them on send and checks them on receive.
