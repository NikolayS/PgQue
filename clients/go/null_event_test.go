// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"testing"
)

// TestReceive_NullTypeAndPayload verifies that a stored event with NULL
// ev_type and ev_data (legal in PgQ: direct pgque.insert_event calls,
// trigger-based producers) does not error the whole batch. A scan
// failure here is a poison-message livelock: the batch is never acked
// and next_batch redelivers it forever.
func TestReceive_NullTypeAndPayload(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	var evID int64
	if err := client.Pool().QueryRow(ctx,
		"select pgque.insert_event($1, null, null)", queue).Scan(&evID); err != nil {
		t.Fatal("insert_event:", err)
	}
	tick(t, client, queue)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal("receive with NULL type/payload:", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].MsgID != evID {
		t.Fatalf("expected msg_id %d, got %d", evID, msgs[0].MsgID)
	}
	if msgs[0].Type != "" {
		t.Fatalf("expected NULL type mapped to empty string, got %q", msgs[0].Type)
	}
	if msgs[0].Payload != "" {
		t.Fatalf("expected NULL payload mapped to empty string, got %q", msgs[0].Payload)
	}

	// The batch must be ackable so the consumer cursor advances.
	n, err := client.Ack(ctx, msgs[0].BatchID)
	if err != nil {
		t.Fatal("ack:", err)
	}
	if n != 1 {
		t.Fatalf("expected ack row-count 1, got %d", n)
	}
}

// TestReceiveCoop_NullTypeAndPayload covers the same NULL-safety for the
// cooperative-consumer scan path.
func TestReceiveCoop_NullTypeAndPayload(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshCoopGroup(t, client)
	ctx := context.Background()

	if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, "worker-1"); err != nil {
		t.Fatal("subscribe subconsumer:", err)
	}

	var evID int64
	if err := client.Pool().QueryRow(ctx,
		"select pgque.insert_event($1, null, null)", queue).Scan(&evID); err != nil {
		t.Fatal("insert_event:", err)
	}
	tick(t, client, queue)

	msgs, err := client.ReceiveCoop(ctx, queue, consumer, "worker-1")
	if err != nil {
		t.Fatal("receive_coop with NULL type/payload:", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].MsgID != evID {
		t.Fatalf("expected msg_id %d, got %d", evID, msgs[0].MsgID)
	}
	if msgs[0].Type != "" {
		t.Fatalf("expected NULL type mapped to empty string, got %q", msgs[0].Type)
	}
	if msgs[0].Payload != "" {
		t.Fatalf("expected NULL payload mapped to empty string, got %q", msgs[0].Payload)
	}

	if _, err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatal("ack:", err)
	}
}
