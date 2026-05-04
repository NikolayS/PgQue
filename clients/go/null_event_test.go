// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"testing"
)

// TestReceive_NullEvTypeAndData covers the corner case where a message
// is enqueued via the low-level pgque.insert_event(queue, null, null)
// primitive: the resulting row has SQL-NULL ev_type and ev_data. The
// driver's Receive path must scan the row without errors and surface
// the NULLs in the Message.Type and Message.Payload fields.
//
// Regression for NikolayS/pgque#143.
func TestReceive_NullEvTypeAndData(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	// Bypass pgque.send() and call the low-level PgQ primitive that can
	// produce SQL-NULL ev_type / ev_data. This is the shape every driver
	// must tolerate even though pgque.send() itself never emits it.
	if _, err := client.Pool().Exec(ctx,
		"select pgque.insert_event($1, null::text, null::text)", queue); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatalf("Receive returned error for NULL ev_type/ev_data: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	m := msgs[0]
	if m.Type != nil {
		t.Errorf("expected Type to be nil for NULL ev_type, got %q", *m.Type)
	}
	if m.Payload != nil {
		t.Errorf("expected Payload to be nil for NULL ev_data, got %q", *m.Payload)
	}
	if err := client.Ack(ctx, m.BatchID); err != nil {
		t.Fatal(err)
	}
}
