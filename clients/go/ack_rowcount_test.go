// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"testing"

	pgque "github.com/NikolayS/pgque-go"
)

// TestAck_DirectCall_ReturnsRowcount verifies the Client.Ack method itself
// returns (int64, error). This test requires a live database; it is skipped
// when PGQUE_TEST_DSN is not reachable.
func TestAck_DirectCall_ReturnsRowcount(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ctx := context.Background()

	if _, err := client.Send(ctx, queue, pgque.Event{
		Type: "ack.rowcount.test", Payload: map[string]any{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)

	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Fatal("no messages received after tick — harness bug, not a skip-worthy condition")
	}

	batchID := msgs[0].BatchID

	// First ack: expects rowcount 1.
	n1, err := client.Ack(ctx, batchID)
	if err != nil {
		t.Fatalf("first Ack returned error: %v", err)
	}
	if n1 != 1 {
		t.Fatalf("first Ack: expected rowcount 1, got %d", n1)
	}

	// Second ack of the same batch: expects rowcount 0 (stale ack).
	n2, err := client.Ack(ctx, batchID)
	if err != nil {
		t.Fatalf("second Ack returned error: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second Ack (double-ack): expected rowcount 0, got %d", n2)
	}
}
