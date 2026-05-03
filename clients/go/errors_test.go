// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	pgque "github.com/NikolayS/pgque-go"
)

// TestConnect_BadDSN: a syntactically invalid DSN must error from Connect,
// not panic.
func TestConnect_BadDSN(t *testing.T) {
	ctx := context.Background()
	_, err := pgque.Connect(ctx, "not a real dsn :: garbage")
	if err == nil {
		t.Fatal("expected error from invalid DSN, got nil")
	}
}

// TestSend_MissingQueue: sending to a queue that does not exist must surface
// an error that mentions the queue or a "not found" / "does not exist" condition.
func TestSend_MissingQueue(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	ctx := context.Background()

	_, err := client.Send(ctx, "queue_that_definitely_does_not_exist_"+randSuffix(t), pgque.Event{
		Type: "x", Payload: map[string]any{"y": 1},
	})
	if err == nil {
		t.Fatal("expected error sending to missing queue")
	}
	if !strings.Contains(err.Error(), "queue") && !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error does not mention queue or not-found condition: %v", err)
	}
}

// TestSend_AfterClose verifies Send returns an error (not a panic) after the
// client has been closed. A second live client handles cleanup so that
// dropping the queue does not run on a closed pool.
func TestSend_AfterClose(t *testing.T) {
	// cleaner stays open for t.Cleanup; testClient is closed mid-test.
	cleaner := connectOrSkip(t)
	defer cleaner.Close()
	testClient := connectOrSkip(t)

	ctx := context.Background()
	suffix := randSuffix(t)
	queue := "gotest_q_" + suffix
	consumer := "gotest_c_" + suffix
	if _, err := cleaner.Pool().Exec(ctx, "select pgque.create_queue($1)", queue); err != nil {
		t.Fatal(err)
	}
	if _, err := cleaner.Pool().Exec(ctx, "select pgque.register_consumer($1, $2)", queue, consumer); err != nil {
		cleaner.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		cleaner.Pool().Exec(ctx, "select pgque.unregister_consumer($1, $2)", queue, consumer)
		cleaner.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
	})

	testClient.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Send after Close panicked: %v", r)
		}
	}()
	_, err := testClient.Send(context.Background(), queue, pgque.Event{Type: "x", Payload: nil})
	if err == nil {
		t.Fatal("expected error from Send after Close")
	}
}

// TestSend_ContextCancelled: passing an already-cancelled context must
// surface ctx.Err (or wrap it) without panic.
func TestSend_ContextCancelled(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, _ := setupFreshQueue(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Send(ctx, queue, pgque.Event{Type: "x", Payload: nil})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to wrap context.Canceled, got: %v", err)
	}
}

// TestReceive_AfterClose: Receive on a closed client must error, not panic.
// A second live client handles cleanup so that dropping the queue does not
// run on a closed pool.
func TestReceive_AfterClose(t *testing.T) {
	cleaner := connectOrSkip(t)
	defer cleaner.Close()
	testClient := connectOrSkip(t)

	queue, consumer := setupFreshQueue(t, cleaner)
	testClient.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Receive after Close panicked: %v", r)
		}
	}()
	_, err := testClient.Receive(context.Background(), queue, consumer, 10)
	if err == nil {
		t.Fatal("expected error from Receive after Close")
	}
}

// TestReceive_MissingConsumer surfaces a clear error when the consumer is
// not registered for the given queue.
func TestReceive_MissingConsumer(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, _ := setupFreshQueue(t, client)
	ctx := context.Background()

	_, err := client.Receive(ctx, queue, "no_such_consumer_"+randSuffix(t), 10)
	if err == nil {
		t.Fatal("expected error from Receive with missing consumer")
	}
}

// TestReceive_ContextCancelled cancels mid-call.
func TestReceive_ContextCancelled(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Receive(ctx, queue, consumer, 10)
	if err == nil {
		t.Fatal("expected error from Receive with cancelled context")
	}
}

// TestAck_NonExistentBatch_ReturnsZero verifies the new contract: acking a
// batch_id that does not exist returns (0, nil). pgque.finish_batch is
// defined to return the count of rows updated; a non-existent batch_id
// matches no rows, so the count is 0 and no SQL error is raised. Callers
// should treat a 0 rowcount as a warn-level log, not a hard error.
func TestAck_NonExistentBatch_ReturnsZero(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	ctx := context.Background()

	// 999_999_999 is far outside the range of any real batch_id that could
	// exist in a freshly-initialised test database.
	n, err := client.Ack(ctx, 999_999_999)
	if err != nil {
		t.Fatalf("expected (0, nil) for non-existent batch, got err: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected rowcount 0 for non-existent batch, got %d", n)
	}
}

// TestAck_AfterClose
func TestAck_AfterClose(t *testing.T) {
	client := connectOrSkip(t)
	client.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Ack after Close panicked: %v", r)
		}
	}()
	if _, err := client.Ack(context.Background(), 1); err == nil {
		t.Fatal("expected error from Ack after Close")
	}
}

// TestNack_AfterClose
func TestNack_AfterClose(t *testing.T) {
	client := connectOrSkip(t)
	client.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Nack after Close panicked: %v", r)
		}
	}()
	msg := pgque.Message{MsgID: 1, BatchID: 1, Type: "x"}
	if err := client.Nack(context.Background(), 1, msg); err == nil {
		t.Fatal("expected error from Nack after Close")
	}
}

// TestSend_UnmarshalablePayload: a payload containing a chan or func returns
// a marshal error rather than panicking.
func TestSend_UnmarshalablePayload(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, _ := setupFreshQueue(t, client)
	ctx := context.Background()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Send with unmarshalable payload panicked: %v", r)
		}
	}()
	ch := make(chan int)
	_, err := client.Send(ctx, queue, pgque.Event{Type: "x", Payload: ch})
	if err == nil {
		t.Fatal("expected marshal error for chan payload")
	}
}
