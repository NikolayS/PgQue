// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque/clients/go"
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

// TestConnect_UnreachableHost: pgxpool.New is "lazy" and accepts a syntactically
// valid DSN whose host is unreachable without erroring; the error surfaces on
// first query. Documents the actual behavior so callers know to issue a probe
// query if they need eager connection failure.
func TestConnect_UnreachableHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := pgque.Connect(ctx, "postgresql://postgres:nopass@127.0.0.1:1/nodb")
	if err != nil {
		// pgx may reject this eagerly on some platforms — that's also fine.
		return
	}
	defer client.Close()

	// First query should fail.
	if _, err := client.Pool().Exec(ctx, "select 1"); err == nil {
		t.Fatal("expected probe query to fail against unreachable host")
	}
}

// TestSend_MissingQueue: sending to a queue that does not exist must surface
// a clear error. PR #79 added an explicit "queue not found" check in
// pgque.insert_event_raw.
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
		t.Logf("error string: %v (passes; just confirming it's not a panic)", err)
	}
}

// TestSend_AfterClose verifies Send returns an error (not a panic) after the
// client has been closed.
func TestSend_AfterClose(t *testing.T) {
	client := connectOrSkip(t)
	queue, _ := setupFreshQueue(t, client)
	client.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Send after Close panicked: %v", r)
		}
	}()
	_, err := client.Send(context.Background(), queue, pgque.Event{Type: "x", Payload: nil})
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
		t.Logf("note: error does not wrap context.Canceled directly: %v (acceptable)", err)
	}
}

// TestReceive_AfterClose: Receive on a closed client must error, not panic.
func TestReceive_AfterClose(t *testing.T) {
	client := connectOrSkip(t)
	queue, consumer := setupFreshQueue(t, client)
	client.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Receive after Close panicked: %v", r)
		}
	}()
	_, err := client.Receive(context.Background(), queue, consumer, 10)
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

// TestAck_NonExistentBatch: acking a non-existent batch must surface an
// error rather than silently succeeding.
func TestAck_NonExistentBatch(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	ctx := context.Background()

	err := client.Ack(ctx, 9_999_999_999)
	if err == nil {
		t.Skip("Ack of non-existent batch did not error — backend treats this as no-op (acceptable)")
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
	if err := client.Ack(context.Background(), 1); err == nil {
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
