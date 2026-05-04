// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"errors"
	"testing"

	pgque "github.com/NikolayS/pgque-go"
)

// TestTypedError_QueueNotFound: sending to a queue that does not exist must
// surface an error matched by errors.Is(err, pgque.ErrQueueNotFound).
func TestTypedError_QueueNotFound(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	ctx := context.Background()

	_, err := client.Send(ctx, "queue_typed_err_missing_"+randSuffix(t), pgque.Event{
		Type: "x", Payload: map[string]any{"y": 1},
	})
	if err == nil {
		t.Fatal("expected error sending to missing queue")
	}
	if !errors.Is(err, pgque.ErrQueueNotFound) {
		t.Errorf("expected errors.Is(err, ErrQueueNotFound) to be true, got: %v", err)
	}
}

// TestTypedError_ConsumerNotFound: receiving from a non-registered consumer
// must surface an error matched by errors.Is(err, pgque.ErrConsumerNotFound).
func TestTypedError_ConsumerNotFound(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, _ := setupFreshQueue(t, client)
	ctx := context.Background()

	_, err := client.Receive(ctx, queue, "no_such_consumer_"+randSuffix(t), 10)
	if err == nil {
		t.Fatal("expected error from Receive with missing consumer")
	}
	if !errors.Is(err, pgque.ErrConsumerNotFound) {
		t.Errorf("expected errors.Is(err, ErrConsumerNotFound) to be true, got: %v", err)
	}
}

// TestTypedError_ConnectionError: a syntactically invalid DSN must surface an
// error matched by errors.Is(err, pgque.ErrConnection).
func TestTypedError_ConnectionError(t *testing.T) {
	ctx := context.Background()
	_, err := pgque.Connect(ctx, "not a real dsn :: garbage")
	if err == nil {
		t.Fatal("expected error from invalid DSN, got nil")
	}
	if !errors.Is(err, pgque.ErrConnection) {
		t.Errorf("expected errors.Is(err, ErrConnection) to be true, got: %v", err)
	}
}

// TestTypedError_SqlErrorCarriesSqlstate: a generic SQL error (e.g. unknown
// queue raised by PgQ as a P0001 raise_exception) must be unwrappable to a
// *pgque.SqlError that exposes the SQLSTATE.
func TestTypedError_SqlErrorCarriesSqlstate(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	ctx := context.Background()

	_, err := client.Send(ctx, "queue_typed_err_sqlstate_"+randSuffix(t), pgque.Event{
		Type: "x", Payload: map[string]any{"y": 1},
	})
	if err == nil {
		t.Fatal("expected error sending to missing queue")
	}
	var sqlErr *pgque.SqlError
	if !errors.As(err, &sqlErr) {
		t.Fatalf("expected errors.As to extract *SqlError, got: %v", err)
	}
	if sqlErr.SQLSTATE == "" {
		t.Errorf("expected non-empty SQLSTATE on SqlError, got empty")
	}
}
