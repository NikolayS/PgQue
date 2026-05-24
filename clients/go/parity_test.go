// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"testing"

	pgque "github.com/NikolayS/pgque-go"
)

func TestSubscribeUnsubscribeWrappers(t *testing.T) {
	ctx := context.Background()
	client := connectOrSkip(t)
	defer client.Close()
	queue := "gotest_q_" + randSuffix(t)
	consumer := "gotest_c_" + randSuffix(t)

	if _, err := client.Pool().Exec(ctx, "select pgque.create_queue($1)", queue); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
	})

	n, err := client.Subscribe(ctx, queue, consumer)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Subscribe first call = %d, want 1", n)
	}
	n, err = client.Subscribe(ctx, queue, consumer)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("Subscribe duplicate = %d, want 0", n)
	}
	n, err = client.Unsubscribe(ctx, queue, consumer)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Unsubscribe existing = %d, want 1", n)
	}
	n, err = client.Unsubscribe(ctx, queue, consumer)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("Unsubscribe missing = %d, want 0", n)
	}
}

func TestTickerWrappers(t *testing.T) {
	ctx := context.Background()
	client := connectOrSkip(t)
	defer client.Close()
	queue, _ := setupFreshQueue(t, client)

	if _, err := client.Send(ctx, queue, pgque.Event{Payload: map[string]any{"x": 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ForceNextTick(ctx, queue); err != nil {
		t.Fatal(err)
	}
	tickID, err := client.Ticker(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	if tickID == nil || *tickID == 0 {
		t.Fatalf("Ticker tickID = %v, want non-zero", tickID)
	}

	if _, err := client.Send(ctx, queue, pgque.Event{Payload: map[string]any{"x": 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ForceNextTick(ctx, queue); err != nil {
		t.Fatal(err)
	}
	n, err := client.TickerAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("TickerAll = %d, want >= 1", n)
	}
}
