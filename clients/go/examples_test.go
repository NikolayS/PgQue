// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"log"

	pgque "github.com/NikolayS/pgque-go"
)

// Example showing the canonical send / receive / ack flow.
//
// Assumes the PgQue schema is installed and the queue + consumer
// already exist (e.g. created in a migration with
// pgque.create_queue and pgque.register_consumer).
func Example_sendReceiveAck() {
	ctx := context.Background()

	client, err := pgque.Connect(ctx, "postgres://user:pass@localhost/mydb")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Send(ctx, "orders", pgque.Event{
		Type:    "order.created",
		Payload: map[string]any{"order_id": 42},
	}); err != nil {
		log.Fatal(err)
	}

	msgs, err := client.Receive(ctx, "orders", "order_worker", 100)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range msgs {
		// msg.Type and msg.Payload are *string: rows produced by the
		// low-level pgque.insert_event(queue, null, null) primitive
		// arrive with nil values. Pure pgque.Send producers always
		// see non-nil pointers.
		log.Printf("got %v: %v", msg.Type, msg.Payload)
	}
	if len(msgs) > 0 {
		if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
			log.Fatal(err)
		}
	}
}

// Example showing the higher-level Consumer that polls and dispatches
// to per-event-type handlers.
func ExampleClient_NewConsumer() {
	ctx := context.Background()

	client, err := pgque.Connect(ctx, "postgres://user:pass@localhost/mydb")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	consumer := client.NewConsumer("orders", "order_worker")
	consumer.Handle("order.created", func(ctx context.Context, msg pgque.Message) error {
		log.Printf("processing %v", msg.Type)
		return nil
	})

	if err := consumer.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
