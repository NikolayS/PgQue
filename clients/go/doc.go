// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

// Package pgque is the Go client for PgQue, the PgQ-based universal
// PostgreSQL queue. It provides a thin, idiomatic wrapper around the
// pgque-api SQL functions: send, receive, ack, nack.
//
// Install the Go module:
//
//	go get github.com/NikolayS/pgque/clients/go
//
// Quick start:
//
//	ctx := context.Background()
//	client, err := pgque.Connect(ctx, "postgres://user:pass@host/db")
//	if err != nil { /* ... */ }
//	defer client.Close()
//
//	_, err = client.Send(ctx, "my_queue", pgque.Event{
//	    Type:    "order.created",
//	    Payload: map[string]any{"order_id": 42},
//	})
//
//	consumer := client.NewConsumer("my_queue", "my_worker")
//	consumer.Handle("order.created", func(ctx context.Context, msg pgque.Message) error {
//	    // do work
//	    return nil
//	})
//	consumer.Start(ctx)
//
// PgQue itself is installed via "\i pgque.sql" — no PostgreSQL extension
// required. See https://github.com/NikolayS/pgque for the schema install
// and full documentation.
package pgque
