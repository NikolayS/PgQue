// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"log"
	"time"
)

// HandlerFunc processes a single message. Returning a non-nil error
// causes the entire batch to be NOT acked, so PgQue redelivers it on
// the next Receive.
type HandlerFunc func(ctx context.Context, msg Message) error

// Consumer polls a queue and dispatches messages to registered
// handlers. Create one via Client.NewConsumer.
type Consumer struct {
	client       *Client
	queue        string
	name         string
	pollInterval time.Duration
	handlers     map[string]HandlerFunc
}

// Handle registers fn as the handler for messages whose Type matches
// eventType. Messages with no registered handler are silently skipped
// for now; a future release will surface them via a default handler.
func (c *Consumer) Handle(eventType string, fn HandlerFunc) {
	c.handlers[eventType] = fn
}

// Start begins the poll loop and blocks until ctx is cancelled. On
// receive errors it logs and retries after the configured poll
// interval. A batch is acked only if every handled message in it
// returned nil.
func (c *Consumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgs, err := c.client.Receive(ctx, c.queue, c.name, 100)
		if err != nil {
			log.Printf("pgque: receive error: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.pollInterval):
			}
			continue
		}

		if len(msgs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.pollInterval):
			}
			continue
		}

		var batchID int64
		for _, msg := range msgs {
			batchID = msg.BatchID
			handler, ok := c.handlers[msg.Type]
			if !ok {
				log.Printf("pgque: no handler registered for event type %q, nacking message %d", msg.Type, msg.MsgID)
				if nackErr := c.client.Nack(ctx, batchID, msg); nackErr != nil {
					log.Printf("pgque: nack error for unhandled type %s: %v", msg.Type, nackErr)
				}
				continue
			}
			if err := handler(ctx, msg); err != nil {
				log.Printf("pgque: handler error for %s: %v", msg.Type, err)
				if nackErr := c.client.Nack(ctx, batchID, msg); nackErr != nil {
					log.Printf("pgque: nack error for %s: %v", msg.Type, nackErr)
				}
				continue
			}
		}

		if batchID != 0 {
			if err := c.client.Ack(ctx, batchID); err != nil {
				log.Printf("pgque: ack error: %v", err)
			}
		}
	}
}
