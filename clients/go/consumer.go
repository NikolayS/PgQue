// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"log"
	"time"
)

// HandlerFunc processes a single message. Return nil to indicate success.
type HandlerFunc func(ctx context.Context, msg Message) error

// Consumer polls a queue and dispatches messages to registered handlers.
type Consumer struct {
	client       *Client
	queue        string
	name         string
	pollInterval time.Duration
	handlers     map[string]HandlerFunc
}

// Handle registers a handler for the given event type.
func (c *Consumer) Handle(eventType string, fn HandlerFunc) {
	c.handlers[eventType] = fn
}

// Start begins the poll loop, blocking until ctx is cancelled.
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
		hasError := false
		for _, msg := range msgs {
			batchID = msg.BatchID
			if handler, ok := c.handlers[msg.Type]; ok {
				if err := handler(ctx, msg); err != nil {
					log.Printf("pgque: handler error for %s: %v", msg.Type, err)
					hasError = true
					// Don't ack — batch will be redelivered
				}
			}
		}

		if batchID != 0 && !hasError {
			if err := c.client.Ack(ctx, batchID); err != nil {
				log.Printf("pgque: ack error: %v", err)
			}
		}
	}
}
