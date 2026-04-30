// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"fmt"
	"log"
	"time"
)

// HandlerFunc processes a single message. Returning a non-nil error
// causes that message to be Nacked individually (routed to retry/DLQ
// per the queue's retry policy) while sibling messages in the same
// batch continue to be processed. The batch is then Acked, but only
// if every required Nack succeeded; if any Nack failed the batch is
// left unfinished so PgQ redelivers it.
type HandlerFunc func(ctx context.Context, msg Message) error

// Consumer polls a queue and dispatches messages to registered
// handlers. Create one via Client.NewConsumer.
type Consumer struct {
	client        *Client
	queue         string
	name          string
	pollInterval  time.Duration
	maxMessages   int
	handlers      map[string]HandlerFunc
	unknownPolicy UnknownHandlerPolicy
}

// Handle registers fn as the handler for messages whose Type matches
// eventType. Messages whose Type has no registered handler are
// dispatched to the configured UnknownHandlerPolicy: by default they
// are Nacked (routed to retry/DLQ); pass WithUnknownHandlerPolicy to
// override.
func (c *Consumer) Handle(eventType string, fn HandlerFunc) {
	c.handlers[eventType] = fn
}

// dispatchWithRecover calls fn and converts any panic into a non-nil
// error so that the caller can nack the message and keep polling.
func (c *Consumer) dispatchWithRecover(ctx context.Context, fn HandlerFunc, msg Message) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return fn(ctx, msg)
}

// Start begins the poll loop and blocks until ctx is cancelled. On
// receive errors it logs and retries after the configured poll
// interval. For each batch:
//   - every message is dispatched to its handler (or routed by the
//     UnknownHandlerPolicy if no handler is registered);
//   - per-message failures (handler error, panic, unknown type under
//     NackUnknown) trigger an individual Nack;
//   - the batch is Acked only if every Nack call that was required
//     succeeded. If any Nack failed, the batch is left unfinished so
//     PgQ redelivers it via the existing batch lifecycle.
func (c *Consumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgs, err := c.client.Receive(ctx, c.queue, c.name, c.maxMessages)
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
		nackFailed := false
		for _, msg := range msgs {
			batchID = msg.BatchID
			handler, ok := c.handlers[msg.Type]
			if !ok {
				if c.unknownPolicy == AckUnknown {
					log.Printf("pgque: no handler registered for event type %q, acking msg %d (AckUnknown policy)", msg.Type, msg.MsgID)
					continue
				}
				log.Printf("pgque: no handler registered for event type %q, nacking msg %d", msg.Type, msg.MsgID)
				if nackErr := c.client.Nack(ctx, batchID, msg, WithReason(fmt.Sprintf("no handler for type=%s", msg.Type))); nackErr != nil {
					log.Printf("pgque: nack error for unhandled type %s: %v", msg.Type, nackErr)
					nackFailed = true
				}
				continue
			}
			if handlerErr := c.dispatchWithRecover(ctx, handler, msg); handlerErr != nil {
				log.Printf("pgque: handler error for %s: %v", msg.Type, handlerErr)
				if nackErr := c.client.Nack(ctx, batchID, msg); nackErr != nil {
					log.Printf("pgque: nack error for %s: %v", msg.Type, nackErr)
					nackFailed = true
				}
				continue
			}
		}

		if batchID != 0 {
			if nackFailed {
				log.Printf("pgque: skipping batch %d ack: one or more nacks failed; PgQ will redeliver", batchID)
				continue
			}
			if err := c.client.Ack(ctx, batchID); err != nil {
				log.Printf("pgque: ack error: %v", err)
			}
		}
	}
}
