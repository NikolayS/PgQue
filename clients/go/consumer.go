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
// causes the Consumer to issue a per-message Nack for that message
// (routing it to retry_queue or, once max_retries is exceeded, to the
// dead_letter table). Other messages in the same batch are still
// dispatched to their handlers; the batch as a whole is acked only if
// every required Nack succeeded.
type HandlerFunc func(ctx context.Context, msg Message) error

// consumerBackend is the subset of Client used by Consumer. Defining
// it as an interface keeps the Consumer testable: a stub backend can
// simulate Nack failures without a live database.
type consumerBackend interface {
	Receive(ctx context.Context, queue, consumer string, maxMessages int) ([]Message, error)
	// Ack returns the row-count from pgque.finish_batch: 1 on success,
	// 0 for a stale/double ack (batch already finished or not found).
	Ack(ctx context.Context, batchID int64) (int64, error)
	Nack(ctx context.Context, batchID int64, msg Message, opts ...NackOption) error
}

// Consumer polls a queue and dispatches messages to registered
// handlers. Create one via Client.NewConsumer.
type Consumer struct {
	backend       consumerBackend
	queue         string
	name          string
	pollInterval  time.Duration
	maxMessages   int
	handlers      map[string]HandlerFunc
	unknownPolicy UnknownHandlerPolicy
}

// Handle registers fn as the handler for messages whose Type matches
// eventType. Messages with no registered handler are dispatched per
// the consumer's UnknownHandlerPolicy: by default each is Nack'd
// individually (routing it to retry_queue or eventually the DLQ);
// with WithUnknownHandlerPolicy(AckUnknown) they are logged and
// silently skipped instead.
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
// interval.
//
// Per-batch dispatch semantics:
//
//   - Each message is delivered to its registered handler. If the
//     handler returns a non-nil error or panics, the message is
//     individually Nack'd (routed to retry_queue, eventually the DLQ).
//   - Messages with no registered handler are Nack'd or skipped
//     depending on the configured UnknownHandlerPolicy (default: Nack).
//   - If every required Nack call succeeded (or none were needed), the
//     batch is Ack'd. If any Nack fails, the batch is left unacked so
//     that PgQue redelivers it on the next Receive — losing the Nack
//     would otherwise mean losing the failure information for that
//     message.
func (c *Consumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgs, err := c.backend.Receive(ctx, c.queue, c.name, c.maxMessages)
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
					log.Printf("pgque: no handler registered for event type %q, skipping message %d (AckUnknown policy)", msg.Type, msg.MsgID)
					continue
				}
				log.Printf("pgque: no handler registered for event type %q, nacking message %d", msg.Type, msg.MsgID)
				if nackErr := c.backend.Nack(ctx, batchID, msg); nackErr != nil {
					log.Printf("pgque: nack error for unhandled type %s: %v", msg.Type, nackErr)
					nackFailed = true
				}
				continue
			}
			if handlerErr := c.dispatchWithRecover(ctx, handler, msg); handlerErr != nil {
				log.Printf("pgque: handler error for %s: %v", msg.Type, handlerErr)
				if nackErr := c.backend.Nack(ctx, batchID, msg); nackErr != nil {
					log.Printf("pgque: nack error for %s: %v", msg.Type, nackErr)
					nackFailed = true
				}
				continue
			}
		}

		if batchID != 0 {
			if nackFailed {
				log.Printf("pgque: skipping ack for batch %d due to prior nack failures; PgQue will redeliver", batchID)
				continue
			}
			n, err := c.backend.Ack(ctx, batchID)
			if err != nil {
				log.Printf("pgque: ack error: %v", err)
			} else if n == 0 {
				log.Printf("pgque: ack batch %d returned 0 rows — stale or double ack (batch already finished or not found)", batchID)
			}
		}
	}
}
