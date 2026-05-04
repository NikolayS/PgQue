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
// dispatched to their own handlers; the batch as a whole is Ack'd
// only if every per-message Nack call returned without error. A
// panic raised inside the handler is recovered and routed identically
// to a returned error.
type HandlerFunc func(ctx context.Context, msg Message) error

// consumerBackend is the subset of Client used by Consumer. Defining
// it as an interface keeps the Consumer testable: a stub backend can
// simulate Nack failures without a live database.
type consumerBackend interface {
	Receive(ctx context.Context, queue, consumer string, maxMessages int) ([]Message, error)
	Ack(ctx context.Context, batchID int64) error
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
// eventType. Messages with no registered handler are dispatched
// according to the Consumer's UnknownHandlerPolicy: by default
// (NackUnknown) each is logged and Nack'd individually, routing it to
// retry_queue or eventually the DLQ; with
// WithUnknownHandlerPolicy(AckUnknown) each is logged and silently
// skipped (the surrounding batch is still Ack'd).
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
// Receive errors it logs and retries after the configured poll
// interval.
//
// Per-batch dispatch semantics (the runtime contract of this loop):
//
//   - Each message is delivered to its registered handler. A handler
//     that returns a non-nil error, or panics, has its message
//     individually Nack'd (routed to retry_queue, eventually the DLQ).
//     The handler error is logged and dispatch continues to the next
//     message in the batch.
//   - Messages with no registered handler follow the configured
//     UnknownHandlerPolicy: NackUnknown (default) logs and Nacks each;
//     AckUnknown logs and skips each, leaving the batch Ack to handle
//     them.
//   - After every message has been processed, the batch is Ack'd —
//     unless one of the per-message Nack calls returned an error, in
//     which case the batch is left unacked so PgQue redelivers it on
//     the next Receive. Acking a batch whose Nack failed would silently
//     drop the failure information.
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
			if err := c.backend.Ack(ctx, batchID); err != nil {
				log.Printf("pgque: ack error: %v", err)
			}
		}
	}
}
