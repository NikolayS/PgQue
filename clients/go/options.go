// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import "time"

// Option configures a Consumer at construction time. Pass options to
// Client.NewConsumer.
type Option func(*Consumer)

// WithPollInterval sets the interval the Consumer waits between poll
// cycles when Receive returns no messages or fails. Default is 30s.
func WithPollInterval(d time.Duration) Option {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithMaxMessages sets the upper bound on messages requested per
// pgque.receive call. Default is 500, which matches the default
// pgque.queue.queue_ticker_max_count so a single Receive can drain a
// full batch. Setting maxMessages below the queue's ticker_max_count
// risks leaving rows from a batch unreturned for the rest of that
// batch's lifetime, so callers should keep maxMessages >=
// ticker_max_count.
func WithMaxMessages(n int) Option {
	return func(c *Consumer) { c.maxMessages = n }
}

// UnknownHandlerPolicy controls what the Consumer does when a message
// arrives with no registered handler.
type UnknownHandlerPolicy int

const (
	// NackUnknown (default) nacks the message with a reason of
	// "no handler for type=X". The message is routed to the retry
	// queue (or dead-letter queue if past the retry limit), never
	// silently dropped.
	NackUnknown UnknownHandlerPolicy = iota
	// AckUnknown logs a warning and lets the batch ack consume the
	// message. Use this when handler registration is intentionally an
	// allow-list filter and unhandled types should be discarded.
	AckUnknown
)

// WithUnknownHandlerPolicy selects the policy applied to messages whose
// Type has no registered handler. Default is NackUnknown.
func WithUnknownHandlerPolicy(p UnknownHandlerPolicy) Option {
	return func(c *Consumer) { c.unknownPolicy = p }
}
