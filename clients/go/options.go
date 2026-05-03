// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import "time"

// ConsumerOption configures a Consumer at construction time. Pass options
// to Client.NewConsumer.
type ConsumerOption func(*Consumer)

// Option is the legacy alias for ConsumerOption. Kept for backward
// compatibility with code written against earlier releases.
type Option = ConsumerOption

// WithPollInterval sets the interval the Consumer waits between poll
// cycles when Receive returns no messages or fails. Default is 30s.
func WithPollInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithMaxMessages sets the per-Receive limit. By default the Consumer
// requests PostgreSQL's int maximum so it drains the whole PgQ batch before
// acknowledging it.
//
// WARNING: pgque.ack(batch_id) finishes the entire underlying PgQ batch,
// including rows the consumer never received because of this limit. If you
// set maxMessages below the real batch size, unreturned rows are skipped
// after ack. Only lower this value when it is at least as large as the
// queue's possible batch size for your workload.
func WithMaxMessages(n int) ConsumerOption {
	return func(c *Consumer) {
		if n > 0 {
			c.maxMessages = n
		}
	}
}

// UnknownHandlerPolicy controls how the Consumer responds to a message
// whose Type has no registered handler.
type UnknownHandlerPolicy int

const (
	// NackUnknown sends a per-message Nack for unknown types. The message
	// is routed to retry_queue (or DLQ once retry_count exceeds the queue
	// max_retries). This is the default and matches the cross-driver
	// at-least-once contract: a producer-driver mismatch never silently
	// drops messages.
	NackUnknown UnknownHandlerPolicy = iota

	// AckUnknown silently drops messages whose Type has no registered
	// handler: the consumer logs the unknown type and proceeds. The batch
	// is acked as long as every other message succeeds. Use this only
	// when you intentionally want to ignore certain event types on this
	// consumer (e.g. fan-out where one worker handles a strict subset).
	AckUnknown
)

// WithUnknownHandlerPolicy overrides the default policy for messages
// whose Type has no registered handler. Default is NackUnknown.
func WithUnknownHandlerPolicy(p UnknownHandlerPolicy) ConsumerOption {
	return func(c *Consumer) { c.unknownPolicy = p }
}

// NackOption configures a single Nack call. Pass to Client.Nack.
type NackOption func(*nackOptions)

// nackOptions captures the optional Nack parameters. Defaults match the
// SQL function: 60s retry delay, NULL reason.
type nackOptions struct {
	retryAfter    time.Duration
	retryAfterSet bool
	reason        string
	reasonSet     bool
}

// WithRetryAfter sets the delay before the message becomes eligible for
// redelivery from retry_queue. Maps to the i_retry_after argument of
// pgque.nack. Default is 60 seconds.
func WithRetryAfter(d time.Duration) NackOption {
	return func(o *nackOptions) {
		o.retryAfter = d
		o.retryAfterSet = true
	}
}

// WithReason sets the human-readable reason recorded on the dead_letter
// row when this nack exhausts the retry budget. Maps to the i_reason
// argument of pgque.nack. Default is NULL (the SQL function then records
// "max retries exceeded").
func WithReason(reason string) NackOption {
	return func(o *nackOptions) {
		o.reason = reason
		o.reasonSet = true
	}
}
