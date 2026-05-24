// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import "time"

// ConsumerOption configures a Consumer at construction time. Pass options
// to Client.NewConsumer.
type ConsumerOption func(*Consumer)

// WithPollInterval sets the interval the Consumer waits between poll
// cycles when Receive returns no messages or fails. Default is 30s.
func WithPollInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = d }
}

// WithMaxMessages sets the per-Receive limit. By default the Consumer
// requests PostgreSQL's int maximum so it drains the whole PgQ batch before
// acknowledging it. Panics if n <= 0.
//
// WARNING: pgque.ack(batch_id) finishes the entire underlying PgQ batch,
// including rows the consumer never received because of this limit. If you
// set maxMessages below the real batch size, unreturned rows are skipped
// after ack. Only lower this value when it is at least as large as the
// queue's possible batch size for your workload.
func WithMaxMessages(n int) ConsumerOption {
	if n <= 0 {
		panic("pgque: WithMaxMessages requires n > 0")
	}
	return func(c *Consumer) {
		c.maxMessages = n
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

// WithRetryAfter sets the redelivery delay used by the high-level Consumer
// when it nacks messages whose handler failed or whose type is unknown.
// Default is the SQL/client Nack default of 60 seconds. Panics if d < 0.
func WithRetryAfter(d time.Duration) ConsumerOption {
	if d < 0 {
		panic("pgque: WithRetryAfter requires d >= 0")
	}
	return func(c *Consumer) { c.retryAfter = &d }
}

// NackOptions tunes a single Client.Nack call. A zero-value NackOptions
// uses the SQL-side defaults: 60s retry delay, NULL reason.
type NackOptions struct {
	// RetryAfter sets the delay before the message becomes eligible for
	// redelivery from retry_queue. Maps to the i_retry_after argument of
	// pgque.nack. Nil means 60 seconds.
	RetryAfter *time.Duration

	// Reason sets the human-readable reason recorded on the dead_letter
	// row when this nack exhausts the retry budget. Maps to the i_reason
	// argument of pgque.nack. Nil means SQL NULL (the SQL function then
	// records "max retries exceeded").
	Reason *string
}

/*
Cooperative consumers (experimental in PgQue 0.2).

Function names, edge-case behavior, and client API shape may change
before this feature is marked stable. Do not use this as the only
processing path for critical workloads without idempotent handlers and
stale-worker takeover tests.
*/

// WithSubconsumer enables cooperative-consumer mode on the Consumer.
// When set, the poll loop calls Client.ReceiveCoop with the given
// subconsumer name; without it the Consumer behaves exactly as before
// (calls Client.Receive). The empty string disables coop mode.
func WithSubconsumer(name string) ConsumerOption {
	return func(c *Consumer) { c.subconsumer = name }
}

// WithDeadInterval sets the dead-worker takeover interval used in
// cooperative mode. A subconsumer whose batch is older than this
// duration may have its batch stolen by another member under a fresh
// batch_id. The default of zero disables takeover. Has no effect
// outside cooperative mode (set via WithSubconsumer).
func WithDeadInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.deadInterval = d }
}

// receiveCoopConfig holds the resolved options for one ReceiveCoop call.
type receiveCoopConfig struct {
	maxMessages  int
	deadInterval time.Duration
}

func newReceiveCoopConfig() *receiveCoopConfig {
	return &receiveCoopConfig{maxMessages: 100}
}

// ReceiveCoopOption tunes a single Client.ReceiveCoop call.
type ReceiveCoopOption func(*receiveCoopConfig)

// WithCoopMaxMessages sets the per-call message limit (maps to the
// i_max_return argument of pgque.receive_coop). Default is 100. Panics
// if n <= 0.
//
// As with Receive, ack(batch_id) finishes the entire underlying batch
// regardless of how many rows are returned; a low limit can therefore
// drop rows. Match this to ticker_max_count (or larger) when you care
// about per-message dispatch.
func WithCoopMaxMessages(n int) ReceiveCoopOption {
	if n <= 0 {
		panic("pgque: WithCoopMaxMessages requires n > 0")
	}
	return func(c *receiveCoopConfig) { c.maxMessages = n }
}

// WithCoopDeadInterval enables stale-worker takeover for this
// ReceiveCoop call. A subconsumer whose batch is older than d may have
// its batch stolen under a fresh batch_id. Zero (the default) disables
// takeover.
func WithCoopDeadInterval(d time.Duration) ReceiveCoopOption {
	return func(c *receiveCoopConfig) { c.deadInterval = d }
}

// unsubscribeSubconsumerConfig holds the resolved options for one
// Client.UnsubscribeSubconsumer call.
type unsubscribeSubconsumerConfig struct {
	batchHandling int
}

func newUnsubscribeSubconsumerConfig() *unsubscribeSubconsumerConfig {
	return &unsubscribeSubconsumerConfig{batchHandling: 0}
}

// UnsubscribeSubconsumerOption tunes a single
// Client.UnsubscribeSubconsumer call.
type UnsubscribeSubconsumerOption func(*unsubscribeSubconsumerConfig)

// WithBatchHandlingRetry routes the unsubscribed subconsumer's active
// batch (if any) through retry/DLQ on its way out, equivalent to
// nacking each message. The default (no option) raises a SQL error
// when an active batch exists, forcing the caller to ack or nack
// before unsubscribing.
func WithBatchHandlingRetry() UnsubscribeSubconsumerOption {
	return func(c *unsubscribeSubconsumerConfig) { c.batchHandling = 1 }
}
