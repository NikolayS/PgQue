// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client is the PgQue client. It is safe for concurrent use; the
// underlying pgx pool handles connection multiplexing.
type Client struct {
	pool *pgxpool.Pool
}

// Connect opens a pgx connection pool to the given DSN and returns a
// ready-to-use Client. The DSN format is the standard libpq connection
// string (postgres://user:pass@host/db?...). Connect validates
// connectivity by pinging the pool before returning; a bad DSN or
// unreachable host surfaces as an error here, not on the first query.
func Connect(ctx context.Context, dsn string) (*Client, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, wrapConnectError(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, wrapConnectError(err)
	}
	return &Client{pool: pool}, nil
}

// Close releases the connection pool. After Close, the Client must not
// be used.
func (c *Client) Close() { c.pool.Close() }

// Pool returns the underlying pgxpool. Use this for transactional
// enqueueing (call pgque.send inside your own pgx.Tx) or to invoke
// pgque-api functions that the Client does not yet wrap directly.
func (c *Client) Pool() *pgxpool.Pool { return c.pool }

// Send publishes an event to the named queue and returns the assigned
// event ID. Payload is JSON-marshalled; an empty Type defaults to
// "default".
func (c *Client) Send(ctx context.Context, queue string, ev Event) (int64, error) {
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		return 0, fmt.Errorf("pgque: marshal payload: %w", err)
	}
	typ := ev.Type
	if typ == "" {
		typ = "default"
	}
	var eid int64
	err = c.pool.QueryRow(ctx,
		"SELECT pgque.send($1, $2, $3::jsonb)", queue, typ, string(payload),
	).Scan(&eid)
	if err != nil {
		return 0, wrapSQLError("send", err)
	}
	return eid, nil
}

// SendBatch publishes multiple payloads with the same event type in one SQL call.
// It returns event IDs in input order. PostgreSQL executes the SQL function as
// one atomic statement: if any payload is rejected, no message from the batch is
// inserted. An empty typ defaults to "default".
func (c *Client) SendBatch(ctx context.Context, queue, typ string, payloads []any) ([]int64, error) {
	jsonPayloads := make([]string, len(payloads))
	for i, payload := range payloads {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("pgque: marshal batch payload %d: %w", i, err)
		}
		jsonPayloads[i] = string(encoded)
	}
	if typ == "" {
		typ = "default"
	}

	var ids []int64
	err := c.pool.QueryRow(ctx,
		"select pgque.send_batch($1, $2, $3::jsonb[])", queue, typ, jsonPayloads,
	).Scan(&ids)
	if err != nil {
		return nil, wrapSQLError("send batch", err)
	}
	return ids, nil
}

// Subscribe registers consumer on queue, wrapping pgque.subscribe. The
// returned int64 is the SQL row-count: 1 for a new subscription and 0
// when the consumer was already subscribed.
func (c *Client) Subscribe(ctx context.Context, queue, consumer string) (int64, error) {
	var n int64
	err := c.pool.QueryRow(ctx, "select pgque.subscribe($1, $2)", queue, consumer).Scan(&n)
	if err != nil {
		return 0, wrapSQLError("subscribe", err)
	}
	return n, nil
}

// Unsubscribe removes consumer from queue, wrapping pgque.unsubscribe. The
// returned int64 is the SQL row-count: 1 when a subscription was removed and
// 0 when no row existed.
func (c *Client) Unsubscribe(ctx context.Context, queue, consumer string) (int64, error) {
	var n int64
	err := c.pool.QueryRow(ctx, "select pgque.unsubscribe($1, $2)", queue, consumer).Scan(&n)
	if err != nil {
		return 0, wrapSQLError("unsubscribe", err)
	}
	return n, nil
}

// Receive fetches up to maxMessages from the next batch for the named
// consumer. Returns an empty slice when no batch is available; in that
// case the caller should sleep before polling again. Each returned
// Message carries a BatchID that must be passed to Ack once all
// messages in the batch have been processed.
func (c *Client) Receive(ctx context.Context, queue, consumer string, maxMessages int) ([]Message, error) {
	rows, err := c.pool.Query(ctx,
		"SELECT * FROM pgque.receive($1, $2, $3)", queue, consumer, maxMessages)
	if err != nil {
		return nil, wrapSQLError("receive", err)
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var createdAt time.Time
		err := rows.Scan(
			&m.MsgID, &m.BatchID, &m.Type, &m.Payload,
			&m.RetryCount, &createdAt,
			&m.Extra1, &m.Extra2, &m.Extra3, &m.Extra4,
		)
		if err != nil {
			return nil, fmt.Errorf("pgque: scan message: %w", err)
		}
		m.CreatedAt = createdAt
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapSQLError("receive rows", err)
	}
	return msgs, nil
}

// Ack finishes a batch, advancing the consumer's position past it. PgQue
// delivers at-least-once: failing to Ack a batch causes redelivery on
// the next Receive.
//
// The returned int64 is the row-count from pgque.finish_batch:
//   - 1: the batch was active and has been finished (normal success).
//   - 0: no active batch was finished — the batch_id was not found,
//     already finished (stale/double ack), or belongs to a different
//     consumer. Callers should log this at warn level; it is not a SQL
//     error and does not indicate a connection problem.
func (c *Client) Ack(ctx context.Context, batchID int64) (int64, error) {
	var n int64
	err := c.pool.QueryRow(ctx, "SELECT pgque.ack($1)", batchID).Scan(&n)
	if err != nil {
		return 0, wrapSQLError("ack", err)
	}
	return n, nil
}

// Ticker runs the per-queue ticker for queue, wrapping pgque.ticker(queue).
// It returns the new tick id when a tick was inserted, or nil when no tick
// was needed.
func (c *Client) Ticker(ctx context.Context, queue string) (*int64, error) {
	var tickID *int64
	err := c.pool.QueryRow(ctx, "select pgque.ticker($1)", queue).Scan(&tickID)
	if err != nil {
		return nil, wrapSQLError("ticker", err)
	}
	return tickID, nil
}

// TickerAll runs the global ticker across all eligible queues, wrapping
// zero-argument pgque.ticker(). It returns the number of queues that received
// a tick during this call.
func (c *Client) TickerAll(ctx context.Context) (int64, error) {
	var n int64
	err := c.pool.QueryRow(ctx, "select pgque.ticker()").Scan(&n)
	if err != nil {
		return 0, wrapSQLError("ticker all", err)
	}
	return n, nil
}

// ForceNextTick bumps the event-seq threshold for queue so the next
// pgque.ticker(queue) call produces a tick. It wraps pgque.force_next_tick.
//
// The function does not insert a tick itself; call pgque.ticker afterwards
// (via Pool().Exec or a scheduler). It returns the current last tick id, or nil
// when SQL returns NULL for a brand-new / skipped queue.
func (c *Client) ForceNextTick(ctx context.Context, queue string) (*int64, error) {
	var tickID *int64
	err := c.pool.QueryRow(ctx, "SELECT pgque.force_next_tick($1)", queue).Scan(&tickID)
	if err != nil {
		return nil, wrapSQLError("force next tick", err)
	}
	return tickID, nil
}

// ForceTick is a deprecated compatibility alias for ForceNextTick.
func (c *Client) ForceTick(ctx context.Context, queue string) (*int64, error) {
	return c.ForceNextTick(ctx, queue)
}

// Nack negatively acknowledges a single message, routing it to retry or DLQ.
// pgque.message has 10 fields: msg_id, batch_id, type, payload, retry_count,
// created_at, extra1, extra2, extra3, extra4 — placeholders $2..$11.
//
// NackOptions tunes the call:
//
//   - opts.RetryAfter overrides the default 60s redelivery delay (nil = 60s).
//   - opts.Reason sets the reason recorded on the dead_letter row when the
//     retry budget is exhausted (nil = SQL NULL).
func (c *Client) Nack(ctx context.Context, batchID int64, msg Message, opts NackOptions) error {
	retryAfter := 60 * time.Second
	if opts.RetryAfter != nil {
		retryAfter = *opts.RetryAfter
	}
	var reason any
	if opts.Reason != nil {
		reason = *opts.Reason
	}
	interval := pgtype.Interval{Microseconds: retryAfter.Microseconds(), Valid: true}
	_, err := c.pool.Exec(ctx,
		"SELECT pgque.nack($1, ROW($2,$3,$4,$5,$6,$7,$8,$9,$10,$11)::pgque.message, $12::interval, $13)",
		batchID, msg.MsgID, msg.BatchID, msg.Type, msg.Payload,
		msg.RetryCount, msg.CreatedAt,
		msg.Extra1, msg.Extra2, msg.Extra3, msg.Extra4,
		interval, reason)
	if err != nil {
		return wrapSQLError("nack", err)
	}
	return nil
}

// SubscribeSubconsumer registers a subconsumer under the given logical
// consumer on the queue, wrapping pgque.subscribe_subconsumer. The
// returned int64 is the SQL row-count: 1 for a new registration and 0
// when the subconsumer was already registered.
//
// Cooperative consumers are experimental in PgQue 0.2 — the function
// names, edge-case behavior, and client API shape may change before
// the feature is marked stable.
func (c *Client) SubscribeSubconsumer(ctx context.Context, queue, consumer, subconsumer string) (int64, error) {
	var n int64
	err := c.pool.QueryRow(ctx,
		"select pgque.subscribe_subconsumer($1, $2, $3)", queue, consumer, subconsumer,
	).Scan(&n)
	if err != nil {
		return 0, wrapSQLError("subscribe subconsumer", err)
	}
	return n, nil
}

// UnsubscribeSubconsumer removes a subconsumer from the cooperative
// group, wrapping pgque.unsubscribe_subconsumer. The returned int64 is
// the SQL row-count: 1 if a subscription was removed, 0 otherwise.
//
// By default the call raises if the subconsumer holds an active batch.
// Pass WithBatchHandlingRetry() to route active messages through
// retry/DLQ on the way out (equivalent to nacking each message).
//
// Experimental in PgQue 0.2.
func (c *Client) UnsubscribeSubconsumer(ctx context.Context, queue, consumer, subconsumer string, opts ...UnsubscribeSubconsumerOption) (int64, error) {
	cfg := newUnsubscribeSubconsumerConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	var n int64
	err := c.pool.QueryRow(ctx,
		"select pgque.unsubscribe_subconsumer($1, $2, $3, $4)",
		queue, consumer, subconsumer, cfg.batchHandling,
	).Scan(&n)
	if err != nil {
		return 0, wrapSQLError("unsubscribe subconsumer", err)
	}
	return n, nil
}

// ReceiveCoop fetches the next batch for one subconsumer of a
// cooperative group, wrapping pgque.receive_coop. Returns an empty
// slice when no batch is available. Each returned Message carries a
// BatchID that must be passed to Ack once all messages have been
// processed.
//
// receive_coop auto-registers the cooperative main row and the
// subconsumer on first call, so an explicit SubscribeSubconsumer is
// not required. WithCoopMaxMessages tunes the per-call row cap
// (default 100); WithCoopDeadInterval enables stale-worker takeover.
//
// Experimental in PgQue 0.2.
func (c *Client) ReceiveCoop(ctx context.Context, queue, consumer, subconsumer string, opts ...ReceiveCoopOption) ([]Message, error) {
	cfg := newReceiveCoopConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	var deadArg any
	if cfg.deadInterval > 0 {
		deadArg = pgtype.Interval{Microseconds: cfg.deadInterval.Microseconds(), Valid: true}
	}
	rows, err := c.pool.Query(ctx,
		"select * from pgque.receive_coop($1, $2, $3, $4, $5::interval)",
		queue, consumer, subconsumer, cfg.maxMessages, deadArg)
	if err != nil {
		return nil, wrapSQLError("receive coop", err)
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var createdAt time.Time
		err := rows.Scan(
			&m.MsgID, &m.BatchID, &m.Type, &m.Payload,
			&m.RetryCount, &createdAt,
			&m.Extra1, &m.Extra2, &m.Extra3, &m.Extra4,
		)
		if err != nil {
			return nil, fmt.Errorf("pgque: scan message: %w", err)
		}
		m.CreatedAt = createdAt
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapSQLError("receive coop rows", err)
	}
	return msgs, nil
}

// TouchSubconsumer updates the heartbeat on a registered subconsumer
// row, wrapping pgque.touch_subconsumer. The returned int64 is the
// SQL row-count: 1 when the row was found and touched, 0 otherwise
// (e.g. the subconsumer was never registered).
//
// Heartbeats are not auto-emitted from the Consumer poll loop; call
// this method manually when you want to advertise liveness ahead of a
// dead-interval takeover by another worker.
//
// Experimental in PgQue 0.2.
func (c *Client) TouchSubconsumer(ctx context.Context, queue, consumer, subconsumer string) (int64, error) {
	var n int64
	err := c.pool.QueryRow(ctx,
		"select pgque.touch_subconsumer($1, $2, $3)", queue, consumer, subconsumer,
	).Scan(&n)
	if err != nil {
		return 0, wrapSQLError("touch subconsumer", err)
	}
	return n, nil
}

// NewConsumer creates a Consumer that polls the given queue under the
// given consumer name. The consumer must already be registered in PgQue
// (e.g. via pgque.register_consumer).
//
// Defaults — override with the matching ConsumerOption:
//
//   - poll interval: 30s   (WithPollInterval)
//   - max messages:  MaxInt32 (WithMaxMessages; drains the whole batch by default)
//   - unknown type:  Nack  (WithUnknownHandlerPolicy)
//   - retry delay:   60s   (WithRetryAfter; used for consumer-issued Nacks)
func (c *Client) NewConsumer(queue, name string, opts ...ConsumerOption) *Consumer {
	consumer := &Consumer{
		backend:       c,
		queue:         queue,
		name:          name,
		pollInterval:  30 * time.Second,
		maxMessages:   math.MaxInt32,
		handlers:      make(map[string]HandlerFunc),
		unknownPolicy: NackUnknown,
	}
	for _, opt := range opts {
		opt(consumer)
	}
	return consumer
}
