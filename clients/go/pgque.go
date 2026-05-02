// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
		return nil, fmt.Errorf("pgque: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgque: connect: %w", err)
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
		return 0, fmt.Errorf("pgque: send: %w", err)
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
		return nil, fmt.Errorf("pgque: send batch: %w", err)
	}
	return ids, nil
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
		return nil, fmt.Errorf("pgque: receive: %w", err)
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
		return nil, fmt.Errorf("pgque: receive rows: %w", err)
	}
	return msgs, nil
}

// Ack finishes a batch, advancing the consumer's position past it. PgQue
// delivers at-least-once: failing to Ack a batch causes redelivery on
// the next Receive.
func (c *Client) Ack(ctx context.Context, batchID int64) error {
	_, err := c.pool.Exec(ctx, "SELECT pgque.ack($1)", batchID)
	if err != nil {
		return fmt.Errorf("pgque: ack: %w", err)
	}
	return nil
}

// Nack negatively acknowledges a single message, routing it to retry or DLQ.
// pgque.message has 10 fields: msg_id, batch_id, type, payload, retry_count,
// created_at, extra1, extra2, extra3, extra4 — placeholders $2..$11.
func (c *Client) Nack(ctx context.Context, batchID int64, msg Message) error {
	_, err := c.pool.Exec(ctx,
		"SELECT pgque.nack($1, ROW($2,$3,$4,$5,$6,$7,$8,$9,$10,$11)::pgque.message, '60 seconds'::interval, null)",
		batchID, msg.MsgID, msg.BatchID, msg.Type, msg.Payload,
		msg.RetryCount, msg.CreatedAt,
		msg.Extra1, msg.Extra2, msg.Extra3, msg.Extra4)
	if err != nil {
		return fmt.Errorf("pgque: nack: %w", err)
	}
	return nil
}

// NewConsumer creates a Consumer that polls the given queue under the
// given consumer name. The consumer must already be registered in PgQue
// (e.g. via pgque.register_consumer). Default poll interval is 30s; use
// WithPollInterval to override.
func (c *Client) NewConsumer(queue, name string, opts ...Option) *Consumer {
	consumer := &Consumer{
		client:       c,
		queue:        queue,
		name:         name,
		pollInterval: 30 * time.Second,
		handlers:     make(map[string]HandlerFunc),
	}
	for _, opt := range opts {
		opt(consumer)
	}
	return consumer
}
