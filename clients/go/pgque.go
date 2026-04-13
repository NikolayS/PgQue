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

// Client is the main PgQue client backed by a pgx connection pool.
type Client struct {
	pool *pgxpool.Pool
}

// Connect creates a new Client connected to the given DSN.
func Connect(ctx context.Context, dsn string) (*Client, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgque: connect: %w", err)
	}
	return &Client{pool: pool}, nil
}

// Close releases the connection pool.
func (c *Client) Close() { c.pool.Close() }

// Pool returns the underlying pgxpool for direct SQL access.
func (c *Client) Pool() *pgxpool.Pool { return c.pool }

// Send publishes an event to the named queue and returns the event ID.
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

// Receive fetches up to maxMessages from the next batch for the consumer.
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

// Ack acknowledges (finishes) a batch, advancing the consumer position.
func (c *Client) Ack(ctx context.Context, batchID int64) error {
	_, err := c.pool.Exec(ctx, "SELECT pgque.ack($1)", batchID)
	if err != nil {
		return fmt.Errorf("pgque: ack: %w", err)
	}
	return nil
}

// NewConsumer creates a Consumer for the given queue and consumer name.
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
