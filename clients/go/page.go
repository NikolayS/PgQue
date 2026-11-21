// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type Page struct {
	Status     string
	BatchID    *int64
	PageToken  *string
	PageNumber *int64
	IsLast     *bool
	Messages   []Message
	LeaseUntil *time.Time
	FenceEpoch *int64
}

type PageFailure struct {
	MsgID             string  `json:"msg_id"`
	RetryAfterSeconds *int32  `json:"retry_after_seconds,omitempty"`
	Reason            *string `json:"reason,omitempty"`
}

type AckPageResult struct {
	Status        string
	BatchFinished bool
}
type ProcessPageResult struct {
	Status         string
	ProcessedCount int
	BatchFinished  *bool
}
type PageHandlerFunc func(context.Context, Message) error

const pageSelect = `select p.status, p.batch_id as page_batch_id, p.page_token::text,
       p.page_number, p.is_last, p.lease_until, p.fence_epoch,
       m.msg_id, m.batch_id, m.type, m.payload, m.retry_count,
       m.created_at, m.extra1, m.extra2, m.extra3, m.extra4, m.ordinality
from %s as p
left join lateral unnest(p.messages) with ordinality as m on true
order by m.ordinality`

func (c *Client) ReceivePage(ctx context.Context, queue, consumer, worker string, pageSize int, lease time.Duration) (Page, error) {
	return c.queryPage(ctx, "receive page", "pgque.receive_page($1,$2,$3,$4,$5::interval)", queue, consumer, worker, pageSize, durationInterval(lease))
}

func (c *Client) ReceivePageCoop(ctx context.Context, queue, consumer, subconsumer, worker string, pageSize int, deadInterval *time.Duration, lease time.Duration) (Page, error) {
	var dead any
	if deadInterval != nil {
		dead = durationInterval(*deadInterval)
	}
	return c.queryPage(ctx, "receive page coop", "pgque.receive_page_coop($1,$2,$3,$4,$5,$6::interval,$7::interval)", queue, consumer, subconsumer, worker, pageSize, dead, durationInterval(lease))
}

func (c *Client) ReceivePagePartitioned(ctx context.Context, queue, consumer string, slot, n int, worker string, pageSize int) (Page, error) {
	return c.queryPage(ctx, "receive page partitioned", "pgque.receive_page_partitioned($1,$2,$3,$4,$5,$6)", queue, consumer, slot, n, worker, pageSize)
}

func (c *Client) AckPage(ctx context.Context, pageToken, worker string, failures []PageFailure) (AckPageResult, error) {
	if failures == nil {
		failures = []PageFailure{}
	}
	encoded, err := json.Marshal(failures)
	if err != nil {
		return AckPageResult{}, fmt.Errorf("pgque: marshal page failures: %w", err)
	}
	var result AckPageResult
	err = c.pool.QueryRow(ctx, "select status, batch_finished from pgque.ack_page($1::uuid,$2,$3::jsonb)", pageToken, worker, string(encoded)).Scan(&result.Status, &result.BatchFinished)
	if err != nil {
		return AckPageResult{}, wrapSQLError("ack page", err)
	}
	return result, nil
}

func (c *Client) RenewPage(ctx context.Context, pageToken, worker string) (time.Time, error) {
	var until time.Time
	err := c.pool.QueryRow(ctx, "select pgque.renew_page($1::uuid,$2)", pageToken, worker).Scan(&until)
	if err != nil {
		return time.Time{}, wrapSQLError("renew page", err)
	}
	return until, nil
}

func (c *Client) ProcessPage(ctx context.Context, queue, consumer, worker string, pageSize int, lease time.Duration, handler PageHandlerFunc) (ProcessPageResult, error) {
	if handler == nil {
		return ProcessPageResult{}, fmt.Errorf("pgque: page handler must not be nil")
	}
	page, err := c.ReceivePage(ctx, queue, consumer, worker, pageSize, lease)
	if err != nil {
		return ProcessPageResult{}, err
	}
	result := ProcessPageResult{Status: page.Status}
	if page.Status != "page" {
		return result, nil
	}
	for _, message := range page.Messages {
		if err := handler(ctx, message); err != nil {
			return ProcessPageResult{}, err
		}
		result.ProcessedCount++
	}
	ack, err := c.AckPage(ctx, *page.PageToken, worker, nil)
	if err != nil {
		return ProcessPageResult{}, err
	}
	result.BatchFinished = &ack.BatchFinished
	return result, nil
}

func durationInterval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

func (c *Client) queryPage(ctx context.Context, op, function string, args ...any) (Page, error) {
	rows, err := c.pool.Query(ctx, fmt.Sprintf(pageSelect, function), args...)
	if err != nil {
		return Page{}, wrapSQLError(op, err)
	}
	defer rows.Close()
	var page Page
	found := false
	for rows.Next() {
		var status string
		var pageBatchID, pageNumber, fenceEpoch *int64
		var pageToken *string
		var isLast *bool
		var leaseUntil *time.Time
		var msgID, batchID *int64
		var typ, payload *string
		var retry *int
		var created *time.Time
		var e1, e2, e3, e4 *string
		var ord *int64
		if err := rows.Scan(&status, &pageBatchID, &pageToken, &pageNumber, &isLast, &leaseUntil, &fenceEpoch, &msgID, &batchID, &typ, &payload, &retry, &created, &e1, &e2, &e3, &e4, &ord); err != nil {
			return Page{}, fmt.Errorf("pgque: scan page: %w", err)
		}
		if found && (status != page.Status || !equalPtr(pageBatchID, page.BatchID) || !equalPtr(pageToken, page.PageToken) || !equalPtr(pageNumber, page.PageNumber) || !equalPtr(isLast, page.IsLast) || !equalPtr(leaseUntil, page.LeaseUntil) || !equalPtr(fenceEpoch, page.FenceEpoch)) {
			return Page{}, fmt.Errorf("pgque: inconsistent page metadata rows")
		}
		page.Status, page.BatchID, page.PageToken, page.PageNumber = status, pageBatchID, pageToken, pageNumber
		page.IsLast, page.LeaseUntil, page.FenceEpoch = isLast, leaseUntil, fenceEpoch
		found = true
		if msgID != nil {
			m := Message{MsgID: *msgID, BatchID: *batchID, RetryCount: retry, Extra1: e1, Extra2: e2, Extra3: e3, Extra4: e4}
			if typ != nil {
				m.Type = *typ
			}
			if payload != nil {
				m.Payload = *payload
			}
			if created != nil {
				m.CreatedAt = *created
			}
			page.Messages = append(page.Messages, m)
		}
	}
	if err := rows.Err(); err != nil {
		return Page{}, wrapSQLError(op+" rows", err)
	}
	if !found {
		return Page{}, fmt.Errorf("pgque: %s returned no metadata row", op)
	}
	return page, nil
}

func equalPtr[T comparable](a, b *T) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
