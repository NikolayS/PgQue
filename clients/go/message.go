// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import "time"

// Message represents a message received from a PgQue queue.
type Message struct {
	MsgID      int64     `json:"msg_id"`
	BatchID    int64     `json:"batch_id"`
	Type       string    `json:"type"`
	Payload    string    `json:"payload"`
	RetryCount *int      `json:"retry_count"`
	CreatedAt  time.Time `json:"created_at"`
	Extra1     *string   `json:"extra1"`
	Extra2     *string   `json:"extra2"`
	Extra3     *string   `json:"extra3"`
	Extra4     *string   `json:"extra4"`
}

// Event represents an event to be sent to a PgQue queue.
type Event struct {
	Type    string
	Payload any // will be JSON-marshaled
}
