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
