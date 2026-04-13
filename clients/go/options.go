// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import "time"

// Option configures a Consumer.
type Option func(*Consumer)

// WithPollInterval sets the interval between poll cycles when no messages
// are available.
func WithPollInterval(d time.Duration) Option {
	return func(c *Consumer) { c.pollInterval = d }
}
