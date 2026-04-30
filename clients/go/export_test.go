// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

// ConsumerBackend is the externally visible name of the Consumer's
// backing surface (Receive / Ack / Nack). Exported in this _test.go
// file so tests in package pgque_test can supply a stub backend
// without exposing the interface in the public API.
type ConsumerBackend = consumerBackend

// SetConsumerBackend replaces the backend a Consumer dispatches to.
// Test-only: call after Client.NewConsumer and before Consumer.Start.
func SetConsumerBackend(c *Consumer, b ConsumerBackend) {
	c.backend = b
}
