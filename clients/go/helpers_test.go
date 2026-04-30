// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	pgque "github.com/NikolayS/pgque/clients/go"
)

// freshDSN returns the DSN for the integration test database.
// Tests are skipped via t.Skip if no DSN is reachable.
func freshDSN() string {
	if v := os.Getenv("PGQUE_TEST_DSN"); v != "" {
		return v
	}
	return "postgresql://postgres:pgque_test@localhost/pgque_test"
}

// connectOrSkip connects to the test database and skips the test if the
// connection fails. Caller must defer client.Close.
func connectOrSkip(t *testing.T) *pgque.Client {
	t.Helper()
	ctx := context.Background()
	client, err := pgque.Connect(ctx, freshDSN())
	if err != nil {
		t.Skip("PGQUE_TEST_DSN not reachable:", err)
	}
	// Probe the connection: Connect pings the pool eagerly, but a second
	// check guards against race conditions in test infrastructure.
	if _, err := client.Pool().Exec(ctx, "select 1"); err != nil {
		client.Close()
		t.Skip("PGQUE_TEST_DSN not reachable:", err)
	}
	return client
}

// randSuffix returns a short random hex suffix for unique queue / consumer
// names so parallel tests cannot collide.
func randSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// setupFreshQueue creates a fresh queue + consumer named with a random
// suffix, registered for cleanup via t.Cleanup. Returns the queue name and
// consumer name.
func setupFreshQueue(t *testing.T, client *pgque.Client) (queue, consumer string) {
	t.Helper()
	ctx := context.Background()
	suffix := randSuffix(t)
	queue = "gotest_q_" + suffix
	consumer = "gotest_c_" + suffix

	if _, err := client.Pool().Exec(ctx,
		"select pgque.create_queue($1)", queue); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Pool().Exec(ctx,
		"select pgque.register_consumer($1, $2)", queue, consumer); err != nil {
		client.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		client.Pool().Exec(ctx, "select pgque.unregister_consumer($1, $2)", queue, consumer)
		client.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
	})
	return queue, consumer
}

// tick advances the queue past one tick so events become visible to receive.
// Uses the per-queue pgque.ticker($1) overload to avoid cross-test side
// effects (other test queues running in parallel are not ticked).
func tick(t *testing.T, client *pgque.Client, queue string) {
	t.Helper()
	ctx := context.Background()
	if _, err := client.Pool().Exec(ctx, "select pgque.force_tick($1)", queue); err != nil {
		t.Fatal("force_tick:", err)
	}
	if _, err := client.Pool().Exec(ctx, "select pgque.ticker($1)", queue); err != nil {
		t.Fatal("ticker:", err)
	}
}

// retryQueueCount returns the number of retry_queue rows for the given queue.
func retryQueueCount(t *testing.T, client *pgque.Client, queue string) int {
	t.Helper()
	ctx := context.Background()
	var n int
	if err := client.Pool().QueryRow(ctx, `
		select count(*) from pgque.retry_queue rq
		join pgque.queue q on q.queue_id = rq.ev_queue
		where q.queue_name = $1`, queue).Scan(&n); err != nil {
		t.Fatal("retry_queue count:", err)
	}
	return n
}

// dlqCount returns the number of dead_letter rows for the given queue.
func dlqCount(t *testing.T, client *pgque.Client, queue string) int {
	t.Helper()
	ctx := context.Background()
	var n int
	if err := client.Pool().QueryRow(ctx, `
		select count(*) from pgque.dead_letter dl
		join pgque.queue q on q.queue_id = dl.dl_queue_id
		where q.queue_name = $1`, queue).Scan(&n); err != nil {
		t.Fatal("dlq count:", err)
	}
	return n
}
