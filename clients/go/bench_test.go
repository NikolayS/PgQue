// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque/clients/go"
)

// BenchmarkSend measures single-Send latency.
//
//	PGQUE_TEST_DSN=postgres://... go test -bench=BenchmarkSend ./clients/go
func BenchmarkSend(b *testing.B) {
	client := benchClient(b)
	defer client.Close()
	queue, _ := benchSetup(b, client)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "bench.send", Payload: map[string]any{"i": i},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReceive_Empty measures Receive overhead on an empty queue.
func BenchmarkReceive_Empty(b *testing.B) {
	client := benchClient(b)
	defer client.Close()
	queue, consumer := benchSetup(b, client)
	ctx := context.Background()

	if _, err := client.Pool().Exec(ctx, "select pgque.ticker($1)", queue); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Receive(ctx, queue, consumer, 100); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSendReceiveAck end-to-end: send, tick, receive, ack one event.
// Captures the full round-trip cost.
func BenchmarkSendReceiveAck(b *testing.B) {
	client := benchClient(b)
	defer client.Close()
	queue, consumer := benchSetup(b, client)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "bench.rt", Payload: map[string]any{"i": i},
		}); err != nil {
			b.Fatal(err)
		}
		if _, err := client.Pool().Exec(ctx, "select pgque.ticker($1)", queue); err != nil {
			b.Fatal(err)
		}
		msgs, err := client.Receive(ctx, queue, consumer, 100)
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) > 0 {
			if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkConsumer_DispatchThroughput measures the rate at which the
// Consumer dispatches messages to a no-op handler. Reports messages/second.
func BenchmarkConsumer_DispatchThroughput(b *testing.B) {
	client := benchClient(b)
	defer client.Close()
	queue, consumer := benchSetup(b, client)
	ctx := context.Background()

	// Pre-load b.N events.
	for i := 0; i < b.N; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type: "bench.dispatch", Payload: map[string]any{"i": i},
		}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := client.Pool().Exec(ctx, "select pgque.ticker($1)", queue); err != nil {
		b.Fatal(err)
	}

	var seen int64
	c := client.NewConsumer(queue, consumer, pgque.WithPollInterval(20*time.Millisecond))
	c.Handle("bench.dispatch", func(ctx context.Context, m pgque.Message) error {
		atomic.AddInt64(&seen, 1)
		return nil
	})

	consumerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go c.Start(consumerCtx)

	b.ResetTimer()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&seen) >= int64(b.N) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.StopTimer()

	got := atomic.LoadInt64(&seen)
	if got < int64(b.N) {
		b.Logf("dispatched only %d/%d before timeout", got, b.N)
	}
}

// benchClient is a bench-friendly variant of connectOrSkip that uses b.Skip.
func benchClient(b *testing.B) *pgque.Client {
	b.Helper()
	ctx := context.Background()
	client, err := pgque.Connect(ctx, freshDSN())
	if err != nil {
		b.Skip("PGQUE_TEST_DSN not reachable:", err)
	}
	if _, err := client.Pool().Exec(ctx, "select 1"); err != nil {
		client.Close()
		b.Skip("PGQUE_TEST_DSN not reachable:", err)
	}
	return client
}

// benchSetup creates a fresh queue + consumer and registers cleanup.
func benchSetup(b *testing.B, client *pgque.Client) (queue, consumer string) {
	b.Helper()
	ctx := context.Background()
	suffix := benchSuffix(b)
	queue = "gobench_q_" + suffix
	consumer = "gobench_c_" + suffix
	if _, err := client.Pool().Exec(ctx, "select pgque.create_queue($1)", queue); err != nil {
		b.Fatal(err)
	}
	if _, err := client.Pool().Exec(ctx, "select pgque.register_consumer($1, $2)", queue, consumer); err != nil {
		client.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx := context.Background()
		client.Pool().Exec(ctx, "select pgque.unregister_consumer($1, $2)", queue, consumer)
		client.Pool().Exec(ctx, "select pgque.drop_queue($1)", queue)
	})
	return queue, consumer
}

func benchSuffix(b *testing.B) string {
	b.Helper()
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		b.Fatal(err)
	}
	return hex.EncodeToString(buf)
}
