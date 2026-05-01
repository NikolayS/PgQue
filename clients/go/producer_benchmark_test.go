// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque/clients/go"
)

// TestProducerBenchmarks compares producer latency/throughput for the default
// Go client API paths:
//   - loop over Client.Send(...)
//   - one Client.SendBatch(...) call
//
// It is opt-in because it is a microbenchmark-style integration test:
//
//	PGQUE_RUN_PRODUCER_BENCH=1 PGQUE_TEST_DSN=postgres://... go test -run TestProducerBenchmarks -v
func TestProducerBenchmarks(t *testing.T) {
	if os.Getenv("PGQUE_RUN_PRODUCER_BENCH") != "1" {
		t.Skip("PGQUE_RUN_PRODUCER_BENCH=1 not set")
	}

	client := connectOrSkip(t)
	defer client.Close()

	batchSizes := []int{1, 100, 1000}
	repeats := 3
	if raw := os.Getenv("PGQUE_BENCH_REPEATS"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &repeats); err != nil || repeats < 1 {
			t.Fatalf("invalid PGQUE_BENCH_REPEATS=%q", raw)
		}
	}

	t.Log("# pgque Go producer benchmark")
	t.Log("| method | batch_size | median_ms | events_per_sec | repeats |")
	t.Log("|---|---:|---:|---:|---:|")
	for _, n := range batchSizes {
		measureProducer(t, client, "send_loop", n, repeats, sendLoopGo)
		measureProducer(t, client, "send_batch", n, repeats, sendBatchGo)
	}
	t.Log("csv:language,method,batch_size,median_ms,events_per_sec,repeats")
}

type producerFn func(context.Context, *pgque.Client, string, []any) error

func measureProducer(t *testing.T, client *pgque.Client, method string, n, repeats int, fn producerFn) {
	t.Helper()
	ctx := context.Background()
	durations := make([]time.Duration, 0, repeats)
	for r := 0; r < repeats; r++ {
		queue := fmt.Sprintf("gobench_%s_%d_%s", method, n, randSuffix(t))
		payloads := make([]any, n)
		for i := range payloads {
			payloads[i] = map[string]any{"i": i, "lang": "go", "method": method}
		}
		if _, err := client.Pool().Exec(ctx, "select pgque.create_queue($1)", queue); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		err := fn(ctx, client, queue, payloads)
		elapsed := time.Since(start)
		if err != nil {
			client.Pool().Exec(ctx, "select pgque.drop_queue($1, true)", queue)
			t.Fatal(err)
		}
		verifyProducerCount(t, ctx, client, queue, n)
		durations = append(durations, elapsed)
		if _, err := client.Pool().Exec(ctx, "select pgque.drop_queue($1, true)", queue); err != nil {
			t.Fatal(err)
		}
	}

	median := medianDuration(durations)
	medianMs := float64(median) / float64(time.Millisecond)
	eps := float64(n) / median.Seconds()
	t.Logf("| %s | %d | %.3f | %.0f | %d |", method, n, medianMs, eps, repeats)
	t.Logf("csv:go,%s,%d,%.3f,%.0f,%d", method, n, medianMs, eps, repeats)
}

func sendLoopGo(ctx context.Context, client *pgque.Client, queue string, payloads []any) error {
	for _, payload := range payloads {
		if _, err := client.Send(ctx, queue, pgque.Event{Type: "bench.producer", Payload: payload}); err != nil {
			return err
		}
	}
	return nil
}

func sendBatchGo(ctx context.Context, client *pgque.Client, queue string, payloads []any) error {
	_, err := client.SendBatch(ctx, queue, "bench.producer", payloads)
	return err
}

func verifyProducerCount(t *testing.T, ctx context.Context, client *pgque.Client, queue string, expected int) {
	t.Helper()
	var table string
	if err := client.Pool().QueryRow(ctx, "select pgque.current_event_table($1)", queue).Scan(&table); err != nil {
		t.Fatal(err)
	}
	var got int
	if err := client.Pool().QueryRow(ctx, "select count(*) from "+table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != expected {
		t.Fatalf("%s: expected %d events, got %d", queue, expected, got)
	}
}

func medianDuration(values []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}
