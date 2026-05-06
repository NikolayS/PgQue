// pgque-go -- cooperative consumer scaling benchmark
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

// Drives N goroutines (subconsumers) under one logical consumer and
// measures how fast they jointly drain a pre-published queue. Output
// is a single CSV row per run on stdout:
//
//	subconsumers,events_per_sec,seconds
//
// Reports the median over -runs runs.
//
// Usage:
//
//	PGQUE_TEST_DSN=postgres://... go run ./bench/coop_scaling \
//	  -subconsumers=4 -events=5000 -payload=64 -runs=3
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pgque "github.com/NikolayS/pgque-go"
)

func main() {
	subN := flag.Int("subconsumers", 1, "number of cooperative subconsumers")
	events := flag.Int("events", 5000, "number of events to publish per run")
	payload := flag.Int("payload", 64, "payload size in bytes per event")
	runs := flag.Int("runs", 3, "number of runs to take the median over")
	maxMessages := flag.Int("max-messages", 500, "per-call ReceiveCoop max messages")
	pollInterval := flag.Duration("poll-interval", 5*time.Millisecond, "idle poll backoff")
	flag.Parse()

	dsn := os.Getenv("PGQUE_TEST_DSN")
	if dsn == "" {
		log.Fatal("PGQUE_TEST_DSN must be set")
	}

	ctx := context.Background()
	client, err := pgque.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// Build the payload string once.
	payloadStr := strings.Repeat("x", *payload)

	rates := make([]float64, 0, *runs)
	durations := make([]float64, 0, *runs)
	for r := 0; r < *runs; r++ {
		eps, secs, err := runOne(ctx, client, *subN, *events, payloadStr, *maxMessages, *pollInterval)
		if err != nil {
			log.Fatalf("run %d: %v", r, err)
		}
		rates = append(rates, eps)
		durations = append(durations, secs)
	}

	sort.Float64s(rates)
	sort.Float64s(durations)
	medRate := rates[len(rates)/2]
	medSecs := durations[len(durations)/2]
	fmt.Printf("%d,%.2f,%.4f\n", *subN, medRate, medSecs)
}

func runOne(
	ctx context.Context,
	client *pgque.Client,
	subN, events int,
	payloadStr string,
	maxMessages int,
	pollInterval time.Duration,
) (float64, float64, error) {
	suffix := randSuffix()
	queue := "coop_scal_q_" + suffix
	consumer := "coop_scal_c_" + suffix

	if _, err := client.Pool().Exec(ctx, "select pgque.create_queue($1)", queue); err != nil {
		return 0, 0, fmt.Errorf("create_queue: %w", err)
	}
	defer func() {
		bg := context.Background()
		// Best-effort cleanup of any subconsumers that might be left.
		for i := 0; i < subN; i++ {
			client.Pool().Exec(bg,
				"select pgque.unsubscribe_subconsumer($1, $2, $3, 1)",
				queue, consumer, subName(i))
		}
		client.Pool().Exec(bg, "select pgque.drop_queue($1, true)", queue)
	}()

	// Subscribe all subconsumers BEFORE producing so their cursors
	// predate the events.
	for i := 0; i < subN; i++ {
		if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, subName(i)); err != nil {
			return 0, 0, fmt.Errorf("subscribe %s: %w", subName(i), err)
		}
	}

	// Pre-publish events as many smaller batches across many ticks so
	// the SQL allocator has multiple batches to hand out to different
	// subconsumers in parallel. Without per-tick fanout the whole
	// queue would land in one batch and only one worker could drain
	// it (each batch is allocated to one subconsumer at a time).
	// Fixed chunkSize across N keeps "one batch = unit of work"
	// constant; throughput then reflects the parallelism the SQL
	// allocator exposes, not how big each batch happens to be.
	chunkSize := 100
	for i := 0; i < events; i += chunkSize {
		end := i + chunkSize
		if end > events {
			end = events
		}
		payloads := make([]any, end-i)
		for j := range payloads {
			payloads[j] = map[string]any{"i": i + j, "p": payloadStr}
		}
		if _, err := client.SendBatch(ctx, queue, "scal.event", payloads); err != nil {
			return 0, 0, fmt.Errorf("send_batch: %w", err)
		}
		// Tick after each chunk so the events become visible as a
		// distinct batch.
		if _, err := client.ForceNextTick(ctx, queue); err != nil {
			return 0, 0, fmt.Errorf("force_next_tick: %w", err)
		}
		if _, err := client.Pool().Exec(ctx, "select pgque.ticker($1)", queue); err != nil {
			return 0, 0, fmt.Errorf("ticker: %w", err)
		}
	}

	// Spin up workers.
	var processed int64
	workerCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(subN)
	for i := 0; i < subN; i++ {
		name := subName(i)
		go func() {
			defer wg.Done()
			drain(workerCtx, client, queue, consumer, name, maxMessages, pollInterval, &processed, int64(events))
		}()
	}

	// Periodically issue ticks while the queue drains. With many
	// workers and small batches the queue may need additional ticks
	// to release further events.
	tickerLoop := time.NewTicker(50 * time.Millisecond)
	defer tickerLoop.Stop()
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-done:
				return
			case <-tickerLoop.C:
				if atomic.LoadInt64(&processed) >= int64(events) {
					return
				}
				client.Pool().Exec(workerCtx, "select pgque.ticker($1)", queue)
			}
		}
	}()

	// Wait for completion.
	for {
		if atomic.LoadInt64(&processed) >= int64(events) {
			cancel()
			break
		}
		select {
		case <-workerCtx.Done():
			break
		default:
		}
		if workerCtx.Err() != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(done)
	wg.Wait()
	elapsed := time.Since(start)

	got := atomic.LoadInt64(&processed)
	if got < int64(events) {
		return 0, 0, fmt.Errorf("processed only %d/%d before timeout", got, events)
	}

	secs := elapsed.Seconds()
	return float64(events) / secs, secs, nil
}

// drain runs a tight ReceiveCoop -> Ack loop until the shared
// processed counter reaches `target` or the context is cancelled.
func drain(
	ctx context.Context,
	client *pgque.Client,
	queue, consumer, name string,
	maxMessages int,
	pollInterval time.Duration,
	processed *int64,
	target int64,
) {
	for {
		if atomic.LoadInt64(processed) >= target {
			return
		}
		if ctx.Err() != nil {
			return
		}
		msgs, err := client.ReceiveCoop(ctx, queue, consumer, name,
			pgque.WithCoopMaxMessages(maxMessages))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Don't spam logs; continue.
			time.Sleep(pollInterval)
			continue
		}
		if len(msgs) == 0 {
			time.Sleep(pollInterval)
			continue
		}
		batchID := msgs[0].BatchID
		if _, err := client.Ack(ctx, batchID); err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		atomic.AddInt64(processed, int64(len(msgs)))
	}
}

func subName(i int) string {
	return fmt.Sprintf("worker-%d", i)
}

func randSuffix() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
