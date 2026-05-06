// pgque-go -- cooperative consumer demo
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

// Demo of two cooperative subconsumers under one logical consumer
// sharing a queue. Each worker prints the messages it dispatches; on
// shutdown the program prints a per-worker count and the sum so the
// user can verify disjoint delivery.
//
// Usage:
//
//	PGQUE_TEST_DSN=postgres://user@host/db go run ./bench/coop_demo
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pgque "github.com/NikolayS/pgque-go"
)

func main() {
	dsn := os.Getenv("PGQUE_TEST_DSN")
	if dsn == "" {
		log.Fatal("PGQUE_TEST_DSN must be set (e.g. postgres://nik@localhost/pgque_coop_go)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := pgque.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	suffix := randSuffix()
	queue := "coop_demo_q_" + suffix
	consumer := "coop_demo_c_" + suffix

	if _, err := client.Pool().Exec(ctx, "select pgque.create_queue($1)", queue); err != nil {
		log.Fatalf("create_queue: %v", err)
	}
	defer func() {
		// Best-effort cleanup. If subconsumers remain, unsubscribe
		// them with the retry-handling option so drop_queue(force)
		// is allowed.
		bg := context.Background()
		for _, name := range []string{"worker-1", "worker-2"} {
			client.Pool().Exec(bg,
				"select pgque.unsubscribe_subconsumer($1, $2, $3, 1)",
				queue, consumer, name)
		}
		client.Pool().Exec(bg, "select pgque.drop_queue($1, true)", queue)
	}()

	// Subscribe both subconsumers BEFORE publishing so their cursors
	// predate the events; otherwise the first auto-registration sits
	// at the current tick and the just-published events are invisible.
	for _, name := range []string{"worker-1", "worker-2"} {
		if _, err := client.SubscribeSubconsumer(ctx, queue, consumer, name); err != nil {
			log.Fatalf("subscribe %s: %v", name, err)
		}
	}

	// Force a tick so events become visible to the workers.
	tick := func() {
		if _, err := client.ForceNextTick(ctx, queue); err != nil {
			log.Printf("force_next_tick: %v", err)
			return
		}
		if _, err := client.Pool().Exec(ctx, "select pgque.ticker($1)", queue); err != nil {
			log.Printf("ticker: %v", err)
		}
	}

	// Publish events one tick at a time so the SQL allocator hands the
	// resulting batches alternately to the two subconsumers. Without
	// per-tick fanout one worker would drain the whole batch.
	const ticks = 12
	for i := 0; i < ticks; i++ {
		if _, err := client.Send(ctx, queue, pgque.Event{
			Type:    "demo.event",
			Payload: map[string]any{"i": i},
		}); err != nil {
			log.Fatalf("send: %v", err)
		}
		tick()
	}

	// Two workers under one logical consumer.
	var (
		seen [2]int64
	)
	makeWorker := func(idx int, name string) *pgque.Consumer {
		c := client.NewConsumer(queue, consumer,
			pgque.WithPollInterval(50*time.Millisecond),
			pgque.WithSubconsumer(name))
		c.Handle("demo.event", func(ctx context.Context, m pgque.Message) error {
			fmt.Printf("%s got msg %d type %s\n", name, m.MsgID, m.Type)
			atomic.AddInt64(&seen[idx], 1)
			return nil
		})
		return c
	}
	w1 := makeWorker(0, "worker-1")
	w2 := makeWorker(1, "worker-2")

	runCtx, runCancel := context.WithTimeout(ctx, 5*time.Second)
	defer runCancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = w1.Start(runCtx) }()
	go func() { defer wg.Done(); _ = w2.Start(runCtx) }()

	// Periodically tick during the run so the SQL allocator has new
	// batches to hand out alternately.
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-runCtx.Done():
			break loop
		case <-ticker.C:
			tick()
		}
	}
	runCancel()
	wg.Wait()

	a := atomic.LoadInt64(&seen[0])
	b := atomic.LoadInt64(&seen[1])
	fmt.Printf("\nsummary: worker-1: %d, worker-2: %d, sum=%d\n", a, b, a+b)
}

func randSuffix() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
