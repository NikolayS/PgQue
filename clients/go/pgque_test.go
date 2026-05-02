package pgque_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque/clients/go"
)

func getDSN() string {
	dsn := os.Getenv("PGQUE_TEST_DSN")
	if dsn == "" {
		dsn = "postgresql://postgres:pgque_test@localhost/pgque_test"
	}
	return dsn
}

func setupQueue(t *testing.T, client *pgque.Client) {
	t.Helper()
	ctx := context.Background()
	_, err := client.Pool().Exec(ctx, "SELECT pgque.create_queue('gotest_queue')")
	if err != nil {
		t.Fatal(err)
	}
	// Lower the ticker thresholds so tests don't have to wait 3 s for a tick.
	// Parameter names are the column name suffix: queue_ticker_max_count →
	// "ticker_max_count". Setting count=1 means one event is enough; lag=1ms.
	_, err = client.Pool().Exec(ctx,
		"SELECT pgque.set_queue_config('gotest_queue', 'ticker_max_count', '1')")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Pool().Exec(ctx,
		"SELECT pgque.set_queue_config('gotest_queue', 'ticker_max_lag', '1ms')")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Pool().Exec(ctx, "SELECT pgque.register_consumer('gotest_queue', 'gotest_consumer')")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Open a fresh connection for cleanup: the test's defer client.Close()
		// runs before t.Cleanup fires (defer is in the test body scope),
		// so the outer client pool is already closed at this point.
		cleanDSN := getDSN()
		cleanClient, cleanErr := pgque.Connect(ctx, cleanDSN)
		if cleanErr != nil {
			t.Logf("setupQueue cleanup: cannot connect for teardown: %v", cleanErr)
			return
		}
		defer cleanClient.Close()

		// Remove retry_queue / dead_letter entries before dropping the queue
		// so stale rows don't leak into a subsequent test that gets the same
		// queue_id from the sequence.
		cleanClient.Pool().Exec(ctx, `
			DELETE FROM pgque.retry_queue rq
			USING pgque.queue q
			WHERE q.queue_id = rq.ev_queue
			  AND q.queue_name = 'gotest_queue'`)
		cleanClient.Pool().Exec(ctx, `
			DELETE FROM pgque.dead_letter dl
			USING pgque.queue q
			WHERE q.queue_id = dl.dl_queue_id
			  AND q.queue_name = 'gotest_queue'`)
		cleanClient.Pool().Exec(ctx, "SELECT pgque.unregister_consumer('gotest_queue', 'gotest_consumer')")
		cleanClient.Pool().Exec(ctx, "SELECT pgque.drop_queue('gotest_queue')")
	})
}

func TestSend(t *testing.T) {
	ctx := context.Background()
	client, err := pgque.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	eid, err := client.Send(ctx, "gotest_queue", pgque.Event{
		Type:    "order.created",
		Payload: map[string]any{"order_id": 42},
	})
	if err != nil {
		t.Fatal(err)
	}
	if eid == 0 {
		t.Fatal("expected non-zero event ID")
	}
}

func TestSendBatch(t *testing.T) {
	ctx := context.Background()
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)

	ids, err := client.SendBatch(ctx, queue, "batch.test", []any{
		map[string]any{"n": 1},
		map[string]any{"n": 2},
		map[string]any{"n": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}
	if ids[0] >= ids[1] || ids[1] >= ids[2] {
		t.Fatalf("expected IDs in input order, got %v", ids)
	}

	tick(t, client, queue)
	msgs, err := client.Receive(ctx, queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].MsgID < msgs[j].MsgID })
	for i, msg := range msgs {
		if msg.Type != "batch.test" {
			t.Fatalf("expected type batch.test, got %s", msg.Type)
		}
		var payload map[string]int
		if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["n"] != i+1 {
			t.Fatalf("expected payload n=%d, got %v", i+1, payload)
		}
	}
}

func TestSendBatchEmptySlice(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	ids, err := client.SendBatch(context.Background(), queue, "batch.empty", []any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no IDs, got %v", ids)
	}
	tick(t, client, queue)
	msgs, err := client.Receive(context.Background(), queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected no messages, got %d", len(msgs))
	}
}

func TestSendBatchEmptyTypeDefaults(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	if _, err := client.SendBatch(context.Background(), queue, "", []any{map[string]any{"x": 1}}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)
	msgs, err := client.Receive(context.Background(), queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Type != "default" {
		t.Fatalf("expected one default message, got %#v", msgs)
	}
}

func TestSendBatchNilPayloadProducesJSONNull(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, consumer := setupFreshQueue(t, client)
	if _, err := client.SendBatch(context.Background(), queue, "batch.null", []any{nil}); err != nil {
		t.Fatal(err)
	}
	tick(t, client, queue)
	msgs, err := client.Receive(context.Background(), queue, consumer, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Payload != "null" {
		t.Fatalf("expected JSON null payload, got %#v", msgs)
	}
}

func TestSendBatchMissingQueue(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	_, err := client.SendBatch(context.Background(), "missing_"+randSuffix(t), "x", []any{map[string]any{"x": 1}})
	if err == nil {
		t.Fatal("expected missing queue error")
	}
}

func TestSendBatchUnmarshalablePayload(t *testing.T) {
	client := connectOrSkip(t)
	defer client.Close()
	queue, _ := setupFreshQueue(t, client)
	_, err := client.SendBatch(context.Background(), queue, "x", []any{make(chan int)})
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestSendAndReceive(t *testing.T) {
	ctx := context.Background()
	client, err := pgque.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	// Send
	_, err = client.Send(ctx, "gotest_queue", pgque.Event{
		Type:    "test.type",
		Payload: map[string]any{"key": "value"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ticker
	_, err = client.Pool().Exec(ctx, "SELECT pgque.ticker('gotest_queue')")
	if err != nil {
		t.Fatal(err)
	}

	// Receive
	msgs, err := client.Receive(ctx, "gotest_queue", "gotest_consumer", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Type != "test.type" {
		t.Fatalf("expected type test.type, got %s", msgs[0].Type)
	}

	// Ack
	err = client.Ack(ctx, msgs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNack(t *testing.T) {
	ctx := context.Background()
	client, err := pgque.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	if _, err = client.Send(ctx, "gotest_queue", pgque.Event{
		Type:    "nack.test",
		Payload: map[string]any{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Pool().Exec(ctx, "SELECT pgque.ticker('gotest_queue')"); err != nil {
		t.Fatal(err)
	}

	msgs, err := client.Receive(ctx, "gotest_queue", "gotest_consumer", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// retry_count starts at null which coalesces to 0; default queue_max_retries
	// is 5; so this nack should route to retry_queue, not the DLQ.
	if err := client.Nack(ctx, msgs[0].BatchID, msgs[0]); err != nil {
		t.Fatal(err)
	}
	if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatal(err)
	}

	var retryCount int
	if err := client.Pool().QueryRow(ctx, `
		SELECT count(*) FROM pgque.retry_queue rq
		JOIN pgque.queue q ON q.queue_id = rq.ev_queue
		WHERE q.queue_name = $1`, "gotest_queue").Scan(&retryCount); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("expected 1 row in retry_queue, got %d", retryCount)
	}

	var dlqCount int
	if err := client.Pool().QueryRow(ctx, `
		SELECT count(*) FROM pgque.dead_letter dl
		JOIN pgque.queue q ON q.queue_id = dl.dl_queue_id
		WHERE q.queue_name = $1`, "gotest_queue").Scan(&dlqCount); err != nil {
		t.Fatal(err)
	}
	if dlqCount != 0 {
		t.Fatalf("expected 0 rows in DLQ (under retry limit), got %d", dlqCount)
	}
}

// TestNackPlaceholderCount is a red/green regression for a bug where the
// Nack() SQL string had 12 placeholders but only 11 args were passed,
// because the ROW(...) cast targeting pgque.message (10 fields) included
// a trailing $12 typo. pgx fails the query with "expected 12 arguments,
// got 11". Pre-fix this test fails on the call to client.Nack(); post-fix
// it passes. The bug shipped because client-smoke ran `go test -run
// TestSmoke` which matched no tests, so TestNack never executed in CI.
func TestNackPlaceholderCount(t *testing.T) {
	ctx := context.Background()
	client, err := pgque.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	if _, err = client.Send(ctx, "gotest_queue", pgque.Event{
		Type:    "nack.placeholder",
		Payload: map[string]any{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Pool().Exec(ctx, "SELECT pgque.ticker('gotest_queue')"); err != nil {
		t.Fatal(err)
	}

	msgs, err := client.Receive(ctx, "gotest_queue", "gotest_consumer", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// The whole point: this call must succeed against a live DB.
	if err := client.Nack(ctx, msgs[0].BatchID, msgs[0]); err != nil {
		t.Fatalf("Nack() returned error (placeholder/arg mismatch?): %v", err)
	}
}

// TestConnect_UnreachableHostFailsImmediately is a red/green regression
// for Connect() accepting unreachable hosts because pgxpool.New is lazy.
// Pre-fix Connect returns nil error; the failure surfaces only on the
// first query, far from the call site. Post-fix Connect eagerly pings
// the pool and returns an error within ~3s.
func TestConnect_UnreachableHostFailsImmediately(t *testing.T) {
	// Port 1 is reserved/unassigned and reliably refuses connections.
	dsn := "postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=2"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	client, err := pgque.Connect(ctx, dsn)
	elapsed := time.Since(start)

	if err == nil {
		if client != nil {
			client.Close()
		}
		t.Fatalf("expected Connect to fail for unreachable host, got nil error after %s", elapsed)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("Connect took %s; should fail within ~3s with eager ping", elapsed)
	}
}

// TestConsumer_HandlerPanicRecoversAndContinues is a red/green regression
// for Consumer.Start letting handler panics propagate, killing the poll
// goroutine and stalling the queue. Pre-fix: the panic on the first
// message kills the consumer goroutine; the second message never reaches
// its handler; the test fails with "got 1, expected 2". Post-fix: the
// consumer recovers, treats the panic as a handler error (nack), and
// continues processing.
//
// The goroutine's defer recover() in this test is purely a safety net so
// that pre-fix runs do not crash the test process — it does not mask the
// failure mode (which is "consumer never sees the second message").
func TestConsumer_HandlerPanicRecoversAndContinues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := pgque.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	for i := 0; i < 2; i++ {
		if _, err = client.Send(ctx, "gotest_queue", pgque.Event{
			Type:    "panic.test",
			Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = client.Pool().Exec(ctx, "SELECT pgque.ticker('gotest_queue')"); err != nil {
		t.Fatal(err)
	}

	var seen int32
	consumer := client.NewConsumer("gotest_queue", "gotest_consumer",
		pgque.WithPollInterval(100*time.Millisecond),
	)
	consumer.Handle("panic.test", func(ctx context.Context, msg pgque.Message) error {
		n := atomic.AddInt32(&seen, 1)
		if n == 1 {
			panic("simulated handler panic")
		}
		return nil
	})

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()
	go func() {
		// Test-side safety net: contain the panic so the test process
		// survives pre-fix (when production code does not recover).
		// Survival of the goroutine itself is what we are testing —
		// detected by whether the second message gets handled.
		defer func() { _ = recover() }()
		_ = consumer.Start(consumerCtx)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&seen) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if atomic.LoadInt32(&seen) < 2 {
		t.Fatalf("expected handler to be called for both messages (panic recovered), got %d", seen)
	}
	consumerCancel()

	// The panicking message should have been nack'd, not silently dropped.
	var retryCount int
	if err := client.Pool().QueryRow(ctx, `
		SELECT count(*) FROM pgque.retry_queue rq
		JOIN pgque.queue q ON q.queue_id = rq.ev_queue
		WHERE q.queue_name = $1`, "gotest_queue").Scan(&retryCount); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("expected 1 retry_queue entry (panicking message), got %d", retryCount)
	}
}

func TestConsumerHandlerNacksOnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := pgque.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	// Two messages in the same batch: first handler errors, second succeeds.
	// The failing one should be nack'd individually; the successful one should
	// still complete; the batch overall should be ack'd.
	for i := 0; i < 2; i++ {
		if _, err = client.Send(ctx, "gotest_queue", pgque.Event{
			Type:    "fail.test",
			Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = client.Pool().Exec(ctx, "SELECT pgque.ticker('gotest_queue')"); err != nil {
		t.Fatal(err)
	}

	var seen int32
	consumer := client.NewConsumer("gotest_queue", "gotest_consumer",
		pgque.WithPollInterval(100*time.Millisecond),
	)
	consumer.Handle("fail.test", func(ctx context.Context, msg pgque.Message) error {
		n := atomic.AddInt32(&seen, 1)
		if n == 1 {
			return errors.New("simulated handler failure")
		}
		return nil
	})

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()
	go consumer.Start(consumerCtx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&seen) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if atomic.LoadInt32(&seen) < 2 {
		t.Fatalf("expected handler to be called for both messages, got %d", seen)
	}
	consumerCancel()

	// The failing message should have landed in retry_queue (per-message nack),
	// not been silently dropped along with the batch.
	var retryCount int
	if err := client.Pool().QueryRow(ctx, `
		SELECT count(*) FROM pgque.retry_queue rq
		JOIN pgque.queue q ON q.queue_id = rq.ev_queue
		WHERE q.queue_name = $1`, "gotest_queue").Scan(&retryCount); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("expected 1 retry_queue entry (the failing message), got %d", retryCount)
	}
}

func TestConsumerHandlerDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := pgque.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	// Send event
	_, err = client.Send(ctx, "gotest_queue", pgque.Event{
		Type:    "dispatch.test",
		Payload: map[string]any{"dispatched": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Pool().Exec(ctx, "SELECT pgque.ticker('gotest_queue')")

	received := make(chan pgque.Message, 1)
	consumer := client.NewConsumer("gotest_queue", "gotest_consumer",
		pgque.WithPollInterval(100*time.Millisecond),
	)
	consumer.Handle("dispatch.test", func(ctx context.Context, msg pgque.Message) error {
		received <- msg
		return nil
	})

	go consumer.Start(ctx)

	select {
	case msg := <-received:
		if msg.Type != "dispatch.test" {
			t.Fatalf("expected dispatch.test, got %s", msg.Type)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}
