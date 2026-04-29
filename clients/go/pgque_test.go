package pgque_test

import (
	"context"
	"errors"
	"os"
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
	_, err = client.Pool().Exec(ctx, "SELECT pgque.register_consumer('gotest_queue', 'gotest_consumer')")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Pool().Exec(ctx, "SELECT pgque.unregister_consumer('gotest_queue', 'gotest_consumer')")
		client.Pool().Exec(ctx, "SELECT pgque.drop_queue('gotest_queue')")
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
	_, err = client.Pool().Exec(ctx, "SELECT pgque.ticker()")
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
	if _, err = client.Pool().Exec(ctx, "SELECT pgque.ticker()"); err != nil {
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
	if _, err = client.Pool().Exec(ctx, "SELECT pgque.ticker()"); err != nil {
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
	client.Pool().Exec(ctx, "SELECT pgque.ticker()")

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
