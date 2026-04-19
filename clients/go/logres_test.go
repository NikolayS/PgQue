package logres_test

import (
	"context"
	"os"
	"testing"
	"time"

	logres "github.com/NikolayS/logres/clients/go"
)

func getDSN() string {
	dsn := os.Getenv("PGQUE_TEST_DSN")
	if dsn == "" {
		dsn = "postgresql://postgres:logres_test@localhost/logres_test"
	}
	return dsn
}

func setupQueue(t *testing.T, client *logres.Client) {
	t.Helper()
	ctx := context.Background()
	_, err := client.Pool().Exec(ctx, "SELECT logres.create_queue('gotest_queue')")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Pool().Exec(ctx, "SELECT logres.register_consumer('gotest_queue', 'gotest_consumer')")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Pool().Exec(ctx, "SELECT logres.unregister_consumer('gotest_queue', 'gotest_consumer')")
		client.Pool().Exec(ctx, "SELECT logres.drop_queue('gotest_queue')")
	})
}

func TestSend(t *testing.T) {
	ctx := context.Background()
	client, err := logres.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	eid, err := client.Send(ctx, "gotest_queue", logres.Event{
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
	client, err := logres.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	// Send
	_, err = client.Send(ctx, "gotest_queue", logres.Event{
		Type:    "test.type",
		Payload: map[string]any{"key": "value"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ticker
	_, err = client.Pool().Exec(ctx, "SELECT logres.ticker()")
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

func TestConsumerHandlerDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := logres.Connect(ctx, getDSN())
	if err != nil {
		t.Skip("Cannot connect to PG:", err)
	}
	defer client.Close()
	setupQueue(t, client)

	// Send event
	_, err = client.Send(ctx, "gotest_queue", logres.Event{
		Type:    "dispatch.test",
		Payload: map[string]any{"dispatched": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Pool().Exec(ctx, "SELECT logres.ticker()")

	received := make(chan logres.Message, 1)
	consumer := client.NewConsumer("gotest_queue", "gotest_consumer",
		logres.WithPollInterval(100*time.Millisecond),
	)
	consumer.Handle("dispatch.test", func(ctx context.Context, msg logres.Message) error {
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
