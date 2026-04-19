package pg_current

import (
	"context"
	"testing"
)

func TestSmoke(t *testing.T) {
	ctx := context.Background()
	client, err := Connect(ctx, "postgresql://postgres:pg_current_test@localhost:5432/pg_current_test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	pool := client.Pool()
	if _, err := pool.Exec(ctx, "select pg_current.subscribe('smoke_go', 'go-smoke')"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ev := Event{Type: "smoke.test", Payload: map[string]any{"hello": "world"}}
	if _, err := client.Send(ctx, "smoke_go", ev); err != nil {
		t.Fatalf("send: %v", err)
	}

	if _, err := pool.Exec(ctx, "select pg_current.force_tick('smoke_go')"); err != nil {
		t.Fatalf("force_tick: %v", err)
	}
	if _, err := pool.Exec(ctx, "select pg_current.ticker()"); err != nil {
		t.Fatalf("ticker: %v", err)
	}

	msgs, err := client.Receive(ctx, "smoke_go", "go-smoke", 10)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected at least one message")
	}
	if err := client.Ack(ctx, msgs[0].BatchID); err != nil {
		t.Fatalf("ack: %v", err)
	}
}
