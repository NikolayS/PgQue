package pgque

import (
	"context"
	"testing"
)

func TestSmoke(t *testing.T) {
	ctx := context.Background()
	client, err := Connect(ctx, "postgresql://postgres:pgque_test@localhost:5432/pgque_test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close(ctx)

	if err := client.Send(ctx, "smoke_go", "smoke.test", map[string]any{"hello": "world"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := client.Subscribe(ctx, "smoke_go", "go-smoke"); err != nil {
		t.Fatalf("subscribe: %v", err)
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
