// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
package pgque_test

import (
	"context"
	"fmt"
	pgque "github.com/NikolayS/pgque-go"
	"os"
	"testing"
	"time"
)

func TestPageLiveRoundTrip(t *testing.T) {
	dsn := os.Getenv("PGQUE_TEST_DSN")
	if dsn == "" {
		t.Skip("PGQUE_TEST_DSN not set")
	}
	ctx := context.Background()
	client, err := pgque.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	q := fmt.Sprintf("page_live_%d", time.Now().UnixNano())
	c, w := "consumer", "live-worker"
	if _, err = client.Pool().Exec(ctx, "select pgque.create_queue($1)", q); err != nil {
		t.Fatal(err)
	}
	defer client.Pool().Exec(ctx, "select pgque.drop_queue($1,true)", q)
	if _, err = client.Subscribe(ctx, q, c); err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 2)
	for i := range ids {
		ids[i], err = client.Send(ctx, q, pgque.Event{Type: "page.live", Payload: map[string]int{"i": i}})
		if err != nil {
			t.Fatal(err)
		}
	}
	const maxID int64 = 9223372036854775807
	if _, err = client.Pool().Exec(ctx, `select pgque.event_retry_raw($1,$2,now() - interval '1 second',$3,
now(),0,'page.live','{"i":"max"}',null,null,null,null)`, q, c, maxID); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Pool().Exec(ctx, "select pgque.maint_retry_events()"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ForceNextTick(ctx, q); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Ticker(ctx, q); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Pool().Exec(ctx, `update pgque.tick set tick_event_seq = $1 where tick_id =
(select max(tick_id) from pgque.tick where tick_queue =
 (select queue_id from pgque.queue where queue_name = $2))`, maxID, q); err != nil {
		t.Fatal(err)
	}
	_, err = client.ProcessPage(ctx, q, c, w, 2, time.Minute, func(context.Context, pgque.Message) error {
		return fmt.Errorf("boom")
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("handler error not propagated: %v", err)
	}
	p, err := client.ReceivePage(ctx, q, c, w, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != "page" || len(p.Messages) != 2 || *p.IsLast || p.Messages[0].MsgID != ids[0] || p.Messages[1].MsgID != ids[1] {
		t.Fatalf("bad page: %#v", p)
	}
	until, err := client.RenewPage(ctx, *p.PageToken, w)
	if err != nil || until.Before(*p.LeaseUntil) {
		t.Fatalf("bad renewal: %v", err)
	}
	zero := int32(0)
	failures := []pgque.PageFailure{{MsgID: fmt.Sprint(ids[0]), RetryAfterSeconds: &zero}}
	a, err := client.AckPage(ctx, *p.PageToken, w, failures)
	if err != nil || a.Status != "acked" || a.BatchFinished {
		t.Fatalf("bad ack: %#v %v", a, err)
	}
	a, err = client.AckPage(ctx, *p.PageToken, w, failures)
	if err != nil || a.Status != "already_acked" || a.BatchFinished {
		t.Fatalf("bad replay: %#v %v", a, err)
	}
	var seen []int64
	r, err := client.ProcessPage(ctx, q, c, w, 2, time.Minute, func(_ context.Context, m pgque.Message) error { seen = append(seen, m.MsgID); return nil })
	if err != nil || r.ProcessedCount != 1 || r.BatchFinished == nil || !*r.BatchFinished || len(seen) != 1 || seen[0] != maxID {
		t.Fatalf("bad helper: %#v %v", r, err)
	}
	p, err = client.ReceivePage(ctx, q, c, w, 2, time.Minute)
	if err != nil || (p.Status != "idle" && p.Status != "advanced") || len(p.Messages) != 0 || p.PageToken != nil {
		t.Fatalf("bad idle: %#v %v", p, err)
	}
}
