// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
package pgque_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pgque "github.com/NikolayS/pgque-go"
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
	firstToken := *p.PageToken
	var seen []int64
	r, err := client.ProcessPage(ctx, q, c, w, 2, time.Minute, func(_ context.Context, m pgque.Message) error {
		seen = append(seen, m.MsgID)
		return nil
	})
	if err != nil || r.ProcessedCount != 2 || r.BatchFinished == nil || *r.BatchFinished || len(seen) != 2 || seen[0] != ids[0] || seen[1] != ids[1] {
		t.Fatalf("bad helper: %#v %v", r, err)
	}
	a, err := client.AckPage(ctx, firstToken, w, nil)
	if err != nil || a.Status != "already_acked" || a.BatchFinished {
		t.Fatalf("bad replay: %#v %v", a, err)
	}
	p, err = client.ReceivePage(ctx, q, c, w, 2, time.Minute)
	if err != nil || len(p.Messages) != 1 || p.Messages[0].MsgID != maxID {
		t.Fatalf("bad max-int8 page: %#v %v", p, err)
	}
	failures := []pgque.PageFailure{{MsgID: fmt.Sprint(maxID), RetryAfterSeconds: &zero}}
	a, err = client.AckPage(ctx, *p.PageToken, w, failures)
	if err != nil || a.Status != "acked" || !a.BatchFinished {
		t.Fatalf("bad max-int8 failure ack: %#v %v", a, err)
	}
	var retryID int64
	err = client.Pool().QueryRow(ctx, `select ev_id from pgque.retry_queue where ev_queue =
(select queue_id from pgque.queue where queue_name = $1)`, q).Scan(&retryID)
	if err != nil || retryID != maxID {
		t.Fatalf("max-int8 failure not routed: %d %v", retryID, err)
	}
	p, err = client.ReceivePage(ctx, q, c, w, 2, time.Minute)
	if err != nil || (p.Status != "idle" && p.Status != "advanced") || len(p.Messages) != 0 || p.PageToken != nil {
		t.Fatalf("bad idle: %#v %v", p, err)
	}
}

func TestPageModeAdapters(t *testing.T) {
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
	queue := fmt.Sprintf("page_modes_%d", time.Now().UnixNano())
	coop, member, coopWorker := queue+"_group", "member-one", "coop-process"
	partitioned, partitionWorker := queue+"_partitioned", "partition-process"
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := client.Pool().Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("select pgque.create_queue($1)", queue)
	defer func() {
		if _, err := client.Pool().Exec(ctx, "select pgque.drop_queue($1,true)", queue); err != nil {
			t.Errorf("mode queue cleanup: %v", err)
		}
	}()
	exec("select pgque.register_subconsumer($1,$2,$3)", queue, coop, member)
	exec("select pgque.subscribe_partitioned($1,$2,3)", queue, partitioned)
	var epoch int64
	if err := client.Pool().QueryRow(ctx,
		"select pgque.claim_slot($1,$2,2,$3,interval '3 minutes')",
		queue, partitioned, partitionWorker).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	keys := make(map[int]string)
	for _, slot := range []int{0, 2} {
		var key string
		if err := client.Pool().QueryRow(ctx, `select 'adapter-key-' || i
from generate_series(1,1000) as i
where (hashtextextended('adapter-key-' || i,0) % 3 + 3) % 3 = $1
order by i limit 1`, slot).Scan(&key); err != nil {
			t.Fatal(err)
		}
		keys[slot] = key
	}
	payloads := []string{"target-first", "other-slot", "target-last"}
	eventKeys := []string{keys[2], keys[0], keys[2]}
	ids := make([]int64, len(payloads))
	for i, payload := range payloads {
		if err := client.Pool().QueryRow(ctx,
			"select pgque.send($1,'adapter.mode',$2::text,$3::text)",
			queue, payload, eventKeys[i]).Scan(&ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	exec("select pgque.force_next_tick($1)", queue)
	exec("select pgque.ticker($1)", queue)
	checkPage := func(page pgque.Page, indexes []int) {
		t.Helper()
		if page.Status != "page" || page.PageToken == nil || *page.PageToken == "" ||
			page.BatchID == nil || page.PageNumber == nil || *page.PageNumber != 1 ||
			page.IsLast == nil || !*page.IsLast || page.LeaseUntil == nil || len(page.Messages) != len(indexes) {
			t.Fatalf("bad mode page: %#v", page)
		}
		for i, index := range indexes {
			m := page.Messages[i]
			if m.MsgID != ids[index] || m.BatchID != *page.BatchID || m.Type != "adapter.mode" ||
				m.Payload != payloads[index] || m.Extra1 == nil || *m.Extra1 != eventKeys[index] {
				t.Fatalf("wrong mode message %d: %#v", i, m)
			}
		}
	}
	checkAck := func(page pgque.Page, worker string) {
		t.Helper()
		_, err := client.AckPage(ctx, *page.PageToken, "not-the-owner", nil)
		var sqlErr *pgque.SQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLSTATE != "PQP01" {
			t.Fatalf("wrong-owner ack error: %v", err)
		}
		for _, status := range []string{"acked", "already_acked"} {
			ack, err := client.AckPage(ctx, *page.PageToken, worker, nil)
			if err != nil || ack.Status != status || !ack.BatchFinished {
				t.Fatalf("mode ack/replay: %#v %v", ack, err)
			}
		}
	}
	page, err := client.ReceivePageCoop(ctx, queue, coop, member, coopWorker, 3, nil, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	checkPage(page, []int{0, 1, 2})
	if page.FenceEpoch != nil {
		t.Fatalf("cooperative page has partition epoch: %v", *page.FenceEpoch)
	}
	checkAck(page, coopWorker)
	deadInterval := 5 * time.Minute
	empty, err := client.ReceivePageCoop(ctx, queue, coop, member, coopWorker, 3, &deadInterval, 3*time.Minute)
	if err != nil || (empty.Status != "idle" && empty.Status != "advanced") || len(empty.Messages) != 0 || empty.PageToken != nil {
		t.Fatalf("bad cooperative empty response: %#v %v", empty, err)
	}
	page, err = client.ReceivePagePartitioned(ctx, queue, partitioned, 2, 3, partitionWorker, 2)
	if err != nil {
		t.Fatal(err)
	}
	checkPage(page, []int{0, 2})
	if page.FenceEpoch == nil || *page.FenceEpoch != epoch {
		t.Fatalf("partition epoch not decoded: %#v, expected %d", page.FenceEpoch, epoch)
	}
	checkAck(page, partitionWorker)
	empty, err = client.ReceivePagePartitioned(ctx, queue, partitioned, 2, 3, partitionWorker, 2)
	if err != nil || (empty.Status != "idle" && empty.Status != "advanced") || len(empty.Messages) != 0 || empty.PageToken != nil {
		t.Fatalf("bad partition empty response: %#v %v", empty, err)
	}
}
