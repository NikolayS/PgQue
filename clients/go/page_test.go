// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPageFailureUsesSQLContractNames(t *testing.T) {
	delay := int32(7)
	reason := "later"
	b, err := json.Marshal([]PageFailure{{MsgID: "9223372036854775807", RetryAfterSeconds: &delay, Reason: &reason}})
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"msg_id":"9223372036854775807","retry_after_seconds":7,"reason":"later"}]`
	if string(b) != want {
		t.Fatalf("got %s want %s", b, want)
	}
}

func TestPageLeaseIntervalPreservesDuration(t *testing.T) {
	got := durationInterval(1250 * time.Millisecond)
	if !got.Valid || got.Microseconds != 1250000 {
		t.Fatalf("unexpected interval: %#v", got)
	}
}
