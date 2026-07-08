// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

// White-box tests for the typed-error layer. Lives in package pgque so
// it can drive classifyPgMessage and wrapSQLError directly without a
// running PostgreSQL.

package pgque

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestClassifyPgMessage_AllFragments ensures every message fragment we
// match against devel/sql/pgque.sql maps to the expected sentinel. Locks the
// classifier against silent drift if a typo is introduced.
func TestClassifyPgMessage_AllFragments(t *testing.T) {
	cases := []struct {
		msg  string
		want error
	}{
		// ErrQueueNotFound fragments
		{"queue not found", ErrQueueNotFound},
		{"queue not found: orders", ErrQueueNotFound},
		{"Queue not found", ErrQueueNotFound},
		{"no such queue", ErrQueueNotFound},
		{"No such event queue", ErrQueueNotFound},
		{"Event queue not found", ErrQueueNotFound},
		{"Event queue not created yet", ErrQueueNotFound},

		// ErrConsumerNotFound fragments
		{"consumer not registered", ErrConsumerNotFound},
		{"consumer not found", ErrConsumerNotFound},
		{"Not subscriber to queue: orders/worker", ErrConsumerNotFound},

		// ErrBatchNotFound fragments
		{"batch not found", ErrBatchNotFound},
		{"Cannot find data for batch 42", ErrBatchNotFound},

		// No-match cases
		{"some unrelated error", nil},
		{"", nil},
	}

	for _, tc := range cases {
		got := classifyPgMessage(tc.msg)
		if got != tc.want {
			t.Errorf("classifyPgMessage(%q) = %v, want %v", tc.msg, got, tc.want)
		}
	}
}

// TestWrapSQLError_NoSentinelMatch covers the *pgconn.PgError-without-
// recognized-fragment branch: the result must be a plain *SQLError that
// errors.As can extract, with SQLSTATE preserved and no sentinel chain.
func TestWrapSQLError_NoSentinelMatch(t *testing.T) {
	pgErr := &pgconn.PgError{
		Code:    "42601", // syntax_error
		Message: "syntax error at or near \"frob\"",
	}
	wrapped := wrapSQLError("send", pgErr)

	var sqlErr *SQLError
	if !errors.As(wrapped, &sqlErr) {
		t.Fatalf("expected errors.As to extract *SQLError, got: %v", wrapped)
	}
	if sqlErr.SQLSTATE != "42601" {
		t.Errorf("expected SQLSTATE=42601, got %q", sqlErr.SQLSTATE)
	}
	if sqlErr.Op != "send" {
		t.Errorf("expected Op=send, got %q", sqlErr.Op)
	}
	for _, sentinel := range []error{ErrQueueNotFound, ErrConsumerNotFound, ErrBatchNotFound, ErrConnection} {
		if errors.Is(wrapped, sentinel) {
			t.Errorf("expected wrapped error to NOT match %v, but it did", sentinel)
		}
	}
}

// TestWrapSQLError_SentinelChain covers the recognized-fragment branch:
// the result must satisfy BOTH errors.Is(err, ErrXxx) AND
// errors.As(err, &sqlErr) — the dual-match guarantee documented on the
// wrapping helper.
func TestWrapSQLError_SentinelChain(t *testing.T) {
	pgErr := &pgconn.PgError{
		Code:    "P0001",
		Message: "queue not found: orders",
	}
	wrapped := wrapSQLError("send", pgErr)

	if !errors.Is(wrapped, ErrQueueNotFound) {
		t.Errorf("expected errors.Is(err, ErrQueueNotFound) to be true, got: %v", wrapped)
	}
	var sqlErr *SQLError
	if !errors.As(wrapped, &sqlErr) {
		t.Errorf("expected errors.As to also extract *SQLError, got: %v", wrapped)
	}
	if sqlErr != nil && sqlErr.SQLSTATE != "P0001" {
		t.Errorf("expected SQLSTATE=P0001, got %q", sqlErr.SQLSTATE)
	}
}

// TestWrapConnectError_PgError covers connect-time *pgconn.PgError such
// as 28P01 (wrong password) or 3D000 (missing database): the chain must
// match ErrConnection AND extract *SQLError with SQLSTATE.
func TestWrapConnectError_PgError(t *testing.T) {
	pgErr := &pgconn.PgError{
		Code:    "28P01",
		Message: "password authentication failed for user \"alice\"",
	}
	wrapped := wrapConnectError(pgErr)

	if !errors.Is(wrapped, ErrConnection) {
		t.Errorf("expected errors.Is(err, ErrConnection) to be true, got: %v", wrapped)
	}
	var sqlErr *SQLError
	if !errors.As(wrapped, &sqlErr) {
		t.Errorf("expected errors.As to extract *SQLError, got: %v", wrapped)
	}
	if sqlErr != nil {
		if sqlErr.SQLSTATE != "28P01" {
			t.Errorf("expected SQLSTATE=28P01, got %q", sqlErr.SQLSTATE)
		}
		if sqlErr.Op != "connect" {
			t.Errorf("expected Op=connect, got %q", sqlErr.Op)
		}
	}
}

// TestWrapConnectError_NonPgError covers connect-time non-PgError such
// as a parse error or network drop: the chain must match ErrConnection
// without exposing a *SQLError (no SQLSTATE to extract).
func TestWrapConnectError_NonPgError(t *testing.T) {
	wrapped := wrapConnectError(errors.New("dial tcp: connection refused"))

	if !errors.Is(wrapped, ErrConnection) {
		t.Errorf("expected errors.Is(err, ErrConnection) to be true, got: %v", wrapped)
	}
	var sqlErr *SQLError
	if errors.As(wrapped, &sqlErr) {
		t.Errorf("expected errors.As to NOT extract *SQLError for non-PgError, got: %+v", sqlErr)
	}
}
