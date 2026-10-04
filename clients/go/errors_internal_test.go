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

func TestWrapReceiveError_Overflow(t *testing.T) {
	tests := []struct {
		op      string
		ceiling int
		message string
	}{
		{"receive", 10, "pgque.receive: batch exceeds max_return of 10"},
		{"receive coop", 25, "pgque.receive_coop: batch exceeds max_return of 25"},
	}

	for _, tc := range tests {
		t.Run(tc.op, func(t *testing.T) {
			pgErr := &pgconn.PgError{Code: "54000", Message: tc.message, Hint: "recover the complete batch"}
			err := wrapReceiveError(tc.op, tc.ceiling, pgErr)

			if !errors.Is(err, ErrReceiveOverflow) {
				t.Fatalf("expected ErrReceiveOverflow, got %v", err)
			}
			var overflow *ReceiveOverflowError
			if !errors.As(err, &overflow) {
				t.Fatalf("expected *ReceiveOverflowError, got %T: %v", err, err)
			}
			if overflow.Op != tc.op || overflow.Ceiling != tc.ceiling || overflow.SQLSTATE != "54000" || overflow.Hint != pgErr.Hint {
				t.Fatalf("overflow metadata = %+v", overflow)
			}
			var sqlErr *SQLError
			if !errors.As(err, &sqlErr) {
				t.Fatalf("overflow must retain SQLError compatibility: %v", err)
			}
			if sqlErr.Op != tc.op || sqlErr.SQLSTATE != "54000" {
				t.Fatalf("SQLError metadata = %+v", sqlErr)
			}
			var underlying *pgconn.PgError
			if !errors.As(err, &underlying) {
				t.Fatalf("overflow must retain *pgconn.PgError compatibility: %v", err)
			}
			if underlying != pgErr {
				t.Fatalf("underlying PgError identity changed: got %p, want %p", underlying, pgErr)
			}
		})
	}
}

func TestWrapReceiveError_DoesNotOverclassify(t *testing.T) {
	tests := []struct {
		name    string
		op      string
		ceiling int
		code    string
		message string
	}{
		{"other 54000", "receive", 10, "54000", "statement is too complex"},
		{"wrong message", "receive", 10, "54000", "pgque.receive: batch exceeds max_return of ten"},
		{"wrong operation", "receive", 10, "54000", "pgque.receive_coop: batch exceeds max_return of 10"},
		{"wrong ceiling", "receive", 10, "54000", "pgque.receive: batch exceeds max_return of 11"},
		{"wrong code", "receive", 10, "P0001", "pgque.receive: batch exceeds max_return of 10"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := wrapReceiveError(tc.op, tc.ceiling, &pgconn.PgError{Code: tc.code, Message: tc.message})
			if errors.Is(err, ErrReceiveOverflow) {
				t.Fatalf("unexpected overflow classification: %v", err)
			}
			var overflow *ReceiveOverflowError
			if errors.As(err, &overflow) {
				t.Fatalf("unexpected *ReceiveOverflowError: %+v", overflow)
			}
			var sqlErr *SQLError
			if !errors.As(err, &sqlErr) {
				t.Fatalf("expected ordinary *SQLError, got %T: %v", err, err)
			}
		})
	}
}

func TestReceiveOverflowError_ZeroValueDoesNotPanic(t *testing.T) {
	err := (&ReceiveOverflowError{}).Error()
	if err == "" {
		t.Fatal("zero-value ReceiveOverflowError returned an empty message")
	}
}

// TestClassifyPgMessage_AllFragments ensures every message fragment we
// match against sql/pgque.sql maps to the expected sentinel. Locks the
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
