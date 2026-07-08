// pgque-go -- Go client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

package pgque

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// Sentinel errors. Use errors.Is to match.
//
// These mirror the typed exceptions exposed by the Python and TypeScript
// clients (PgqueQueueNotFound, PgqueConsumerNotFound, PgqueBatchNotFound,
// PgqueConnectionError) so application code can write equivalent
// recovery logic across the three drivers.
var (
	// ErrConnection is returned when the underlying PostgreSQL connection
	// cannot be established or has dropped.
	ErrConnection = errors.New("pgque: connection error")

	// ErrQueueNotFound is returned when a SQL call references a queue
	// that does not exist.
	ErrQueueNotFound = errors.New("pgque: queue not found")

	// ErrConsumerNotFound is returned when a SQL call references a
	// consumer that is not registered on the queue.
	ErrConsumerNotFound = errors.New("pgque: consumer not registered")

	// ErrBatchNotFound is returned when a SQL call references a batch ID
	// that does not exist or has already been finished.
	ErrBatchNotFound = errors.New("pgque: batch not found")
)

// SQLError wraps a PostgreSQL-side failure with the SQLSTATE code and the
// op label that produced it. It is returned for SQL errors that do not
// match any of the typed sentinels above. Use errors.As to extract.
type SQLError struct {
	// Op identifies the client-side operation that produced the error
	// (e.g. "send", "receive", "ack", "nack", "send batch", "connect").
	Op string

	// SQLSTATE is the 5-character PostgreSQL error code if the underlying
	// error is a *pgconn.PgError; otherwise empty.
	SQLSTATE string

	// Err is the underlying error (typically *pgconn.PgError).
	Err error
}

func (e *SQLError) Error() string {
	if e.SQLSTATE != "" {
		return fmt.Sprintf("pgque: %s: %s [SQLSTATE %s]", e.Op, e.Err, e.SQLSTATE)
	}
	return fmt.Sprintf("pgque: %s: %s", e.Op, e.Err)
}

func (e *SQLError) Unwrap() error { return e.Err }

// wrapSQLError maps a raw error from pgx into a typed pgque error.
//
// The caller passes the operation label (e.g. "send", "receive") and the
// underlying error. The returned error is suitable for errors.Is /
// errors.As: callers can match the typed sentinels above, the underlying
// pgconn.PgError, or context.Canceled / context.DeadlineExceeded.
//
// Mapping precedence:
//  1. nil → nil.
//  2. context.Canceled / context.DeadlineExceeded → returned as-is wrapped
//     with the op label so context-aware callers can still match.
//  3. *pgconn.PgError with a recognized message fragment → wrapped with
//     the matching sentinel and a *SQLError that carries SQLSTATE.
//  4. *pgconn.PgError without a recognized fragment → wrapped in
//     *SQLError with SQLSTATE.
//  5. Anything else (typically connection-level pgx failures) → wrapped
//     with ErrConnection so errors.Is(err, ErrConnection) matches.
func wrapSQLError(op string, err error) error {
	if err == nil {
		return nil
	}

	// Preserve context-cancellation and deadline-exceeded so callers can
	// still match them with errors.Is(err, context.Canceled) /
	// errors.Is(err, context.DeadlineExceeded).
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("pgque: %s: %w", op, err)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		sqlErr := &SQLError{Op: op, SQLSTATE: pgErr.Code, Err: err}
		if sentinel := classifyPgMessage(pgErr.Message); sentinel != nil {
			return fmt.Errorf("%w: %w", sentinel, sqlErr)
		}
		return sqlErr
	}

	// Non-PgError: typically a pgx connection-level failure (pool closed,
	// network drop, bad DSN). Tag as ErrConnection.
	return fmt.Errorf("pgque: %s: %w: %w", op, ErrConnection, err)
}

// wrapConnectError wraps a connect-time error so callers can match
// ErrConnection AND extract *SQLError if the underlying failure is a
// *pgconn.PgError (e.g. wrong password 28P01, missing database 3D000,
// pg_hba rejection 28000). Without this helper, wrapSQLError would expose
// SQLSTATE for *pgconn.PgError but skip the ErrConnection tag, while
// non-PgError failures would get ErrConnection but no SQLSTATE — the two
// kinds of connect failure would be inconsistent.
func wrapConnectError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		sqlErr := &SQLError{Op: "connect", SQLSTATE: pgErr.Code, Err: err}
		return fmt.Errorf("pgque: connect: %w: %w", ErrConnection, sqlErr)
	}
	return fmt.Errorf("pgque: connect: %w: %w", ErrConnection, err)
}

// classifyPgMessage maps the message text of a PgError to one of the typed
// sentinels, or nil if no fragment matches.
//
// PgQue raises these conditions via plpgsql `raise exception '...'` which
// produces SQLSTATE P0001 — the message text is the only signal, so the
// match is by recognizable substrings drawn from devel/sql/pgque.sql. Matching
// is case-insensitive.
func classifyPgMessage(msg string) error {
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(low, "queue not found"),
		strings.Contains(low, "no such queue"),
		strings.Contains(low, "no such event queue"),
		strings.Contains(low, "event queue not found"),
		strings.Contains(low, "event queue not created"):
		return ErrQueueNotFound
	case strings.Contains(low, "consumer not registered"),
		strings.Contains(low, "consumer not found"),
		strings.Contains(low, "not subscriber to queue"):
		return ErrConsumerNotFound
	case strings.Contains(low, "batch not found"),
		strings.Contains(low, "cannot find data for batch"):
		return ErrBatchNotFound
	}
	return nil
}
