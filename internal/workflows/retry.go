package workflows

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"syscall"
	"time"
)

// transientError marks an error as worth retrying.
type transientError struct{ err error }

func (e transientError) Error() string { return e.err.Error() }
func (e transientError) Unwrap() error { return e.err }

// Transient marks err as transient: the step may succeed if it is retried.
// Steps return this for failures they know to be temporary, such as a
// dependency answering 503.
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return transientError{err}
}

// IsRetryable reports whether a step error is worth another attempt: it was
// marked Transient, or it is a network timeout, a refused or reset
// connection, or a connection cut mid-response. Everything else, including
// validation and conflict errors, is permanent, so retrying it only repeats
// the failure. Cancellation is never retryable.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var transient transientError
	if errors.As(err, &transient) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, io.ErrUnexpectedEOF)
}

// Backoff is the delay schedule between attempts of a step: exponential from
// Base, doubling per attempt, capped at Max, with full jitter so that
// workers retrying the same dependency spread out.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// DefaultBackoff starts at 100ms and caps at 5s.
var DefaultBackoff = Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}

// Delay returns the wait before attempt n+1, after n failed attempts (n >= 1).
func (b Backoff) Delay(n int) time.Duration {
	ceiling := b.Base
	for i := 1; i < n && ceiling < b.Max; i++ {
		ceiling *= 2
	}
	if ceiling > b.Max {
		ceiling = b.Max
	}
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// WithBackoff sets the delay schedule between step attempts.
func WithBackoff(b Backoff) Option { return func(e *Engine) { e.backoff = b } }

// WithRetryClassifier replaces IsRetryable for deciding which step errors are
// retried.
func WithRetryClassifier(f func(error) bool) Option { return func(e *Engine) { e.retryable = f } }

// sleep waits for d or until ctx is done, and reports whether it waited the
// whole time.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
