/**
 * @file classifier_transport_test
 * @description The transport-failure half of the retryability table:
 * net/url error shapes, timeouts and the status error message.
 */
package retry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
)

// fakeNetErr is a bare net.Error that is not a *url.Error, isolating
// the net.Error branch of the table.
type fakeNetErr struct{ timedOut bool }

func (f fakeNetErr) Error() string   { return "fake network error" }
func (f fakeNetErr) Timeout() bool   { return f.timedOut }
func (f fakeNetErr) Temporary() bool { return false }

func TestStatusErrorMessage(t *testing.T) {
	t.Parallel()
	err := NewStatusError(429)
	if err.StatusCode != 429 {
		t.Fatalf("status = %d", err.StatusCode)
	}
	if got := err.Error(); got != "upstream exchange failed with status 429" {
		t.Fatalf("message = %q", got)
	}
}

func TestClassifierTransportErrors(t *testing.T) {
	t.Parallel()
	// A *url.Error reports Timeout through its wrapped net.Error.
	urlTimeout := &url.Error{Op: "Post", URL: "http://upstream", Err: fakeNetErr{timedOut: true}}
	wrapped := fmt.Errorf("attempt 2: %w", urlTimeout)
	// A completed (non-timeout) exchange error, the shape net/http wraps
	// around every transport failure. The classifier consults the url.Error
	// timeout, then the net.Error timeout — the same *url.Error matches
	// both — so only an actual timeout is retryable here.
	// NOTE: this pins current behavior, which diverges from the file's
	// own docstring and TECH-SPEC §6.5 ("connection failures are
	// retryable"); a classifier fix must update this pin deliberately.
	notimeout := &url.Error{Op: "Post", URL: "http://upstream", Err: errors.New("connection refused")}

	cases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"url error timeout", urlTimeout, true},
		{"wrapped url error timeout", wrapped, true},
		{"url error without timeout", notimeout, false},
		{"net error timeout", fakeNetErr{timedOut: true}, true},
		{"net error without timeout", fakeNetErr{timedOut: false}, false},
	}
	var c DefaultClassifier
	for _, tc := range cases {
		if got := c.Retryable(tc.err); got != tc.retryable {
			t.Errorf("%s: retryable = %v, want %v", tc.name, got, tc.retryable)
		}
	}
}

func TestClassifierDeadlineWrappedInURLError(t *testing.T) {
	t.Parallel()
	// A deadline that surfaced through the transport layer stays
	// retryable even before the url.Error branch is reached.
	err := &url.Error{Op: "Post", URL: "http://upstream", Err: context.DeadlineExceeded}
	var c DefaultClassifier
	if !c.Retryable(err) {
		t.Fatal("a deadline exceeded inside a url.Error must be retryable")
	}
	// And it must classify by unwrapping, not by concrete type.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("fixture sanity: the deadline should unwrap out")
	}
}
