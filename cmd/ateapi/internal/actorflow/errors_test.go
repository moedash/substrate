// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actorflow

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestLifecycleCodeReadsBothErrorShapes covers the two shapes a lifecycle step
// fails in: a status error it built, and a store sentinel it passed through.
// The legacy RPC handlers map both, so a caller has to see the same code
// whichever orchestrator ran the step.
func TestLifecycleCodeReadsBothErrorShapes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"no error", nil, codes.OK},
		{"a status error", status.Error(codes.NotFound, "Actor team-a/actor-1 not found"), codes.NotFound},
		{"a wrapped status error", fmt.Errorf("while fetching actor: %w", status.Error(codes.FailedPrecondition, "no")), codes.FailedPrecondition},
		{"a missing record", store.ErrNotFound, codes.NotFound},
		{"a wrapped missing record", fmt.Errorf("while fetching actor: %w", store.ErrNotFound), codes.NotFound},
		{"a version conflict", store.ErrVersionConflict, codes.Aborted},
		{"a uid conflict", store.ErrUIDConflict, codes.Aborted},
		{"lease contention", store.ErrLeaseConflict, codes.Aborted},
		{"a store precondition", store.ErrFailedPrecondition, codes.FailedPrecondition},
		{"anything else", errors.New("connection refused"), codes.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lifecycleCode(tt.err); got != tt.want {
				t.Errorf("lifecycleCode(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestLifecycleErrorRoundTrip covers the classification surviving the trip
// through Temporal: the code goes out on the error type and comes back as the
// status the RPC caller reads.
func TestLifecycleErrorRoundTrip(t *testing.T) {
	for _, code := range carriedCodes {
		t.Run(code.String(), func(t *testing.T) {
			in := status.Error(code, "something happened")
			out := lifecycleError(in)

			var appErr *temporal.ApplicationError
			if !errors.As(out, &appErr) {
				t.Fatalf("lifecycleError(%v) = %T, want an application error", in, out)
			}
			if got := status.Code(StatusFromError(out)); got != code {
				t.Errorf("round-tripped code = %v, want %v", got, code)
			}
			if got := status.Convert(StatusFromError(out)).Message(); got == "" {
				t.Error("round-tripped message is empty, want the step's message")
			}
		})
	}
}

// TestLifecycleErrorClassifiesPermanence covers what the retry policy acts on.
func TestLifecycleErrorClassifiesPermanence(t *testing.T) {
	for _, code := range carriedCodes {
		t.Run(code.String(), func(t *testing.T) {
			var appErr *temporal.ApplicationError
			if !errors.As(lifecycleError(status.Error(code, "x")), &appErr) {
				t.Fatal("want an application error")
			}
			wantPermanent := false
			for _, permanent := range permanentCodes {
				if permanent == code {
					wantPermanent = true
				}
			}
			if appErr.NonRetryable() != wantPermanent {
				t.Errorf("NonRetryable() = %v, want %v", appErr.NonRetryable(), wantPermanent)
			}
		})
	}
}

// TestLifecycleErrorPassesNilThrough keeps a successful step from being turned
// into a failure by the classifier.
func TestLifecycleErrorPassesNilThrough(t *testing.T) {
	if err := lifecycleError(nil); err != nil {
		t.Errorf("lifecycleError(nil) = %v, want nil", err)
	}
	if err := StatusFromError(nil); err != nil {
		t.Errorf("StatusFromError(nil) = %v, want nil", err)
	}
}

// TestStatusFromErrorLeavesUnclassifiedErrorsAlone covers an error this package
// cannot place: inventing a code for it would tell the caller something nobody
// checked.
func TestStatusFromErrorLeavesUnclassifiedErrorsAlone(t *testing.T) {
	in := errors.New("something nobody classified")
	if got := StatusFromError(in); got != in {
		t.Errorf("StatusFromError(%v) = %v, want the error unchanged", in, got)
	}

	// An application error from somewhere else keeps its own identity too.
	other := temporal.NewApplicationError("boom", "SomeoneElsesType")
	if got := StatusFromError(other); got != other {
		t.Errorf("StatusFromError(%v) = %v, want the error unchanged", other, got)
	}
}

// TestStatusFromErrorRecodesTemporalFailures covers every shape Temporal itself
// fails in. None of them says anything about the actor, so none is passed
// through: a caller acts on the code, and the ingress router retries
// UNAVAILABLE and ABORTED the way it does when the in-process orchestrator's
// ate-api is restarting.
func TestStatusFromErrorRecodesTemporalFailures(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("failed workflow update: %w", err) }
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "a step that hit StartToClose",
			err:  temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil),
			want: codes.DeadlineExceeded,
		},
		{
			name: "a step that stopped heartbeating",
			err:  temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil),
			want: codes.DeadlineExceeded,
		},
		{
			name: "a timeout wrapped the way an update reports it",
			err: fmt.Errorf("activity error: %w",
				temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)),
			want: codes.DeadlineExceeded,
		},
		{
			name: "a cancelled step",
			err:  temporal.NewCanceledError("shutting down"),
			want: codes.Canceled,
		},
		{
			name: "an update the caller stopped waiting on",
			err:  client.NewWorkflowUpdateServiceTimeoutOrCanceledError(context.DeadlineExceeded),
			want: codes.DeadlineExceeded,
		},
		{
			name: "a frontend that cannot be reached",
			err:  wrap(serviceerror.NewUnavailable("connection refused")),
			want: codes.Unavailable,
		},
		{
			name: "a request Temporal ran out of time on",
			err:  serviceerror.NewDeadlineExceeded("context deadline exceeded"),
			want: codes.Unavailable,
		},
		{
			name: "a code Temporal has no type for",
			err:  status.Error(codes.Unauthenticated, "bad API key"),
			want: codes.Internal,
		},
		{
			name: "a persistence failure",
			err:  serviceerror.NewInternal("shard closed"),
			want: codes.Unavailable,
		},
		{
			name: "an owner completing under an update-with-start",
			err:  wrap(serviceerror.NewMultiOperationAborted("Operation was aborted.")),
			want: codes.Aborted,
		},
		{
			name: "an owner that converted its completion into a retry",
			err:  wrap(serviceerror.NewAborted("workflow execution already completed")),
			want: codes.Aborted,
		},
		{
			name: "an owner rolling its history over",
			err:  wrap(serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW, "closing")),
			want: codes.Aborted,
		},
		{
			name: "a Temporal NOT_FOUND is not a missing actor",
			err:  serviceerror.NewNotFound("workflow not found"),
			want: codes.Internal,
		},
		{
			name: "a namespace that does not exist",
			err:  serviceerror.NewNamespaceNotFound("nope"),
			want: codes.Internal,
		},
		{
			name: "a request Temporal refused",
			err:  serviceerror.NewInvalidArgument("bad request"),
			want: codes.Internal,
		},
		{
			name: "a credential Temporal rejected",
			err:  serviceerror.NewPermissionDenied("denied", ""),
			want: codes.Internal,
		},
		{
			name: "a bare context deadline",
			err:  fmt.Errorf("while dialing: %w", context.DeadlineExceeded),
			want: codes.DeadlineExceeded,
		},
		{
			name: "a bare context cancellation",
			err:  context.Canceled,
			want: codes.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StatusFromError(tt.err)
			if code := status.Code(got); code != tt.want {
				t.Errorf("code = %v, want %v (err %v)", code, tt.want, got)
			}
			if status.Convert(got).Message() == "" {
				t.Error("message is empty, want Temporal's reason kept")
			}
		})
	}
}

// TestStatusFromErrorNeverPassesATemporalCodeThrough walks every gRPC code
// Temporal can answer with and checks the caller only ever sees the three codes
// the mapping owns. A code Temporal adds lands in INTERNAL, which is the safe
// side: a caller acts on nothing it was not told about.
//
// UNKNOWN is skipped: the SDK turns it into a plain error with no status, which
// is the unclassified shape the caller gets as it stands.
func TestStatusFromErrorNeverPassesATemporalCodeThrough(t *testing.T) {
	for code := codes.OK + 1; code <= codes.Unauthenticated; code++ {
		if code == codes.Unknown {
			continue
		}
		got := status.Code(StatusFromError(serviceerror.FromStatus(status.New(code, "from Temporal"))))
		switch got {
		case codes.Aborted, codes.Unavailable, codes.Internal:
		default:
			t.Errorf("Temporal %v reached the caller as %v, want it re-coded", code, got)
		}
	}
}

// TestLifecycleErrorKeepsACommittedCrash covers the one ABORTED that must not
// be retried: the step has itself moved the actor to CRASHED and says so. A
// retry would find CRASHED and refuse with FAILED_PRECONDITION, and the caller,
// which keys on ABORTED plus the word, would never learn what happened.
func TestLifecycleErrorKeepsACommittedCrash(t *testing.T) {
	in := status.Errorf(codes.Aborted, "actor %s crashed", testActor.Ref())
	out := lifecycleError(in)

	var appErr *temporal.ApplicationError
	if !errors.As(out, &appErr) {
		t.Fatalf("lifecycleError(%v) = %T, want an application error", in, out)
	}
	if !appErr.NonRetryable() {
		t.Error("NonRetryable() = false, want a committed crash left alone")
	}
	if got := status.Code(StatusFromError(out)); got != codes.Aborted {
		t.Errorf("round-tripped code = %v, want %v kept for the caller", got, codes.Aborted)
	}

	// The retryable ABORTED keeps its retry.
	conflict := lifecycleError(status.Error(codes.Aborted, "concurrent update conflict, please retry"))
	if !errors.As(conflict, &appErr) || appErr.NonRetryable() {
		t.Errorf("a version conflict = %v, want it left retryable", conflict)
	}
}

// TestRejectionErrorReachesTheCallerAsFailedPrecondition covers the code a
// refused update has to arrive as, which is the one the legacy handlers return
// for an illegal transition.
func TestRejectionErrorReachesTheCallerAsFailedPrecondition(t *testing.T) {
	err := rejectionError(rejection(UpdateRevert, 0))
	if got := status.Code(StatusFromError(err)); got != codes.FailedPrecondition {
		t.Errorf("code = %v, want %v", got, codes.FailedPrecondition)
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Errorf("rejection = %v, want a permanent application error", err)
	}
}

// TestLifecycleErrorLeavesACancellationAlone covers a step that was cancelled
// rather than one that failed. Classifying it would report a shutting-down
// step as one that can never succeed, and the SDK reads the cancellation off
// the error itself.
func TestLifecycleErrorLeavesACancellationAlone(t *testing.T) {
	in := fmt.Errorf("while dialing atelet: %w", context.Canceled)
	out := lifecycleError(in)

	if out != in {
		t.Fatalf("lifecycleError(%v) = %v, want the error unchanged", in, out)
	}
	if !errors.Is(out, context.Canceled) {
		t.Error("the cancellation did not survive, so the SDK cannot see it")
	}
	var appErr *temporal.ApplicationError
	if errors.As(out, &appErr) {
		t.Errorf("classified as %v, want a cancellation left as it is", appErr)
	}
}
