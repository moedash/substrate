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
// did not classify: inventing a code for it would tell the caller something
// nobody checked.
func TestStatusFromErrorLeavesUnclassifiedErrorsAlone(t *testing.T) {
	in := errors.New("the workflow task timed out")
	if got := StatusFromError(in); got != in {
		t.Errorf("StatusFromError(%v) = %v, want the error unchanged", in, got)
	}

	// An application error from somewhere else keeps its own identity too.
	other := temporal.NewApplicationError("boom", "SomeoneElsesType")
	if got := StatusFromError(other); got != other {
		t.Errorf("StatusFromError(%v) = %v, want the error unchanged", other, got)
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
