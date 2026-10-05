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
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorTypePrefix starts the type of every failure this package classifies.
// The type carries the gRPC code the failure has to reach the caller as, so a
// lifecycle error is read once, where the error itself is still in hand, and
// the RPC layer rebuilds the status from the type rather than re-deriving it
// from a message.
const errorTypePrefix = "Lifecycle"

// permanentCodes are the outcomes another attempt cannot change: the actor or
// its template is not there, the transition is not legal from the state the
// record is in, the request is malformed, or the actor has already crashed.
// Everything else, conflicts and lease contention included, is worth retrying.
var permanentCodes = []codes.Code{
	codes.NotFound,
	codes.FailedPrecondition,
	codes.InvalidArgument,
	codes.OutOfRange,
	codes.AlreadyExists,
	codes.PermissionDenied,
	codes.Unauthenticated,
	codes.Unimplemented,
	codes.DataLoss,
}

// carriedCodes are the codes a classified failure can carry. Kept as a list
// because codes.Code cannot be enumerated and a round trip through the error
// type has to agree in both directions.
var carriedCodes = append([]codes.Code{
	codes.Canceled,
	codes.Aborted,
	codes.Unavailable,
	codes.DeadlineExceeded,
	codes.ResourceExhausted,
	codes.Internal,
	codes.Unknown,
}, permanentCodes...)

// lifecycleCode is the gRPC code a lifecycle step's error means.
//
// A step returns either a status error it built itself or a store sentinel it
// passed through, and the legacy RPC handlers map both. Reading them the same
// way here keeps the code a caller sees the same on either orchestrator.
func lifecycleCode(err error) codes.Code {
	switch {
	case err == nil:
		return codes.OK
	case errors.Is(err, store.ErrNotFound):
		return codes.NotFound
	case errors.Is(err, store.ErrVersionConflict),
		errors.Is(err, store.ErrUIDConflict),
		errors.Is(err, store.ErrLeaseConflict):
		return codes.Aborted
	case errors.Is(err, store.ErrFailedPrecondition):
		return codes.FailedPrecondition
	}
	return status.Code(err)
}

// committedCrashSuffix ends the status a lifecycle step builds when it has
// itself moved the actor to CRASHED. The step reports that as ABORTED, which
// is otherwise the retryable code, and the callers that key on it (the
// benchmark client among them) read the code and this word together.
const committedCrashSuffix = " crashed"

// committedCrash reports whether err is a step saying it crashed the actor.
func committedCrash(err error) bool {
	return status.Code(err) == codes.Aborted &&
		strings.HasSuffix(status.Convert(err).Message(), committedCrashSuffix)
}

// lifecycleError classifies err for Temporal. A permanent outcome fails the
// activity for good, so a caller waiting on the update gets the answer on the
// first attempt instead of after the retry budget.
//
// A crash the step committed is permanent although it carries the retryable
// code: another attempt would find CRASHED and refuse with a different code,
// and the caller would never see the one that says what happened.
func lifecycleError(err error) error {
	if err == nil {
		return nil
	}
	// A step that was cancelled is not a failure to classify. The SDK reads
	// the cancellation off the error itself, and wrapping it in an application
	// error would report a shutting-down step as one that can never succeed.
	if errors.Is(err, context.Canceled) {
		return err
	}
	code := lifecycleCode(err)
	errType := errorTypePrefix + code.String()
	if slices.Contains(permanentCodes, code) || committedCrash(err) {
		return temporal.NewNonRetryableApplicationError(err.Error(), errType, nil)
	}
	return temporal.NewApplicationError(err.Error(), errType)
}

// rejectionError is what the entity refuses an illegal transition with. A
// rejection never reaches an activity, so it is classified here rather than by
// the step.
func rejectionError(message string) error {
	return entityError(codes.FailedPrecondition, message)
}

// missingActorError refuses a request addressed to an actor whose record is
// gone, with the code a caller reading the store directly would get.
func missingActorError(message string) error {
	return entityError(codes.NotFound, message)
}

// expiredCallerError is what an operation is skipped with when its caller
// stopped waiting before it could run. Nobody reads it, but the update has to
// end with something.
func expiredCallerError(message string) error {
	return entityError(codes.DeadlineExceeded, message)
}

// entityError is a permanent failure the entity built itself, carrying code the
// way a classified step failure does.
func entityError(code codes.Code, message string) error {
	return temporal.NewNonRetryableApplicationError(message, errorTypePrefix+code.String(), nil)
}

// codeOf reports the gRPC code a failed step carries, so workflow code can
// branch on the reason without unwrapping the failure itself. Unclassified
// failures read as Unknown, which is not a reason to stop trying.
func codeOf(err error) codes.Code {
	return status.Code(StatusFromError(err))
}

// StatusFromError maps an error a start, an update or a query returned back to
// the gRPC status the caller expects, so the two orchestrators fail the same
// way for the same reason.
//
// A failure a step classified comes back as the code it carried. A failure
// Temporal itself produced is re-coded so a caller can act on it: a step that
// ran out of its budget is DEADLINE_EXCEEDED, a frontend that is unreachable
// is UNAVAILABLE, and none of Temporal's own codes are passed through as if
// they said something about the actor. An error this package cannot place is
// returned as it stands: inventing a code for it would tell the caller
// something nobody checked.
func StatusFromError(err error) error {
	if err == nil {
		return nil
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		if code, ok := codeFromErrorType(appErr.Type()); ok {
			return status.Error(code, appErr.Message())
		}
	}
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return status.Error(codes.DeadlineExceeded, "the step ran out of time: "+timeoutErr.Error())
	}
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		return status.Error(codes.Canceled, "the step was cancelled: "+canceledErr.Error())
	}
	var updateErr *client.WorkflowUpdateServiceTimeoutOrCanceledError
	if errors.As(err, &updateErr) {
		return status.Error(codes.DeadlineExceeded,
			"the caller stopped waiting before the actor's owner answered: "+updateErr.Error())
	}
	if st, ok := temporalStatus(err); ok {
		return recodeTemporal(st)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	return err
}

// temporalStatus reads the status off a failure the Temporal service reported,
// in either of the shapes the SDK hands back: one of its own error types, or
// the gRPC status for a code it has no type for.
func temporalStatus(err error) (*status.Status, bool) {
	var svcErr serviceerror.ServiceError
	if errors.As(err, &svcErr) {
		return svcErr.Status(), true
	}
	var grpcErr interface{ GRPCStatus() *status.Status }
	if errors.As(err, &grpcErr) {
		return grpcErr.GRPCStatus(), true
	}
	return nil, false
}

// recodeTemporal re-codes a failure the Temporal service reported.
//
// Temporal's codes describe the service, not the actor, so none of them is
// passed through: a NOT_FOUND from Temporal is not a missing actor, and a
// caller told so would stop looking for one that exists. A transient failure
// is UNAVAILABLE, which is what a caller sees when the in-process orchestrator's
// ate-api is restarting, and the router retries it the same way. ABORTED is
// kept for the two answers that ask for an immediate retry: Temporal's own
// abort, and the busy signal an owner sends while it rolls its history over.
// Anything else is a misconfiguration or a bug and reads as INTERNAL. The
// message stays Temporal's, so the reason is not lost with the code.
func recodeTemporal(st *status.Status) error {
	switch st.Code() {
	case codes.Aborted, codes.ResourceExhausted:
		return status.Error(codes.Aborted, st.Message())
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled,
		codes.Internal, codes.Unknown, codes.DataLoss:
		return status.Error(codes.Unavailable, "Temporal is unavailable: "+st.Message())
	}
	return status.Error(codes.Internal, "Temporal refused the request: "+st.Message())
}

// codeFromErrorType reverses the type lifecycleError built.
func codeFromErrorType(errType string) (codes.Code, bool) {
	for _, code := range carriedCodes {
		if errType == errorTypePrefix+code.String() {
			return code, true
		}
	}
	return codes.Unknown, false
}
