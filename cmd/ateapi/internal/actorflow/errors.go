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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
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

// lifecycleError classifies err for Temporal. A permanent outcome fails the
// activity for good, so a caller waiting on the update gets the answer on the
// first attempt instead of after the retry budget.
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
	if slices.Contains(permanentCodes, code) {
		return temporal.NewNonRetryableApplicationError(err.Error(), errType, nil)
	}
	return temporal.NewApplicationError(err.Error(), errType)
}

// rejectionError is what an update validator refuses with. A rejection never
// reaches an activity, so it is classified here rather than by the step.
func rejectionError(message string) error {
	return temporal.NewNonRetryableApplicationError(message, errorTypePrefix+codes.FailedPrecondition.String(), nil)
}

// missingActorError refuses a request addressed to an actor whose record is
// gone, with the code a caller reading the store directly would get.
func missingActorError(message string) error {
	return temporal.NewNonRetryableApplicationError(message, errorTypePrefix+codes.NotFound.String(), nil)
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
// An error this package did not classify is returned as it stands: the gRPC
// layer reports it the way it reports any other unclassified failure, and
// inventing a code for it would tell the caller something nobody checked.
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
	return err
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
