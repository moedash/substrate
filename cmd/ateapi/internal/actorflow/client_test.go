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
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// blockedHandle stands in for a worker-loss report queued behind a long step:
// its outcome never arrives while the caller is waiting.
type blockedHandle struct{}

func (blockedHandle) WorkflowID() string { return "actor/team-a/actor-1" }
func (blockedHandle) RunID() string      { return "" }
func (blockedHandle) UpdateID() string   { return UpdateWorkerLost + "/worker-7" }

func (blockedHandle) Get(ctx context.Context, _ any) error {
	<-ctx.Done()
	return client.NewWorkflowUpdateServiceTimeoutOrCanceledError(ctx.Err())
}

var _ client.WorkflowUpdateHandle = blockedHandle{}

// TestWorkerLostWaitIsBounded covers the sweep not being held for the whole
// RPC cap by a busy actor: past the bound the caller gets DEADLINE_EXCEEDED and
// requeues, while the report it left behind runs when the guard frees.
func TestWorkerLostWaitIsBounded(t *testing.T) {
	r := NewRouter(nil, "", false)
	r.workerLostWait = 10 * time.Millisecond

	err := r.awaitCommit(context.Background(), blockedHandle{})

	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("code = %v, want %v (err %v)", got, codes.DeadlineExceeded, err)
	}
}

// TestOperationInputCarriesTheCallersDeadline covers what the entity skips a
// queued operation on: the deadline of the RPC that asked for it, or nothing
// when the RPC set none.
func TestOperationInputCarriesTheCallersDeadline(t *testing.T) {
	if got := operationInput(context.Background()); !got.Deadline.IsZero() {
		t.Errorf("deadline without one on the context = %v, want zero", got.Deadline)
	}

	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if got := operationInput(ctx); !got.Deadline.Equal(deadline) {
		t.Errorf("deadline = %v, want %v", got.Deadline, deadline)
	}
}

// TestRetryIfOwnerCompleting covers the one NOT_FOUND an update-with-start can
// get, an owner completing under the request, being turned into the ABORTED a
// caller retries on. Everything else is left for StatusFromError.
func TestRetryIfOwnerCompleting(t *testing.T) {
	completing := fmt.Errorf("failed workflow update: %w",
		serviceerror.NewNotFound("workflow execution already completed"))
	if got := status.Code(retryIfOwnerCompleting(completing)); got != codes.Aborted {
		t.Errorf("code = %v, want %v", got, codes.Aborted)
	}

	other := serviceerror.NewUnavailable("connection refused")
	if got := retryIfOwnerCompleting(other); !errors.Is(got, other) {
		t.Errorf("retryIfOwnerCompleting(%v) = %v, want the error unchanged", other, got)
	}
	if got := retryIfOwnerCompleting(nil); got != nil {
		t.Errorf("retryIfOwnerCompleting(nil) = %v, want nil", got)
	}
}
