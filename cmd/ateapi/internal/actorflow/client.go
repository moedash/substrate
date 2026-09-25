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
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Router hands a lifecycle operation to the workflow that owns the actor. It is
// what the RPC handlers dispatch through when the Temporal orchestrator is
// selected, and it answers in the same shapes the handlers return.
type Router struct {
	client               client.Client
	taskQueue            string
	autoResumeAfterCrash bool

	// workerLostWait bounds the wait for a worker-loss report's commit. A
	// field, so a test can shorten it.
	workerLostWait time.Duration
}

// NewRouter builds a router over an established Temporal client.
func NewRouter(c client.Client, taskQueue string, autoResumeAfterCrash bool) *Router {
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}
	return &Router{
		client:               c,
		taskQueue:            taskQueue,
		autoResumeAfterCrash: autoResumeAfterCrash,
		workerLostWait:       workerLostWait,
	}
}

// Resume asks the actor's owner to put it back on a worker.
func (r *Router) Resume(
	ctx context.Context, actorRef resources.ActorRef,
) (*ateapipb.ResumeActorResponse, error) {
	return update[*ateapipb.ResumeActorResponse](ctx, r, actorRef, UpdateResume, operationInput(ctx))
}

// Suspend asks the actor's owner to checkpoint it and free its worker.
func (r *Router) Suspend(
	ctx context.Context, actorRef resources.ActorRef,
) (*ateapipb.SuspendActorResponse, error) {
	return update[*ateapipb.SuspendActorResponse](ctx, r, actorRef, UpdateSuspend, operationInput(ctx))
}

// Pause asks the actor's owner to checkpoint it to its node.
func (r *Router) Pause(
	ctx context.Context, actorRef resources.ActorRef,
) (*ateapipb.PauseActorResponse, error) {
	return update[*ateapipb.PauseActorResponse](ctx, r, actorRef, UpdatePause, operationInput(ctx))
}

// Revert asks the actor's owner to discard its current execution.
func (r *Router) Revert(
	ctx context.Context, actorRef resources.ActorRef,
) (*ateapipb.RevertActorResponse, error) {
	return update[*ateapipb.RevertActorResponse](ctx, r, actorRef, UpdateRevert, operationInput(ctx))
}

// Delete asks the actor's owner to tear it down. The owner completes with it.
func (r *Router) Delete(
	ctx context.Context, actorRef resources.ActorRef, anyState bool,
) (*ateapipb.Actor, error) {
	in := DeleteOperationInput{OperationInput: operationInput(ctx), AnyState: anyState}
	return update[*ateapipb.Actor](ctx, r, actorRef, UpdateDelete, in)
}

// WorkerLost tells the actor's owner that the worker it was on is gone, and
// returns once the crash has been committed to the store.
//
// An update rather than a signal, and waited on rather than fired: the caller
// is about to delete the Worker record, and after that nothing points at the
// actor for anything to retry from. Started with the update, so an actor whose
// entity is not running is still crashed and recovered. The revert that follows
// is the entity's own work and this does not wait for it.
//
// The wait for the commit is bounded, because the report queues behind
// whatever the entity is doing and a long step would otherwise hold the sweep
// for the whole RPC cap. Past the bound the caller gets DEADLINE_EXCEEDED and
// requeues; the report stays queued, and the retry finds it again by its ID.
func (r *Router) WorkerLost(
	ctx context.Context, actorRef resources.ActorRef, worker, sandboxClass string,
) error {
	in := WorkerLostInput{Worker: worker, SandboxClass: sandboxClass}
	opts := client.UpdateWorkflowOptions{
		UpdateName: UpdateWorkerLost,
		UpdateID:   UpdateWorkerLost + "/" + worker,
		Args:       []any{in},
		// Admission is enough to hand the wait over to a bounded context; the
		// commit is waited for below.
		WaitForStage: client.WorkflowUpdateStageAccepted,
	}
	handle, err := r.send(ctx, ActorKeyFromRef(actorRef), opts)
	if err != nil {
		return StatusFromError(err)
	}
	return r.awaitCommit(ctx, handle)
}

// awaitCommit waits for a worker-loss report's outcome for at most
// workerLostWait. The handler answers with an error alone, so there is nothing
// to decode; the wait is the point.
func (r *Router) awaitCommit(ctx context.Context, handle client.WorkflowUpdateHandle) error {
	waitCtx, cancel := context.WithTimeout(ctx, r.workerLostWait)
	defer cancel()
	return StatusFromError(handle.Get(waitCtx, nil))
}

// State reads what the actor's owner believes the record says. Nothing in the
// control plane reads it: it is there for an operator holding an actor's name
// and wondering what owns it.
func (r *Router) State(ctx context.Context, actorRef resources.ActorRef) (StateSnapshot, error) {
	var snapshot StateSnapshot
	key := ActorKeyFromRef(actorRef)
	encoded, err := r.client.QueryWorkflow(ctx, key.WorkflowID(), "", QueryState)
	if err != nil {
		return snapshot, StatusFromError(err)
	}
	if err := encoded.Get(&snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

// operationInput carries the caller's deadline to the entity, so an operation
// the caller has given up on is not run for nobody.
func operationInput(ctx context.Context) OperationInput {
	if deadline, ok := ctx.Deadline(); ok {
		return OperationInput{Deadline: deadline}
	}
	return OperationInput{}
}

// update sends op to the actor's entity and decodes what it answered with.
func update[T any](ctx context.Context, r *Router, actorRef resources.ActorRef, op string, arg any) (T, error) {
	var out T
	opts := client.UpdateWorkflowOptions{
		UpdateName: op,
		Args:       []any{arg},
		// The caller is an RPC waiting on the outcome, so acceptance is not
		// enough: it needs the record the step committed.
		WaitForStage: client.WorkflowUpdateStageCompleted,
	}
	handle, err := r.send(ctx, ActorKeyFromRef(actorRef), opts)
	if err != nil {
		return out, StatusFromError(err)
	}
	if err := handle.Get(ctx, &out); err != nil {
		return out, StatusFromError(err)
	}
	return out, nil
}

// send starts the actor's entity if it is not running and sends the update to
// it, returning once the update has reached the stage the options ask for.
//
// Update-with-start is one round trip for what is otherwise a start, a race to
// find out who won it, and then an update. The entity reads the record itself,
// so the input names the actor and nothing about its state: a caller that
// guessed would hand the entity a state the record had already left.
//
// The one NOT_FOUND Temporal can answer here is the owner completing under the
// request: the namespace has its own error type, and a workflow ID that does
// not exist is what the start is for. The next attempt lands on a fresh
// owner, so the caller is told to make one.
func (r *Router) send(
	ctx context.Context, key ActorKey, opts client.UpdateWorkflowOptions,
) (client.WorkflowUpdateHandle, error) {
	start := r.client.NewWithStartWorkflowOperation(r.startOptions(key), ActorEntityWorkflow, r.entityInput(key))
	handle, err := r.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions:          opts,
	})
	if err != nil {
		return nil, retryIfOwnerCompleting(err)
	}
	return handle, nil
}

// retryIfOwnerCompleting turns the NOT_FOUND an update-with-start gets from an
// owner that completed under it into the ABORTED a caller retries on. Any other
// failure is left for StatusFromError.
func retryIfOwnerCompleting(err error) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return status.Error(codes.Aborted, "the actor's owner was completing, retry: "+err.Error())
	}
	return err
}

func (r *Router) entityInput(key ActorKey) ActorEntityInput {
	return ActorEntityInput{Actor: key, AutoResumeAfterCrash: r.autoResumeAfterCrash}
}

// startOptions place the entity on the actor's business key.
func (r *Router) startOptions(key ActorKey) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:        key.WorkflowID(),
		TaskQueue: r.taskQueue,
		// The actor already has an owner in the common case, and a request for
		// it belongs on that owner rather than on a rival execution.
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		// An actor created again under a name that has been deleted gets a new
		// entity: the previous one completed with the record it owned, and
		// refusing to start would leave the new actor unmanaged.
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
}
