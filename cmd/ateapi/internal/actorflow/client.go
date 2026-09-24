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

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Router hands a lifecycle operation to the workflow that owns the actor. It is
// what the RPC handlers dispatch through when the Temporal orchestrator is
// selected, and it answers in the same shapes the handlers return.
type Router struct {
	client               client.Client
	taskQueue            string
	autoResumeAfterCrash bool
}

// NewRouter builds a router over an established Temporal client.
func NewRouter(c client.Client, taskQueue string, autoResumeAfterCrash bool) *Router {
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}
	return &Router{client: c, taskQueue: taskQueue, autoResumeAfterCrash: autoResumeAfterCrash}
}

// Resume asks the actor's owner to put it back on a worker.
func (r *Router) Resume(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.ResumeActorResponse, error) {
	return update[*ateapipb.ResumeActorResponse](ctx, r, actorRef, UpdateResume)
}

// Suspend asks the actor's owner to checkpoint it and free its worker.
func (r *Router) Suspend(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.SuspendActorResponse, error) {
	return update[*ateapipb.SuspendActorResponse](ctx, r, actorRef, UpdateSuspend)
}

// Pause asks the actor's owner to checkpoint it to its node.
func (r *Router) Pause(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.PauseActorResponse, error) {
	return update[*ateapipb.PauseActorResponse](ctx, r, actorRef, UpdatePause)
}

// Revert asks the actor's owner to discard its current execution.
func (r *Router) Revert(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.RevertActorResponse, error) {
	return update[*ateapipb.RevertActorResponse](ctx, r, actorRef, UpdateRevert)
}

// Delete asks the actor's owner to tear it down. The owner completes with it.
func (r *Router) Delete(ctx context.Context, actorRef resources.ActorRef, anyState bool) (*ateapipb.Actor, error) {
	return update[*ateapipb.Actor](ctx, r, actorRef, UpdateDelete, anyState)
}

// WorkerLost tells the actor's owner that the worker it was on is gone, and
// returns as soon as the entity has the news: the recovery that follows has no
// caller to report to.
//
// Signal-with-start, so an actor whose entity is not running is still
// recovered. The record is what the entity acts on, and it outlives any
// execution that happened to be up when the pod went away.
func (r *Router) WorkerLost(ctx context.Context, actorRef resources.ActorRef, worker string) error {
	key := ActorKeyFromRef(actorRef)
	_, err := r.client.SignalWithStartWorkflow(ctx, key.WorkflowID(), SignalWorkerLost,
		WorkerLostInput{Worker: worker},
		r.startOptions(key), ActorEntityWorkflow, r.entityInput(key))
	return StatusFromError(err)
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

// update starts the actor's entity if it is not running, sends op to it, and
// waits for the result.
//
// Update-with-start is one round trip for what is otherwise a start, a race to
// find out who won it, and then an update. The entity reads the record itself,
// so the input names the actor and nothing about its state: a caller that
// guessed would hand the entity a state the record had already left.
func update[T any](ctx context.Context, r *Router, actorRef resources.ActorRef, op string, args ...any) (T, error) {
	var out T
	key := ActorKeyFromRef(actorRef)
	start := r.client.NewWithStartWorkflowOperation(r.startOptions(key), ActorEntityWorkflow, r.entityInput(key))
	handle, err := r.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName: op,
			Args:       args,
			// The caller is an RPC waiting on the outcome, so acceptance is not
			// enough: it needs the record the step committed.
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return out, StatusFromError(err)
	}
	if err := handle.Get(ctx, &out); err != nil {
		return out, StatusFromError(err)
	}
	return out, nil
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
