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
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
)

// entityChangeID and entityVersion1 pin the shape of the entity's decision
// sequence. The hook is taken before the first branch, so a later change to the
// order the entity loads, waits and recovers in can be introduced without
// breaking the executions already running on the current order.
const entityChangeID = "actor-entity"

const entityVersion1 workflow.Version = 1

// ActorEntityWorkflow is the lifecycle of one actor.
//
// It runs for as long as the actor exists: every lifecycle operation arrives as
// an update, worker loss arrives as an update too, and the execution completes
// only once the actor is deleted. Serializing the operations in one execution
// is what replaces the optimistic version checks the in-process orchestrator
// retries, and having an owner is what lets an actor whose worker went away
// recover without an operator.
//
// The workflow itself only decides. Everything that reads or writes the store,
// dials atelet or talks to the scheduler happens in an activity.
func ActorEntityWorkflow(ctx workflow.Context, in ActorEntityInput) error {
	_ = workflow.GetVersion(ctx, entityChangeID, entityVersion1, entityVersion1)

	known := in.State != ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED
	e := &entity{
		in:     in,
		state:  in.State,
		loaded: known,
		opened: known,
	}
	if err := e.register(ctx); err != nil {
		return err
	}
	return e.run(ctx)
}

// entity is one actor's mirrored lifecycle state, and the guard that keeps two
// operations off it at once.
type entity struct {
	in ActorEntityInput

	// state is what the entity last saw the record say. It is a guard, not the
	// truth: the steps read the row under the actor's lease and refuse what the
	// row refuses.
	state ateapipb.ActorState

	// loaded reports whether state came from a read.
	loaded bool

	// loading is held by the one read in flight, so an operation that arrives
	// while the entity is reading the record waits for that read rather than
	// starting a second one.
	loading bool

	// opened reports that the read the entity begins with has settled, one way
	// or the other. Operations wait for it, because that read is patient and
	// turning a caller away for an outage the entity is already waiting out
	// would throw away the one thing that knows it will pass.
	opened bool

	// busy is held by the one operation in flight. An entity with an owner is
	// the point of the pattern: a second operation waits rather than racing the
	// first to the store and losing on a version check.
	busy bool

	// gone ends the execution: the record the entity owned is not there any
	// more, because a delete removed it or because there never was one.
	gone bool

	// recovery reports a crash that has been committed with the revert that
	// follows it still to run. The entity owes that revert to an actor nobody
	// is waiting for, so it is finished before the history rolls over.
	recovery bool

	// recoveryFailed reports a recovery that gave up, which leaves the actor
	// CRASHED for an operator to revert.
	recoveryFailed bool
}

// register installs the handlers before anything blocks, so an update that
// arrives on the entity's first workflow task has somewhere to land.
//
// The validators only refuse an entity whose record is gone. The transition
// table is checked inside the handler, under the guard and against a record
// that has been read: a validator cannot block, so it could only check the
// mirror, and the mirror is stale whenever another writer moved the record.
func (e *entity) register(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, QueryState, e.snapshot); err != nil {
		return err
	}

	handlers := []struct {
		name    string
		handler any
	}{
		{UpdateResume, func(ctx workflow.Context, in OperationInput) (*ateapipb.ResumeActorResponse, error) {
			return operate[*ateapipb.ResumeActorResponse](
				ctx, e, UpdateResume, activities.Resume, resumeTimeout, in)
		}},
		{UpdateSuspend, func(ctx workflow.Context, in OperationInput) (*ateapipb.SuspendActorResponse, error) {
			return operate[*ateapipb.SuspendActorResponse](
				ctx, e, UpdateSuspend, activities.Suspend, suspendTimeout, in)
		}},
		{UpdatePause, func(ctx workflow.Context, in OperationInput) (*ateapipb.PauseActorResponse, error) {
			return operate[*ateapipb.PauseActorResponse](
				ctx, e, UpdatePause, activities.Pause, pauseTimeout, in)
		}},
		{UpdateRevert, func(ctx workflow.Context, in OperationInput) (*ateapipb.RevertActorResponse, error) {
			return operate[*ateapipb.RevertActorResponse](
				ctx, e, UpdateRevert, activities.Revert, revertTimeout, in)
		}},
	}
	for _, h := range handlers {
		opts := workflow.UpdateHandlerOptions{
			Validator: func(workflow.Context, OperationInput) error { return e.validateRecordExists() },
		}
		if err := workflow.SetUpdateHandlerWithOptions(ctx, h.name, h.handler, opts); err != nil {
			return err
		}
	}

	// Worker loss is accepted from any state, because what it means for the
	// actor is read off the record rather than decided by the caller.
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateWorkerLost, e.workerLost,
		workflow.UpdateHandlerOptions{
			Validator: func(workflow.Context, WorkerLostInput) error { return e.validateRecordExists() },
		}); err != nil {
		return err
	}

	// Delete ends the execution and takes the request's any_state, so it does
	// not fit the table above.
	return workflow.SetUpdateHandlerWithOptions(ctx, UpdateDelete, e.delete,
		workflow.UpdateHandlerOptions{
			Validator: func(workflow.Context, DeleteOperationInput) error { return e.validateRecordExists() },
		})
}

// run drives the entity until the record it owns is gone.
//
// It reports an error only when the execution is cancelled. An entity that
// failed for any other reason would leave its actor with no owner, and whatever
// asked for one has already been told it has it.
func (e *entity) run(ctx workflow.Context) error {
	owned, err := e.openingLoad(ctx)
	if err != nil {
		return e.shutDown(ctx, err)
	}
	if !owned {
		// No record to own. Whatever asked for this entity is answered by the
		// handler's own read, which reports the actor missing.
		workflow.GetLogger(ctx).Info("No actor record to own", "actor", e.in.Actor.String())
		e.gone = true
	}

	for !e.gone {
		err := workflow.Await(ctx, func() bool {
			return e.gone || e.recovery || e.readyToContinue(ctx)
		})
		if err != nil {
			return e.shutDown(ctx, err)
		}
		if e.gone {
			break
		}

		// The revert owed to a crashed actor is finished before the history is
		// rolled over, so a recovery is never dropped on the floor.
		if e.recovery {
			e.finishRecovery(ctx)
			continue
		}
		return workflow.NewContinueAsNewError(ctx, ActorEntityWorkflow, e.carry())
	}

	// A handler can still be finishing a step that outlived the update's
	// caller; completing here would abandon it mid-transition.
	return workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
}

// openingLoad reads the record the entity is going to own, and keeps trying.
//
// It reports whether there is a record at all. A missing one is an answer, not
// a failure: the actor was deleted, or never existed, and there is nothing to
// own either way. Anything else is the store being unreachable, which is worth
// waiting out: giving up would leave the actor ownerless, and the caller that
// started this entity has no way to know it has to start another.
func (e *entity) openingLoad(ctx workflow.Context) (bool, error) {
	log := workflow.GetLogger(ctx)
	for attempt := 1; ; attempt++ {
		err := e.load(ctx)
		if err == nil {
			e.opened = true
			return true, nil
		}
		if codeOf(err) == codes.NotFound {
			e.opened = true
			return false, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		wait := openingLoadBackoff(attempt)
		log.Error("Could not read the actor this entity owns, retrying",
			"actor", e.in.Actor.String(), "attempt", attempt, "retryIn", wait.String(), "error", err)
		if err := workflow.Sleep(ctx, wait); err != nil {
			return false, err
		}
	}
}

// load reads the state the entity is going to police. The record is the
// authority on what an actor is doing, so a fresh entity reads it instead of
// trusting the state whoever started it believed.
func (e *entity) load(ctx workflow.Context) error {
	if err := workflow.Await(ctx, func() bool { return !e.loading }); err != nil {
		return err
	}
	if e.loaded {
		return nil
	}
	e.loading = true
	defer func() { e.loading = false }()

	actCtx := workflow.WithActivityOptions(ctx, requestOptions(loadStateTimeout, false))
	var status *ateapipb.ActorStatus
	err := workflow.ExecuteActivity(actCtx, activities.LoadState, ActorInput{Actor: e.in.Actor}).
		Get(actCtx, &status)
	if err != nil {
		return err
	}
	e.state = status.GetState()
	e.loaded = true
	return nil
}

// readyToContinue reports whether the history can be rolled over. Only between
// operations: rolling over while a step is in flight would abandon it.
func (e *entity) readyToContinue(ctx workflow.Context) bool {
	return workflow.GetInfo(ctx).GetContinueAsNewSuggested() &&
		!e.busy && !e.recovery && workflow.AllHandlersFinished(ctx)
}

// carry is the input the next execution picks up from. A recovery is never
// carried: the loop finishes one before it rolls the history over.
func (e *entity) carry() ActorEntityInput {
	in := e.in
	in.State = e.state
	return in
}

// validateRecordExists refuses anything addressed to an entity whose record is
// gone. Reported as a missing actor, which is what a caller reading the store
// directly would get.
func (e *entity) validateRecordExists() error {
	if e.gone {
		return missingActorError("Actor " + e.in.Actor.String() + " not found")
	}
	return nil
}

// begin takes the entity for one operation, and makes sure the state the
// operation is about to be checked against came from a read.
//
// It waits for the read the entity opens with, which does not give up, so an
// operation that arrives during a store outage waits it out with the entity
// rather than being turned away. After that the read is on the operation's own
// budget: a step that failed may have committed a transition before it did, so
// what it left behind in the mirror is worth nothing.
//
// An operation whose caller's deadline passed while it was queued is skipped.
// Its caller has been told nothing happened, and running it now would make
// that answer wrong minutes later.
func (e *entity) begin(ctx workflow.Context, op string, deadline time.Time) error {
	if err := workflow.Await(ctx, func() bool { return e.opened && !e.busy }); err != nil {
		return err
	}
	e.busy = true
	if e.gone {
		e.busy = false
		return missingActorError("Actor " + e.in.Actor.String() + " not found")
	}
	if !deadline.IsZero() && !workflow.Now(ctx).Before(deadline) {
		e.busy = false
		workflow.GetLogger(ctx).Info("Skipping an operation whose caller stopped waiting for it",
			"actor", e.in.Actor.String(), "operation", op, "deadline", deadline)
		return expiredCallerError(fmt.Sprintf(
			"%s of actor %s was still queued when its caller's deadline passed", op, e.in.Actor))
	}
	if err := e.load(ctx); err != nil {
		e.busy = false
		return e.failed(err)
	}
	return nil
}

func (e *entity) end() {
	e.busy = false
}

// admit refuses an operation the record cannot take.
//
// The mirror is checked first, because it is free. A refusal it would give is
// confirmed against a fresh read before it is issued: another writer can move
// the record without this entity seeing it, and turning a legal request away
// off a stale mirror would be the entity's mistake, not the caller's. A
// refusal is the rare path, so the extra read costs nothing that matters.
func (e *entity) admit(ctx workflow.Context, op string) error {
	if accepts(op, e.state) {
		return nil
	}
	e.stale()
	if err := e.load(ctx); err != nil {
		return e.failed(err)
	}
	if accepts(op, e.state) {
		return nil
	}
	return rejectionError(rejection(op, e.state))
}

// failed is the bookkeeping a failed step or read owes the entity. The mirror
// is dropped either way: the step may have committed a transition before it
// failed. A record that is not there ends the execution, whatever removed it:
// there is nothing left to own, and an entity kept alive over a missing record
// would answer every later request with the same NOT_FOUND until its history
// grew large enough to roll over.
func (e *entity) failed(err error) error {
	e.stale()
	if codeOf(err) == codes.NotFound {
		e.gone = true
	}
	return err
}

// stepResult is what a lifecycle activity answers with: the actor as it stands
// after the step, in the shape of the matching RPC response.
type stepResult interface {
	GetActor() *ateapipb.Actor
}

// operate runs one lifecycle step, one at a time, and records what the step
// reported committing.
func operate[T stepResult](
	ctx workflow.Context, e *entity, op string, step any, timeout time.Duration, in OperationInput,
) (T, error) {
	var out T
	if err := e.begin(ctx, op, in.Deadline); err != nil {
		return out, err
	}
	defer e.end()

	if err := e.admit(ctx, op); err != nil {
		return out, err
	}

	actCtx := workflow.WithActivityOptions(ctx, requestOptions(timeout, true))
	err := workflow.ExecuteActivity(actCtx, step, ActorInput{Actor: e.in.Actor}).Get(actCtx, &out)
	if err != nil {
		return out, e.failed(err)
	}
	e.observe(out.GetActor())
	return out, nil
}

// delete tears the actor down and ends the execution.
func (e *entity) delete(ctx workflow.Context, in DeleteOperationInput) (*ateapipb.Actor, error) {
	if err := e.begin(ctx, UpdateDelete, in.Deadline); err != nil {
		return nil, err
	}
	defer e.end()

	// any_state is the request's way of saying the state machine does not
	// apply, and the step honors it.
	if !in.AnyState {
		if err := e.admit(ctx, UpdateDelete); err != nil {
			return nil, err
		}
	}

	actCtx := workflow.WithActivityOptions(ctx, requestOptions(deleteTimeout, true))
	activityIn := DeleteInput{Actor: e.in.Actor, AnyState: in.AnyState}
	var deleted *ateapipb.Actor
	err := workflow.ExecuteActivity(actCtx, activities.Delete, activityIn).Get(actCtx, &deleted)
	if err != nil {
		return nil, e.failed(err)
	}
	// Only now: the record is gone, so there is nothing left for this
	// execution to own.
	e.gone = true
	return deleted, nil
}

// workerLost commits the crash an actor takes when the worker under it is
// gone, and returns once the record says so.
//
// The caller is the sweep that is about to delete the Worker record, and once
// it has there is nothing left pointing at the actor for anything to retry
// from. So the crash is committed here, on the caller's own retry budget, and a
// failure is reported so the sweep stops and a retry re-drives it. The revert
// that brings the actor back is left to the entity, which nobody is waiting on.
//
// A record that already says CRASHED is recovery owed, not a report to ignore.
// The sweep only reports an actor it found bound to the worker, so the crash
// it finds was committed a moment ago by the step that was running on that
// worker and saw it drain: same cause, different writer, and the worker is
// gone in either case.
func (e *entity) workerLost(ctx workflow.Context, in WorkerLostInput) error {
	if err := e.begin(ctx, UpdateWorkerLost, time.Time{}); err != nil {
		return err
	}
	defer e.end()

	log := workflow.GetLogger(ctx)
	if e.state == ateapipb.ActorState_ACTOR_STATE_CRASHED {
		log.Info("Recovering an actor already crashed by the step its worker's loss interrupted",
			"actor", e.in.Actor.String(), "worker", in.Worker)
		e.recoveryFailed = false
		e.recovery = true
		return nil
	}
	if !lostWithWorker(e.state) {
		log.Info("Ignoring worker loss: the actor was not running on it",
			"actor", e.in.Actor.String(), "worker", in.Worker, "state", e.state.String())
		return nil
	}

	log.Info("Crashing an actor whose worker went away",
		"actor", e.in.Actor.String(), "worker", in.Worker, "state", e.state.String())

	actCtx := workflow.WithActivityOptions(ctx, requestOptions(crashTimeout, false))
	crash := CrashInput{
		Actor:        e.in.Actor,
		Reason:       ateattr.ReasonWorkerPodGone,
		SandboxClass: in.SandboxClass,
	}
	if err := workflow.ExecuteActivity(actCtx, activities.Crash, crash).Get(actCtx, nil); err != nil {
		return e.failed(err)
	}
	e.state = ateapipb.ActorState_ACTOR_STATE_CRASHED
	e.recoveryFailed = false
	e.recovery = true
	return nil
}

// finishRecovery brings a crashed actor back to a state it can be resumed from,
// with no operator in the loop. The crash is already committed, so a reader,
// the crash record and the crash counter have all seen it.
//
// Giving up leaves the actor CRASHED, which is where the in-process
// orchestrator leaves one too. Holding the entity while a step that cannot
// succeed is retried would be worse: every operation queues behind it, and an
// operator could not even delete the actor.
func (e *entity) finishRecovery(ctx workflow.Context) {
	log := workflow.GetLogger(ctx)
	if err := e.begin(ctx, "Recovery", time.Time{}); err != nil {
		return
	}
	defer e.end()
	defer func() { e.recovery = false }()

	revertCtx := workflow.WithActivityOptions(ctx, recoveryOptions(revertTimeout, true))
	var reverted *ateapipb.RevertActorResponse
	err := workflow.ExecuteActivity(revertCtx, activities.Revert, ActorInput{Actor: e.in.Actor}).
		Get(revertCtx, &reverted)
	if err != nil {
		log.Error("Gave up reverting a crashed actor; it stays crashed for an operator",
			"actor", e.in.Actor.String(), "error", err)
		e.giveUp(ctx, ateattr.OperationRevert, err)
		return
	}
	e.observe(reverted.GetActor())

	if !e.in.AutoResumeAfterCrash {
		return
	}
	resumeCtx := workflow.WithActivityOptions(ctx, recoveryOptions(resumeTimeout, true))
	var resumed *ateapipb.ResumeActorResponse
	err = workflow.ExecuteActivity(resumeCtx, activities.Resume, ActorInput{Actor: e.in.Actor}).
		Get(resumeCtx, &resumed)
	if err != nil {
		log.Error("Gave up resuming a recovered actor; it stays suspended and resumable",
			"actor", e.in.Actor.String(), "error", err)
		e.giveUp(ctx, ateattr.OperationResume, err)
		return
	}
	e.observe(resumed.GetActor())
}

// giveUp records a recovery step the entity stopped retrying. The State query
// reports it, but nothing in the control plane reads that, so an actor event
// is written too: it is the one record that names the actor an operator now
// has to move by hand.
func (e *entity) giveUp(ctx workflow.Context, operation string, cause error) {
	e.recoveryFailed = true
	e.failed(cause)
	if e.gone {
		return
	}
	actCtx := workflow.WithActivityOptions(ctx, requestOptions(recoveryFailedTimeout, false))
	in := RecoveryFailedInput{Actor: e.in.Actor, Operation: operation}
	if err := workflow.ExecuteActivity(actCtx, activities.RecoveryFailed, in).Get(actCtx, nil); err != nil {
		workflow.GetLogger(ctx).Error("Could not record that a recovery gave up",
			"actor", e.in.Actor.String(), "operation", operation, "error", err)
	}
}

// shutDown is what a cancelled entity does on its way out: it waits for the
// step in flight, and reports what it leaves behind.
//
// A cancellation lands while a step may be mid-transition. The record is left
// as it stands, transitional or not: cancelling an entity is an operator's
// act, and only the step that entered a transitional state knows how to finish
// it. Crashing the actor here would free a worker whose sandbox may still be
// restoring, which is worse than an honest record an operator has to move.
func (e *entity) shutDown(ctx workflow.Context, cause error) error {
	disconnected, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()

	if err := workflow.Await(disconnected, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return cause
	}

	log := workflow.GetLogger(ctx)
	// Reads the record only when the mirror was dropped, which is what a step
	// that failed does on its way out. A mirror that still holds came from a
	// step that reported, and re-reading it would say the same thing.
	if err := e.load(disconnected); err != nil {
		log.Error("Could not read an actor left behind by a cancelled entity",
			"actor", e.in.Actor.String(), "error", err)
		return cause
	}
	if transitional(e.state) {
		log.Warn("A cancelled entity left its actor mid-transition; an operator has to move it",
			"actor", e.in.Actor.String(), "state", e.state.String())
	}
	return cause
}

// snapshot answers the State query.
func (e *entity) snapshot() (StateSnapshot, error) {
	return StateSnapshot{
		Actor:           e.in.Actor,
		State:           e.state,
		StateName:       e.state.String(),
		Busy:            e.busy,
		Loaded:          e.loaded,
		RecoveryPending: e.recovery,
		RecoveryFailed:  e.recoveryFailed,
	}, nil
}

// observe records the state the step reported committing.
func (e *entity) observe(actor *ateapipb.Actor) {
	if actor.GetStatus() == nil {
		e.stale()
		return
	}
	e.state = actor.GetStatus().GetState()
	e.loaded = true
}

// stale drops the mirror, so the next operation reads the record instead of
// acting on what an interrupted one left behind.
func (e *entity) stale() {
	e.state = ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED
	e.loaded = false
}
