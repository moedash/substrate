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
// an update, worker loss arrives as a signal, and the execution completes only
// once the actor is deleted. Serializing the operations in one execution is
// what replaces the optimistic version checks the in-process orchestrator
// retries, and having an owner is what lets an actor whose worker went away
// recover without an operator.
//
// The workflow itself only decides. Everything that reads or writes the store,
// dials atelet or talks to the scheduler happens in an activity.
func ActorEntityWorkflow(ctx workflow.Context, in ActorEntityInput) error {
	_ = workflow.GetVersion(ctx, entityChangeID, workflow.DefaultVersion, entityVersion1)

	known := in.State != ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED
	e := &entity{
		in:       in,
		state:    in.State,
		loaded:   known,
		opened:   known,
		recovery: in.RecoveryPending,
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
	// is waiting for, so it survives continue-as-new.
	recovery bool

	// recoveryFailed reports a recovery that gave up, which leaves the actor
	// CRASHED for an operator to revert.
	recoveryFailed bool
}

// register installs the handlers before anything blocks, so an update that
// arrives on the entity's first workflow task has somewhere to land.
func (e *entity) register(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, QueryState, e.snapshot); err != nil {
		return err
	}

	handlers := []struct {
		name    string
		handler any
	}{
		{UpdateResume, func(ctx workflow.Context) (*ateapipb.ResumeActorResponse, error) {
			return operate[*ateapipb.ResumeActorResponse](ctx, e, UpdateResume, activities.Resume, resumeTimeout)
		}},
		{UpdateSuspend, func(ctx workflow.Context) (*ateapipb.SuspendActorResponse, error) {
			return operate[*ateapipb.SuspendActorResponse](ctx, e, UpdateSuspend, activities.Suspend, suspendTimeout)
		}},
		{UpdatePause, func(ctx workflow.Context) (*ateapipb.PauseActorResponse, error) {
			return operate[*ateapipb.PauseActorResponse](ctx, e, UpdatePause, activities.Pause, pauseTimeout)
		}},
		{UpdateRevert, func(ctx workflow.Context) (*ateapipb.RevertActorResponse, error) {
			return operate[*ateapipb.RevertActorResponse](ctx, e, UpdateRevert, activities.Revert, revertTimeout)
		}},
	}
	for _, h := range handlers {
		opts := workflow.UpdateHandlerOptions{
			Validator: func(workflow.Context) error { return e.validate(h.name) },
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
			Validator: func(_ workflow.Context, anyState bool) error {
				// any_state is the request's way of saying the state machine
				// does not apply, and the step honors it.
				if anyState {
					return nil
				}
				return e.validate(UpdateDelete)
			},
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
		// rolled over, and carried across it when the rollover wins the race,
		// so a recovery is never dropped on the floor.
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
	if err := workflow.ExecuteActivity(actCtx, activities.LoadState, ActorInput{Actor: e.in.Actor}).Get(actCtx, &status); err != nil {
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

// carry is the input the next execution picks up from.
func (e *entity) carry() ActorEntityInput {
	in := e.in
	in.State = e.state
	in.RecoveryPending = e.recovery
	return in
}

// validate refuses an operation the state the entity last read cannot take. A
// validator is read-only and cannot block, so it checks the mirror and leaves
// the record to the step.
func (e *entity) validate(op string) error {
	if err := e.validateRecordExists(); err != nil {
		return err
	}
	if !accepts(op, e.state) {
		return rejectionError(rejection(op, e.state))
	}
	return nil
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
func (e *entity) begin(ctx workflow.Context) error {
	if err := workflow.Await(ctx, func() bool { return e.opened && !e.busy }); err != nil {
		return err
	}
	e.busy = true
	if e.gone {
		e.busy = false
		return missingActorError("Actor " + e.in.Actor.String() + " not found")
	}
	if err := e.load(ctx); err != nil {
		e.busy = false
		return err
	}
	return nil
}

func (e *entity) end() {
	e.busy = false
}

// stepResult is what a lifecycle activity answers with: the actor as it stands
// after the step, in the shape of the matching RPC response.
type stepResult interface {
	GetActor() *ateapipb.Actor
}

// operate runs one lifecycle step, one at a time, and records what the step
// reported committing.
func operate[T stepResult](ctx workflow.Context, e *entity, op string, step any, timeout time.Duration) (T, error) {
	var out T
	if err := e.begin(ctx); err != nil {
		return out, err
	}
	defer e.end()

	// Rechecked under the guard: the validator ran before whatever was in
	// flight finished, and that may have moved the actor somewhere this
	// operation is not accepted from.
	if !accepts(op, e.state) {
		return out, rejectionError(rejection(op, e.state))
	}

	actCtx := workflow.WithActivityOptions(ctx, requestOptions(timeout, true))
	if err := workflow.ExecuteActivity(actCtx, step, ActorInput{Actor: e.in.Actor}).Get(actCtx, &out); err != nil {
		// The step may have committed a transition before failing, so the
		// mirror is worthless either way.
		e.stale()
		return out, err
	}
	e.observe(out.GetActor())
	return out, nil
}

// delete tears the actor down and ends the execution.
func (e *entity) delete(ctx workflow.Context, anyState bool) (*ateapipb.Actor, error) {
	if err := e.begin(ctx); err != nil {
		return nil, err
	}
	defer e.end()

	if !anyState && !accepts(UpdateDelete, e.state) {
		return nil, rejectionError(rejection(UpdateDelete, e.state))
	}

	actCtx := workflow.WithActivityOptions(ctx, requestOptions(deleteTimeout, true))
	in := DeleteInput{Actor: e.in.Actor, AnyState: anyState}
	var deleted *ateapipb.Actor
	if err := workflow.ExecuteActivity(actCtx, activities.Delete, in).Get(actCtx, &deleted); err != nil {
		e.stale()
		return nil, err
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
func (e *entity) workerLost(ctx workflow.Context, in WorkerLostInput) error {
	if err := e.begin(ctx); err != nil {
		return err
	}
	defer e.end()

	log := workflow.GetLogger(ctx)
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
		e.stale()
		return err
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
	if err := e.begin(ctx); err != nil {
		return
	}
	defer e.end()
	defer func() { e.recovery = false }()

	revertCtx := workflow.WithActivityOptions(ctx, recoveryOptions(revertTimeout, true))
	var reverted *ateapipb.RevertActorResponse
	if err := workflow.ExecuteActivity(revertCtx, activities.Revert, ActorInput{Actor: e.in.Actor}).Get(revertCtx, &reverted); err != nil {
		log.Error("Gave up reverting a crashed actor; it stays crashed for an operator",
			"actor", e.in.Actor.String(), "error", err)
		e.recoveryFailed = true
		e.stale()
		return
	}
	e.observe(reverted.GetActor())

	if !e.in.AutoResumeAfterCrash {
		return
	}
	resumeCtx := workflow.WithActivityOptions(ctx, recoveryOptions(resumeTimeout, true))
	var resumed *ateapipb.ResumeActorResponse
	if err := workflow.ExecuteActivity(resumeCtx, activities.Resume, ActorInput{Actor: e.in.Actor}).Get(resumeCtx, &resumed); err != nil {
		log.Error("Gave up resuming a recovered actor; it stays suspended and resumable",
			"actor", e.in.Actor.String(), "error", err)
		e.recoveryFailed = true
		e.stale()
		return
	}
	e.observe(resumed.GetActor())
}

// shutDown is the one cleanup a cancelled entity owes the record.
//
// A cancellation lands while a step may be mid-transition, and nothing else
// finishes one: the actor would sit in RESUMING or SUSPENDING with no owner,
// and no operation is accepted from there. The crash is committed on a
// disconnected context, which outlives the cancellation, and on the bounded
// retry policy, because a cancelled entity still has to go away.
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
	if !transitional(e.state) {
		return cause
	}

	log.Info("Crashing an actor left mid-transition by a cancelled entity",
		"actor", e.in.Actor.String(), "state", e.state.String())

	actCtx := workflow.WithActivityOptions(disconnected, cleanupOptions(crashTimeout))
	crash := CrashInput{Actor: e.in.Actor, Reason: ateattr.ReasonUnknown}
	if err := workflow.ExecuteActivity(actCtx, activities.Crash, crash).Get(actCtx, nil); err != nil {
		log.Error("Could not crash an actor left mid-transition", "actor", e.in.Actor.String(), "error", err)
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
