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
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.temporal.io/sdk/activity"
	"google.golang.org/grpc/codes"
)

// LifecycleSteps is the lifecycle implementation the activities drive. The
// in-process orchestrator's ActorWorkflow satisfies it, so both orchestrators
// run the same steps against the same store, hold the same per-actor lease,
// and emit the same records and metrics.
//
// Every method is idempotent: it derives how far a previous attempt got from
// the persisted actor and fast-forwards, which is what lets Temporal retry one
// without a compensation.
type LifecycleSteps interface {
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	ResumeActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, bool, error)
	SuspendActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	PauseActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	RevertActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	DeleteActor(ctx context.Context, actorRef resources.ActorRef, anyState bool) (*ateapipb.Actor, error)
	CrashActor(ctx context.Context, actorRef resources.ActorRef, reason, sandboxClass string) error
}

// ActorActivities runs the lifecycle steps as activities. The steps hold the
// store, the atelet dialer, the scheduler, the object store and the
// instruments, so injecting the steps injects all of them and neither
// orchestrator can drift from the other's dependencies.
//
// Two of the steps are not idempotent across their own completion: a revert
// that committed SUSPENDED refuses to run again, and a delete that removed the
// row reports it missing. An attempt after the first exists only because an
// earlier one did not report, so on a retry those two read the record and
// answer for the attempt that was lost.
type ActorActivities struct {
	steps LifecycleSteps
}

// NewActorActivities builds the activity implementation over steps.
func NewActorActivities(steps LifecycleSteps) *ActorActivities {
	return &ActorActivities{steps: steps}
}

// activities exists for its method names. It is a nil pointer on purpose: the
// SDK reads the name off a method value without calling it, so the workflow
// names its activities with the compiler's help rather than with strings.
var activities *ActorActivities

// retrying reports whether this attempt follows one that may have committed
// and never reported back.
func retrying(ctx context.Context) bool {
	return activity.GetInfo(ctx).Attempt > 1
}

// LoadState reads the lifecycle state the entity mirrors. The record is the
// authority on what an actor is doing, so the entity reads it rather than
// trusting the state a caller handed it.
func (a *ActorActivities) LoadState(ctx context.Context, in ActorInput) (*ateapipb.ActorStatus, error) {
	actor, err := a.steps.GetActor(ctx, in.Actor.Ref())
	if err != nil {
		return nil, lifecycleError(err)
	}
	return actor.GetStatus(), nil
}

// Resume puts the actor back on a worker and restores its snapshot.
//
// The step answers an actor it finds RUNNING from the record, with resumed
// false. On a retry that answer is this activity's own work reported late: the
// entity only sends a Resume for an actor that was not running, so the attempt
// that did not report is the one that resumed it.
func (a *ActorActivities) Resume(ctx context.Context, in ActorInput) (*ateapipb.ResumeActorResponse, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.ResumeActorResponse, error) {
		actor, resumed, err := a.steps.ResumeActor(ctx, in.Actor.Ref())
		if err != nil {
			return nil, lifecycleError(err)
		}
		if !resumed && retrying(ctx) &&
			actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			resumed = true
		}
		return &ateapipb.ResumeActorResponse{Actor: actor, Resumed: resumed}, nil
	})
}

// Suspend checkpoints the actor to object storage and frees its worker.
func (a *ActorActivities) Suspend(ctx context.Context, in ActorInput) (*ateapipb.SuspendActorResponse, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.SuspendActorResponse, error) {
		actor, err := a.steps.SuspendActor(ctx, in.Actor.Ref())
		if err != nil {
			return nil, lifecycleError(err)
		}
		return &ateapipb.SuspendActorResponse{Actor: actor}, nil
	})
}

// Pause checkpoints the actor to its node and frees its worker.
func (a *ActorActivities) Pause(ctx context.Context, in ActorInput) (*ateapipb.PauseActorResponse, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.PauseActorResponse, error) {
		actor, err := a.steps.PauseActor(ctx, in.Actor.Ref())
		if err != nil {
			return nil, lifecycleError(err)
		}
		return &ateapipb.PauseActorResponse{Actor: actor}, nil
	})
}

// Revert discards the actor's current execution and returns it to SUSPENDED,
// leaving the durable snapshot a later resume restores from untouched.
//
// The step refuses a SUSPENDED actor, because reporting success on one would
// hide that there was nothing to recover. On a retry that refusal is read
// against the record: SUSPENDED with no worker and no half-written snapshot is
// what a finished revert leaves, and the attempt that did not report is the
// one that finished it.
func (a *ActorActivities) Revert(ctx context.Context, in ActorInput) (*ateapipb.RevertActorResponse, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.RevertActorResponse, error) {
		actor, err := a.steps.RevertActor(ctx, in.Actor.Ref())
		if err != nil && retrying(ctx) && lifecycleCode(err) == codes.FailedPrecondition {
			if reverted, ok := a.revertedBefore(ctx, in.Actor); ok {
				return &ateapipb.RevertActorResponse{Actor: reverted}, nil
			}
		}
		if err != nil {
			return nil, lifecycleError(err)
		}
		return &ateapipb.RevertActorResponse{Actor: actor}, nil
	})
}

// revertedBefore reads the record and reports whether it is what a finished
// revert leaves behind.
func (a *ActorActivities) revertedBefore(ctx context.Context, key ActorKey) (*ateapipb.Actor, bool) {
	actor, err := a.steps.GetActor(ctx, key.Ref())
	if err != nil {
		return nil, false
	}
	st := actor.GetStatus()
	done := st.GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED &&
		st.GetWorkerAssignment() == nil &&
		st.GetInProgressSnapshotUri() == "" &&
		st.GetInProgressLocalSnapshotName() == ""
	return actor, done
}

// Delete tears the actor down and removes its record.
//
// The step reports a missing record as NOT_FOUND. On a retry the record this
// entity opened with is missing because the attempt that did not report
// removed it, so the answer is the success that attempt owed, with the name as
// the only thing left to answer with.
func (a *ActorActivities) Delete(ctx context.Context, in DeleteInput) (*ateapipb.Actor, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.Actor, error) {
		actor, err := a.steps.DeleteActor(ctx, in.Actor.Ref(), in.AnyState)
		if err != nil && retrying(ctx) && lifecycleCode(err) == codes.NotFound {
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: in.Actor.Atespace, Name: in.Actor.Name},
			}, nil
		}
		if err != nil {
			return nil, lifecycleError(err)
		}
		return actor, nil
	})
}

// Crash commits the transition an actor takes when the sandbox under it is
// gone. The entity runs it before recovering the actor, so a reader and the
// crash counter see the crash the same way they do when a step observes one.
func (a *ActorActivities) Crash(ctx context.Context, in CrashInput) error {
	return lifecycleError(a.steps.CrashActor(ctx, in.Actor.Ref(), in.Reason, in.SandboxClass))
}

// RecoveryFailed writes the actor event for a recovery the entity gave up on.
// The record is read for the actor's identity: the event is the one place a
// crash nobody is going to revert is attributed to one actor, and metric
// labels may not carry that.
func (a *ActorActivities) RecoveryFailed(ctx context.Context, in RecoveryFailedInput) error {
	actor, err := a.steps.GetActor(ctx, in.Actor.Ref())
	if err != nil {
		return lifecycleError(err)
	}
	attrs := ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor))
	attrs = append(attrs,
		slog.String(string(ateattr.ActorOperationNameKey), ateattr.NormalizeOperationName(in.Operation)),
		slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateValue(actor.GetStatus().GetState())))
	actorevent.Log(ctx, actorevent.RecoveryFailed, attrs)
	return nil
}

// heartbeating runs fn while reporting progress.
//
// A resume restores a snapshot onto a node and a suspend uploads one, either of
// which outlasts any sensible silence budget. The heartbeat is what separates a
// step that is still working from an ateapi pod that went away, and it is how a
// cancellation reaches fn at all: the service delivers it on the heartbeat
// response.
//
// fn runs on its own goroutine so the heartbeat keeps ticking, and shares the
// activity's context, so a cancelled activity cancels the step's store and
// atelet calls. The wait for fn is unconditional: the step holds the actor's
// lease and is mid-transition, and returning before it reports would leave the
// entity guessing what the record says.
func heartbeating[T any](ctx context.Context, every time.Duration, fn func(context.Context) (T, error)) (T, error) {
	var (
		out T
		err error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		out, err = fn(ctx)
	}()

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return out, err
		case <-ticker.C:
			activity.RecordHeartbeat(ctx)
		}
	}
}
