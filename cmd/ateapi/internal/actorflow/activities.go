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
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.temporal.io/sdk/activity"
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
	CrashActor(ctx context.Context, actorRef resources.ActorRef, reason string) error
}

// ActorActivities runs the lifecycle steps as activities. The steps hold the
// store, the atelet dialer, the scheduler, the object store and the
// instruments, so injecting the steps injects all of them and neither
// orchestrator can drift from the other's dependencies.
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
func (a *ActorActivities) Resume(ctx context.Context, in ActorInput) (*ateapipb.ResumeActorResponse, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.ResumeActorResponse, error) {
		actor, resumed, err := a.steps.ResumeActor(ctx, in.Actor.Ref())
		if err != nil {
			return nil, lifecycleError(err)
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
func (a *ActorActivities) Revert(ctx context.Context, in ActorInput) (*ateapipb.RevertActorResponse, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.RevertActorResponse, error) {
		actor, err := a.steps.RevertActor(ctx, in.Actor.Ref())
		if err != nil {
			return nil, lifecycleError(err)
		}
		return &ateapipb.RevertActorResponse{Actor: actor}, nil
	})
}

// Delete tears the actor down and removes its record.
func (a *ActorActivities) Delete(ctx context.Context, in DeleteInput) (*ateapipb.Actor, error) {
	return heartbeating(ctx, heartbeatInterval, func(ctx context.Context) (*ateapipb.Actor, error) {
		actor, err := a.steps.DeleteActor(ctx, in.Actor.Ref(), in.AnyState)
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
	return lifecycleError(a.steps.CrashActor(ctx, in.Actor.Ref(), in.Reason))
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
