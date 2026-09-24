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

// Package actorflow orchestrates the actor lifecycle as a Temporal entity
// workflow: one long-lived workflow per actor, which serializes the lifecycle
// operations, and recovers an actor whose worker went away without an operator.
//
// It is the alternative to the in-process orchestrator in controlapi, selected
// by the --orchestrator flag. Both drive the same lifecycle steps against the
// same store, so an actor is readable and resumable whichever one moved it.
package actorflow

import (
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// The updates and query an actor entity workflow answers to. They are the
// contract between the RPC handlers and the workflow, so a rename has to happen
// in one place and reach both ends.
//
// Worker loss is an update rather than a signal because its caller is about to
// delete the Worker record and has nothing left to retry with: it needs to know
// the crash was committed, which only a reply can tell it.
const (
	UpdateResume     = "Resume"
	UpdateSuspend    = "Suspend"
	UpdatePause      = "Pause"
	UpdateRevert     = "Revert"
	UpdateDelete     = "Delete"
	UpdateWorkerLost = "WorkerLost"

	QueryState = "State"
)

// DefaultTaskQueue is the task queue ateapi both serves and dispatches on when
// nothing else is configured.
const DefaultTaskQueue = "ate-actors"

// ActorKey identifies the actor one entity workflow owns. It is the workflow's
// input and the argument of every activity, so it stays a pair of strings: a
// history a person has to read is worth more than a reused wire type.
type ActorKey struct {
	Atespace string `json:"atespace"`
	Name     string `json:"name"`
}

// ActorKeyFromRef converts a lifecycle reference to an entity key.
func ActorKeyFromRef(ref resources.ActorRef) ActorKey {
	return ActorKey{Atespace: ref.Atespace, Name: ref.Name}
}

// Ref converts the key back to the reference the lifecycle steps take.
func (k ActorKey) Ref() resources.ActorRef {
	return resources.ActorRef{Atespace: k.Atespace, Name: k.Name}
}

func (k ActorKey) String() string {
	return k.Atespace + "/" + k.Name
}

// WorkflowID is the business key of the actor's entity workflow. Deriving it
// from the atespace and name rather than from a UID is what makes the workflow
// findable by anyone holding the actor's identity, and what keeps a second
// caller's request on the same execution instead of starting a rival one.
func (k ActorKey) WorkflowID() string {
	return "actor/" + k.Atespace + "/" + k.Name
}

// ActorEntityInput is what an entity workflow starts from, and what it carries
// across continue-as-new.
type ActorEntityInput struct {
	Actor ActorKey `json:"actor"`

	// State is the lifecycle state the entity mirrors. A fresh execution leaves
	// it unspecified and reads the record; a continued execution carries what
	// the previous one knew.
	State ateapipb.ActorState `json:"state"`

	// AutoResumeAfterCrash lets the entity put a recovered actor back on a
	// worker instead of leaving it suspended. Off by default: a resume books
	// capacity, and an actor that crashed once may crash again, so the choice
	// stays with whoever runs the control plane.
	AutoResumeAfterCrash bool `json:"autoResumeAfterCrash"`

	// RecoveryPending carries an unfinished recovery across continue-as-new:
	// the crash is committed and the revert that follows it is not, and a
	// rolled-over history would otherwise forget that anything is owed.
	RecoveryPending bool `json:"recoveryPending"`
}

// ActorInput is the argument of an activity that acts on the whole actor.
type ActorInput struct {
	Actor ActorKey `json:"actor"`
}

// DeleteInput is the argument of the Delete update and its activity.
type DeleteInput struct {
	Actor ActorKey `json:"actor"`

	// AnyState deletes an actor that is not in a deletable state, the way the
	// DeleteActor request's any_state does.
	AnyState bool `json:"anyState"`
}

// CrashInput is the argument of the crash activity.
type CrashInput struct {
	Actor ActorKey `json:"actor"`

	// Reason is the failure reason recorded with the crash.
	Reason string `json:"reason"`

	// SandboxClass is the class of the worker the actor was on. The crash
	// counter is labelled with it, and the step that commits the crash can no
	// longer read it off a Worker record that has been deleted.
	SandboxClass string `json:"sandboxClass"`
}

// WorkerLostInput is what the worker teardown path tells the entity.
type WorkerLostInput struct {
	// Worker names the worker that went away, for the entity's log.
	Worker string `json:"worker"`

	// SandboxClass is the class of that worker, read while its record still
	// existed. Without it the crash counter loses its class, because the sweep
	// deletes the record the crash would have read it from.
	SandboxClass string `json:"sandboxClass"`
}

// StateSnapshot is what the State query answers with. It reports what the
// entity believes, which is the record as of its last read: a reader that needs
// the record itself calls GetActor.
type StateSnapshot struct {
	Actor ActorKey            `json:"actor"`
	State ateapipb.ActorState `json:"state"`

	// StateName spells State out, so a person reading a query result does not
	// have to map the enum by hand.
	StateName string `json:"stateName"`

	// Busy reports a lifecycle step in flight.
	Busy bool `json:"busy"`

	// Loaded reports whether the entity has read the record yet.
	Loaded bool `json:"loaded"`

	// RecoveryPending reports a crash that has been committed with the revert
	// that follows it still to run.
	RecoveryPending bool `json:"recoveryPending"`

	// RecoveryFailed reports a recovery that gave up. The actor is left
	// CRASHED, which is where the in-process orchestrator leaves one too, so
	// an operator's RevertActor is what moves it now.
	RecoveryFailed bool `json:"recoveryFailed"`
}
