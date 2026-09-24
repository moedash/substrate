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
	"slices"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// acceptedFrom lists the states each lifecycle operation is accepted from.
//
// It mirrors the preconditions the lifecycle steps enforce against the store,
// so an update that cannot succeed is refused before an activity runs. The
// steps remain the authority: the record can move under a mirrored state, and
// an operation the table lets through can still be refused by the step that
// reads the row under the actor's lease.
//
// The in-progress state of each operation is accepted so a re-entered operation
// fast-forwards, and the operation's own end state is accepted where the step
// treats it as a no-op success. Revert is the exception: it is the recovery
// verb, and reporting success on an already-suspended actor would hide that
// there was nothing to recover.
var acceptedFrom = map[string][]ateapipb.ActorState{
	UpdateResume: {
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ateapipb.ActorState_ACTOR_STATE_PAUSED,
		ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_RUNNING,
	},
	UpdateSuspend: {
		ateapipb.ActorState_ACTOR_STATE_RUNNING,
		ateapipb.ActorState_ACTOR_STATE_PAUSED,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
	},
	UpdatePause: {
		ateapipb.ActorState_ACTOR_STATE_RUNNING,
		ateapipb.ActorState_ACTOR_STATE_PAUSING,
		ateapipb.ActorState_ACTOR_STATE_PAUSED,
	},
	UpdateRevert: {
		ateapipb.ActorState_ACTOR_STATE_RUNNING,
		ateapipb.ActorState_ACTOR_STATE_PAUSED,
		ateapipb.ActorState_ACTOR_STATE_CRASHED,
		ateapipb.ActorState_ACTOR_STATE_REVERTING,
	},
	UpdateDelete: {
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ateapipb.ActorState_ACTOR_STATE_CRASHED,
		ateapipb.ActorState_ACTOR_STATE_DELETING,
	},
}

// transitionalStates are the states an actor passes through while a step is
// driving it. Nothing outside the step that entered one can finish it, so an
// actor left in one has to be crashed before anything else can move it.
var transitionalStates = []ateapipb.ActorState{
	ateapipb.ActorState_ACTOR_STATE_RESUMING,
	ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
	ateapipb.ActorState_ACTOR_STATE_PAUSING,
	ateapipb.ActorState_ACTOR_STATE_REVERTING,
}

// recoverableFromWorkerLoss lists the states that imply the actor was on the
// worker that went away. A suspended or paused actor saved its state before the
// pod went, and a crashed or deleting one has nothing left on the node.
var recoverableFromWorkerLoss = append([]ateapipb.ActorState{
	ateapipb.ActorState_ACTOR_STATE_RUNNING,
}, transitionalStates...)

// accepts reports whether op may run from state.
//
// An unspecified state is accepted: the entity has not read the record yet, so
// there is nothing to check it against and the step decides.
func accepts(op string, state ateapipb.ActorState) bool {
	if state == ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED {
		return true
	}
	return slices.Contains(acceptedFrom[op], state)
}

// transitional reports whether state is one a step has to finish.
func transitional(state ateapipb.ActorState) bool {
	return slices.Contains(transitionalStates, state)
}

// lostWithWorker reports whether an actor in state was running on the worker
// that went away.
func lostWithWorker(state ateapipb.ActorState) bool {
	return slices.Contains(recoverableFromWorkerLoss, state)
}

// rejection describes why an operation cannot run from the entity's state, in
// the terms the lifecycle steps use.
func rejection(op string, state ateapipb.ActorState) string {
	return fmt.Sprintf("%s is not accepted from state %v (want one of %v)", op, state, acceptedFrom[op])
}
