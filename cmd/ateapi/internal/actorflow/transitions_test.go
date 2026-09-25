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
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// allActorStates enumerates every ActorState. Derived from the generated enum
// map, so a state added to the proto shows up here and the tables below have
// to say what it means.
var allActorStates = func() []ateapipb.ActorState {
	numbers := make([]int32, 0, len(ateapipb.ActorState_name))
	for number := range ateapipb.ActorState_name {
		numbers = append(numbers, number)
	}
	slices.Sort(numbers)
	states := make([]ateapipb.ActorState, 0, len(numbers))
	for _, number := range numbers {
		states = append(states, ateapipb.ActorState(number))
	}
	return states
}()

// TestAcceptsEveryState spells the transition table out state by state, as a
// second reading of what each operation is for. A change to either one has to
// be a change to both.
func TestAcceptsEveryState(t *testing.T) {
	wants := map[string]map[ateapipb.ActorState]bool{
		UpdateResume: {
			ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true, // not read yet: the step decides
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   true,
			ateapipb.ActorState_ACTOR_STATE_PAUSED:      true,
			ateapipb.ActorState_ACTOR_STATE_RESUMING:    true, // a previous attempt got this far
			ateapipb.ActorState_ACTOR_STATE_RUNNING:     true, // already there; the router asks per request
			ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  false,
			ateapipb.ActorState_ACTOR_STATE_PAUSING:     false,
			ateapipb.ActorState_ACTOR_STATE_REVERTING:   false,
			ateapipb.ActorState_ACTOR_STATE_CRASHED:     false, // revert first
			ateapipb.ActorState_ACTOR_STATE_DELETING:    false,
		},
		UpdateSuspend: {
			ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true,
			ateapipb.ActorState_ACTOR_STATE_RUNNING:     true,
			ateapipb.ActorState_ACTOR_STATE_PAUSED:      true, // uploads what the pause captured
			ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  true,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   true,
			ateapipb.ActorState_ACTOR_STATE_RESUMING:    false,
			ateapipb.ActorState_ACTOR_STATE_PAUSING:     false,
			ateapipb.ActorState_ACTOR_STATE_REVERTING:   false,
			ateapipb.ActorState_ACTOR_STATE_CRASHED:     false,
			ateapipb.ActorState_ACTOR_STATE_DELETING:    false,
		},
		UpdatePause: {
			ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true,
			ateapipb.ActorState_ACTOR_STATE_RUNNING:     true,
			ateapipb.ActorState_ACTOR_STATE_PAUSING:     true,
			ateapipb.ActorState_ACTOR_STATE_PAUSED:      true,
			ateapipb.ActorState_ACTOR_STATE_RESUMING:    false,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  false,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   false,
			ateapipb.ActorState_ACTOR_STATE_REVERTING:   false,
			ateapipb.ActorState_ACTOR_STATE_CRASHED:     false,
			ateapipb.ActorState_ACTOR_STATE_DELETING:    false,
		},
		UpdateRevert: {
			ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true,
			ateapipb.ActorState_ACTOR_STATE_RUNNING:     true,
			ateapipb.ActorState_ACTOR_STATE_PAUSED:      true,
			ateapipb.ActorState_ACTOR_STATE_CRASHED:     true, // the recovery verb
			ateapipb.ActorState_ACTOR_STATE_REVERTING:   true,
			ateapipb.ActorState_ACTOR_STATE_RESUMING:    false,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  false,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   false, // nothing to recover
			ateapipb.ActorState_ACTOR_STATE_PAUSING:     false,
			ateapipb.ActorState_ACTOR_STATE_DELETING:    false,
		},
		UpdateDelete: {
			ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   true,
			ateapipb.ActorState_ACTOR_STATE_CRASHED:     true,
			ateapipb.ActorState_ACTOR_STATE_DELETING:    true,
			ateapipb.ActorState_ACTOR_STATE_RUNNING:     false, // suspend or revert it first
			ateapipb.ActorState_ACTOR_STATE_RESUMING:    false,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  false,
			ateapipb.ActorState_ACTOR_STATE_PAUSING:     false,
			ateapipb.ActorState_ACTOR_STATE_PAUSED:      false,
			ateapipb.ActorState_ACTOR_STATE_REVERTING:   false,
		},
	}

	for op, want := range wants {
		t.Run(op, func(t *testing.T) {
			if len(want) != len(allActorStates) {
				t.Fatalf("table covers %d states, want all %d", len(want), len(allActorStates))
			}
			for _, state := range allActorStates {
				accepted, listed := want[state]
				if !listed {
					t.Errorf("state %v is not in the %s table", state, op)
					continue
				}
				if got := accepts(op, state); got != accepted {
					t.Errorf("accepts(%s, %v) = %v, want %v", op, state, got, accepted)
				}
			}
		})
	}

	if len(wants) != len(acceptedFrom) {
		t.Errorf("table covers %d operations, want all %d", len(wants), len(acceptedFrom))
	}
}

// TestTransitionalStates covers the states nothing but the step that entered
// one can leave, which is what a cancelled entity has to clean up after.
func TestTransitionalStates(t *testing.T) {
	want := map[ateapipb.ActorState]bool{
		ateapipb.ActorState_ACTOR_STATE_RESUMING:    true,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  true,
		ateapipb.ActorState_ACTOR_STATE_PAUSING:     true,
		ateapipb.ActorState_ACTOR_STATE_REVERTING:   true,
		ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: false,
		ateapipb.ActorState_ACTOR_STATE_RUNNING:     false,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   false,
		ateapipb.ActorState_ACTOR_STATE_PAUSED:      false,
		ateapipb.ActorState_ACTOR_STATE_CRASHED:     false,
		ateapipb.ActorState_ACTOR_STATE_DELETING:    false,
	}
	if len(want) != len(allActorStates) {
		t.Fatalf("table covers %d states, want all %d", len(want), len(allActorStates))
	}
	for _, state := range allActorStates {
		if got := transitional(state); got != want[state] {
			t.Errorf("transitional(%v) = %v, want %v", state, got, want[state])
		}
	}
}

// TestLostWithWorker covers the states that mean the actor was on the worker
// that went away, and so the ones automatic recovery applies to.
func TestLostWithWorker(t *testing.T) {
	want := map[ateapipb.ActorState]bool{
		ateapipb.ActorState_ACTOR_STATE_RUNNING:    true,
		ateapipb.ActorState_ACTOR_STATE_RESUMING:   true,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING: true,
		ateapipb.ActorState_ACTOR_STATE_PAUSING:    true,
		ateapipb.ActorState_ACTOR_STATE_REVERTING:  true,
		// A suspended or paused actor saved its state before the pod went, and
		// a crashed or deleting one has nothing left on the node.
		ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: false,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   false,
		ateapipb.ActorState_ACTOR_STATE_PAUSED:      false,
		ateapipb.ActorState_ACTOR_STATE_CRASHED:     false,
		ateapipb.ActorState_ACTOR_STATE_DELETING:    false,
	}
	if len(want) != len(allActorStates) {
		t.Fatalf("table covers %d states, want all %d", len(want), len(allActorStates))
	}
	for _, state := range allActorStates {
		if got := lostWithWorker(state); got != want[state] {
			t.Errorf("lostWithWorker(%v) = %v, want %v", state, got, want[state])
		}
	}
}

// TestRejectionNamesTheStates covers the message a refused caller reads: it has
// to say what the actor is doing and what would have been accepted.
func TestRejectionNamesTheStates(t *testing.T) {
	message := rejection(UpdateRevert, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	for _, want := range []string{UpdateRevert, "ACTOR_STATE_SUSPENDED", "ACTOR_STATE_CRASHED"} {
		if !strings.Contains(message, want) {
			t.Errorf("rejection message %q does not name %q", message, want)
		}
	}
}
