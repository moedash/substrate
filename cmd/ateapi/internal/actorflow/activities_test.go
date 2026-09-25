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
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeSteps records what the activity asked the lifecycle for, and answers
// with what the test wants it to have found.
type fakeSteps struct {
	actor *ateapipb.Actor
	err   error

	// resumed is what ResumeActor reports about whether it did the work.
	resumed bool

	// block holds a step until the test releases it, for the tests about what
	// happens while a step is running.
	block chan struct{}

	calls        atomic.Int32
	ref          resources.ActorRef
	anyState     bool
	reason       string
	sandboxClass string
}

func (f *fakeSteps) record(ref resources.ActorRef) {
	f.calls.Add(1)
	f.ref = ref
	if f.block != nil {
		<-f.block
	}
}

func (f *fakeSteps) GetActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) ResumeActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, bool, error) {
	f.record(ref)
	return f.actor, f.resumed, f.err
}

func (f *fakeSteps) SuspendActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) PauseActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) RevertActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) DeleteActor(_ context.Context, ref resources.ActorRef, anyState bool) (*ateapipb.Actor, error) {
	f.record(ref)
	f.anyState = anyState
	return f.actor, f.err
}

func (f *fakeSteps) CrashActor(_ context.Context, ref resources.ActorRef, reason, sandboxClass string) error {
	f.record(ref)
	f.reason, f.sandboxClass = reason, sandboxClass
	return f.err
}

var _ LifecycleSteps = (*fakeSteps)(nil)

// newActivityEnv builds an activity environment over steps.
func newActivityEnv(steps LifecycleSteps) *testsuite.TestActivityEnvironment {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(NewActorActivities(steps))
	return env
}

// TestActivitiesReachTheLifecycleSteps covers the arguments each activity hands
// the steps, and the response shape it answers the entity with.
func TestActivitiesReachTheLifecycleSteps(t *testing.T) {
	suspended := actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	t.Run("LoadState", func(t *testing.T) {
		steps := &fakeSteps{actor: suspended}
		val, err := newActivityEnv(steps).ExecuteActivity(activities.LoadState, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("LoadState = %v, want nil", err)
		}
		var got *ateapipb.ActorStatus
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode LoadState = %v, want nil", err)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; got.GetState() != want {
			t.Errorf("state = %v, want %v", got.GetState(), want)
		}
		if steps.ref != testActor.Ref() {
			t.Errorf("GetActor ref = %v, want %v", steps.ref, testActor.Ref())
		}
	})

	t.Run("Resume", func(t *testing.T) {
		steps := &fakeSteps{actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING), resumed: true}
		val, err := newActivityEnv(steps).ExecuteActivity(activities.Resume, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("Resume = %v, want nil", err)
		}
		var got *ateapipb.ResumeActorResponse
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Resume = %v, want nil", err)
		}
		if !got.GetResumed() {
			t.Error("Resumed = false, want the step's answer carried through")
		}
		if want := ateapipb.ActorState_ACTOR_STATE_RUNNING; got.GetActor().GetStatus().GetState() != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Suspend", func(t *testing.T) {
		steps := &fakeSteps{actor: suspended}
		val, err := newActivityEnv(steps).ExecuteActivity(activities.Suspend, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("Suspend = %v, want nil", err)
		}
		var got *ateapipb.SuspendActorResponse
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Suspend = %v, want nil", err)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; got.GetActor().GetStatus().GetState() != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Pause", func(t *testing.T) {
		steps := &fakeSteps{actor: actorIn(ateapipb.ActorState_ACTOR_STATE_PAUSED)}
		val, err := newActivityEnv(steps).ExecuteActivity(activities.Pause, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("Pause = %v, want nil", err)
		}
		var got *ateapipb.PauseActorResponse
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Pause = %v, want nil", err)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_PAUSED; got.GetActor().GetStatus().GetState() != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Revert", func(t *testing.T) {
		steps := &fakeSteps{actor: suspended}
		val, err := newActivityEnv(steps).ExecuteActivity(activities.Revert, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("Revert = %v, want nil", err)
		}
		var got *ateapipb.RevertActorResponse
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Revert = %v, want nil", err)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; got.GetActor().GetStatus().GetState() != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Delete carries any_state", func(t *testing.T) {
		steps := &fakeSteps{actor: actorIn(ateapipb.ActorState_ACTOR_STATE_DELETING)}
		val, err := newActivityEnv(steps).ExecuteActivity(activities.Delete, DeleteInput{Actor: testActor, AnyState: true})
		if err != nil {
			t.Fatalf("Delete = %v, want nil", err)
		}
		var got *ateapipb.Actor
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Delete = %v, want nil", err)
		}
		if got.GetMetadata().GetName() != testActor.Name {
			t.Errorf("deleted actor = %q, want %q", got.GetMetadata().GetName(), testActor.Name)
		}
		if !steps.anyState {
			t.Error("DeleteActor anyState = false, want the request's flag carried through")
		}
	})

	t.Run("Crash carries the reason and the sandbox class", func(t *testing.T) {
		steps := &fakeSteps{}
		in := CrashInput{Actor: testActor, Reason: ateattr.ReasonWorkerPodGone, SandboxClass: "gvisor"}
		_, err := newActivityEnv(steps).ExecuteActivity(activities.Crash, in)
		if err != nil {
			t.Fatalf("Crash = %v, want nil", err)
		}
		if steps.reason != ateattr.ReasonWorkerPodGone {
			t.Errorf("CrashActor reason = %q, want %q", steps.reason, ateattr.ReasonWorkerPodGone)
		}
		// Without this the crash counter loses the class, because the step
		// commits the crash after the Worker record has been deleted.
		if steps.sandboxClass != "gvisor" {
			t.Errorf("CrashActor sandboxClass = %q, want %q", steps.sandboxClass, "gvisor")
		}
	})
}

// TestActivitiesClassifyStepFailures covers the classification the retry
// policy acts on: a permanent outcome fails the activity for good, a transient
// one is left retryable.
func TestActivitiesClassifyStepFailures(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantCode     codes.Code
		wantRetrying bool
	}{
		{
			name:     "a missing actor is permanent",
			err:      status.Errorf(codes.NotFound, "Actor %s not found", testActor),
			wantCode: codes.NotFound,
		},
		{
			name:     "a store sentinel for a missing actor is permanent",
			err:      store.ErrNotFound,
			wantCode: codes.NotFound,
		},
		{
			name:     "an illegal transition is permanent",
			err:      status.Error(codes.FailedPrecondition, "MarkPausing prerequisite not met"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "a crashed actor is permanent",
			err:      status.Errorf(codes.DataLoss, "actor %s crashed", testActor),
			wantCode: codes.DataLoss,
		},
		{
			name:     "a crash the step committed keeps the code the caller keys on",
			err:      status.Errorf(codes.Aborted, "actor %s crashed", testActor.Ref()),
			wantCode: codes.Aborted,
		},
		{
			name:         "a version conflict is worth another attempt",
			err:          store.ErrVersionConflict,
			wantCode:     codes.Aborted,
			wantRetrying: true,
		},
		{
			name:         "lease contention is worth another attempt",
			err:          status.Error(codes.Aborted, "another operation is in progress for this actor"),
			wantCode:     codes.Aborted,
			wantRetrying: true,
		},
		{
			name:         "an unclassified failure is worth another attempt",
			err:          errors.New("while dialing atelet: connection refused"),
			wantCode:     codes.Unknown,
			wantRetrying: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steps := &fakeSteps{err: tt.err}
			_, err := newActivityEnv(steps).ExecuteActivity(activities.Suspend, ActorInput{Actor: testActor})
			if err == nil {
				t.Fatal("Suspend = nil, want the step's failure")
			}
			if got := status.Code(StatusFromError(err)); got != tt.wantCode {
				t.Errorf("code = %v, want %v (err %v)", got, tt.wantCode, err)
			}

			var appErr *temporal.ApplicationError
			if !errors.As(err, &appErr) {
				t.Fatalf("error = %v, want an application error", err)
			}
			if retrying := !appErr.NonRetryable(); retrying != tt.wantRetrying {
				t.Errorf("retryable = %v, want %v", retrying, tt.wantRetrying)
			}
		})
	}
}

// TestActivitiesHeartbeatWhileAStepRuns covers the report a long step owes
// Temporal: without it a step that is still working is indistinguishable from
// an ateapi pod that went away, and a cancellation never reaches the step.
func TestActivitiesHeartbeatWhileAStepRuns(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()

	// Registered with a heartbeat interval a test can wait for, rather than the
	// one a real snapshot upload is paced against.
	slowStep := func(ctx context.Context) (string, error) {
		return heartbeating(ctx, time.Millisecond, func(context.Context) (string, error) {
			time.Sleep(50 * time.Millisecond)
			return "done", nil
		})
	}
	env.RegisterActivity(slowStep)

	var beats atomic.Int32
	env.SetOnActivityHeartbeatListener(func(*activity.Info, converter.EncodedValues) { beats.Add(1) })

	val, err := env.ExecuteActivity(slowStep)
	if err != nil {
		t.Fatalf("slowStep = %v, want nil", err)
	}
	var got string
	if err := val.Get(&got); err != nil {
		t.Fatalf("decode slowStep = %v, want nil", err)
	}
	if got != "done" {
		t.Errorf("result = %q, want %q", got, "done")
	}
	if beats.Load() == 0 {
		t.Error("heartbeats = 0, want the step to have reported progress")
	}
}

// TestActivityWaitsForTheStepToReport covers the wait the activity owes the
// record while the heartbeat ticks past it: the step holds the actor's lease
// and is mid-transition, so the activity reports what the step found rather
// than returning without it.
func TestActivityWaitsForTheStepToReport(t *testing.T) {
	release := make(chan struct{})
	steps := &fakeSteps{actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED), block: release}
	env := newActivityEnv(steps)

	go func() {
		time.Sleep(10 * time.Millisecond)
		close(release)
	}()

	val, err := env.ExecuteActivity(activities.Suspend, ActorInput{Actor: testActor})
	if err != nil {
		t.Fatalf("Suspend = %v, want nil", err)
	}
	var got *ateapipb.SuspendActorResponse
	if err := val.Get(&got); err != nil {
		t.Fatalf("decode Suspend = %v, want nil", err)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; got.GetActor().GetStatus().GetState() != want {
		t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
	}
	if steps.calls.Load() != 1 {
		t.Errorf("step calls = %d, want 1", steps.calls.Load())
	}
}
