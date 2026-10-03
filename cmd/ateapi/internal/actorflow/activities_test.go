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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
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
	// happens while a step is running. started is closed when the step is
	// reached, so the test knows the block is holding something.
	block   chan struct{}
	started chan struct{}

	calls        atomic.Int32
	ref          resources.ActorRef
	anyState     bool
	precondition store.DeletePreconditions
	message      string
	sandboxClass string
}

func (f *fakeSteps) record(ref resources.ActorRef) {
	f.calls.Add(1)
	f.ref = ref
	if f.started != nil {
		close(f.started)
	}
	if f.block != nil {
		<-f.block
	}
}

func (f *fakeSteps) GetActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) ResumeActor(
	_ context.Context,
	ref resources.ActorRef,
) (*ateapipb.Actor, bool, error) {
	f.record(ref)
	return f.actor, f.resumed, f.err
}

func (f *fakeSteps) SuspendActor(
	_ context.Context,
	ref resources.ActorRef,
) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) PauseActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) RevertActor(
	_ context.Context,
	ref resources.ActorRef,
) (*ateapipb.Actor, error) {
	f.record(ref)
	return f.actor, f.err
}

func (f *fakeSteps) DeleteActor(
	_ context.Context,
	ref resources.ActorRef,
	anyState bool,
	precondition store.DeletePreconditions,
) (*ateapipb.Actor, error) {
	f.record(ref)
	f.anyState, f.precondition = anyState, precondition
	return f.actor, f.err
}

func (f *fakeSteps) CrashActor(
	_ context.Context,
	ref resources.ActorRef,
	message, sandboxClass string,
) error {
	f.record(ref)
	f.message, f.sandboxClass = message, sandboxClass
	return f.err
}

var _ LifecycleSteps = (*fakeSteps)(nil)

// stateOf reads the lifecycle state off an answered record.
func stateOf(actor *ateapipb.Actor) ateapipb.ActorState {
	return actor.GetStatus().GetState()
}

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
		env := newActivityEnv(steps)
		val, err := env.ExecuteActivity(activities.LoadState, ActorInput{Actor: testActor})
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
		env := newActivityEnv(steps)
		val, err := env.ExecuteActivity(activities.Resume, ActorInput{Actor: testActor})
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
		if want := ateapipb.ActorState_ACTOR_STATE_RUNNING; stateOf(got.GetActor()) != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Suspend", func(t *testing.T) {
		steps := &fakeSteps{actor: suspended}
		env := newActivityEnv(steps)
		val, err := env.ExecuteActivity(activities.Suspend, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("Suspend = %v, want nil", err)
		}
		var got *ateapipb.SuspendActorResponse
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Suspend = %v, want nil", err)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; stateOf(got.GetActor()) != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Pause", func(t *testing.T) {
		steps := &fakeSteps{actor: actorIn(ateapipb.ActorState_ACTOR_STATE_PAUSED)}
		env := newActivityEnv(steps)
		val, err := env.ExecuteActivity(activities.Pause, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("Pause = %v, want nil", err)
		}
		var got *ateapipb.PauseActorResponse
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Pause = %v, want nil", err)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_PAUSED; stateOf(got.GetActor()) != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Revert", func(t *testing.T) {
		steps := &fakeSteps{actor: suspended}
		env := newActivityEnv(steps)
		val, err := env.ExecuteActivity(activities.Revert, ActorInput{Actor: testActor})
		if err != nil {
			t.Fatalf("Revert = %v, want nil", err)
		}
		var got *ateapipb.RevertActorResponse
		if err := val.Get(&got); err != nil {
			t.Fatalf("decode Revert = %v, want nil", err)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; stateOf(got.GetActor()) != want {
			t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
		}
	})

	t.Run("Delete carries any_state and the precondition", func(t *testing.T) {
		steps := &fakeSteps{actor: actorIn(ateapipb.ActorState_ACTOR_STATE_DELETING)}
		env := newActivityEnv(steps)
		precondition := store.DeletePreconditions{UID: "actor-uid-1", Version: 7}
		in := DeleteInput{Actor: testActor, AnyState: true, Precondition: precondition}
		val, err := env.ExecuteActivity(activities.Delete, in)
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
		if steps.precondition != precondition {
			t.Errorf("DeleteActor precondition = %+v, want %+v carried through",
				steps.precondition, precondition)
		}
	})

	t.Run("Crash carries the message and the sandbox class", func(t *testing.T) {
		steps := &fakeSteps{}
		in := CrashInput{
			Actor:        testActor,
			Message:      "worker pod went away while hosting the actor",
			SandboxClass: "gvisor",
		}
		_, err := newActivityEnv(steps).ExecuteActivity(activities.Crash, in)
		if err != nil {
			t.Fatalf("Crash = %v, want nil", err)
		}
		if steps.message != in.Message {
			t.Errorf("CrashActor message = %q, want %q", steps.message, in.Message)
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
			name: "lease contention is worth another attempt",
			err: status.Error(
				codes.Aborted,
				"another operation is in progress for this actor",
			),
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
			env := newActivityEnv(steps)
			_, err := env.ExecuteActivity(activities.Suspend, ActorInput{Actor: testActor})
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

	// The step runs until the first heartbeat has been seen, so the test
	// waits on the report itself rather than on the clock.
	beat := make(chan struct{}, 1)
	var beats atomic.Int32
	env.SetOnActivityHeartbeatListener(func(*activity.Info, converter.EncodedValues) {
		beats.Add(1)
		select {
		case beat <- struct{}{}:
		default:
		}
	})

	// Registered with a heartbeat interval a test can wait for, rather than the
	// one a real snapshot upload is paced against.
	slowStep := func(ctx context.Context) (string, error) {
		return heartbeating(ctx, time.Millisecond, func(ctx context.Context) (string, error) {
			select {
			case <-beat:
				return "done", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
	}
	env.RegisterActivity(slowStep)

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
	started := make(chan struct{})
	steps := &fakeSteps{
		actor:   actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED),
		block:   release,
		started: started,
	}
	env := newActivityEnv(steps)

	// Released only once the step is being held, so the activity is seen
	// waiting on it.
	go func() {
		<-started
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
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; stateOf(got.GetActor()) != want {
		t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
	}
	if steps.calls.Load() != 1 {
		t.Errorf("step calls = %d, want 1", steps.calls.Load())
	}
}

// TestActivitiesRecordARecoveryThatGaveUp covers the event an operator finds a
// stranded actor by. It carries the actor's identity off the record, which no
// metric label may, and names the step that gave up and the state it left.
func TestActivitiesRecordARecoveryThatGaveUp(t *testing.T) {
	events := captureActorEvents(t)
	crashed := actorIn(ateapipb.ActorState_ACTOR_STATE_CRASHED)
	crashed.Metadata.Uid = "actor-uid-1"
	crashed.ActorTemplate = &ateapipb.ObjectRef{Atespace: testActor.Atespace, Name: "tmpl"}
	steps := &fakeSteps{actor: crashed}

	in := RecoveryFailedInput{Actor: testActor, Operation: ateattr.OperationRevert}
	if _, err := newActivityEnv(steps).ExecuteActivity(activities.RecoveryFailed, in); err != nil {
		t.Fatalf("RecoveryFailed = %v, want nil", err)
	}

	got := events()
	if len(got) != 1 || got[0].name != actorevent.RecoveryFailed.Name {
		t.Fatalf("events = %+v, want one %s", got, actorevent.RecoveryFailed.Name)
	}
	want := map[string]string{
		string(ateattr.ActorUIDKey):           "actor-uid-1",
		string(ateattr.ActorNameKey):          testActor.Name,
		string(ateattr.ActorOperationNameKey): ateattr.OperationRevert,
		string(ateattr.ActorStateKey):         ateattr.ActorStateCrashed,
	}
	for key, value := range want {
		if got[0].attrs[key] != value {
			t.Errorf("attribute %s = %q, want %q", key, got[0].attrs[key], value)
		}
	}
	for _, key := range actorevent.RecoveryFailed.Keys {
		if _, ok := got[0].attrs[key]; !ok {
			t.Errorf("attribute %s missing, want every key the event promises", key)
		}
	}
}

// capturedEvent is one record from the OTLP copy of an actor event.
type capturedEvent struct {
	name  string
	attrs map[string]string
}

var (
	eventSinkOnce sync.Once
	eventSinkMu   sync.Mutex
	eventSink     []capturedEvent
)

type eventSinkExporter struct{}

func (eventSinkExporter) Export(_ context.Context, records []sdklog.Record) error {
	eventSinkMu.Lock()
	defer eventSinkMu.Unlock()
	for _, r := range records {
		e := capturedEvent{name: r.EventName(), attrs: map[string]string{}}
		r.WalkAttributes(func(kv attribute.KeyValue) bool {
			e.attrs[string(kv.Key)] = kv.Value.Emit()
			return true
		})
		eventSink = append(eventSink, e)
	}
	return nil
}

func (eventSinkExporter) Shutdown(context.Context) error   { return nil }
func (eventSinkExporter) ForceFlush(context.Context) error { return nil }

// captureActorEvents installs a sink for the OTLP copy of actor events and
// returns what was captured while the test ran. The global provider only
// delegates once, so one sink serves the whole binary and each call clears it,
// which keeps its callers off t.Parallel.
func captureActorEvents(t *testing.T) func() []capturedEvent {
	t.Helper()
	eventSinkOnce.Do(func() {
		global.SetLoggerProvider(sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewSimpleProcessor(eventSinkExporter{}))))
	})
	clear := func() {
		eventSinkMu.Lock()
		defer eventSinkMu.Unlock()
		eventSink = nil
	}
	clear()
	t.Cleanup(clear)
	return func() []capturedEvent {
		eventSinkMu.Lock()
		defer eventSinkMu.Unlock()
		return slices.Clone(eventSink)
	}
}
