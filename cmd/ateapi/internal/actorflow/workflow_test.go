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
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testActor is the actor every entity test owns.
var testActor = ActorKey{Atespace: "team-a", Name: "actor-1"}

// newEnv builds a test environment that checks its mocks were all used, so a
// test that claims a step runs has to see it run.
func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	t.Cleanup(func() { env.AssertExpectations(t) })
	return env
}

// statusIn is what LoadState answers with.
func statusIn(state ateapipb.ActorState) *ateapipb.ActorStatus {
	return &ateapipb.ActorStatus{State: state}
}

// actorIn is the record a step reports committing.
func actorIn(state ateapipb.ActorState) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testActor.Atespace, Name: testActor.Name},
		Status:   &ateapipb.ActorStatus{State: state},
	}
}

// onLoadState mocks the read a fresh entity opens with.
func onLoadState(env *testsuite.TestWorkflowEnvironment, state ateapipb.ActorState) {
	env.OnActivity(activities.LoadState, mock.Anything, ActorInput{Actor: testActor}).
		Return(statusIn(state), nil).Once()
}

// runEntity executes the entity, firing each step a second apart on the
// workflow clock and cancelling at the end: an entity only completes when its
// actor is deleted, so a test that does not delete has to cancel to finish.
func runEntity(t *testing.T, env *testsuite.TestWorkflowEnvironment, in ActorEntityInput, steps ...func()) {
	t.Helper()
	delay := time.Second
	for _, step := range steps {
		env.RegisterDelayedCallback(step, delay)
		delay += time.Second
	}
	env.RegisterDelayedCallback(env.CancelWorkflow, delay)
	if in.Actor == (ActorKey{}) {
		in.Actor = testActor
	}
	env.ExecuteWorkflow(ActorEntityWorkflow, in)
}

// queryState reads what the entity believes the record says.
func queryState(t *testing.T, env *testsuite.TestWorkflowEnvironment) StateSnapshot {
	t.Helper()
	encoded, err := env.QueryWorkflow(QueryState)
	if err != nil {
		t.Fatalf("QueryWorkflow(%s) = %v, want nil", QueryState, err)
	}
	var snapshot StateSnapshot
	if err := encoded.Get(&snapshot); err != nil {
		t.Fatalf("decode %s = %v, want nil", QueryState, err)
	}
	return snapshot
}

// sendUpdate delivers an update and reports how it ended. Exactly one of the
// returned values is set.
func sendUpdate(env *testsuite.TestWorkflowEnvironment, name string, args ...any) (*updateOutcome, func()) {
	outcome := &updateOutcome{}
	return outcome, func() {
		env.UpdateWorkflow(name, name+"-1", &testsuite.TestUpdateCallback{
			OnAccept:   func() { outcome.accepted = true },
			OnReject:   func(err error) { outcome.rejected = err },
			OnComplete: func(result any, err error) { outcome.result, outcome.failed = result, err },
		}, args...)
	}
}

type updateOutcome struct {
	accepted bool
	rejected error
	failed   error
	result   any
}

// code is the gRPC code the outcome reaches an RPC caller as.
func (o *updateOutcome) code() codes.Code {
	switch {
	case o.rejected != nil:
		return status.Code(StatusFromError(o.rejected))
	case o.failed != nil:
		return status.Code(StatusFromError(o.failed))
	default:
		return codes.OK
	}
}

func TestActorEntityResume(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	env.OnActivity(activities.Resume, mock.Anything, ActorInput{Actor: testActor}).
		Return(&ateapipb.ResumeActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING), Resumed: true}, nil).Once()

	outcome, resume := sendUpdate(env, UpdateResume)
	var snapshot StateSnapshot
	runEntity(t, env, ActorEntityInput{}, resume, func() { snapshot = queryState(t, env) })

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if !temporal.IsCanceledError(env.GetWorkflowError()) {
		t.Fatalf("workflow error = %v, want a cancellation", env.GetWorkflowError())
	}
	if !outcome.accepted || outcome.code() != codes.OK {
		t.Fatalf("Resume outcome = %+v, want accepted and OK", outcome)
	}
	resumed, ok := outcome.result.(*ateapipb.ResumeActorResponse)
	if !ok {
		t.Fatalf("Resume result = %T, want *ateapipb.ResumeActorResponse", outcome.result)
	}
	if !resumed.GetResumed() {
		t.Error("ResumeActorResponse.Resumed = false, want true")
	}
	if want := ateapipb.ActorState_ACTOR_STATE_RUNNING; snapshot.State != want {
		t.Errorf("queried state = %v, want %v", snapshot.State, want)
	}
	if snapshot.Busy {
		t.Error("queried Busy = true, want false once the step is done")
	}
}

// TestActorEntityRejectsIllegalTransitions covers the validator: an operation
// the record cannot take is refused before any step runs, which is why no
// activity beyond the opening read is mocked here.
func TestActorEntityRejectsIllegalTransitions(t *testing.T) {
	tests := []struct {
		name  string
		from  ateapipb.ActorState
		op    string
		args  []any
		wants codes.Code
	}{
		{"resume a crashed actor", ateapipb.ActorState_ACTOR_STATE_CRASHED, UpdateResume, nil, codes.FailedPrecondition},
		{"resume a deleting actor", ateapipb.ActorState_ACTOR_STATE_DELETING, UpdateResume, nil, codes.FailedPrecondition},
		{"suspend a resuming actor", ateapipb.ActorState_ACTOR_STATE_RESUMING, UpdateSuspend, nil, codes.FailedPrecondition},
		{"pause a suspended actor", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, UpdatePause, nil, codes.FailedPrecondition},
		{"pause a suspending actor", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, UpdatePause, nil, codes.FailedPrecondition},
		{"revert a suspended actor", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, UpdateRevert, nil, codes.FailedPrecondition},
		{"revert a suspending actor", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, UpdateRevert, nil, codes.FailedPrecondition},
		{"delete a running actor", ateapipb.ActorState_ACTOR_STATE_RUNNING, UpdateDelete, []any{false}, codes.FailedPrecondition},
		{"delete a paused actor", ateapipb.ActorState_ACTOR_STATE_PAUSED, UpdateDelete, []any{false}, codes.FailedPrecondition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newEnv(t)
			onLoadState(env, tt.from)

			outcome, send := sendUpdate(env, tt.op, tt.args...)
			runEntity(t, env, ActorEntityInput{}, send)

			if outcome.rejected == nil {
				t.Fatalf("%s from %v was not rejected (outcome %+v)", tt.op, tt.from, outcome)
			}
			if got := outcome.code(); got != tt.wants {
				t.Errorf("rejection code = %v, want %v (err %v)", got, tt.wants, outcome.rejected)
			}
		})
	}
}

// TestActorEntityAcceptsNoOpTransitions covers the states the steps treat as an
// operation that has already happened. The entity has to let them reach the
// step, which reports success, rather than refusing them.
func TestActorEntityAcceptsNoOpTransitions(t *testing.T) {
	tests := []struct {
		name string
		from ateapipb.ActorState
		op   string
	}{
		{"resume a running actor", ateapipb.ActorState_ACTOR_STATE_RUNNING, UpdateResume},
		{"resume a resuming actor", ateapipb.ActorState_ACTOR_STATE_RESUMING, UpdateResume},
		{"suspend a suspended actor", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, UpdateSuspend},
		{"suspend a paused actor", ateapipb.ActorState_ACTOR_STATE_PAUSED, UpdateSuspend},
		{"pause a paused actor", ateapipb.ActorState_ACTOR_STATE_PAUSED, UpdatePause},
		{"revert a crashed actor", ateapipb.ActorState_ACTOR_STATE_CRASHED, UpdateRevert},
		{"revert a reverting actor", ateapipb.ActorState_ACTOR_STATE_REVERTING, UpdateRevert},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newEnv(t)
			onLoadState(env, tt.from)
			switch tt.op {
			case UpdateResume:
				env.OnActivity(activities.Resume, mock.Anything, mock.Anything).
					Return(&ateapipb.ResumeActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING)}, nil).Once()
			case UpdateSuspend:
				env.OnActivity(activities.Suspend, mock.Anything, mock.Anything).
					Return(&ateapipb.SuspendActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()
			case UpdatePause:
				env.OnActivity(activities.Pause, mock.Anything, mock.Anything).
					Return(&ateapipb.PauseActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_PAUSED)}, nil).Once()
			case UpdateRevert:
				env.OnActivity(activities.Revert, mock.Anything, mock.Anything).
					Return(&ateapipb.RevertActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()
			}

			outcome, send := sendUpdate(env, tt.op)
			runEntity(t, env, ActorEntityInput{}, send)

			if outcome.rejected != nil {
				t.Fatalf("%s from %v was rejected: %v", tt.op, tt.from, outcome.rejected)
			}
			if got := outcome.code(); got != codes.OK {
				t.Errorf("outcome code = %v, want OK (err %v)", got, outcome.failed)
			}
		})
	}
}

func TestActorEntityDeleteCompletesTheEntity(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	env.OnActivity(activities.Delete, mock.Anything, DeleteInput{Actor: testActor}).
		Return(actorIn(ateapipb.ActorState_ACTOR_STATE_DELETING), nil).Once()

	outcome, del := sendUpdate(env, UpdateDelete, false)
	// No cancellation: the delete is what ends this execution.
	env.RegisterDelayedCallback(del, time.Second)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil: a deleted actor ends its entity cleanly", err)
	}
	if outcome.rejected != nil || outcome.failed != nil {
		t.Fatalf("Delete outcome = %+v, want success", outcome)
	}
	if _, ok := outcome.result.(*ateapipb.Actor); !ok {
		t.Errorf("Delete result = %T, want *ateapipb.Actor", outcome.result)
	}
}

// TestActorEntityDeleteAnyState covers the request flag that says the state
// machine does not apply.
func TestActorEntityDeleteAnyState(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Delete, mock.Anything, DeleteInput{Actor: testActor, AnyState: true}).
		Return(actorIn(ateapipb.ActorState_ACTOR_STATE_DELETING), nil).Once()

	outcome, del := sendUpdate(env, UpdateDelete, true)
	env.RegisterDelayedCallback(del, time.Second)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil", err)
	}
	if outcome.rejected != nil || outcome.failed != nil {
		t.Fatalf("Delete outcome = %+v, want success", outcome)
	}
}

// TestActorEntityRecoversFromWorkerLoss is the transition the in-process
// orchestrator leaves to an operator: the actor is crashed so readers and the
// counter see it, and then reverted to a state it can be resumed from, with
// nobody asking for it.
func TestActorEntityRecoversFromWorkerLoss(t *testing.T) {
	for _, from := range recoverableFromWorkerLoss {
		t.Run(from.String(), func(t *testing.T) {
			env := newEnv(t)
			onLoadState(env, from)
			env.OnActivity(activities.Crash, mock.Anything, CrashInput{Actor: testActor, Reason: ateattr.ReasonWorkerPodGone}).
				Return(nil).Once()
			env.OnActivity(activities.Revert, mock.Anything, ActorInput{Actor: testActor}).
				Return(&ateapipb.RevertActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()

			var snapshot StateSnapshot
			runEntity(t, env, ActorEntityInput{},
				func() { env.SignalWorkflow(SignalWorkerLost, WorkerLostInput{Worker: "worker-7"}) },
				func() { snapshot = queryState(t, env) })

			if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; snapshot.State != want {
				t.Errorf("state after recovery = %v, want %v", snapshot.State, want)
			}
		})
	}
}

func TestActorEntityAutoResumesAfterCrash(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Crash, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(activities.Revert, mock.Anything, mock.Anything).
		Return(&ateapipb.RevertActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()
	env.OnActivity(activities.Resume, mock.Anything, mock.Anything).
		Return(&ateapipb.ResumeActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING), Resumed: true}, nil).Once()

	var snapshot StateSnapshot
	runEntity(t, env, ActorEntityInput{AutoResumeAfterCrash: true},
		func() { env.SignalWorkflow(SignalWorkerLost, WorkerLostInput{Worker: "worker-7"}) },
		func() { snapshot = queryState(t, env) })

	if want := ateapipb.ActorState_ACTOR_STATE_RUNNING; snapshot.State != want {
		t.Errorf("state after recovery = %v, want %v", snapshot.State, want)
	}
}

// TestActorEntityIgnoresWorkerLossOffWorker covers the states that were not on
// the worker that went away. Nothing is mocked beyond the opening read, so a
// crash or a revert here fails the test.
func TestActorEntityIgnoresWorkerLossOffWorker(t *testing.T) {
	offWorker := []ateapipb.ActorState{
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ateapipb.ActorState_ACTOR_STATE_PAUSED,
		ateapipb.ActorState_ACTOR_STATE_CRASHED,
		ateapipb.ActorState_ACTOR_STATE_DELETING,
	}
	for _, from := range offWorker {
		t.Run(from.String(), func(t *testing.T) {
			env := newEnv(t)
			onLoadState(env, from)

			var snapshot StateSnapshot
			runEntity(t, env, ActorEntityInput{},
				func() { env.SignalWorkflow(SignalWorkerLost, WorkerLostInput{Worker: "worker-7"}) },
				func() { snapshot = queryState(t, env) })

			if snapshot.State != from {
				t.Errorf("state = %v, want %v: worker loss does not apply here", snapshot.State, from)
			}
		})
	}
}

// TestActorEntityRecoveryOutlastsAFailedCrash covers the recovery giving up
// rather than reverting an actor it could not crash first: the revert would
// hide a crash nothing ever recorded.
func TestActorEntityRecoveryStopsWhenTheCrashFails(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Crash, mock.Anything, mock.Anything).
		Return(temporal.NewNonRetryableApplicationError("gone", errorTypePrefix+codes.NotFound.String(), nil)).Once()

	var snapshot StateSnapshot
	runEntity(t, env, ActorEntityInput{},
		func() { env.SignalWorkflow(SignalWorkerLost, WorkerLostInput{Worker: "worker-7"}) },
		func() { snapshot = queryState(t, env) })

	if snapshot.Loaded {
		t.Errorf("snapshot = %+v, want the mirror dropped after a failed crash", snapshot)
	}
}

// TestActorEntityContinuesAsNew covers the history rollover: the entity carries
// the state it has read into the next execution rather than reading it again.
func TestActorEntityContinuesAsNew(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	env.SetContinueAsNewSuggested(true)

	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor, AutoResumeAfterCrash: true})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	var continued *workflow.ContinueAsNewError
	err := env.GetWorkflowError()
	if !errors.As(err, &continued) {
		t.Fatalf("workflow error = %v, want a continue-as-new", err)
	}
	var carried ActorEntityInput
	if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &carried); err != nil {
		t.Fatalf("decode carried input = %v, want nil", err)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; carried.State != want {
		t.Errorf("carried state = %v, want %v", carried.State, want)
	}
	if carried.Actor != testActor {
		t.Errorf("carried actor = %v, want %v", carried.Actor, testActor)
	}
	if !carried.AutoResumeAfterCrash {
		t.Error("carried AutoResumeAfterCrash = false, want it preserved")
	}
}

// TestActorEntityCrashesOnCancellationMidTransition covers the cleanup a
// cancelled entity owes an actor no other operation could move.
func TestActorEntityCrashesOnCancellationMidTransition(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RESUMING)
	env.OnActivity(activities.Crash, mock.Anything, CrashInput{Actor: testActor, Reason: ateattr.ReasonUnknown}).
		Return(nil).Once()

	runEntity(t, env, ActorEntityInput{})

	if !temporal.IsCanceledError(env.GetWorkflowError()) {
		t.Fatalf("workflow error = %v, want a cancellation", env.GetWorkflowError())
	}
}

// TestActorEntityLeavesASettledActorAloneOnCancellation is the other half: an
// actor that is not mid-transition needs no crash, and mocking none here is
// what proves the entity does not write one.
func TestActorEntityLeavesASettledActorAloneOnCancellation(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	runEntity(t, env, ActorEntityInput{})

	if !temporal.IsCanceledError(env.GetWorkflowError()) {
		t.Fatalf("workflow error = %v, want a cancellation", env.GetWorkflowError())
	}
}

// TestActorEntitySerializesOperations covers the guard: two operations arriving
// together run one after the other, so neither loses a version check to the
// other the way concurrent callers do without an owner.
func TestActorEntitySerializesOperations(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	inFlight := &concurrencyProbe{}
	env.OnActivity(activities.Resume, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in ActorInput) (*ateapipb.ResumeActorResponse, error) {
			defer inFlight.enter()()
			return &ateapipb.ResumeActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING), Resumed: true}, nil
		}).Once()
	env.OnActivity(activities.Suspend, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in ActorInput) (*ateapipb.SuspendActorResponse, error) {
			defer inFlight.enter()()
			return &ateapipb.SuspendActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil
		}).Once()

	resumeOutcome, resume := sendUpdate(env, UpdateResume)
	suspendOutcome, suspend := sendUpdate(env, UpdateSuspend)
	runEntity(t, env, ActorEntityInput{}, func() {
		resume()
		suspend()
	})

	if inFlight.overlapped() {
		t.Error("two lifecycle steps ran at once, want them serialized")
	}
	if resumeOutcome.code() != codes.OK {
		t.Errorf("Resume outcome = %+v, want OK", resumeOutcome)
	}
	if suspendOutcome.code() != codes.OK {
		t.Errorf("Suspend outcome = %+v, want OK", suspendOutcome)
	}
}

// TestActorEntityFailsForAMissingActor covers the entity started for an actor
// that is not there: the read fails permanently, the execution fails with it,
// and the caller waiting on the update gets NOT_FOUND rather than a retry loop.
func TestActorEntityFailsForAMissingActor(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).
		Return(nil, lifecycleError(status.Errorf(codes.NotFound, "Actor %s not found", testActor))).Once()

	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	err := env.GetWorkflowError()
	if got := status.Code(StatusFromError(err)); got != codes.NotFound {
		t.Errorf("workflow error code = %v, want %v (err %v)", got, codes.NotFound, err)
	}
}

// concurrencyProbe records whether two activities were ever in flight at the
// same time. The activity mocks run on their own goroutines, so the count is
// guarded.
type concurrencyProbe struct {
	mu      sync.Mutex
	running int
	overlap bool
}

// enter marks one activity in flight and returns the function that marks it
// done.
func (p *concurrencyProbe) enter() func() {
	p.mu.Lock()
	p.running++
	if p.running > 1 {
		p.overlap = true
	}
	p.mu.Unlock()

	return func() {
		p.mu.Lock()
		p.running--
		p.mu.Unlock()
	}
}

func (p *concurrencyProbe) overlapped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.overlap
}
