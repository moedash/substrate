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

// testWorkerLost is the worker-loss report the tests send, with the class the
// sweep read off the Worker record before deleting it.
var testWorkerLost = WorkerLostInput{Worker: "worker-7", SandboxClass: "gvisor"}

// crashOf is the crash the entity commits for a worker-loss report.
func crashOf(in WorkerLostInput) CrashInput {
	return CrashInput{Actor: testActor, Reason: ateattr.ReasonWorkerPodGone, SandboxClass: in.SandboxClass}
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
// workflow clock and canceling at the end: an entity only completes when its
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
			env.OnActivity(activities.Crash, mock.Anything, crashOf(testWorkerLost)).
				Return(nil).Once()
			env.OnActivity(activities.Revert, mock.Anything, ActorInput{Actor: testActor}).
				Return(&ateapipb.RevertActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()

			outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
			var snapshot StateSnapshot
			runEntity(t, env, ActorEntityInput{}, lost, func() { snapshot = queryState(t, env) })

			// The caller is about to delete the Worker record, so it has to be
			// told the crash is durable before it does.
			if got := outcome.code(); got != codes.OK {
				t.Fatalf("WorkerLost code = %v, want OK (outcome %+v)", got, outcome)
			}
			if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; snapshot.State != want {
				t.Errorf("state after recovery = %v, want %v", snapshot.State, want)
			}
			if snapshot.RecoveryPending || snapshot.RecoveryFailed {
				t.Errorf("snapshot = %+v, want the recovery finished", snapshot)
			}
		})
	}
}

// TestActorEntityReportsAFailedCrashToTheSweep covers the answer the sweep acts
// on: it is about to delete the last thing pointing at the actor, so a crash it
// could not commit has to come back as a failure.
func TestActorEntityReportsAFailedCrashToTheSweep(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Crash, mock.Anything, mock.Anything).
		Return(lifecycleError(status.Error(codes.Unavailable, "store is down")))

	outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
	var snapshot StateSnapshot
	runEntity(t, env, ActorEntityInput{}, lost, func() { snapshot = queryState(t, env) })

	if got := outcome.code(); got == codes.OK {
		t.Fatalf("WorkerLost code = OK, want the failure reported (outcome %+v)", outcome)
	}
	if snapshot.RecoveryPending {
		t.Error("RecoveryPending = true, want nothing owed for a crash that was never committed")
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

	_, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
	var snapshot StateSnapshot
	runEntity(t, env, ActorEntityInput{AutoResumeAfterCrash: true}, lost,
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

			outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
			var snapshot StateSnapshot
			runEntity(t, env, ActorEntityInput{}, lost, func() { snapshot = queryState(t, env) })

			// Reported as success: there is nothing to recover, and the sweep
			// has to be free to delete the Worker record.
			if got := outcome.code(); got != codes.OK {
				t.Errorf("WorkerLost code = %v, want OK (outcome %+v)", got, outcome)
			}
			if snapshot.State != from {
				t.Errorf("state = %v, want %v: worker loss does not apply here", snapshot.State, from)
			}
		})
	}
}

// TestActorEntityRecoveryStopsWhenTheCrashFails covers the recovery not
// reverting an actor it could not crash first: the revert would hide a crash
// nothing ever recorded.
func TestActorEntityRecoveryStopsWhenTheCrashFails(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Crash, mock.Anything, mock.Anything).
		Return(temporal.NewNonRetryableApplicationError("gone", errorTypePrefix+codes.NotFound.String(), nil)).Once()

	outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
	var snapshot StateSnapshot
	runEntity(t, env, ActorEntityInput{}, lost, func() { snapshot = queryState(t, env) })

	if got := outcome.code(); got != codes.NotFound {
		t.Errorf("WorkerLost code = %v, want %v (outcome %+v)", got, codes.NotFound, outcome)
	}
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

// TestActorEntityReportsAMissingActorToTheCaller covers an entity started for
// an actor that is not there: the caller waiting on the update gets NOT_FOUND,
// the code the in-process orchestrator returns, and the execution completes
// cleanly because there is nothing for it to own.
func TestActorEntityReportsAMissingActorToTheCaller(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).
		Return(nil, lifecycleError(status.Errorf(codes.NotFound, "Actor %s not found", testActor))).Once()

	outcome, resume := sendUpdate(env, UpdateResume)
	// Delivered with the start, the way update-with-start delivers one, so it
	// is waiting while the entity reads the record.
	env.RegisterDelayedCallback(resume, 0)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Errorf("workflow error = %v, want nil: an entity with nothing to own is not a failure", err)
	}
	if got := outcome.code(); got != codes.NotFound {
		t.Errorf("Resume code = %v, want %v (outcome %+v)", got, codes.NotFound, outcome)
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

// TestActorEntityRereadsAfterAFailedStep covers what the operation after a
// failed one has to do: a step that failed may have committed a transition
// before it did, so the mirror it left behind is worthless and the record is
// read again.
func TestActorEntityRereadsAfterAFailedStep(t *testing.T) {
	env := newEnv(t)
	// Twice: once for the entity, once for the operation that follows the
	// failure and cannot trust what the failure left behind.
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).
		Return(statusIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED), nil).Twice()
	env.OnActivity(activities.Resume, mock.Anything, mock.Anything).
		Return(nil, lifecycleError(status.Error(codes.DataLoss, "actor crashed"))).Once()
	env.OnActivity(activities.Suspend, mock.Anything, mock.Anything).
		Return(&ateapipb.SuspendActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()

	resumeOutcome, resume := sendUpdate(env, UpdateResume)
	suspendOutcome, suspend := sendUpdate(env, UpdateSuspend)
	runEntity(t, env, ActorEntityInput{}, resume, suspend)

	if got := resumeOutcome.code(); got != codes.DataLoss {
		t.Errorf("Resume code = %v, want %v (outcome %+v)", got, codes.DataLoss, resumeOutcome)
	}
	if got := suspendOutcome.code(); got != codes.OK {
		t.Fatalf("Suspend code = %v, want OK: the operation after a failure still runs (outcome %+v)", got, suspendOutcome)
	}
}

// TestActorEntityServesAnUpdateOnItsFirstTask covers an update delivered with
// the entity's first workflow task, the way update-with-start delivers one: it
// waits for the read the entity opens with and is served from it.
//
// A second read of the same record is what the replay test catches: this
// environment delivers the update on a task of its own, so it cannot see the
// two coroutines reach the read together.
func TestActorEntityServesAnUpdateOnItsFirstTask(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).
		Return(statusIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED), nil).Once()
	env.OnActivity(activities.Resume, mock.Anything, mock.Anything).
		Return(&ateapipb.ResumeActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING), Resumed: true}, nil).Once()

	outcome, resume := sendUpdate(env, UpdateResume)
	// Zero delay, so the update lands with the entity's first workflow task,
	// the way update-with-start delivers one.
	env.RegisterDelayedCallback(resume, 0)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if got := outcome.code(); got != codes.OK {
		t.Errorf("Resume code = %v, want OK (outcome %+v)", got, outcome)
	}
}

// TestActorEntitySurvivesAnUnreadableRecord covers the read an entity opens
// with failing and then succeeding.
//
// Failing the execution here would be the worst outcome available: whatever
// started this entity has been told the actor has an owner, and a worker-loss
// report queued behind the read would die with the execution while the sweep
// that sent it has already deleted the Worker record.
func TestActorEntitySurvivesAnUnreadableRecord(t *testing.T) {
	env := newEnv(t)
	unreachable := lifecycleError(status.Error(codes.Unavailable, "store is down"))
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).Return(nil, unreachable).Times(3)
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).
		Return(statusIn(ateapipb.ActorState_ACTOR_STATE_RUNNING), nil).Once()
	env.OnActivity(activities.Crash, mock.Anything, crashOf(testWorkerLost)).Return(nil).Once()
	env.OnActivity(activities.Revert, mock.Anything, mock.Anything).
		Return(&ateapipb.RevertActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()

	outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
	var snapshot StateSnapshot
	// The report arrives while the record is still unreadable, and the entity
	// is given longer than the backoff needs before anything cancels it.
	env.RegisterDelayedCallback(lost, time.Second)
	env.RegisterDelayedCallback(func() { snapshot = queryState(t, env) }, 5*time.Minute)
	env.RegisterDelayedCallback(env.CancelWorkflow, 6*time.Minute)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if !temporal.IsCanceledError(env.GetWorkflowError()) {
		t.Fatalf("workflow error = %v, want a cancellation: the entity has to outlive the outage", env.GetWorkflowError())
	}
	if got := outcome.code(); got != codes.OK {
		t.Fatalf("WorkerLost code = %v, want OK once the record could be read (outcome %+v)", got, outcome)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; snapshot.State != want {
		t.Errorf("state after recovery = %v, want %v", snapshot.State, want)
	}
}

// TestActorEntityCompletesWhenThereIsNoRecord covers the one answer that is not
// worth waiting out: an actor that is not there is nothing to own, so the
// entity completes instead of retrying a read that will never succeed.
func TestActorEntityCompletesWhenThereIsNoRecord(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).
		Return(nil, lifecycleError(status.Errorf(codes.NotFound, "Actor %s not found", testActor))).Once()

	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil: there is nothing to own", err)
	}
}

// TestActorEntityRecoveryGivesUpAndReleasesTheEntity covers the budget on a
// recovery step. Retrying a failure that never reclassifies would hold the one
// operation slot for as long as the entity lives, and an operator could not
// even delete the actor.
func TestActorEntityRecoveryGivesUpAndReleasesTheEntity(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Crash, mock.Anything, mock.Anything).Return(nil).Once()
	// Unbounded by count and retryable, so only the budget stops it.
	env.OnActivity(activities.Revert, mock.Anything, mock.Anything).
		Return(nil, lifecycleError(status.Error(codes.Internal, "the store keeps failing")))
	env.OnActivity(activities.LoadState, mock.Anything, mock.Anything).
		Return(statusIn(ateapipb.ActorState_ACTOR_STATE_CRASHED), nil).Once()
	env.OnActivity(activities.Delete, mock.Anything, DeleteInput{Actor: testActor}).
		Return(actorIn(ateapipb.ActorState_ACTOR_STATE_DELETING), nil).Once()

	_, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
	deleteOutcome, del := sendUpdate(env, UpdateDelete, false)

	env.RegisterDelayedCallback(lost, time.Second)
	// Well past the recovery budget, so the give-up has happened by now.
	env.RegisterDelayedCallback(del, recoveryBudget+time.Minute)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if got := deleteOutcome.code(); got != codes.OK {
		t.Fatalf("Delete code = %v, want OK: a recovery that gave up must not hold the entity (outcome %+v)", got, deleteOutcome)
	}
}

// TestActorEntityRecoveryRetriesARevert covers the recovery outlasting a
// failure that clears. Nobody is waiting on it, so a revert that fails once
// has to be tried again rather than handed to an operator.
func TestActorEntityRecoveryRetriesARevert(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Crash, mock.Anything, crashOf(testWorkerLost)).Return(nil).Once()
	env.OnActivity(activities.Revert, mock.Anything, mock.Anything).
		Return(nil, lifecycleError(status.Error(codes.Unavailable, "store is down"))).Once()
	env.OnActivity(activities.Revert, mock.Anything, mock.Anything).
		Return(&ateapipb.RevertActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()

	outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
	var snapshot StateSnapshot
	// Read well after the retry backoff, so the assertion is about the
	// recovery and not about how long a retry takes.
	env.RegisterDelayedCallback(lost, time.Second)
	env.RegisterDelayedCallback(func() { snapshot = queryState(t, env) }, 5*time.Minute)
	env.RegisterDelayedCallback(env.CancelWorkflow, 6*time.Minute)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if got := outcome.code(); got != codes.OK {
		t.Fatalf("WorkerLost code = %v, want OK (outcome %+v)", got, outcome)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; snapshot.State != want {
		t.Errorf("state after recovery = %v, want %v", snapshot.State, want)
	}
	if snapshot.RecoveryFailed {
		t.Error("RecoveryFailed = true, want the retry to have carried it through")
	}
}

// TestActorEntityWorkerLossRetriesACrash covers the same for the crash, which
// the sweep is waiting on: a failure that clears inside the caller's budget is
// retried rather than reported.
func TestActorEntityWorkerLossRetriesACrash(t *testing.T) {
	env := newEnv(t)
	onLoadState(env, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	env.OnActivity(activities.Crash, mock.Anything, mock.Anything).
		Return(lifecycleError(status.Error(codes.Aborted, "concurrent update conflict"))).Once()
	env.OnActivity(activities.Crash, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(activities.Revert, mock.Anything, mock.Anything).
		Return(&ateapipb.RevertActorResponse{Actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)}, nil).Once()

	outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
	var snapshot StateSnapshot
	// Read well after the retry backoff, so the assertion is about the
	// recovery and not about how long a retry takes.
	env.RegisterDelayedCallback(lost, time.Second)
	env.RegisterDelayedCallback(func() { snapshot = queryState(t, env) }, 5*time.Minute)
	env.RegisterDelayedCallback(env.CancelWorkflow, 6*time.Minute)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if got := outcome.code(); got != codes.OK {
		t.Fatalf("WorkerLost code = %v, want OK once the conflict cleared (outcome %+v)", got, outcome)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; snapshot.State != want {
		t.Errorf("state after recovery = %v, want %v", snapshot.State, want)
	}
}
