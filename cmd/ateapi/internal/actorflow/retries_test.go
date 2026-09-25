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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The tests here run the real activities inside the workflow environment, over
// lifecycle steps that answer from a script. That is the one arrangement in
// which an activity is retried with its attempt number, which is what the
// activities read to answer for an attempt that committed and never reported.

// stagedCall is one scripted answer from the lifecycle steps.
type stagedCall struct {
	method  string
	actor   *ateapipb.Actor
	resumed bool
	err     error
}

// stagedSteps answers each lifecycle call from a script, in order, and records
// what was asked. A call the script did not stage fails with a message naming
// it, so the test's assertions say which step ran out of turn.
type stagedSteps struct {
	mu     sync.Mutex
	script []stagedCall
	calls  []string
}

func (s *stagedSteps) next(method string) stagedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, method)
	if len(s.script) == 0 || s.script[0].method != method {
		return stagedCall{err: status.Errorf(codes.Internal,
			"unstaged call to %s (calls so far: %v)", method, s.calls)}
	}
	call := s.script[0]
	s.script = s.script[1:]
	return call
}

func (s *stagedSteps) called() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *stagedSteps) GetActor(context.Context, resources.ActorRef) (*ateapipb.Actor, error) {
	c := s.next("GetActor")
	return c.actor, c.err
}

func (s *stagedSteps) ResumeActor(
	context.Context,
	resources.ActorRef,
) (*ateapipb.Actor, bool, error) {
	c := s.next("ResumeActor")
	return c.actor, c.resumed, c.err
}

func (s *stagedSteps) SuspendActor(context.Context, resources.ActorRef) (*ateapipb.Actor, error) {
	c := s.next("SuspendActor")
	return c.actor, c.err
}

func (s *stagedSteps) PauseActor(context.Context, resources.ActorRef) (*ateapipb.Actor, error) {
	c := s.next("PauseActor")
	return c.actor, c.err
}

func (s *stagedSteps) RevertActor(context.Context, resources.ActorRef) (*ateapipb.Actor, error) {
	c := s.next("RevertActor")
	return c.actor, c.err
}

func (s *stagedSteps) DeleteActor(
	context.Context,
	resources.ActorRef,
	bool,
) (*ateapipb.Actor, error) {
	c := s.next("DeleteActor")
	return c.actor, c.err
}

func (s *stagedSteps) CrashActor(context.Context, resources.ActorRef, string, string) error {
	return s.next("CrashActor").err
}

var _ LifecycleSteps = (*stagedSteps)(nil)

// lostAttempt is what the first attempt of a step fails with when the pod
// running it died before it could report: Temporal sees a retryable failure
// and runs the step again on another pod.
var lostAttempt = status.Error(codes.Unavailable, "the pod running this attempt went away")

// newStagedEnv builds a workflow environment running the real activities over
// steps.
func newStagedEnv(t *testing.T, steps *stagedSteps) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(NewActorActivities(steps))
	return env
}

// notRevertable is the refusal the revert step gives a SUSPENDED actor.
var notRevertable = status.Errorf(codes.FailedPrecondition,
	"Actor %s is not in a revertable state (got: ACTOR_STATE_SUSPENDED)", testActor.Ref())

// TestRetriedRevertAnswersForTheAttemptThatFinished covers a revert whose first
// attempt committed SUSPENDED and died before reporting. The retry is refused
// by the step, reads the record, and finds what a finished revert leaves: that
// is the success the first attempt owed, for the recovery and for an
// operator's update alike.
func TestRetriedRevertAnswersForTheAttemptThatFinished(t *testing.T) {
	suspended := actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	t.Run("automatic recovery", func(t *testing.T) {
		steps := &stagedSteps{script: []stagedCall{
			{method: "GetActor", actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING)},
			{method: "CrashActor"},
			{method: "RevertActor", err: lostAttempt},
			{method: "RevertActor", err: notRevertable},
			{method: "GetActor", actor: suspended},
		}}
		env := newStagedEnv(t, steps)

		outcome, lost := sendUpdate(env, UpdateWorkerLost, testWorkerLost)
		var snapshot StateSnapshot
		env.RegisterDelayedCallback(lost, time.Second)
		env.RegisterDelayedCallback(func() { snapshot = queryState(t, env) }, 5*time.Minute)
		env.RegisterDelayedCallback(env.CancelWorkflow, 6*time.Minute)
		env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

		if got := outcome.code(); got != codes.OK {
			t.Fatalf("WorkerLost code = %v, want OK (outcome %+v)", got, outcome)
		}
		if snapshot.RecoveryFailed {
			t.Error("RecoveryFailed = true, want the retry counted as the revert it was")
		}
		if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; snapshot.State != want {
			t.Errorf("state = %v, want %v", snapshot.State, want)
		}
		want := "[GetActor CrashActor RevertActor RevertActor GetActor]"
		if got := fmt.Sprint(steps.called()); got != want {
			t.Errorf("calls = %v, want %v: the retry reads the record after the refusal", got, want)
		}
	})

	t.Run("an operator's revert", func(t *testing.T) {
		steps := &stagedSteps{script: []stagedCall{
			{method: "GetActor", actor: actorIn(ateapipb.ActorState_ACTOR_STATE_CRASHED)},
			{method: "RevertActor", err: lostAttempt},
			{method: "RevertActor", err: notRevertable},
			{method: "GetActor", actor: suspended},
		}}
		env := newStagedEnv(t, steps)

		outcome, revert := sendUpdate(env, UpdateRevert, OperationInput{})
		env.RegisterDelayedCallback(revert, time.Second)
		env.RegisterDelayedCallback(env.CancelWorkflow, 5*time.Minute)
		env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

		if got := outcome.code(); got != codes.OK {
			t.Fatalf("Revert code = %v, want OK: the actor is reverted (outcome %+v)", got, outcome)
		}
		reverted, ok := outcome.result.(*ateapipb.RevertActorResponse)
		if !ok {
			t.Fatalf("Revert result = %T, want *ateapipb.RevertActorResponse", outcome.result)
		}
		if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; stateOf(reverted.GetActor()) != want {
			t.Errorf("answered state = %v, want %v", stateOf(reverted.GetActor()), want)
		}
	})

	t.Run("a record still mid-revert is not counted", func(t *testing.T) {
		halfway := actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
		halfway.Status.InProgressSnapshotUri = "gs://snapshots/half-written"
		steps := &stagedSteps{script: []stagedCall{
			{method: "GetActor", actor: actorIn(ateapipb.ActorState_ACTOR_STATE_CRASHED)},
			{method: "RevertActor", err: lostAttempt},
			{method: "RevertActor", err: notRevertable},
			{method: "GetActor", actor: halfway},
		}}
		env := newStagedEnv(t, steps)

		outcome, revert := sendUpdate(env, UpdateRevert, OperationInput{})
		env.RegisterDelayedCallback(revert, time.Second)
		env.RegisterDelayedCallback(env.CancelWorkflow, 5*time.Minute)
		env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

		if got := outcome.code(); got != codes.FailedPrecondition {
			t.Errorf("Revert code = %v, want %v: the refusal stands (outcome %+v)",
				got, codes.FailedPrecondition, outcome)
		}
	})
}

// TestRetriedDeleteAnswersForTheAttemptThatFinished covers a delete whose first
// attempt removed the row and died before reporting. The retry finds no record,
// which for this entity means the delete happened: the caller gets success and
// the execution completes with the record it owned.
func TestRetriedDeleteAnswersForTheAttemptThatFinished(t *testing.T) {
	steps := &stagedSteps{script: []stagedCall{
		{method: "GetActor", actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)},
		{method: "DeleteActor", err: lostAttempt},
		{
			method: "DeleteActor",
			err:    status.Errorf(codes.NotFound, "Actor %s not found", testActor.Ref()),
		},
	}}
	env := newStagedEnv(t, steps)

	outcome, del := sendUpdate(env, UpdateDelete, DeleteOperationInput{})
	env.RegisterDelayedCallback(del, time.Second)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Errorf("workflow error = %v, want nil: the actor is deleted", err)
	}
	if got := outcome.code(); got != codes.OK {
		t.Fatalf("Delete code = %v, want OK (outcome %+v)", got, outcome)
	}
	deleted, ok := outcome.result.(*ateapipb.Actor)
	if !ok || deleted.GetMetadata().GetName() != testActor.Name {
		t.Errorf("Delete result = %v, want the actor named", outcome.result)
	}
}

// TestRetriedResumeReportsTheResumeItDid covers a resume whose first attempt
// committed RUNNING and died before reporting. The step answers a running
// actor with resumed false, but on a retry that running actor is this
// activity's own work, so the caller is told it was resumed.
func TestRetriedResumeReportsTheResumeItDid(t *testing.T) {
	steps := &stagedSteps{script: []stagedCall{
		{method: "GetActor", actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)},
		{method: "ResumeActor", err: lostAttempt},
		{
			method:  "ResumeActor",
			actor:   actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			resumed: false,
		},
	}}
	env := newStagedEnv(t, steps)

	outcome, resume := sendUpdate(env, UpdateResume, OperationInput{})
	env.RegisterDelayedCallback(resume, time.Second)
	env.RegisterDelayedCallback(env.CancelWorkflow, 5*time.Minute)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	if got := outcome.code(); got != codes.OK {
		t.Fatalf("Resume code = %v, want OK (outcome %+v)", got, outcome)
	}
	resumed, ok := outcome.result.(*ateapipb.ResumeActorResponse)
	if !ok {
		t.Fatalf("Resume result = %T, want *ateapipb.ResumeActorResponse", outcome.result)
	}
	if !resumed.GetResumed() {
		t.Error(
			"Resumed = false, want true: the attempt that did not report is the one that resumed it",
		)
	}
}

// TestFirstAttemptKeepsTheStepsAnswer is the other half of the three above: on
// a first attempt the step's answer stands, because there is no earlier attempt
// to answer for.
func TestFirstAttemptKeepsTheStepsAnswer(t *testing.T) {
	steps := &stagedSteps{script: []stagedCall{
		{method: "GetActor", actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED)},
		{
			method:  "ResumeActor",
			actor:   actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			resumed: false,
		},
	}}
	env := newStagedEnv(t, steps)

	outcome, resume := sendUpdate(env, UpdateResume, OperationInput{})
	env.RegisterDelayedCallback(resume, time.Second)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)
	env.ExecuteWorkflow(ActorEntityWorkflow, ActorEntityInput{Actor: testActor})

	resumed, ok := outcome.result.(*ateapipb.ResumeActorResponse)
	if !ok {
		t.Fatalf(
			"Resume result = %T, want *ateapipb.ResumeActorResponse (outcome %+v)",
			outcome.result,
			outcome,
		)
	}
	if resumed.GetResumed() {
		t.Error("Resumed = true, want the step's own answer on a first attempt")
	}
}
