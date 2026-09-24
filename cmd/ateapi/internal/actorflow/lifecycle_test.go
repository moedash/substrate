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

// The tests here drive the entity workflow with the real lifecycle steps
// behind its activities, against a real store. They live in the external test
// package because the lifecycle imports the orchestrator's interfaces, not the
// other way round.
package actorflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/actorflow"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.temporal.io/sdk/testsuite"
)

const (
	testAtespace = "team-a"
	testTemplate = "tmpl"
	testWorker   = "worker-1"
)

var testActorRef = resources.ActorRef{Atespace: testAtespace, Name: "actor-1"}

// newLifecycle builds the lifecycle steps over a real store, with the
// dependencies these tests never reach left nil, so a step that unexpectedly
// dials atelet or asks the scheduler for a worker fails loudly.
func newLifecycle(t *testing.T) (*controlapi.ActorWorkflow, store.Interface) {
	t.Helper()
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	storetest.MustCreateAtespace(t, ctx, persistence, testAtespace)
	if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata:        &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testTemplate},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://snapshots"},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor",
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	steps := controlapi.NewActorWorkflow(persistence, nil, nil, nil, nil, nil, "", nil, objectstoretest.New())
	return steps, persistence
}

// seedRunningActor stores an actor on a worker whose pod has since gone away,
// which is the state worker loss finds it in.
func seedRunningActor(t *testing.T, persistence store.Interface) {
	t.Helper()
	storetest.MustCreateActor(t, context.Background(), persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testActorRef.Atespace, Name: testActorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testTemplate},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: testWorker},
				WorkerNamespace: "ate-system",
				WorkerPool:      "pool-1",
				WorkerPod:       "worker-pod-1",
				WorkerPodUid:    testWorker,
				WorkerPodIp:     "10.1.2.3",
			},
		},
	})
}

// TestRecoveryDrivesTheRealLifecycle is automatic recovery end to end: the
// entity is told its actor's worker is gone, and the crash and the revert that
// follow are the lifecycle's own steps, committing to a real store.
//
// The in-process orchestrator stops at the crash and waits for an operator to
// revert, so the state this leaves the record in is the difference the
// orchestrator makes.
func TestRecoveryDrivesTheRealLifecycle(t *testing.T) {
	ctx := context.Background()
	steps, persistence := newLifecycle(t)
	seedRunningActor(t, persistence)

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(actorflow.NewActorActivities(steps))

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actorflow.SignalWorkerLost, actorflow.WorkerLostInput{Worker: testWorker})
	}, time.Second)
	env.RegisterDelayedCallback(env.CancelWorkflow, 2*time.Second)

	env.ExecuteWorkflow(actorflow.ActorEntityWorkflow, actorflow.ActorEntityInput{
		Actor: actorflow.ActorKey{Atespace: testActorRef.Atespace, Name: testActorRef.Name},
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	got, err := persistence.GetActor(ctx, testActorRef)
	if err != nil {
		t.Fatalf("GetActor = %v, want nil", err)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; got.GetStatus().GetState() != want {
		t.Errorf("state after recovery = %v, want %v", got.GetStatus().GetState(), want)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
	}
}

// TestDeleteDrivesTheRealLifecycle covers the update that ends an entity, with
// the real teardown behind it: the record is gone, and so is the execution that
// owned it.
func TestDeleteDrivesTheRealLifecycle(t *testing.T) {
	ctx := context.Background()
	steps, persistence := newLifecycle(t)
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testActorRef.Atespace, Name: testActorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testTemplate},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(actorflow.NewActorActivities(steps))

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(actorflow.UpdateDelete, "delete-1", t, false)
	}, time.Second)

	env.ExecuteWorkflow(actorflow.ActorEntityWorkflow, actorflow.ActorEntityInput{
		Actor: actorflow.ActorKey{Atespace: testActorRef.Atespace, Name: testActorRef.Name},
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not finish")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil: a deleted actor ends its entity cleanly", err)
	}
	if _, err := persistence.GetActor(ctx, testActorRef); err == nil {
		t.Error("GetActor = nil error, want the record gone")
	}
}

// TestEntityRefusesAnIllegalTransitionAgainstTheRealLifecycle checks the
// validator's table against the preconditions the steps enforce: a revert is
// refused from SUSPENDED by both, so the caller sees the same answer whether
// the entity caught it or the step did.
func TestEntityRefusesAnIllegalTransitionAgainstTheRealLifecycle(t *testing.T) {
	ctx := context.Background()
	steps, persistence := newLifecycle(t)
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testActorRef.Atespace, Name: testActorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testTemplate},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	// The step's own answer, for the entity's to be checked against.
	if _, err := steps.RevertActor(ctx, testActorRef); err == nil {
		t.Fatal("RevertActor on a suspended actor = nil, want it refused")
	}

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(actorflow.NewActorActivities(steps))

	var rejected error
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(actorflow.UpdateRevert, "revert-1", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { rejected = err },
			OnAccept:   func() {},
			OnComplete: func(any, error) {},
		})
	}, time.Second)
	env.RegisterDelayedCallback(env.CancelWorkflow, 2*time.Second)

	env.ExecuteWorkflow(actorflow.ActorEntityWorkflow, actorflow.ActorEntityInput{
		Actor: actorflow.ActorKey{Atespace: testActorRef.Atespace, Name: testActorRef.Name},
	})

	if rejected == nil {
		t.Fatal("Revert from SUSPENDED was not rejected, want it refused before the step runs")
	}
	got, err := persistence.GetActor(ctx, testActorRef)
	if err != nil {
		t.Fatalf("GetActor = %v, want nil", err)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_SUSPENDED; got.GetStatus().GetState() != want {
		t.Errorf("state = %v, want %v: a refused operation writes nothing", got.GetStatus().GetState(), want)
	}
}
