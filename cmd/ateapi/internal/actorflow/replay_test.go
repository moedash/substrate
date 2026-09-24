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
	"os"
	"testing"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// entityHistoryFile holds a recorded execution of the entity workflow: a
// resume, a worker loss with the recovery that follows, and the delete that
// completes it.
const entityHistoryFile = "testdata/actor_entity_history.json"

// TestReplayEntityHistory replays a recorded execution against the current
// workflow code.
//
// It fails on a change that reorders or removes a decision an entity already
// running has made, which unit tests cannot see: they only ever run the code
// they were written against. Regenerate the history with
// TestRecordEntityHistory when the decision sequence changes on purpose, and
// reach for workflow.GetVersion when it must not.
func TestReplayEntityHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(ActorEntityWorkflow)

	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, entityHistoryFile); err != nil {
		t.Fatalf("replaying %s = %v, want nil", entityHistoryFile, err)
	}
}

// recordHistoryEnv names the dev server TestRecordEntityHistory records
// against.
const recordHistoryEnv = "ACTORFLOW_DEV_SERVER"

// TestRecordEntityHistory records the history TestReplayEntityHistory runs
// against. It needs a server, so it is skipped unless one is named:
//
//	temporal server start-dev --port 7477 --ui-port 8477 --headless
//	ACTORFLOW_DEV_SERVER=localhost:7477 go test ./cmd/ateapi/internal/actorflow \
//	    -run TestRecordEntityHistory -count 1
//	temporal workflow show -w actor/team-a/replay-actor --address localhost:7477 \
//	    -o json > cmd/ateapi/internal/actorflow/testdata/actor_entity_history.json
//
// The steps behind the activities are scripted rather than real: the history is
// about the decisions the entity made, and a real lifecycle would need a
// cluster to make the same ones.
func TestRecordEntityHistory(t *testing.T) {
	address := os.Getenv(recordHistoryEnv)
	if address == "" {
		t.Skipf("set %s to a dev server to record a new history", recordHistoryEnv)
	}
	ctx := context.Background()
	actor := ActorKey{Atespace: "team-a", Name: "replay-actor"}

	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		t.Fatalf("dialing %s = %v, want nil", address, err)
	}
	defer c.Close()

	const taskQueue = "actorflow-replay-recording"
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(ActorEntityWorkflow)
	w.RegisterActivity(NewActorActivities(&scriptedSteps{}))
	if err := w.Start(); err != nil {
		t.Fatalf("starting the worker = %v, want nil", err)
	}
	defer w.Stop()

	router := NewRouter(c, taskQueue, false)
	ref := actor.Ref()

	// A resume, so the history carries an update and the activity it ran.
	if _, err := router.Resume(ctx, ref); err != nil {
		t.Fatalf("Resume = %v, want nil", err)
	}
	// Worker loss, so it carries the signal and the recovery that follows.
	if err := router.WorkerLost(ctx, ref, "worker-1"); err != nil {
		t.Fatalf("WorkerLost = %v, want nil", err)
	}
	// Recovery is asynchronous, so wait for the state it leaves behind before
	// the delete, or the delete races it.
	for {
		state, err := router.State(ctx, ref)
		if err != nil {
			t.Fatalf("State = %v, want nil", err)
		}
		if state.State == ateapipb.ActorState_ACTOR_STATE_SUSPENDED && !state.Busy {
			break
		}
	}
	// The delete completes the execution, so the history is a whole one.
	if _, err := router.Delete(ctx, ref, false); err != nil {
		t.Fatalf("Delete = %v, want nil", err)
	}

	t.Logf("recorded %s on %s; export it with: temporal workflow show -w %s --address %s -o json > %s",
		actor.WorkflowID(), address, actor.WorkflowID(), address, entityHistoryFile)
}

// scriptedSteps answers the way a lifecycle would, without a cluster to answer
// from. The state it reports is the one the step it stands in for commits.
type scriptedSteps struct{}

func (s *scriptedSteps) actor(state ateapipb.ActorState, ref resources.ActorRef) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name, Uid: "replay-actor-uid"},
		Status:   &ateapipb.ActorStatus{State: state},
	}
}

func (s *scriptedSteps) GetActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	return s.actor(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ref), nil
}

func (s *scriptedSteps) ResumeActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, bool, error) {
	return s.actor(ateapipb.ActorState_ACTOR_STATE_RUNNING, ref), true, nil
}

func (s *scriptedSteps) SuspendActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	return s.actor(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ref), nil
}

func (s *scriptedSteps) PauseActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	return s.actor(ateapipb.ActorState_ACTOR_STATE_PAUSED, ref), nil
}

func (s *scriptedSteps) RevertActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	return s.actor(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ref), nil
}

func (s *scriptedSteps) DeleteActor(_ context.Context, ref resources.ActorRef, _ bool) (*ateapipb.Actor, error) {
	return s.actor(ateapipb.ActorState_ACTOR_STATE_DELETING, ref), nil
}

func (s *scriptedSteps) CrashActor(context.Context, resources.ActorRef, string) error {
	return nil
}

var _ LifecycleSteps = (*scriptedSteps)(nil)
