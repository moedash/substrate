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

package controlapi

import (
	"context"
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeActorRouter stands in for the orchestrator that owns the actors. It
// records what it was asked for, so a test can tell an operation that reached
// the owner from one the handler ran itself.
type fakeActorRouter struct {
	// ops lists the operations the handlers routed, in order.
	ops []string

	// actor is the record the owner reports committing.
	actor *ateapipb.Actor

	// err is what the owner reports instead.
	err error

	anyState     bool
	precondition store.DeletePreconditions
	worker       string
	sandboxClass string
	message      string
	refs         []resources.ActorRef

	// crashed is what a real owner commits before answering, which is the
	// invariant the sweep depends on before it deletes the Worker record.
	crashed func(resources.ActorRef)
}

func (f *fakeActorRouter) record(op string, actorRef resources.ActorRef) {
	f.ops = append(f.ops, op)
	f.refs = append(f.refs, actorRef)
}

func (f *fakeActorRouter) Resume(
	_ context.Context, actorRef resources.ActorRef,
) (*ateapipb.ResumeActorResponse, error) {
	f.record("Resume", actorRef)
	if f.err != nil {
		return nil, f.err
	}
	return &ateapipb.ResumeActorResponse{Actor: f.actor, Resumed: true}, nil
}

func (f *fakeActorRouter) Suspend(
	_ context.Context, actorRef resources.ActorRef,
) (*ateapipb.SuspendActorResponse, error) {
	f.record("Suspend", actorRef)
	if f.err != nil {
		return nil, f.err
	}
	return &ateapipb.SuspendActorResponse{Actor: f.actor}, nil
}

func (f *fakeActorRouter) Pause(
	_ context.Context, actorRef resources.ActorRef,
) (*ateapipb.PauseActorResponse, error) {
	f.record("Pause", actorRef)
	if f.err != nil {
		return nil, f.err
	}
	return &ateapipb.PauseActorResponse{Actor: f.actor}, nil
}

func (f *fakeActorRouter) Revert(
	_ context.Context, actorRef resources.ActorRef,
) (*ateapipb.RevertActorResponse, error) {
	f.record("Revert", actorRef)
	if f.err != nil {
		return nil, f.err
	}
	return &ateapipb.RevertActorResponse{Actor: f.actor}, nil
}

func (f *fakeActorRouter) Delete(
	_ context.Context, actorRef resources.ActorRef, anyState bool,
	precondition store.DeletePreconditions,
) (*ateapipb.Actor, error) {
	f.record("Delete", actorRef)
	f.anyState, f.precondition = anyState, precondition
	if f.err != nil {
		return nil, f.err
	}
	return f.actor, f.err
}

func (f *fakeActorRouter) WorkerLost(
	_ context.Context, actorRef resources.ActorRef, worker, sandboxClass, message string,
) error {
	f.record("WorkerLost", actorRef)
	f.worker, f.sandboxClass, f.message = worker, sandboxClass, message
	if f.err != nil {
		return f.err
	}
	if f.crashed != nil {
		f.crashed(actorRef)
	}
	return nil
}

var _ ActorRouter = (*fakeActorRouter)(nil)

// newRoutedService builds a service whose lifecycle operations belong to
// router, over a real store: the handlers still read the record, and a fake
// store would decide the outcome these tests are about.
func newRoutedService(t *testing.T, router ActorRouter) (*RPCService, store.Interface) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	s := NewRPCService(persistence, nil, nil, nil, nil, nil, nil, "", nil, nil, "", nil, nil)
	s.UseActorRouter(router)
	return s, persistence
}

// objectRef addresses the actor the routed tests act on.
func objectRef(actorRef resources.ActorRef) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name}
}

// TestRoutedLifecycleReachesTheOwner covers each lifecycle RPC handing its
// operation to whatever owns the actor, and answering with what the owner
// reported rather than with a shape of its own.
func TestRoutedLifecycleReachesTheOwner(t *testing.T) {
	ctx := context.Background()
	suspended := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: apiActorRef.Atespace, Name: apiActorRef.Name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}

	tests := []struct {
		name string
		call func(*RPCService) (*ateapipb.Actor, error)
		want string
	}{
		{
			name: "resume",
			call: func(s *RPCService) (*ateapipb.Actor, error) {
				out, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: objectRef(apiActorRef)})
				return out.GetActor(), err
			},
			want: "Resume",
		},
		{
			name: "suspend",
			call: func(s *RPCService) (*ateapipb.Actor, error) {
				out, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: objectRef(apiActorRef)})
				return out.GetActor(), err
			},
			want: "Suspend",
		},
		{
			name: "pause",
			call: func(s *RPCService) (*ateapipb.Actor, error) {
				out, err := s.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: objectRef(apiActorRef)})
				return out.GetActor(), err
			},
			want: "Pause",
		},
		{
			name: "revert",
			call: func(s *RPCService) (*ateapipb.Actor, error) {
				out, err := s.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: objectRef(apiActorRef)})
				return out.GetActor(), err
			},
			want: "Revert",
		},
		{
			name: "delete",
			call: func(s *RPCService) (*ateapipb.Actor, error) {
				return s.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: objectRef(apiActorRef)})
			},
			want: "Delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := &fakeActorRouter{actor: suspended}
			s, persistence := newRoutedService(t, router)
			// Resume reads the record before routing, so it has to be there.
			seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

			got, err := tt.call(s)
			if err != nil {
				t.Fatalf("%s = %v, want nil", tt.want, err)
			}
			if len(router.ops) != 1 || router.ops[0] != tt.want {
				t.Fatalf("router saw %v, want exactly [%s]", router.ops, tt.want)
			}
			if router.refs[0] != apiActorRef {
				t.Errorf("routed actor = %v, want %v", router.refs[0], apiActorRef)
			}
			if got.GetMetadata().GetName() != apiActorRef.Name {
				t.Errorf("answered with actor %q, want the owner's %q",
					got.GetMetadata().GetName(), apiActorRef.Name)
			}
		})
	}
}

// TestRoutedDeleteCarriesTheRequest covers the request's flag and guard
// reaching the owner: the flag is the only way it can honor a delete from a
// state the state machine refuses, and the guard is what the step checks the
// record against.
func TestRoutedDeleteCarriesTheRequest(t *testing.T) {
	ctx := context.Background()
	router := &fakeActorRouter{actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: apiActorRef.Atespace, Name: apiActorRef.Name},
	}}
	s, _ := newRoutedService(t, router)

	req := &ateapipb.DeleteActorRequest{
		Actor:    objectRef(apiActorRef),
		AnyState: true,
		Options:  &ateapipb.DeleteOptions{Uid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Version: 7},
	}
	if _, err := s.DeleteActor(ctx, req); err != nil {
		t.Fatalf("DeleteActor = %v, want nil", err)
	}
	if !router.anyState {
		t.Error("routed anyState = false, want the request's flag carried through")
	}
	want := store.DeletePreconditions{UID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Version: 7}
	if router.precondition != want {
		t.Errorf("routed precondition = %+v, want %+v carried through", router.precondition, want)
	}
}

// TestRoutedResumeAnswersARunningActorFromTheRecord covers the hot path: the
// router resumes per routed request, and an actor that is already running is
// answered here rather than by a round trip to its owner.
func TestRoutedResumeAnswersARunningActorFromTheRecord(t *testing.T) {
	ctx := context.Background()
	router := &fakeActorRouter{}
	s, persistence := newRoutedService(t, router)
	seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	got, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: objectRef(apiActorRef)})
	if err != nil {
		t.Fatalf("ResumeActor = %v, want nil", err)
	}
	if len(router.ops) != 0 {
		t.Errorf("router saw %v, want nothing: the actor is already running", router.ops)
	}
	if got.GetResumed() {
		t.Error("Resumed = true, want false: this call did not resume anything")
	}
	if want := ateapipb.ActorState_ACTOR_STATE_RUNNING; got.GetActor().GetStatus().GetState() != want {
		t.Errorf("state = %v, want %v", got.GetActor().GetStatus().GetState(), want)
	}
}

// TestRoutedResumeReportsAMissingActor covers the read before the route: an
// actor that is not there is NOT_FOUND, the same code the in-process
// orchestrator returns, and no owner is started for it.
func TestRoutedResumeReportsAMissingActor(t *testing.T) {
	ctx := context.Background()
	router := &fakeActorRouter{}
	s, _ := newRoutedService(t, router)

	_, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: objectRef(apiActorRef)})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("ResumeActor code = %v, want %v (err %v)", got, codes.NotFound, err)
	}
	if len(router.ops) != 0 {
		t.Errorf("router saw %v, want nothing for an actor that is not there", router.ops)
	}
}

// TestRoutedLifecycleReportsTheOwnersFailure covers the owner's error reaching
// the caller as it stands: the owner classified it, and re-deriving a code here
// would report a different reason for the same failure.
func TestRoutedLifecycleReportsTheOwnersFailure(t *testing.T) {
	ctx := context.Background()
	router := &fakeActorRouter{err: status.Error(codes.FailedPrecondition,
		"Revert is not accepted from state ACTOR_STATE_SUSPENDED")}
	s, _ := newRoutedService(t, router)

	_, err := s.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: objectRef(apiActorRef)})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("RevertActor code = %v, want %v (err %v)", got, codes.FailedPrecondition, err)
	}
}

// TestWorkerDeleteHandsALostActorToItsOwner covers the release with an
// orchestrator in play. The sweep is about to delete the last record pointing
// at the actor, so it waits for the owner to commit the crash and does not
// write the transition itself.
func TestWorkerDeleteHandsALostActorToItsOwner(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	// Stands in for the owner: it commits the crash before answering, the way
	// the entity workflow's WorkerLost update does.
	var crashedAt ateapipb.ActorState
	router := &fakeActorRouter{}
	router.crashed = func(actorRef resources.ActorRef) {
		actor, err := persistence.GetActor(ctx, actorRef)
		if err != nil {
			t.Errorf("GetActor = %v, want nil", err)
			return
		}
		crashedAt = actor.GetStatus().GetState()
		mustUpdateActorStatus(t, ctx, persistence, actor, func(status *ateapipb.ActorStatus) {
			status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
			status.WorkerAssignment = nil
		})
	}
	wf := NewWorkerWorkflow(persistence)
	wf.UseWorkerLostNotifier(router)

	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker = %v, want nil", err)
	}

	if len(router.ops) != 1 || router.ops[0] != "WorkerLost" {
		t.Fatalf("router saw %v, want exactly [WorkerLost]", router.ops)
	}
	if router.worker != apiWorkerName {
		t.Errorf("reported worker = %q, want %q", router.worker, apiWorkerName)
	}
	// Read off the live Worker record, which this sweep is about to delete.
	if want := validWorker(apiWorkerName).GetSandboxClass(); router.sandboxClass != want {
		t.Errorf("reported sandbox class = %q, want %q", router.sandboxClass, want)
	}
	if router.message != crashMessageWorkerPodGone {
		t.Errorf("reported message = %q, want %q", router.message, crashMessageWorkerPodGone)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_RUNNING; crashedAt != want {
		t.Errorf("state when the owner was called = %v, want %v untouched by the sweep", crashedAt, want)
	}
	got, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor = %v, want nil", err)
	}
	if want := ateapipb.ActorState_ACTOR_STATE_CRASHED; got.GetStatus().GetState() != want {
		t.Errorf("actor state = %v, want %v committed before the sweep returned",
			got.GetStatus().GetState(), want)
	}
}

// TestWorkerDeleteStopsWhenTheOwnerCannotBeReached covers the sweep giving up:
// the worker record stays, with the actor still bound to it, so a retry finds
// them both and the actor is not left pointing at a worker that is gone.
func TestWorkerDeleteStopsWhenTheOwnerCannotBeReached(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	router := &fakeActorRouter{err: errors.New("Temporal is unreachable")}
	wf := NewWorkerWorkflow(persistence)
	wf.UseWorkerLostNotifier(router)

	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err == nil {
		t.Fatal("DeleteWorker = nil, want the failure to reach the caller")
	}
	if _, err := persistence.GetWorker(ctx, apiWorkerName); err != nil {
		t.Errorf("GetWorker = %v, want the record left in place for a retry", err)
	}
	if got := firstAssignment(t, persistence, apiWorkerName); got == nil {
		t.Error("worker assignment = nil, want the actor still bound for a retry")
	}
}

// TestWorkerDeleteLeavesASuspendedActorAloneWithAnOwner covers the skip that
// still applies: an actor that suspended cleanly before the pod went away is
// not on the worker any more, so there is nothing for its owner to recover.
func TestWorkerDeleteLeavesASuspendedActorAloneWithAnOwner(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	router := &fakeActorRouter{}
	wf := NewWorkerWorkflow(persistence)
	wf.UseWorkerLostNotifier(router)

	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker = %v, want nil", err)
	}
	if len(router.ops) != 0 {
		t.Errorf("router saw %v, want nothing for an actor that suspended cleanly", router.ops)
	}
}

// TestReconcileAssignmentsHandsALostActorToItsOwner covers the other sweep
// that finds actors on a lost worker: an ateom restart raises the worker's
// epoch, and the actors placed before it are the owner's to crash, with the
// cause that sweep knows.
func TestReconcileAssignmentsHandsALostActorToItsOwner(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	router := &fakeActorRouter{}
	router.crashed = func(actorRef resources.ActorRef) {
		actor, err := persistence.GetActor(ctx, actorRef)
		if err != nil {
			t.Errorf("GetActor = %v, want nil", err)
			return
		}
		mustUpdateActorStatus(t, ctx, persistence, actor, func(status *ateapipb.ActorStatus) {
			status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
			status.WorkerAssignment = nil
		})
	}
	reconciler := NewWorkerAssignmentReconciler(persistence, noWorkerWatcher{})
	reconciler.UseWorkerLostNotifier(router)

	seedEpochWorker(t, ctx, persistence, 1, 1)
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	if err := reconciler.reconcileOne(ctx, apiWorkerName); err != nil {
		t.Fatalf("reconcileOne = %v, want nil", err)
	}

	if len(router.ops) != 1 || router.ops[0] != "WorkerLost" {
		t.Fatalf("router saw %v, want exactly [WorkerLost]", router.ops)
	}
	if router.worker != apiWorkerName {
		t.Errorf("reported worker = %q, want %q", router.worker, apiWorkerName)
	}
	if router.message != crashMessageAteomRestarted {
		t.Errorf("reported message = %q, want %q", router.message, crashMessageAteomRestarted)
	}
	if a := firstAssignment(t, persistence, apiWorkerName); a != nil {
		t.Errorf("worker still hosts %v, want the assignment released once the owner answered", a)
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch = %d, want 2", got)
	}
}

// noWorkerWatcher is a worker watcher nothing subscribes through, for a
// reconciler a test drives by hand.
type noWorkerWatcher struct{}

func (noWorkerWatcher) AddHandler(func(*ateapipb.Worker)) {}

// TestRoutedLifecycleStampsTheSpan covers the trace attributes the in-process
// handlers stamp from the record they committed. A routed handler answers with
// the owner's record, so it has to stamp the same thing, or a routed control
// plane loses actor identity on every lifecycle span.
func TestRoutedLifecycleStampsTheSpan(t *testing.T) {
	committed := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: apiActorRef.Atespace,
			Name:     apiActorRef.Name,
			Uid:      "actor-uid-1",
		},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}

	tests := []struct {
		name string
		call func(context.Context, *RPCService) error
	}{
		{
			name: "suspend",
			call: func(ctx context.Context, s *RPCService) error {
				_, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: objectRef(apiActorRef)})
				return err
			},
		},
		{
			name: "pause",
			call: func(ctx context.Context, s *RPCService) error {
				_, err := s.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: objectRef(apiActorRef)})
				return err
			},
		},
		{
			name: "revert",
			call: func(ctx context.Context, s *RPCService) error {
				_, err := s.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: objectRef(apiActorRef)})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newRoutedService(t, &fakeActorRouter{actor: committed})
			attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
				if err := tt.call(ctx, s); err != nil {
					t.Fatalf("%s = %v, want nil", tt.name, err)
				}
			})
			assertSpanStr(t, attrs, ateattr.ActorUIDKey, "actor-uid-1")
			assertSpanStr(t, attrs, ateattr.ActorNameKey, apiActorRef.Name)
		})
	}
}
