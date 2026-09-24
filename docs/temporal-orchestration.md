# Temporal orchestration of the actor lifecycle

The actor lifecycle can be driven by either of two orchestrators, selected at
startup by `ate-api`'s `--orchestrator` flag. Both run the same lifecycle steps
against the same store, so an actor is readable and resumable whichever one
moved it.

  * `legacy`, the default, runs the steps in the RPC handler's goroutine. Each
    operation takes a distributed lease on the actor, derives how far a previous
    attempt got from the persisted record, and fast-forwards. If the process
    serving the operation dies, nothing picks the operation up.

  * `temporal` gives each actor a long-lived Temporal workflow that owns it. The
    handlers hand their operation to that workflow and wait for what it
    committed. The workflow serializes the operations, retries the steps, and
    recovers an actor whose worker went away without an operator.

The code lives in `cmd/ateapi/internal/actorflow`. The lifecycle steps
themselves are unchanged and still live in `cmd/ateapi/internal/controlapi`.

## What changes

The hand-rolled lifecycle is already a durable state machine: the persisted
actor is the progress record, and a re-entered operation reads it and continues.
What it has never had is something that re-enters. Three things follow from
that, and they are what this path is for.

  * **A dead orchestrator leaves no owner.** An `ate-api` pod that dies mid
    resume leaves the actor in `ACTOR_STATE_RESUMING`, a state no operation is
    accepted from, until something calls `RevertActor`. Under Temporal the
    workflow is the owner, and any worker serving the task queue continues it.

  * **Recovery is manual.** On ungraceful worker death the worker teardown moves
    a running actor to `ACTOR_STATE_CRASHED` and returns `DATA_LOSS`. Nothing
    recovers it; an operator has to call `RevertActor` to get it back to
    `ACTOR_STATE_SUSPENDED`. Under Temporal the crash is still committed, so
    readers and the crash counter see it, and the revert then runs on its own.

  * **Concurrent callers race.** Two operations on one actor race for the
    per-actor lease and the record's version, and the loser gets `ABORTED` to
    retry. The entity workflow admits one operation at a time, so the second
    waits instead of failing.

## The mapping

| Lifecycle step (`controlapi`)      | Entity workflow                                    |
|------------------------------------|----------------------------------------------------|
| `ActorWorkflow.ResumeActor`        | activity `Resume`, run by update `Resume`           |
| `ActorWorkflow.SuspendActor`       | activity `Suspend`, run by update `Suspend`         |
| `ActorWorkflow.PauseActor`         | activity `Pause`, run by update `Pause`             |
| `ActorWorkflow.RevertActor`        | activity `Revert`, run by update `Revert`, and by the recovery the `WorkerLost` signal starts |
| `ActorWorkflow.DeleteActor`        | activity `Delete`, run by update `Delete`, which completes the workflow |
| `ActorWorkflow.CrashActor`         | activity `Crash`, run by the recovery and by the cleanup a cancelled entity does |
| `ActorWorkflow.GetActor`           | activity `LoadState`, run when the entity starts and again after a step that failed |
| `WorkerWorkflow.releaseBoundActor` | signal `WorkerLost`, sent instead of writing `ACTOR_STATE_CRASHED` in place |
| the per-actor lease                | still taken inside each activity, and the entity's own guard on top |
| `store.ErrVersionConflict` → `ABORTED` → caller retries | classified retryable, so the activity retries in place |

One workflow per actor, with the actor's identity as the workflow ID:
`actor/<atespace>/<name>`. Anyone holding an actor's name can find its owner,
and a second caller's request lands on the execution that already owns it
rather than starting a rival one.

The workflow only decides. Everything that reads or writes the store, dials
atelet or talks to the scheduler happens in an activity.

## Legal transitions

An update is refused before any step runs when the state the entity last read
cannot take it. This mirrors the preconditions the steps enforce against the
record; the steps remain the authority, because the record can move under a
mirrored state.

The mirror is dropped whenever a step fails, because a step can commit a
transition before failing, and the operation after one reads the record again
before checking anything against it.

| From \ Operation            | Resume | Suspend | Pause | Revert | Delete |
|-----------------------------|:------:|:-------:|:-----:|:------:|:------:|
| `ACTOR_STATE_SUSPENDED`     | yes    | no-op   | no    | no     | yes    |
| `ACTOR_STATE_RESUMING`      | yes    | no      | no    | no     | no     |
| `ACTOR_STATE_RUNNING`       | no-op  | yes     | yes   | yes    | no     |
| `ACTOR_STATE_SUSPENDING`    | no     | yes     | no    | no     | no     |
| `ACTOR_STATE_PAUSING`       | no     | no      | yes   | no     | no     |
| `ACTOR_STATE_PAUSED`        | yes    | yes     | no-op | yes    | no     |
| `ACTOR_STATE_REVERTING`     | no     | no      | no    | yes    | no     |
| `ACTOR_STATE_CRASHED`       | no     | no      | no    | yes    | yes    |
| `ACTOR_STATE_DELETING`      | no     | no      | no    | no     | yes    |

"yes" is accepted, "no-op" is accepted and reports success without doing
anything, "no" is refused with `FAILED_PRECONDITION`. The in-progress state of
each operation is accepted so a re-entered operation fast-forwards. Revert is
the exception that is refused from `ACTOR_STATE_SUSPENDED`: it is the recovery
verb, and reporting success would hide that there was nothing to recover.

A delete carrying the request's `any_state` skips the table, the way the step
does.

## Automatic recovery

The worker teardown path signals `WorkerLost` for each actor still bound to a
worker whose pod is gone, instead of writing the crash itself. On that signal,
for an actor in `ACTOR_STATE_RUNNING` or in one of the transitional states, the
entity:

  1. runs `Crash`, which releases the worker and commits `ACTOR_STATE_CRASHED`,
     exactly as the teardown used to. A reader, the crash record and
     `ate.actor.crashes` all come off the record, and going straight to the
     revert would erase the fact that the actor crashed at all;
  2. runs `Revert`, which returns the actor to `ACTOR_STATE_SUSPENDED` with its
     last durable snapshot intact. This is the step an operator used to have to
     run by hand;
  3. runs `Resume`, but only with `--temporal-auto-resume-after-crash`. It is
     off by default: a resume books worker capacity, and an actor that crashed
     once may crash again on the state it is restored from.

An actor that was suspended or paused when the pod went away saved its state
first, and a crashed or deleting one has nothing left on the node, so worker
loss does not apply to either and the signal is logged and dropped.

The recovered actor resumes from its last durable snapshot, which is what an
operator's `RevertActor` gives today. Nothing here recovers work done since that
snapshot; a crash still loses it.

Worker loss is the only crash this recovers from. A crash atelet reports during
a step, which the step turns into `ACTOR_STATE_CRASHED` and `DATA_LOSS`, still
waits for an operator's `RevertActor`: the caller asked for that operation and
gets told it failed, and reverting behind them would answer a question nobody
asked. Extending recovery to that class of crash is a separate decision.

## Timeouts and retries

Every activity carries an explicit `StartToClose` budget, sized off what the
step talks to rather than off a house number. The long ones heartbeat, because
without it a step that is still working cannot be told from an `ate-api` pod
that went away, and because a cancellation only reaches the step on the
heartbeat response.

| Activity    | StartToClose | Heartbeat | Why                                                                     |
|-------------|--------------|-----------|-------------------------------------------------------------------------|
| `LoadState` | 30s          | no        | one row read                                                              |
| `Resume`    | 15m          | 1m        | scheduling, volumes, and pulling a snapshot over the network onto a node  |
| `Suspend`   | 30m          | 1m        | writes the guest's memory and filesystem to object storage, the largest thing the lifecycle does |
| `Pause`     | 10m          | 1m        | a checkpoint to the node's disk rather than to object storage             |
| `Revert`    | 15m          | 1m        | tears the sandbox down and discards a half-written snapshot, with no upload |
| `Delete`    | 30m          | 1m        | termination, volume deletion through the CSI controller, snapshot release |
| `Crash`     | 2m           | no        | two store writes                                                          |

Heartbeats are reported every 15 seconds against a one-minute timeout, so one
missed tick is not a lost attempt.

Three retry policies, differing only in whether anything is waiting:

  * **A caller is waiting** (every operation an RPC routes): five attempts,
    one second growing to fifteen. The gRPC caller has its own deadline, so a
    step that keeps failing has to surface the failure while the caller is
    still there to read it; retrying past that turns a returned error into a
    hung request.
  * **Nobody is waiting** (the crash, revert and resume that recovery runs):
    unbounded, one second growing to a minute. Stopping halfway would leave the
    actor in a transitional state with nothing driving it.
  * **On the way out** (the crash a cancelled entity commits): bounded like a
    request, because a cancelled entity has to go away.

A failure is classified by the gRPC code the step meant, read off the status
error the step built or off the store sentinel it passed through, the same two
shapes the legacy handlers map. `NOT_FOUND`, `FAILED_PRECONDITION`,
`INVALID_ARGUMENT`, `DATA_LOSS` and the other outcomes another attempt cannot
change fail the activity for good; `ABORTED` for a version conflict or lease
contention, and anything unclassified, are retried. The code travels on the
error's type and is turned back into a status at the RPC boundary, so a caller
sees the same code on either orchestrator.

## Determinism and versioning

`workflow.GetVersion` is called before the entity's first branch, so a later
change to the order it loads, waits and recovers in can be introduced without
breaking the executions already running on the current order.

`testdata/actor_entity_history.json` is a recorded execution covering a resume,
a worker loss with the recovery it triggers, and the delete that completes the
entity. `TestReplayEntityHistory` replays it against the current code.
`TestRecordEntityHistory` regenerates it, and its doc comment carries the
commands. `workflowcheck` runs clean on the package.

## Running it locally

Start a dev server and point `ate-api` at it:

```sh
temporal server start-dev --port 7233 --ui-port 8233
```

```sh
ate-api \
    --orchestrator=temporal \
    --temporal-address=127.0.0.1:7233 \
    --temporal-namespace=default \
    --temporal-task-queue=ate-actors
```

`--temporal-address=@env` reads `TEMPORAL_ADDRESS` instead, the way the
PostgreSQL flags read theirs, so a deployment can set it from a ConfigMap.
`--temporal-auto-resume-after-crash` turns on step 3 of the recovery above.

`ate-api` serves the task queue itself: it is the only thing that can run the
lifecycle steps, because it holds the store, the atelet dialer and the
scheduler, so a separate worker deployment would only add a hop.

An actor's owner is visible in the Temporal UI under the workflow ID
`actor/<atespace>/<name>`, and the `State` query reports what the entity
believes the record says.

## Out of scope

  * **Choosing per actor.** The orchestrator is a process-wide flag, read at
    startup. An actor half-way through an operation on one orchestrator has no
    owner on the other, so switching under traffic is not supported.
  * **Tags, the template reconciler, and the router's request parking.** They
    stay on the in-process path whichever orchestrator is selected.
  * **The data path.** Nothing here makes a request to an actor exactly-once;
    the orchestrator is about the lifecycle.
  * **New metrics.** The steps record `ate.actor.*` from inside the activities,
    so the existing instruments report both orchestrators without change.
  * **Licenses.** `hack/update/licenses.sh` cannot currently produce a record
    for `github.com/nexus-rpc/nexus-proto-annotations`, a transitive dependency
    of the Temporal SDK that ships no license file of its own. The license
    verification needs a decision on that module before this path ships.
