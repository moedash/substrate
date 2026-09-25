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
    resume leaves the actor in `ACTOR_STATE_RESUMING`, which only another
    `ResumeActor` can move: it fast-forwards past the steps the dead attempt
    finished. Nothing asks for that on the actor's behalf, and `RevertActor`
    refuses the state, so the actor sits there until a request arrives for it.
    Under Temporal the workflow is the owner, and any worker serving the task
    queue continues the operation.

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
| `ActorWorkflow.RevertActor`        | activity `Revert`, run by update `Revert`, and by the recovery the `WorkerLost` update starts |
| `ActorWorkflow.DeleteActor`        | activity `Delete`, run by update `Delete`, which completes the workflow |
| `ActorWorkflow.CrashActor`         | activity `Crash`, run by the recovery               |
| `ActorWorkflow.GetActor`           | activity `LoadState`, run when the entity starts, after a step that failed, and before an operation is refused |
| `WorkerWorkflow.releaseBoundActor` | update `WorkerLost`, sent instead of writing `ACTOR_STATE_CRASHED` in place |
| the per-actor lease                | still taken inside each activity, and the entity's own guard on top |
| `store.ErrVersionConflict` → `ABORTED` → caller retries | classified retryable, so the activity retries in place |

One workflow per actor, with the actor's identity as the workflow ID:
`actor/<atespace>/<name>`. Anyone holding an actor's name can find its owner,
and a second caller's request lands on the execution that already owns it
rather than starting a rival one.

`ResumeActor` is the exception that does not always reach the owner. The router
resumes per routed request, so the handler reads the record first and answers
an actor that is already `ACTOR_STATE_RUNNING` from it, with `resumed` false and
without touching Temporal at all. Only an actor that needs work is handed over.
Sending every routed request to the entity would put the whole request rate
through it for an answer the record already has.

The workflow only decides. Everything that reads or writes the store, dials
atelet or talks to the scheduler happens in an activity.

## Legal transitions

An operation the record cannot take is refused with `FAILED_PRECONDITION`
before any step runs. The check happens inside the update handler, under the
entity's guard, against the state the entity last read. The steps remain the
authority, because the record can move under a mirrored state.

The mirror is dropped whenever a step fails, because a step can commit a
transition before failing, and the operation after one reads the record again
before checking anything against it. A refusal the mirror would give is also
confirmed against a fresh read before it is issued, so a record another writer
moved is honored rather than refused off stale memory. This is what makes a
rolling deploy from `legacy` to `temporal` survivable: for its duration the
fleet is mixed, and a legacy replica can resume an actor the entity last saw
suspended. Do not run a mixed fleet under traffic for longer than a deploy
takes. An actor half-way through an operation on one orchestrator has no owner
on the other, and the entity's guard serializes nothing a legacy replica does.

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

An entity whose record is gone refuses everything with `NOT_FOUND` and
completes. That covers a `DeleteActor` for an actor that never existed, and a
record that vanished under the entity, whichever step notices: the caller sees
the `NOT_FOUND` the in-process orchestrator answers, and no execution is left
owning nothing.

## What an RPC deadline means

This is the one place the two orchestrators differ on purpose, and callers
have to know it.

Under `legacy`, canceling the RPC context cancels the step. Under `temporal`,
an update that has been admitted runs to completion whether or not its caller
is still waiting: the step holds the actor's lease and is mid-transition, and
only it can finish what it started. A `SuspendActor` that times out on the
caller's side after ten minutes still suspends the actor. The caller is told
`DEADLINE_EXCEEDED` and has to read the record to learn what happened.

An operation that is still queued behind another when its caller's deadline
passes is different: it is skipped. The router carries the RPC deadline in the
update's input, and the entity checks it when the operation's turn comes. A
`DeleteActor` issued during a long suspend, given up on, and then found
executed half an hour later is the outcome this prevents. The skipped update
ends with `DEADLINE_EXCEEDED`, which nobody reads.

`WorkerLost` carries no deadline. The worker is gone whether or not the sweep
is still waiting, and the crash is owed to the record either way. The sweep's
own wait is bounded to a minute instead: past it the sweep gets
`DEADLINE_EXCEEDED`, requeues, and its retry attaches to the report it left
behind by the report's update ID rather than queuing a second one.

## Automatic recovery

The worker teardown path sends the `WorkerLost` update for each actor still
bound to a worker whose pod is gone, instead of writing the crash itself. On
that update, for an actor in `ACTOR_STATE_RUNNING` or in one of the
transitional states, the entity:

  1. runs `Crash`, which releases the worker and commits `ACTOR_STATE_CRASHED`,
     as the in-process teardown does. A reader, the crash record and
     `ate.actor.crashes` all come off the record, and going straight to the
     revert would erase the fact that the actor crashed at all;
  2. runs `Revert`, which returns the actor to `ACTOR_STATE_SUSPENDED` with its
     last durable snapshot intact. This is the step an operator runs by hand on
     the in-process path;
  3. runs `Resume`, but only with `--temporal-auto-resume-after-crash`. It is
     off by default: a resume books worker capacity, and an actor that crashed
     once may crash again on the state it is restored from.

An actor that was suspended or paused when the pod went away saved its state
first, and a deleting one has nothing left on the node, so worker loss does not
apply to either and the report is logged and dropped.

A report that finds the actor already `ACTOR_STATE_CRASHED` is recovery owed,
not a report to ignore. The sweep only reports an actor it found bound to the
worker, so a crash it finds was committed a moment earlier by the step that was
running on that worker and saw it drain: the same cause, a different writer.
The entity skips the `Crash` and runs the revert. Which of the two racers
reaches the record first no longer decides whether the actor is recovered.

The recovered actor resumes from its last durable snapshot, which is what an
operator's `RevertActor` gives today. Nothing here recovers work done since that
snapshot; a crash still loses it.

Worker loss is the only crash this recovers from. A crash atelet reports during
a step, which the step turns into `ACTOR_STATE_CRASHED` and `DATA_LOSS`, still
waits for an operator's `RevertActor`: the caller asked for that operation and
gets told it failed, and reverting behind them would answer a question nobody
asked. Extending recovery to that class of crash is a separate decision.

A recovery that gives up leaves the actor `ACTOR_STATE_CRASHED`, which is where
the in-process orchestrator leaves one too, and writes the actor event
`ate.actor.recovery_failed`. That event is the one record naming the actor an
operator now has to move by hand; the `State` query reports the same, but
nothing in the control plane reads it.

## Timeouts and retries

Every activity carries an explicit `StartToClose` budget, sized off what the
step talks to rather than off a house number. The long ones heartbeat, because
without it a step that is still working cannot be told from an `ate-api` pod
that went away, and because a cancellation only reaches the step on the
heartbeat response.

| Activity         | StartToClose | Heartbeat | Why                                                                     |
|------------------|--------------|-----------|-------------------------------------------------------------------------|
| `LoadState`      | 30s          | no        | one row read                                                              |
| `Resume`         | 15m          | 1m        | scheduling, volumes, and pulling a snapshot over the network onto a node  |
| `Suspend`        | 30m          | 1m        | writes the guest's memory and filesystem to object storage, the largest thing the lifecycle does |
| `Pause`          | 10m          | 1m        | a checkpoint to the node's disk rather than to object storage             |
| `Revert`         | 15m          | 1m        | tears the sandbox down and discards a half-written snapshot, with no upload |
| `Delete`         | 30m          | 1m        | termination, volume deletion through the CSI controller, snapshot release |
| `Crash`          | 2m           | no        | two store writes                                                          |
| `RecoveryFailed` | 30s          | no        | one row read and one log record                                           |

Heartbeats are reported every 15 seconds against a one-minute timeout, so one
missed tick is not a lost attempt.

Two retry policies, differing only in whether anything is waiting:

  * **A caller is waiting** (every operation an RPC routes): five attempts,
    one second growing to fifteen. The gRPC caller has its own deadline, so a
    step that keeps failing has to surface the failure while the caller is
    still there to read it; retrying past that turns a returned error into a
    hung request.
  * **Nobody is waiting** (the crash, revert and resume that recovery runs):
    unbounded, one second growing to a minute, under a one-hour budget for the
    whole step. Stopping halfway would leave the actor in a transitional state
    with nothing driving it; never stopping would hold the entity's one
    operation slot for as long as it lives, and an operator could not even
    delete the actor.

A failure is classified by the gRPC code the step meant, read off the status
error the step built or off the store sentinel it passed through, the same two
shapes the legacy handlers map. `NOT_FOUND`, `FAILED_PRECONDITION`,
`INVALID_ARGUMENT`, `DATA_LOSS` and the other outcomes another attempt cannot
change fail the activity for good; `ABORTED` for a version conflict or lease
contention, and anything unclassified, are retried. One `ABORTED` is permanent:
the `actor <x> crashed` a step reports when it has itself committed the crash.
A retry would find `ACTOR_STATE_CRASHED` and answer `FAILED_PRECONDITION`, and
the callers that key on `ABORTED` plus that word would never learn what
happened.

Two steps are not idempotent across their own completion, and a retry after a
lost completion reads the record to answer for the attempt that did not report.
An attempt after the first exists only because an earlier one died before
reporting, so on a retry:

  * `Revert` treats `FAILED_PRECONDITION` against a record that reads
    `ACTOR_STATE_SUSPENDED` with no worker assignment and no in-progress
    snapshot as the success the first attempt owed. That is what a finished
    revert leaves behind, for the recovery and for an operator's update alike.
  * `Delete` treats `NOT_FOUND` as the success the first attempt owed: the
    record this entity opened with is gone because the first attempt removed
    it. The answer names the actor and nothing else, because nothing else is
    left.
  * `Resume` answers `resumed: true` for an actor it finds
    `ACTOR_STATE_RUNNING`. The step reports a running actor as nothing done,
    but the entity only sends a `Resume` for an actor that was not running, so
    the attempt that did not report is the one that resumed it.

The code travels on the error's type and is turned back into a status at the
RPC boundary, so a caller sees the same code on either orchestrator for a
failure a step produced. A failure Temporal itself produced is re-coded there
too, because none of Temporal's codes says anything about the actor:

| Temporal reported                                            | Caller sees          |
|--------------------------------------------------------------|----------------------|
| a step that hit `StartToClose` or stopped heartbeating       | `DEADLINE_EXCEEDED`  |
| an update the caller stopped waiting on                      | `DEADLINE_EXCEEDED`  |
| a cancelled step                                             | `CANCELED`           |
| an owner completing or rolling its history over under an update-with-start | `ABORTED`, retry |
| the frontend unreachable, a request that timed out server-side, a persistence failure | `UNAVAILABLE`, retried by the ingress router like an `ate-api` restart |
| a request Temporal refused: bad namespace, bad credential, bad argument | `INTERNAL`  |

A Temporal `NOT_FOUND` is never passed through as an actor `NOT_FOUND`: a
caller told the actor is missing would stop looking for one that exists.

## Cancellation

Canceling an entity through Temporal is an operator's act, and the entity
writes nothing on its way out. It waits for the step in flight, reads the
record if a failed step dropped the mirror, and logs a warning naming the state
it leaves the actor in. An actor left in a transitional state stays there for
an operator to move, as it would under `legacy`. Crashing it would free a
worker whose sandbox may still be restoring, which is worse than an honest
record.

## Shutdown

On SIGTERM `ate-api` marks itself not ready and, at the same moment, stops the
Temporal worker's pollers, so a pod on its way out picks up no new lifecycle
step: every step it does not take is one another pod serves. The gRPC drain
then runs as it does without Temporal, `--drain-delay` and then
`--drain-timeout`, while a step already in flight gets
`--temporal-worker-stop-timeout` to report before its context is cancelled.
The flag defaults to two minutes and is separate from the drain's fifteen
seconds because the steps run for minutes. The Temporal client is closed last,
once both the drain and the worker are done, because the handlers dispatch
through it for as long as the drain lets them.

The manifest's `terminationGracePeriodSeconds` covers the sum, with the
arithmetic beside it. A step the kubelet SIGKILLs past that is still safe: the
actor's lease expires on the store's TTL, which is shorter than the heartbeat
timeout, so the retry that Temporal schedules on another pod after the missed
heartbeat lands after the lease is free, reads the record, and continues from
where the step got to.

## Determinism and versioning

`workflow.GetVersion` is called before the entity's first branch, so a later
change to the order it loads, waits and recovers in can be introduced without
breaking the executions already running on the current order.

`testdata/actor_entity_history.json` is a recorded execution covering a resume,
a worker loss with the recovery it triggers, and the continue-as-new that rolls
the history over once the recovery is done.
`testdata/actor_entity_continued_history.json` is the run that picked up from
it, and covers the delete that completes the entity. `TestReplayEntityHistory`
replays both against the current code. `TestRecordEntityHistory` regenerates
them, and its doc comment carries the commands, including the dynamic config
value that makes a dev server suggest a rollover after a few dozen events.

`workflowcheck` is run manually with the recipe below, and reports nothing on
this package. Nothing in `hack/` runs it, and wiring it into `make verify` is a
follow-up that has to wait for the tool: the released binary cannot be used as
it comes. `go install go.temporal.io/sdk/contrib/tools/workflowcheck@latest`
builds it against that module's `go 1.24` directive, and its copy of `go/types`
then rejects this repository's Go 1.27 sources with `package requires newer Go
version`. It reports every package as a failed prerequisite and still exits 0,
so in CI it would pass without having checked anything. Build it against the
repository's Go instead:

```sh
git clone https://github.com/temporalio/sdk-go
cd sdk-go/contrib/tools/workflowcheck
go mod edit -go=1.27.0
go get golang.org/x/tools@latest
go mod tidy
go build -o "$(go env GOPATH)/bin/workflowcheck-go1.27" .
```

```sh
workflowcheck-go1.27 -test=false ./cmd/ateapi/internal/actorflow/
```

Anything wiring this into CI has to pin the build toolchain to the
repository's, and treat a "failed prerequisites" line as a failure rather than
reading the exit code.

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

`--orchestrator=temporal` cannot start without Temporal. A frontend that cannot
be dialed at startup is fatal, the way an unreachable store is: nothing on this
path can run without it, and a control plane that came up anyway would accept
lifecycle requests it has no way to serve.

`--temporal-address=@env` reads `TEMPORAL_ADDRESS` instead, the way the
PostgreSQL flags read theirs, so a deployment can set it from a ConfigMap.
`--temporal-auto-resume-after-crash` turns on step 3 of the recovery above.
`--temporal-max-concurrent-steps` caps the steps one pod runs at once as
activities; see "Known gaps" for the default.

A secured frontend takes the flags below. Any one of them turns TLS on.

  * `--temporal-tls` dials over TLS with the system roots, for a frontend
    behind a public certificate and no client credential.
  * `--temporal-tls-ca-certs` names a PEM bundle the frontend's serving
    certificate is verified against instead of the system roots.
  * `--temporal-tls-client-cred-bundle` names the credential bundle presented
    as the client certificate, for a frontend that requires mTLS. It is loaded
    the way `--atelet-client-cred-bundle` is, so a rotated file is picked up.
  * `--temporal-api-key` is sent with every call, which is how Temporal Cloud
    authenticates a client. `@env` reads `TEMPORAL_API_KEY`, so the key can
    come from a Secret the way the PostgreSQL DSN does. The key itself is kept
    out of the startup log.

`ate-api` serves the task queue itself: it is the only thing that can run the
lifecycle steps, because it holds the store, the atelet dialer and the
scheduler, so a separate worker deployment would only add a hop.

An actor's owner is visible in the Temporal UI under the workflow ID
`actor/<atespace>/<name>`, and the `State` query reports what the entity
believes the record says.

## Out of scope

  * **Choosing per actor.** The orchestrator is a process-wide flag, read at
    startup. An actor half-way through an operation on one orchestrator has no
    owner on the other, so switching under traffic is not supported. See
    "Legal transitions" for what a rolling deploy can and cannot survive.
  * **Tags, the template reconciler, and the router's request parking.** They
    stay on the in-process path whichever orchestrator is selected.
  * **The data path.** Nothing here makes a request to an actor exactly-once;
    the orchestrator is about the lifecycle.
  * **New lifecycle metrics.** The steps record `ate.actor.*` from inside the
    activities, so the existing instruments report both orchestrators without
    change. Temporal's own metrics are a known gap, below.

## Known gaps

  * **The Temporal client and worker export no metrics.** Schedule-to-start
    latency, workflow task failures and retried activity attempts are
    invisible, which is why `docs/metrics/substrate.yaml` lists this
    orchestrator under `blind_spots`. The SDK's OpenTelemetry metrics handler
    lives in a module of its own, `go.temporal.io/sdk/contrib/opentelemetry`,
    so wiring it to the `ateapi` meter means vendoring another module,
    recording its licenses, and registering or exempting its instruments in
    the metric registry. That is a change of its own and a follow-up to this
    one.
  * **The concurrency cap is a guess.** `--temporal-max-concurrent-steps`
    defaults to 32 because the SDK's default of a thousand would let one pod
    run hundreds of suspends and resumes against Postgres, atelet and the
    object store at once after an outage, which the in-process orchestrator
    never did. The right figure comes from load, and nothing here has measured
    it. The task queue's own rate limit is not set.
  * **One dependency's license is not recorded.**
    `github.com/nexus-rpc/nexus-proto-annotations` arrives through
    `go.temporal.io/api`, and no Temporal SDK version avoids it. It is MIT
    upstream, but ships no `LICENSE` file in its module at any published
    version, so `go-licenses` classifies it as unknown and refuses to write any
    record at all, for every dependency. `hack/update/licenses.sh` passes
    `--ignore` for that one module so the rest can be recorded. The fix is
    upstream: once a `LICENSE` lands in that repository and the module is
    bumped, drop the ignore.
  * **Only worker loss is recovered automatically.** See the section above.
