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
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Per-step StartToClose budgets: the wall clock one attempt of a step gets
// before Temporal gives up on the attempt and retries it. Each is sized off
// what the step talks to, not off a house number.
const (
	// loadStateTimeout covers a single row read.
	loadStateTimeout = 30 * time.Second

	// resumeTimeout covers scheduling, volume creation and attach, and the
	// atelet Restore plus Run pair. Restore pulls a snapshot over the network
	// onto the node, so this is minutes, not seconds.
	resumeTimeout = 15 * time.Minute

	// suspendTimeout covers a checkpoint: atelet writes the guest's memory and
	// filesystem to object storage, or uploads what a pause already captured.
	// The upload is the largest thing the lifecycle does.
	suspendTimeout = 30 * time.Minute

	// pauseTimeout covers a node-local checkpoint, which writes to the node's
	// disk rather than to object storage.
	pauseTimeout = 10 * time.Minute

	// revertTimeout covers tearing the sandbox down, detaching volumes and
	// discarding a half-written snapshot. No upload, but it still reaches the
	// node and the object store.
	revertTimeout = 15 * time.Minute

	// deleteTimeout covers termination, volume deletion and snapshot release.
	// Volume deletion goes through the CSI controller, which is the slow part.
	deleteTimeout = 30 * time.Minute

	// crashTimeout covers releasing the worker and committing CRASHED, both
	// store writes.
	crashTimeout = 2 * time.Minute
)

// heartbeatTimeout is how long a long step may go without reporting progress
// before Temporal treats the attempt as lost. It has to be long enough that a
// slow snapshot upload between two heartbeats is not mistaken for a dead
// worker, and short enough that a dead ateapi pod frees the step within a
// resume's latency budget rather than within its StartToClose.
const heartbeatTimeout = time.Minute

// heartbeatInterval is how often a running step reports progress. Well under
// heartbeatTimeout, so one missed tick is not a lost attempt.
const heartbeatInterval = 15 * time.Second

// requestRetryPolicy bounds the retries of a step a caller is waiting on. The
// gRPC caller has its own deadline, so a step that keeps failing has to surface
// the failure while the caller is still there to read it; retrying past that
// turns a returned error into a hung request.
var requestRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2,
	MaximumInterval:    15 * time.Second,
	MaximumAttempts:    5,
}

// recoveryRetryPolicy retries a step nobody is waiting on. Automatic recovery
// has no caller and no deadline, and stopping halfway leaves the actor in a
// transitional state with nothing driving it, so these attempts do not give up.
var recoveryRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2,
	MaximumInterval:    time.Minute,
}

// requestOptions are the options for a step an RPC caller is waiting on.
//
// WaitForCancellation keeps the entity from moving on while a cancelled step is
// still finishing: the step holds the actor's lease and is mid-transition, so
// what the record says next is only knowable once it reports.
func requestOptions(startToClose time.Duration, heartbeat bool) workflow.ActivityOptions {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: startToClose,
		RetryPolicy:         requestRetryPolicy,
		WaitForCancellation: true,
	}
	if heartbeat {
		opts.HeartbeatTimeout = heartbeatTimeout
	}
	return opts
}

// recoveryOptions are the options for a step the entity runs on its own, with
// no caller waiting on the result.
func recoveryOptions(startToClose time.Duration, heartbeat bool) workflow.ActivityOptions {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: startToClose,
		RetryPolicy:         recoveryRetryPolicy,
		WaitForCancellation: true,
	}
	if heartbeat {
		opts.HeartbeatTimeout = heartbeatTimeout
	}
	return opts
}

// cleanupOptions are the options for the write a cancelled entity makes on its
// way out. Bounded like a request, because a cancelled entity has to go away:
// the unattended policy never gives up, and holding the execution open waiting
// for a store that is down would defeat the cancellation.
func cleanupOptions(startToClose time.Duration) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: startToClose,
		RetryPolicy:         requestRetryPolicy,
	}
}
