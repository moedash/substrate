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
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

// Config configures the Temporal orchestration path.
type Config struct {
	// Address is the Temporal frontend to dial.
	Address string

	// Namespace is the Temporal namespace the entity workflows live in. Empty
	// leaves the SDK's default.
	Namespace string

	// TaskQueue is the queue ateapi both serves and dispatches on. Empty leaves
	// DefaultTaskQueue.
	TaskQueue string

	// AutoResumeAfterCrash puts a recovered actor back on a worker instead of
	// leaving it suspended.
	AutoResumeAfterCrash bool

	// StopTimeout is how long a step in flight gets to finish when ate-api is
	// shutting down. Zero cancels it at once, which is the SDK's default and
	// not what a mid-transition step needs.
	StopTimeout time.Duration
}

// workerOptions are the options the entity worker runs with.
//
// WorkerStopTimeout is the one that has to be set. Left at zero, a shutdown
// cancels the activity context immediately, so a step holding the actor's lease
// is cut off mid-transition and the record is left in a transitional state for
// the next pod to crash out of.
func workerOptions(cfg Config) worker.Options {
	return worker.Options{
		WorkerStopTimeout: cfg.StopTimeout,
	}
}

// clientOptions are the options the frontend is dialed with.
func clientOptions(cfg Config, logger *slog.Logger) client.Options {
	return client.Options{
		HostPort:  cfg.Address,
		Namespace: cfg.Namespace,
		Logger:    sdklog.NewStructuredLogger(logger),
	}
}

// Runtime is ateapi's half of the Temporal orchestration path: the worker that
// serves the entity workflows and their activities, and the router the RPC
// handlers dispatch through.
//
// One process does both. ateapi is the only thing that can run the lifecycle
// steps, because it holds the store, the atelet dialer and the scheduler, so
// splitting the worker out would only add a hop.
type Runtime struct {
	client client.Client
	worker worker.Worker
	router *Router
}

// Start dials Temporal and starts serving the actor entity workflows. It fails
// when the frontend cannot be reached: nothing here can run without it.
func Start(cfg Config, steps LifecycleSteps, logger *slog.Logger) (*Runtime, error) {
	taskQueue := cfg.TaskQueue
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}

	c, err := client.Dial(clientOptions(cfg, logger))
	if err != nil {
		return nil, fmt.Errorf("while dialing Temporal at %q: %w", cfg.Address, err)
	}

	w := worker.New(c, taskQueue, workerOptions(cfg))
	w.RegisterWorkflow(ActorEntityWorkflow)
	w.RegisterActivity(NewActorActivities(steps))
	if err := w.Start(); err != nil {
		c.Close()
		return nil, fmt.Errorf("while starting the Temporal worker on %q: %w", taskQueue, err)
	}

	return &Runtime{
		client: c,
		worker: w,
		router: NewRouter(c, taskQueue, cfg.AutoResumeAfterCrash),
	}, nil
}

// Router is what the RPC handlers dispatch lifecycle operations through.
func (r *Runtime) Router() *Router {
	return r.router
}

// StopWorker stops polling for new steps and waits for the ones in flight.
//
// Call it as soon as the pod is on its way out, alongside the gRPC drain
// rather than after it: every step this pod does not pick up is one another
// pod serves. A step in flight gets Config.StopTimeout to report before its
// context is cancelled, and whatever is left unfinished is picked up by
// whichever pod serves the task queue next, once its lease has expired and
// its heartbeat has been missed.
func (r *Runtime) StopWorker() {
	r.worker.Stop()
}

// Close closes the client. Only once the RPC handlers are done with it: they
// dispatch through this client for as long as the gRPC drain lets them.
func (r *Runtime) Close() {
	r.client.Close()
}
