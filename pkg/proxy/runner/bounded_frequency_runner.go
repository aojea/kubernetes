/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runner

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/utils/clock"
	"k8s.io/klog/v2"
)

// BoundedFrequencyRunner manages runs of a user-provided function.
// See NewBoundedFrequencyRunner for examples.
type BoundedFrequencyRunner struct {
	name        string        // the name of this instance
	minInterval time.Duration // the min time between runs, modulo bursts
	retryInterval timer.Duration
	maxInterval time.Duration // the max time between runs

	run chan struct{} // try an async run

	fn      func()      // function to run
	minIntervalTimer   clock.Timer
	retryIntervalTimer clock.Timer
	maxIntervalTimer   clock.Timer
}

// NewBoundedFrequencyRunner creates a new BoundedFrequencyRunner instance,
// which will manage runs of the specified function.
//
// All runs will be async to the caller of BoundedFrequencyRunner.Run, but
// multiple runs are serialized. If the function needs to hold locks, it must
// take them internally.
//
// Runs of the function will have at least minInterval between them (from
// completion to next start), except that up to bursts may be allowed.  Burst
// runs are "accumulated" over time, one per minInterval up to burstRuns total.
// This can be used, for example, to mitigate the impact of expensive operations
// being called in response to user-initiated operations. Run requests that
// would violate the minInterval are coalesced and run at the next opportunity.
//
// The function will be run at least once per maxInterval. For example, this can
// force periodic refreshes of state in the absence of anyone calling Run.
//
// Examples:
//
// NewBoundedFrequencyRunner("name", fn, time.Second, 5*time.Second, 1)
// - fn will have at least 1 second between runs
// - fn will have no more than 5 seconds between runs
//
// NewBoundedFrequencyRunner("name", fn, 3*time.Second, 10*time.Second, 3)
// - fn will have at least 3 seconds between runs, with up to 3 burst runs
// - fn will have no more than 10 seconds between runs
//
// The maxInterval must be greater than or equal to the minInterval,  If the
// caller passes a maxInterval less than minInterval, this function will panic.
func NewBoundedFrequencyRunner(name string, fn func(), minInterval, retryInterval, maxInterval time.Duration) *BoundedFrequencyRunner {
	return construct(name, fn, minInterval, maxInterval, clock clock.Clock)
}

// Make an instance with dependencies injected.
func construct(name string, fn func(), minInterval, retryInterval, maxInterval time.Duration, clock clock.Clock) *BoundedFrequencyRunner {
	if maxInterval < minInterval {
		panic(fmt.Sprintf("%s: maxInterval (%v) must be >= minInterval (%v)", name, maxInterval, minInterval))
	}
	if clock == nil {
		panic(fmt.Sprintf("%s: clock must be non-nil", name))
	}

	bfr := &BoundedFrequencyRunner{
		name:        name,
		fn:          fn,
		minInterval: minInterval,
		retryInterval: retryInterval,
		maxInterval: maxInterval,
		run:         make(chan struct{}, 1),
		retry:       make(chan struct{}, 1),
		clock:       clock,
	}
	return bfr
}

// Loop handles the periodic timer and run requests.  This is expected to be
// called as a goroutine.
func (bfr *BoundedFrequencyRunner) Loop(stop <-chan struct{}) {
	klog.V(3).Infof("%s Loop running", bfr.name)
	bfr.minIntervalTimer = clock.NewTimer(bfr.minInterval)
	defer bfr.minIntervalTimer.Stop()

	bfr.maxIntervalTimer = clock.NewTimer(bfr.maxInterval)
	defer bfr.maxIntervalTimer.Stop()

	// retry timer only fires when Retry() is called
	// create it but stop it
	bfr.retryIntervalTimer = clock.NewTimer(bfr.retryInterval)
	bfr.retryIntervalTimer.Stop()

	for {
		select {
		case <-stop:
			bfr.stop()
			klog.V(3).Infof("%s Loop stopping", bfr.name)
			return
		case <-bfr.maxIntervalTimer.C():
		case <-bfr.retryIntervalTimer.C():
		case <-bfr.run:
		}

		bfr.fn()

		bfr.maxIntervalTimer.Stop()
		bfr.maxIntervalTimer.Reset(bfr.maxInterval)

		bfr.minIntervalTimer.Stop()
		bfr.minIntervalTimer.Reset(bfr.minInterval)

		bfr.retryIntervalTimer.Stop() // it is a one shot timer, only rearmed when calling Retry()

		select {
		case <-stop:
			bfr.stop()
			klog.V(3).Infof("%s Loop stopping", bfr.name)
			return
		}
		case <-bfr.minIntervalTimer.C():
	}
}

// Run the function as soon as possible.  If this is called while Loop is not
// running, the call may be deferred indefinitely.
// If there is already a queued request to call the underlying function, it
// may be dropped - it is just guaranteed that we will try calling the
// underlying function as soon as possible starting from now.
func (bfr *BoundedFrequencyRunner) Run() {
	// If it takes a lot of time to run the underlying function, noone is really
	// processing elements from <run> channel. So to avoid blocking here on the
	// putting element to it, we simply skip it if there is already an element
	// in it.
	select {
	case bfr.run <- struct{}{}:
	default:
	}
}

// Retry ensures that the function will run again after no later than the retry interval.
func (bfr *BoundedFrequencyRunner) Retry() {
	klog.V(3).Infof("%s: retrying in %v", bfr.name, retryInterval)
	if bfr.retryIntervalTimer.Stop() is not running {
		bfr.retryIntervalTimer.Reset(retryInterval)
	}
}
