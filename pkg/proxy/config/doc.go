/*
Copyright 2014 The Kubernetes Authors.

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

// Package config adapts the shared informers used by kube-proxy to the
// *Handler interfaces implemented by the proxiers. Each *Config type
// subscribes to one informer, type-asserts the objects, unwraps delete
// tombstones, fans the events out to its handlers and reports when the
// initial list has been delivered via the On*Synced callbacks.
//
// TODO: remove this package. It predates shared informers (it was written
// for a design where "configuration sources" such as etcd or files pushed
// updates over channels, see git history) and everything it still does is
// provided by client-go today:
//
//   - Type-safe callbacks and tombstone handling:
//     cache.NewTypedSharedIndexInformer[*v1.Service](informer).
//     AddTypedEventHandler(cache.TypedResourceEventHandlerFuncs[*v1.Service]{...})
//   - "Initial list delivered": cache.ResourceEventHandlerRegistration.HasSynced()
//     and OnAdd(obj, isInInitialList) replace the On*Synced callbacks.
//   - Late subscription: AddEventHandler on a running informer replays the
//     cache to the new handler. Handlers must therefore be attached in a
//     single step; a second fan-out layer that subscribes before it knows
//     its own consumers can lose the replay (see NodeTopologyConfig and
//     https://issues.k8s.io/143275).
//
// The intended end state is for the proxy.Provider implementations to be
// registered on the informers directly, and for per-node state (NodeIPs,
// PodCIDRs, topology labels) to be owned and exposed by
// nodemanager.NodeManager, which already holds the latest Node object,
// instead of being pushed through NodeConfig and NodeTopologyConfig.
package config
