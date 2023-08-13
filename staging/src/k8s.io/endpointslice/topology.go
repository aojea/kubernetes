/*
Copyright 2023 The Kubernetes Authors.

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

package endpointslice

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/klog/v2"
)

// topologyManager
type topologyManager struct {
	mu      sync.Mutex
	plugins map[string]Topology
}

func NewTopologyManager(plugins ...Topology) *topologyManager {
	t := &topologyManager{
		plugins: make(map[string]Topology),
	}
	for _, p := range plugins {
		t.plugins[p.Name()] = p
	}
	return t
}

func (t *topologyManager) getPlugin(svc *v1.Service) Topology {
	t.mu.Lock()
	defer t.mu.Unlock()
	val, ok := svc.Annotations[v1.AnnotationTopologyMode]
	if !ok || val == "Disabled" {
		return nil
	}

	return t.plugins[val]
}

func (t *topologyManager) AddHints(svc *v1.Service) {
	t.getPlugin(svc).AddHints(svc)
}

type Topology interface {
	Name() string
	AddHints(logger klog.Logger, si *SliceInfo) ([]*discovery.EndpointSlice, []*discovery.EndpointSlice, []*EventBuilder)
	RemoveHints(serviceKey string, addrType discovery.AddressType)
	SetHints(serviceKey string, addrType discovery.AddressType, allocatedHintsByZone EndpointZoneInfo)
}

// EventBuilder let's us construct events in the code.
// We use it to build events and return them from a function instead of publishing them from within it.
// EventType, Reason, and Message fields are equivalent to the v1.Event fields - https://pkg.go.dev/k8s.io/api/core/v1#Event.
type EventBuilder struct {
	EventType string
	Reason    string
	Message   string
}
