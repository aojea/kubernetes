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

package proxy

import (
	"net"
	"strconv"
	"sync"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	coreinformers "k8s.io/client-go/informers/core/v1"
	discoveryinformers "k8s.io/client-go/informers/discovery/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"
)

// Tuple is just a network 3-Tuple IP-Port-Protocol
type Tuple struct {
	IP       string
	Port     int32
	Protocol v1.Protocol
}

func (t *Tuple) Endpoint() string {
	return net.JoinHostPort(t.IP, strconv.Itoa(int(t.Port)))
}

func difference(a, b []Tuple) []Tuple {
	m := make(map[Tuple]struct{})
	result := []Tuple{}
	for _, item := range b {
		m[item] = struct{}{}
	}

	for _, item := range a {
		if _, ok := m[item]; !ok {
			result = append(result, item)
		}
	}
	return result
}

// Frontend represents a logical network aggregation of backends
type Service struct {
	// A service consist in a Logical aggregations of backends
	Frontend Tuple
	Backends []Tuple
	// The frontend use to be virtual IPs that are not present on the host network
	// ClusterIPs, LoadBalancer IPs and ExternalIPs are Virtual IPs
	// NodePorts Frontends are local to the node
	Virtual bool
	// Internal and External is associated to Kubernetes Service Traffic Policies
	// that follows a destination model
	External bool // Allowed to be reachable externally
	Internal bool // Allowed to be reachable internally
}

// FIB Forwarding Information Base
// Contains the information necessary to program the dataplane
// 3-Tuple : [ 3-Tuple, 3-Tuple, 3-Tuple, ...]
type FIB struct {
	mu      sync.Mutex
	entries map[Tuple]Service
}

func NewFIB() *FIB {
	return &FIB{
		entries: map[Tuple]Service{},
	}
}
func (f *FIB) Update(service Service) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[service.Frontend] = service
}

func (f *FIB) Remove(service Service) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries, service.Frontend)
}

func (f *FIB) Get(tuple Tuple) Service {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entries[tuple]
}

func (f *FIB) List() map[Tuple]Service {
	f.mu.Lock()
	defer f.mu.Unlock()
	list := make(map[Tuple]Service)
	// Copy from the original map to the target map
	for key, value := range f.entries {
		list[key] = value
	}
	return list
}

func (f *FIB) Clone() FIB {
	// Create the target map
	clone := FIB{
		entries: f.List(),
	}
	return clone
}

func (f *FIB) Difference(fib FIB) FIB {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Create the target map
	clone := FIB{
		entries: make(map[Tuple]Service),
	}

	for key, value := range fib.List() {
		if service, ok := f.entries[key]; !ok || !cmp.Equal(service, value) {
			clone.entries[key] = value
		}
	}
	return clone
}

// NodePort configuration
type NodePortConfig struct {
	NodePortAddresses []string
}

// Database obtains a FIB from current state of the world
// based on the apiserver information.
type Options struct {
	// only entries related to the NodeName
	NodeName string
	// enable terminating endpoints
	Terminating bool
	// use topology hints
	TopologyHints bool
	// empty means no NodePort entries are generated
	// enable the use of ExternalIPs
	EnableExternalIPs bool
	// if NodePortConfig is nil, it means is disable
	NodePortConfig *NodePortConfig
}

type ServiceController struct {
	serviceLister  corelisters.ServiceLister
	servicesSynced cache.InformerSynced

	endpointSliceLister  discoverylisters.EndpointSliceLister
	endpointSlicesSynced cache.InformerSynced

	nodeLister         corelisters.NodeLister
	nodeInformerSynced cache.InformerSynced
}

func NewServiceController(serviceInformer coreinformers.ServiceInformer,
	endpointSliceInformer discoveryinformers.EndpointSliceInformer,
	nodeInformer coreinformers.NodeInformer,
) *ServiceController {
	s := &ServiceController{
		serviceLister:        serviceInformer.Lister(),
		servicesSynced:       serviceInformer.Informer().HasSynced,
		endpointSliceLister:  endpointSliceInformer.Lister(),
		endpointSlicesSynced: endpointSliceInformer.Informer().HasSynced,
		nodeLister:           nodeInformer.Lister(),
		nodeInformerSynced:   nodeInformer.Informer().HasSynced,
	}
	return s
}

func (s *ServiceController) Ready() bool {
	return s.servicesSynced() && s.endpointSlicesSynced() && s.nodeInformerSynced()
}

// GetFIB returns the Forwarding Information based on the options
// so dataplane implementations just need to program the frontend Tuples
// to direct traffic to the slice of associated backend Tuples
// FIBs are snapshots of the forwarding plane. Snapshot allow to
// obtain the difference between boths, something useful for
// implementations that want to use incremental updates.
func (s *ServiceController) GetFIB(opts Options) (*FIB, error) {
	fib := NewFIB()
	services, err := s.serviceLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, service := range services {
		// Skip Services that doesn't forward Virtual IPs
		if !isIPService(service) {
			continue
		}
		// Obtain the EndpointSlices that correspond to the Service
		esLabelSelector := labels.Set(map[string]string{
			discovery.LabelServiceName: service.Name,
		}).AsSelectorPreValidated()
		endpointSlices, err := s.endpointSliceLister.EndpointSlices(service.Namespace).List(esLabelSelector)
		if err != nil {
			return nil, err
		}

		for _, ip := range service.Spec.ClusterIPs {
			family := v1.IPv4Protocol
			if utilnet.IsIPv6String(ip) {
				family = v1.IPv6Protocol
			}
			// The service.spec.port represents the frontend 3-tuple
			for _, servicePort := range service.Spec.Ports {
				backends := getBackends(service, servicePort, endpointSlices, family, opts)
				// ClusterIP
				tuple := Tuple{IP: ip, Port: servicePort.Port, Protocol: servicePort.Protocol}
				frontend := Service{
					Frontend: tuple,
					Virtual:  true,
					External: false,
					Internal: true,
					Backends: backends,
				}
				fib.Update(frontend)
				// Node Port
				if servicePort.NodePort != 0 && opts.NodePortConfig != nil {
					for _, address := range opts.NodePortConfig.NodePortAddresses {
						tuple := Tuple{IP: address, Port: servicePort.NodePort, Protocol: servicePort.Protocol}
						frontend := Service{
							Frontend: tuple,
							Virtual:  false,
							External: true,
							Internal: true,
							Backends: backends,
						}
						fib.Update(frontend)
					}
				}
				// LoadBalancer and ExternalIPs
				for _, extIP := range getServiceExternalIPs(service, opts) {
					tuple := Tuple{IP: extIP, Port: servicePort.Port, Protocol: servicePort.Protocol}
					frontend := Service{
						Frontend: tuple,
						Virtual:  true,
						External: true,
						Internal: true,
						Backends: backends,
					}
					fib.Update(frontend)
				}
			}
		}

	}

	return fib, nil
}

// getServiceExternalIPs returns the IPv4 and IPv6 addresses used as frontend by the Service in that order
func getServiceExternalIPs(service *v1.Service, opts Options) []string {
	ips := []string{}
	// ExternalIP
	if opts.EnableExternalIPs {
		for _, ip := range service.Spec.ExternalIPs {
			if net.ParseIP(ip) != nil {
				ips = append(ips, ip)
			}
		}
	}
	// LoadBalancer
	for _, ingress := range service.Status.LoadBalancer.Ingress {
		if net.ParseIP(ingress.IP) != nil {
			ips = append(ips, ingress.IP)
		}
	}
	return ips
}

// return the backends that belong to the IPFamily as a slice of IP:Port
func getBackends(service *v1.Service, svcPort v1.ServicePort, slices []*discovery.EndpointSlice, family v1.IPFamily, opts Options) []Tuple {
	// return an empty object so the caller don't have to check for nil and can use it as an iterator
	if len(slices) == 0 {
		return []Tuple{}
	}
	// Endpoint Slices are allowed to have duplicate endpoints
	// we use a set to deduplicate endpoints
	backends := sets.New[Tuple]()
	for _, slice := range slices {
		klog.V(4).Infof("Getting endpoints for slice %s", slice.Name)
		// Only return addresses that belong to the requested IP family
		if slice.AddressType != discovery.AddressType(family) {
			klog.V(4).Infof("Slice %s with different IP Family endpoints, requested: %s received: %s",
				slice.Name, slice.AddressType, family)
			continue
		}

		// build the list of endpoints in the slice
		for _, port := range slice.Ports {
			// If Service port name set it must match the name field in the endpoint
			if svcPort.Name != "" && svcPort.Name != *port.Name {
				klog.V(5).Infof("Slice %s with different Port name, requested: %s received: %s",
					slice.Name, svcPort.Name, *port.Name)
				continue
			}

			// Skip ports that doesn't match the protocol
			if *port.Protocol != svcPort.Protocol {
				klog.V(5).Infof("Slice %s with different Port protocol, requested: %s received: %s",
					slice.Name, svcPort.Protocol, *port.Protocol)
				continue
			}

			for _, endpoint := range slice.Endpoints {
				// Terminating Endpoints
				if !opts.Terminating && endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
					klog.V(4).Infof("Slice endpoints Not Ready")
					continue
				}
				if opts.Terminating && endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving {
					klog.V(4).Infof("Slice endpoints Not Service")
					continue
				}
				// Traffic Policy

				for _, ip := range endpoint.Addresses {
					klog.V(4).Infof("Adding slice %s endpoints: %s %d", slice.Name, ip, *port.Port)
					backends.Insert(Tuple{IP: ip, Port: *port.Port, Protocol: svcPort.Protocol})
				}
			}
		}
	}

	klog.V(4).Infof("LB Endpoints for %s are: %v", slices[0].Labels[discovery.LabelServiceName], backends.UnsortedList())
	return backends.UnsortedList()
}

// isIPService returns true if the Services uses IP
// i.e. is not a DNS Service like Headless or ExternalName
func isIPService(service *v1.Service) bool {
	if service.Spec.Type == v1.ServiceTypeExternalName {
		return false
	}
	if len(service.Spec.ClusterIP) > 0 &&
		service.Spec.ClusterIP != v1.ClusterIPNone {
		return true
	}
	return false
}

func isVirtual(service *v1.Service) bool {
	if service.Spec.Type == v1.ServiceTypeClusterIP ||
		service.Spec.Type == v1.ServiceTypeLoadBalancer {
		return true
	}
	return false
}
