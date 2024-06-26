//go:build linux
// +build linux

/*
Copyright 2024 The Kubernetes Authors.

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

package conntrack

import (
	"net"
	"strconv"

	"github.com/vishvananda/netlink"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	netutils "k8s.io/utils/net"
)

// Stal)Cleaner reoncile the existing conntrack flows on the host with the current
// Services and EndpointSlices to detect and detect the stale UDP entries
type StaleCleaner struct {
	ct                  Interface
	serviceLister       corelisters.ServiceLister
	endpointSliceLister discoverylisters.EndpointSliceLister
}

func NewStaleCleaner(ct Interface, serviceLister corelisters.ServiceLister, endpointSliceLister discoverylisters.EndpointSliceLister) *StaleCleaner {
	return &StaleCleaner{
		ct:                  ct,
		serviceLister:       serviceLister,
		endpointSliceLister: endpointSliceLister,
	}
}

func (s *StaleCleaner) CleanStaleEntries(ipv6 bool) error {
	// Get current conntrack UDP entries that MAY have stale Services entries
	flows, err := netlink.ConntrackTableList(netlink.ConntrackTable, getNetlinkFamily(ipv6))
	if err != nil {
		return err
	}

	// https://go.dev/wiki/SliceTricks
	// filter UDP
	n := 0
	for _, flow := range flows {
		if flow.Forward.Protocol == PROTOCOL_UDP {
			flows[n] = flow
			n++
		}
	}
	flows = flows[:n]
	if n == 0 {
		return nil
	}

	// udp      17 9 src=127.0.0.1 dst=127.0.0.1 sport=33488 dport=53 src=127.0.0.1 dst=127.0.0.1 sport=53 dport=33488 mark=0 use=2
	// udp      17 23 src=127.0.0.1 dst=127.53.53.8 sport=33941 dport=53 src=127.53.53.8 dst=127.0.0.1 sport=53 dport=33941 mark=0 use=1
	// udp      17 22 src=127.0.0.1 dst=127.0.0.1 sport=46674 dport=53 src=127.0.0.1 dst=127.0.0.1 sport=53 dport=46674 mark=0 use=1
	// udp      17 8 src=127.0.0.1 dst=127.53.53.8 sport=35172 dport=53 src=127.53.53.8 dst=127.0.0.1 sport=53 dport=35172 mark=0 use=1

	// Index flows per Forward Destination IP ClusterIP / ExternalIP / LoadBalancerIP
	// and per dport for NodePorts
	idxIPs := map[string][]int{}
	idxPorts := map[int][]int{}
	for n, flow := range flows {
		idx, ok := idxIPs[flow.Forward.DstIP.String()]
		if !ok {
			idxIPs[flow.Forward.DstIP.String()] = []int{n}
		} else {
			idxIPs[flow.Forward.DstIP.String()] = append(idx, n)
		}

		idx, ok = idxPorts[int(flow.Forward.DstPort)]
		if !ok {
			idxPorts[int(flow.Forward.DstPort)] = []int{n}
		} else {
			idxPorts[int(flow.Forward.DstPort)] = append(idx, n)
		}
	}
	// Reconcile the entries against the UDP Services
	services, err := s.serviceLister.List(labels.Everything())
	if err != nil {
		return err
	}
	table := make(map[string][]string) // key: clusterIP:Port  values: array endpoints ip:port
	for _, svc := range services {
		// skip DNS services
		if len(svc.Spec.ClusterIP) == 0 || svc.Spec.ClusterIP == v1.ClusterIPNone {
			continue
		}
		// Get the endpoints of the Service
		esLabelSelector := labels.Set(map[string]string{
			discovery.LabelServiceName: svc.Name,
		}).AsSelectorPreValidated()
		endpointSlices, err := s.endpointSliceLister.EndpointSlices(svc.Namespace).List(esLabelSelector)
		if err != nil {
			return err
		}

		// only UDP ports leave stale entries
		for _, port := range svc.Spec.Ports {
			if port.Protocol != v1.ProtocolUDP {
				continue
			}

			endpointsV4 := getServiceEndpoints(endpointSlices, port, v1.IPv4Protocol)
			endpointsV6 := getServiceEndpoints(endpointSlices, port, v1.IPv6Protocol)
			for _, ip := range svc.Spec.ClusterIPs {
				if netutils.IsIPv6String(ip) {
					table[net.JoinHostPort(ip, strconv.Itoa(int(port.Port)))] = endpointsV6
				} else {
					table[net.JoinHostPort(ip, strconv.Itoa(int(port.Port)))] = endpointsV4
				}
			}
			for _, extIP := range svc.Spec.ExternalIPs {
				// only use the IPs of the same ClusterIP family
				if utilnet.IsIPv6String(extIP) == utilnet.IsIPv6String(ip) {
					externalIPs = append(externalIPs, extIP)
				}
			}
			// LoadBalancer
			for _, ingress := range svc.Status.LoadBalancer.Ingress {
				// only use the IPs of the same ClusterIP family
				if ingress.IP != "" && utilnet.IsIPv6String(ingress.IP) == utilnet.IsIPv6String(ip) {
					externalIPs = append(externalIPs, ingress.IP)
				}
			}

			table[endpoint{ip: ip, port: ports.Port}] = endpoints
			if ports.NodePort != 0 {
				table[endpoint{ip: ip, port: ports.NodePort}] = endpoints
			}
		}

	}
	return nil
}

// return the endpoints that belong to the IPFamily as a slice of IP:Port
func getServiceEndpoints(slices []*discovery.EndpointSlice, svcPort v1.ServicePort, family v1.IPFamily) []string {
	// return an empty object so the caller don't have to check for nil and can use it as an iterator
	if len(slices) == 0 {
		return []string{}
	}
	// Endpoint Slices are allowed to have duplicate endpoints
	// we use a set to deduplicate endpoints
	endpoints := sets.NewString()
	for _, slice := range slices {
		// Only return addresses that belong to the requested IP family
		if slice.AddressType != discovery.AddressType(family) {
			continue
		}

		// build the list of endpoints in the slice
		for _, port := range slice.Ports {
			// If Service port name set it must match the name field in the endpoint
			if svcPort.Name != "" && svcPort.Name != *port.Name {
				continue
			}

			// Skip ports that doesn't match the protocol
			if *port.Protocol != svcPort.Protocol {
				continue
			}

			for _, endpoint := range slice.Endpoints {
				// Skip endpoints that are not ready
				if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
					continue
				}
				for _, ip := range endpoint.Addresses {
					endpoints.Insert(net.JoinHostPort(ip, strconv.Itoa(int(*port.Port))))
				}
			}
		}
	}

	return endpoints.List()
}
