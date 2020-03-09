/*
Copyright 2020 The Kubernetes Authors.

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

package metaproxier

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1beta1"
	"k8s.io/kubernetes/pkg/proxy"
)

const testHostname = "test-hostname"

type FakeProxier struct {
	endpointsChanges *proxy.EndpointChangeTracker
	serviceChanges   *proxy.ServiceChangeTracker
	serviceMap       proxy.ServiceMap
	endpointsMap     proxy.EndpointsMap
	hostname         string
}

func makeServiceMap(fake *FakeProxier, allServices ...*v1.Service) {
	for i := range allServices {
		fake.addService(allServices[i])
	}
}

func (fake *FakeProxier) addService(service *v1.Service) {
	fake.serviceChanges.Update(nil, service)
}

func (fake *FakeProxier) updateService(oldService *v1.Service, service *v1.Service) {
	fake.serviceChanges.Update(oldService, service)
}

func (fake *FakeProxier) deleteService(service *v1.Service) {
	fake.serviceChanges.Update(service, nil)
}

func (fake *FakeProxier) OnEndpointAdd(endpoints *v1.Endpoints) {
}

func (fake *FakeProxier) OnServiceAdd(service *v1.Service) {
}

func (fake *FakeProxier) OnServiceUpdate(oldService, service *v1.Service) {
}

func (fake *FakeProxier) OnServiceDelete(service *v1.Service) {
}

func (fake *FakeProxier) OnServiceSynced() {
}

func (fake *FakeProxier) OnEndpointsAdd(endpoints *v1.Endpoints) {
}

func (fake *FakeProxier) OnEndpointsUpdate(oldEndpoints, endpoints *v1.Endpoints) {
}

func (fake *FakeProxier) OnEndpointsDelete(endpoints *v1.Endpoints) {
}

func (fake *FakeProxier) OnEndpointsSynced() {
}

func (fake *FakeProxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
}

func (fake *FakeProxier) OnEndpointSliceUpdate(oldEndpointSlice, newEndpointSlice *discovery.EndpointSlice) {
}

func (fake *FakeProxier) OnEndpointSliceDelete(endpointSlice *discovery.EndpointSlice) {
}

func (fake *FakeProxier) OnEndpointSlicesSynced() {
}

func (fake *FakeProxier) OnNodeAdd(node *v1.Node) {
}

func (fake *FakeProxier) OnNodeDelete(node *v1.Node) {
}

func (fake *FakeProxier) OnNodeSynced() {
}

func (fake *FakeProxier) OnNodeUpdate(oldNode, node *v1.Node) {
}

func (fake *FakeProxier) Sync() {
}

func (fake *FakeProxier) SyncLoop() {
}

func NewFakeProxier() *FakeProxier {
	return &FakeProxier{
		serviceMap:       make(proxy.ServiceMap),
		serviceChanges:   proxy.NewServiceChangeTracker(nil, nil, nil),
		endpointsMap:     make(proxy.EndpointsMap),
		endpointsChanges: proxy.NewEndpointChangeTracker(testHostname, nil, nil, nil, false),
	}
}

func NewFakeMetaProxier() proxy.Provider {
	return proxy.Provider(&metaProxier{
		ipv4Proxier: NewFakeProxier(),
		ipv6Proxier: NewFakeProxier(),
	})
}

func Test_endpointsIPFamily(t *testing.T) {

	ipv4 := v1.IPv4Protocol
	ipv6 := v1.IPv6Protocol

	tests := []struct {
		name      string
		endpoints *v1.Endpoints
		want      *v1.IPFamily
		wantErr   bool
		errorMsg  string
	}{
		{
			name:      "Endpoints No Subsets",
			endpoints: &v1.Endpoints{},
			want:      nil,
			wantErr:   true,
			errorMsg:  "failed to identify ipfamily for endpoints (no subsets)",
		},
		{
			name:      "Endpoints No Addresses",
			endpoints: &v1.Endpoints{Subsets: []v1.EndpointSubset{{NotReadyAddresses: []v1.EndpointAddress{}}}},
			want:      nil,
			wantErr:   true,
			errorMsg:  "failed to identify ipfamily for endpoints (no addresses)",
		},
		{
			name:      "Endpoints Address Has No IP",
			endpoints: &v1.Endpoints{Subsets: []v1.EndpointSubset{{Addresses: []v1.EndpointAddress{{Hostname: "testhost", IP: ""}}}}},
			want:      nil,
			wantErr:   true,
			errorMsg:  "failed to identify ipfamily for endpoints (address has no ip)",
		},
		{
			name:      "Endpoints Address IPv4",
			endpoints: &v1.Endpoints{Subsets: []v1.EndpointSubset{{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}}}}},
			want:      &ipv4,
			wantErr:   false,
		},
		{
			name:      "Endpoints Address IPv6",
			endpoints: &v1.Endpoints{Subsets: []v1.EndpointSubset{{Addresses: []v1.EndpointAddress{{IP: "2001:db9::2"}}}}},
			want:      &ipv6,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := endpointsIPFamily(tt.endpoints)
			if (err != nil) != tt.wantErr {
				t.Errorf("endpointsIPFamily() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && err.Error() != tt.errorMsg {
				t.Errorf("endpointsIPFamily() error = %v, wantErr %v", err, tt.errorMsg)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("endpointsIPFamily() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_metaProxier_Sync(t *testing.T) {
	tests := []struct {
		name    string
		proxier *metaProxier
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.Sync()
		})
	}
}

func Test_metaProxier_SyncLoop(t *testing.T) {
	tests := []struct {
		name    string
		proxier *metaProxier
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.SyncLoop()
		})
	}
}

func Test_metaProxier_OnServiceAdd(t *testing.T) {
	type args struct {
		service *v1.Service
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnServiceAdd(tt.args.service)
		})
	}
}

func Test_metaProxier_OnServiceUpdate(t *testing.T) {
	type args struct {
		oldService *v1.Service
		service    *v1.Service
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnServiceUpdate(tt.args.oldService, tt.args.service)
		})
	}
}

func Test_metaProxier_OnServiceDelete(t *testing.T) {
	type args struct {
		service *v1.Service
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnServiceDelete(tt.args.service)
		})
	}
}

func Test_metaProxier_OnServiceSynced(t *testing.T) {
	tests := []struct {
		name    string
		proxier *metaProxier
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnServiceSynced()
		})
	}
}

func Test_metaProxier_OnEndpointsAdd(t *testing.T) {
	type args struct {
		endpoints *v1.Endpoints
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointsAdd(tt.args.endpoints)
		})
	}
}

func Test_metaProxier_OnEndpointsUpdate(t *testing.T) {
	type args struct {
		oldEndpoints *v1.Endpoints
		endpoints    *v1.Endpoints
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointsUpdate(tt.args.oldEndpoints, tt.args.endpoints)
		})
	}
}

func Test_metaProxier_OnEndpointsDelete(t *testing.T) {
	type args struct {
		endpoints *v1.Endpoints
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointsDelete(tt.args.endpoints)
		})
	}
}

func Test_metaProxier_OnEndpointsSynced(t *testing.T) {
	tests := []struct {
		name    string
		proxier *metaProxier
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointsSynced()
		})
	}
}

func Test_metaProxier_OnEndpointSliceAdd(t *testing.T) {
	type args struct {
		endpointSlice *discovery.EndpointSlice
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointSliceAdd(tt.args.endpointSlice)
		})
	}
}

func Test_metaProxier_OnEndpointSliceUpdate(t *testing.T) {
	type args struct {
		oldEndpointSlice *discovery.EndpointSlice
		newEndpointSlice *discovery.EndpointSlice
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointSliceUpdate(tt.args.oldEndpointSlice, tt.args.newEndpointSlice)
		})
	}
}

func Test_metaProxier_OnEndpointSliceDelete(t *testing.T) {
	type args struct {
		endpointSlice *discovery.EndpointSlice
	}
	tests := []struct {
		name    string
		proxier *metaProxier
		args    args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointSliceDelete(tt.args.endpointSlice)
		})
	}
}

func Test_metaProxier_OnEndpointSlicesSynced(t *testing.T) {
	tests := []struct {
		name    string
		proxier *metaProxier
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.proxier.OnEndpointSlicesSynced()
		})
	}
}
