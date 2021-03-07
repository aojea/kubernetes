/*
Copyright 2021 The Kubernetes Authors.

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

package iptables

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/proxy/metrics"

	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
	iptablestest "k8s.io/kubernetes/pkg/util/iptables/testing"

	utilnet "k8s.io/utils/net"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

type input struct {
	numberServices      int
	endpointsPerService int
	serviceType         v1.ServiceType
}

type output struct {
	filterRules float64
	natRules    float64
	latency     time.Duration
}

func TestNumberIptablesRules(t *testing.T) {
	// only run manually
	if os.Getenv("TEST_IPTABLES_PERFORMANCE") == "" {
		t.Skip("Skipping test")
	}

	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt, false)
	metrics.RegisterMetrics()

	// Define experiment variables
	maxServices := 1000
	maxEndpoints := 1000
	increment := 100
	serviceTypes := []v1.ServiceType{v1.ServiceTypeClusterIP, v1.ServiceTypeNodePort, v1.ServiceTypeLoadBalancer}
	result := map[input]output{}
	for _, svcType := range serviceTypes {
		// Create a graph with the output
		p, err := plot.New()
		if err != nil {
			panic(err)
		}
		p.Title.Text = "Kube-proxy iptables rules " + string(svcType)
		p.X.Label.Text = "number of services"
		p.Y.Label.Text = "iptables rules"
		var plotColor int

		for j := 1; j < maxEndpoints; j += increment {
			pts := plotter.XYs{}
			for i := 1; i < maxServices; i += increment {
				t.Logf("Generate %d Services Type %s with %d endpoints each", i, svcType, j)
				in := input{
					numberServices:      i,
					endpointsPerService: j,
					serviceType:         svcType,
				}
				svcs, eps := generateServiceEndpoints(svcType, i, j)

				t.Logf("Updating kube-proxy")
				makeServiceMap(fp, svcs...)
				makeEndpointsMap(fp, eps...)

				t.Logf("Syncing kube-proxy")
				start := time.Now()
				fp.syncProxyRules()
				elapsed := time.Since(start)
				t.Logf("kube-proxy sync rules latency: %v", elapsed)

				t.Logf("Gathering kube-proxy metrics")
				nFilterRules, err := testutil.GetGaugeMetricValue(metrics.IptablesRulesTotal.WithLabelValues(string(utiliptables.TableFilter)))
				if err != nil {
					t.Errorf("failed to get %s value, err: %v", metrics.IptablesRulesTotal.Name, err)
				}

				nNatRules, err := testutil.GetGaugeMetricValue(metrics.IptablesRulesTotal.WithLabelValues(string(utiliptables.TableNAT)))
				if err != nil {
					t.Errorf("failed to get %s value, err: %v", metrics.IptablesRulesTotal.Name, err)
				}

				t.Logf("kube-proxy iptables rules: Filter %.0f NAT %.0f on %v", nFilterRules, nNatRules, elapsed)
				result[in] = output{
					filterRules: nFilterRules,
					natRules:    nNatRules,
					latency:     elapsed,
				}

				ptNat := plotter.XY{
					X: float64(i),
					Y: nNatRules,
				}

				pts = append(pts, ptNat)
				// clear metrics
				metrics.IptablesRulesTotal.Delete(map[string]string{"table": string(utiliptables.TableNAT)})
				metrics.IptablesRulesTotal.Delete(map[string]string{"table": string(utiliptables.TableFilter)})

			}
			lpLine, lpPoints, err := plotter.NewLinePoints(pts)
			if err != nil {
				panic(err)
			}
			p.Add(lpLine, lpPoints)
			lpLine.Color = plotutil.Color(plotColor)
			lpLine.Dashes = plotutil.Dashes(plotColor)
			plotColor++
			p.Legend.Add(fmt.Sprintf("%s-%s-%d", svcType, utiliptables.TableNAT, j), lpLine, lpPoints)
		}
		// Save the plot to a PNG file.
		dstFile := fmt.Sprintf("/tmp/iptables-rules-%s.png", string(svcType))
		if err := p.Save(18*vg.Inch, 10*vg.Inch, dstFile); err != nil {
			panic(err)
		}
	}

	// Draw the results
	t.Logf("EXPERIMENT OUTPUT %v", result)

}

// generateServiceEndpoints generate
func generateServiceEndpoints(svcType v1.ServiceType, nServices, nEndpoints int) ([]*v1.Service, []*v1.Endpoints) {
	services := make([]*v1.Service, nServices)
	endpoints := make([]*v1.Endpoints, nServices)

	// base parameters
	basePort := 80
	base := utilnet.BigForIP(net.ParseIP("10.0.0.1"))
	// generate a base endpoint object
	baseEp := utilnet.BigForIP(net.ParseIP("172.16.0.1"))
	epPort := 8080

	ep := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ep",
			Namespace: "namespace",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{},
			Ports: []v1.EndpointPort{{
				Name:     fmt.Sprintf("%d", epPort),
				Port:     int32(epPort),
				Protocol: v1.ProtocolTCP,
			}},
		}},
	}
	for j := 0; j < nEndpoints; j++ {
		ipEp := utilnet.AddIPOffset(baseEp, j)
		address := v1.EndpointAddress{IP: ipEp.String()}
		ep.Subsets[0].Addresses = append(ep.Subsets[0].Addresses, address)
	}

	// Create the Services and associate and endpoint object to each one
	for i := 0; i < nServices; i++ {
		ip := utilnet.AddIPOffset(base, i)
		svcName := fmt.Sprintf("svc%d", i)
		services[i] = &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: "namespace",
			},
			Spec: v1.ServiceSpec{
				ClusterIP: ip.String(),
				Ports: []v1.ServicePort{{
					Name:       fmt.Sprintf("%d", epPort),
					Protocol:   v1.ProtocolTCP,
					Port:       int32(basePort + i),
					TargetPort: intstr.FromInt(epPort),
				}},
				Type: svcType,
			},
		}

		endpoints[i] = ep.DeepCopy()
		endpoints[i].Name = svcName
	}

	return services, endpoints
}
