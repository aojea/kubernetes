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

package service

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/integration/framework"
)

func Test_ServiceUpdateNodePorts(t *testing.T) {
	controlPlaneConfig := framework.NewIntegrationTestControlPlaneConfig()
	_, server, closeFn := framework.RunAnAPIServer(controlPlaneConfig)
	defer closeFn()

	config := restclient.Config{Host: server.URL}
	client, err := clientset.NewForConfig(&config)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateTestingNamespace("test-service-update-node-ports", server, t)
	defer framework.DeleteTestingNamespace(ns, server, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                  corev1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyTypeLocal,
			Ports: []corev1.ServicePort{
				{
					Port: int32(100),
					Name: "port1",
				},
				{
					Port: int32(200),
					Name: "port2",
				},
				{
					Port: int32(300),
					Name: "port3",
				},
			},
			Selector: map[string]string{
				"foo": "bar",
			},
		},
	}

	svc, err := client.CoreV1().Services(ns.Name).Create(context.TODO(), service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	// update the service specifying one of the allocate ports
	service.Spec.Ports[0].NodePort = svc.Spec.Ports[2].NodePort
	svc, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error updating test service: %v", err)
	}

}
