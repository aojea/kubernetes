/*
Copyright The Kubernetes Authors.

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

package pods

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/utils/ptr"
)

func isolatedContainer() v1.Container {
	return v1.Container{Name: "c", Image: "registry.k8s.io/pause:3.10"}
}

// TestPodDefaultNetworkCreate covers defaulting, hostNetwork mirroring and
// validation of spec.defaultNetwork on Pods with the gate enabled.
func TestPodDefaultNetworkCreate(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodDefaultNetwork, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()
	client := clientset.NewForConfigOrDie(server.ClientConfig)
	ns := framework.CreateNamespaceOrDie(client, "pod-default-network", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)
	ctx := context.Background()

	httpProbe := &v1.Probe{ProbeHandler: v1.ProbeHandler{HTTPGet: &v1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080)}}}
	execProbe := &v1.Probe{ProbeHandler: v1.ProbeHandler{Exec: &v1.ExecAction{Command: []string{"true"}}}}

	testCases := []struct {
		name               string
		spec               v1.PodSpec
		wantErr            string
		wantDefaultNetwork v1.PodDefaultNetwork
		wantHostNetwork    bool
		wantDNSPolicy      v1.DNSPolicy
		wantServiceLinks   bool
	}{
		{
			name:               "unset defaults to Pod",
			spec:               v1.PodSpec{Containers: []v1.Container{isolatedContainer()}},
			wantDefaultNetwork: v1.PodDefaultNetworkPod,
			wantDNSPolicy:      v1.DNSClusterFirst,
			wantServiceLinks:   true,
		},
		{
			name:               "old client hostNetwork defaults to Host",
			spec:               v1.PodSpec{HostNetwork: true, Containers: []v1.Container{isolatedContainer()}},
			wantDefaultNetwork: v1.PodDefaultNetworkHost,
			wantHostNetwork:    true,
			wantDNSPolicy:      v1.DNSClusterFirst,
			wantServiceLinks:   true,
		},
		{
			name:               "Host mirrors to hostNetwork",
			spec:               v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkHost), Containers: []v1.Container{isolatedContainer()}},
			wantDefaultNetwork: v1.PodDefaultNetworkHost,
			wantHostNetwork:    true,
			wantDNSPolicy:      v1.DNSClusterFirst,
			wantServiceLinks:   true,
		},
		{
			// hostNetwork validation requires hostPort == containerPort on Pods;
			// "Host" must get the same hostPort defaulting as hostNetwork: true.
			name: "Host defaults hostPort",
			spec: v1.PodSpec{
				DefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
				Containers: []v1.Container{{
					Name: "c", Image: "registry.k8s.io/pause:3.10",
					Ports: []v1.ContainerPort{{ContainerPort: 8080, Protocol: v1.ProtocolTCP}},
				}},
			},
			wantDefaultNetwork: v1.PodDefaultNetworkHost,
			wantHostNetwork:    true,
			wantDNSPolicy:      v1.DNSClusterFirst,
			wantServiceLinks:   true,
		},
		{
			name:               "Pod with hostNetwork becomes Host",
			spec:               v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkPod), HostNetwork: true, Containers: []v1.Container{isolatedContainer()}},
			wantDefaultNetwork: v1.PodDefaultNetworkHost,
			wantHostNetwork:    true,
			wantDNSPolicy:      v1.DNSClusterFirst,
			wantServiceLinks:   true,
		},
		{
			name: "None defaults dnsPolicy and enableServiceLinks",
			spec: v1.PodSpec{
				DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
				Containers: []v1.Container{{
					Name: "c", Image: "registry.k8s.io/pause:3.10", LivenessProbe: execProbe,
					Ports: []v1.ContainerPort{{ContainerPort: 8080, Protocol: v1.ProtocolTCP}},
				}},
			},
			wantDefaultNetwork: v1.PodDefaultNetworkNone,
			wantDNSPolicy:      v1.DNSNone,
			wantServiceLinks:   false,
		},
		{
			name: "None keeps explicit dnsPolicy, dnsConfig and enableServiceLinks",
			spec: v1.PodSpec{
				DefaultNetwork:     ptr.To(v1.PodDefaultNetworkNone),
				DNSPolicy:          v1.DNSNone,
				DNSConfig:          &v1.PodDNSConfig{Nameservers: []string{"127.0.0.53"}},
				EnableServiceLinks: ptr.To(true),
				Containers:         []v1.Container{isolatedContainer()},
			},
			wantDefaultNetwork: v1.PodDefaultNetworkNone,
			wantDNSPolicy:      v1.DNSNone,
			wantServiceLinks:   true,
		},
		{
			name:    "None with hostNetwork is rejected",
			spec:    v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone), HostNetwork: true, Containers: []v1.Container{isolatedContainer()}},
			wantErr: `must not be "None" when hostNetwork is true`,
		},
		{
			name: "None with HTTP probe is rejected",
			spec: v1.PodSpec{
				DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
				Containers:     []v1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10", LivenessProbe: httpProbe}},
			},
			wantErr: `spec.containers[0].livenessProbe.httpGet: Forbidden: may not be set when defaultNetwork is "None"`,
		},
		{
			name: "None with hostPort is rejected",
			spec: v1.PodSpec{
				DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
				Containers: []v1.Container{{
					Name: "c", Image: "registry.k8s.io/pause:3.10",
					Ports: []v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080, Protocol: v1.ProtocolTCP}},
				}},
			},
			wantErr: `spec.containers[0].ports[0].hostPort: Forbidden: may not be set when defaultNetwork is "None"`,
		},
		{
			name:    "unsupported value is rejected",
			spec:    v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetwork("Bridge")), Containers: []v1.Container{isolatedContainer()}},
			wantErr: `spec.defaultNetwork: Unsupported value: "Bridge"`,
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i), Namespace: ns.Name},
				Spec:       tc.spec,
			}
			created, err := client.CoreV1().Pods(ns.Name).Create(ctx, pod, metav1.CreateOptions{})
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got none", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := ptr.Deref(created.Spec.DefaultNetwork, ""); got != tc.wantDefaultNetwork {
				t.Errorf("spec.defaultNetwork = %q, want %q", got, tc.wantDefaultNetwork)
			}
			if created.Spec.HostNetwork != tc.wantHostNetwork {
				t.Errorf("spec.hostNetwork = %v, want %v", created.Spec.HostNetwork, tc.wantHostNetwork)
			}
			if created.Spec.DNSPolicy != tc.wantDNSPolicy {
				t.Errorf("spec.dnsPolicy = %q, want %q", created.Spec.DNSPolicy, tc.wantDNSPolicy)
			}
			if got := ptr.Deref(created.Spec.EnableServiceLinks, false); got != tc.wantServiceLinks {
				t.Errorf("spec.enableServiceLinks = %v, want %v", got, tc.wantServiceLinks)
			}
		})
	}
}

// TestPodDefaultNetworkUpdate covers immutability of the field and the status
// invariant that isolated pods never report pod IPs.
func TestPodDefaultNetworkUpdate(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodDefaultNetwork, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()
	client := clientset.NewForConfigOrDie(server.ClientConfig)
	ns := framework.CreateNamespaceOrDie(client, "pod-default-network-update", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)
	ctx := context.Background()

	pod, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "isolated", Namespace: ns.Name},
		Spec:       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone), Containers: []v1.Container{isolatedContainer()}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	t.Run("defaultNetwork is immutable", func(t *testing.T) {
		update := pod.DeepCopy()
		update.Spec.DefaultNetwork = ptr.To(v1.PodDefaultNetworkPod)
		_, err := client.CoreV1().Pods(ns.Name).Update(ctx, update, metav1.UpdateOptions{})
		if err == nil || !strings.Contains(err.Error(), "pod updates may not change fields other than") {
			t.Fatalf("expected immutability error, got: %v", err)
		}
	})

	t.Run("status update with a pod IP is rejected", func(t *testing.T) {
		update := pod.DeepCopy()
		update.Status.PodIP = "10.0.0.1"
		update.Status.PodIPs = []v1.PodIP{{IP: "10.0.0.1"}}
		_, err := client.CoreV1().Pods(ns.Name).UpdateStatus(ctx, update, metav1.UpdateOptions{})
		if err == nil || !strings.Contains(err.Error(), `status.podIP: Forbidden: must be empty when spec.defaultNetwork is "None"`) {
			t.Fatalf("expected status validation error, got: %v", err)
		}
	})

	t.Run("status update with host IPs only is accepted", func(t *testing.T) {
		update := pod.DeepCopy()
		update.Status.Phase = v1.PodRunning
		update.Status.HostIP = "192.168.0.1"
		update.Status.HostIPs = []v1.HostIP{{IP: "192.168.0.1"}}
		if _, err := client.CoreV1().Pods(ns.Name).UpdateStatus(ctx, update, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("unexpected status update error: %v", err)
		}
	})
}

// TestPodDefaultNetworkWorkloadTemplates covers the field in every workload
// PodTemplateSpec and verifies that creating a Pod from the stored template
// spec produces the expected Pod across all creation, validation and PATCH
// combinations.
func TestPodDefaultNetworkWorkloadTemplates(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodDefaultNetwork, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()
	client := clientset.NewForConfigOrDie(server.ClientConfig)
	ns := framework.CreateNamespaceOrDie(client, "pod-default-network-workloads", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)
	ctx := context.Background()

	labels := map[string]string{"app": "isolated"}
	containerWithPort := func() v1.Container {
		c := isolatedContainer()
		c.Ports = []v1.ContainerPort{{ContainerPort: 8080, Protocol: v1.ProtocolTCP}}
		return c
	}
	template := func(spec v1.PodSpec, restartPolicy v1.RestartPolicy) v1.PodTemplateSpec {
		spec.RestartPolicy = restartPolicy
		if len(spec.Containers) == 0 {
			spec.Containers = []v1.Container{containerWithPort()}
		}
		return v1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec:       spec,
		}
	}

	// create returns the stored template spec and patch applies a strategic merge
	// patch whose template spec is specPatchJSON.
	type workload struct {
		name              string
		create            func(name string, spec v1.PodSpec) (*v1.PodSpec, error)
		patch             func(name string, specPatchJSON string) (*v1.PodSpec, error)
		immutableTemplate bool
	}
	wrapStandardTemplatePatch := func(specPatch string) string {
		return fmt.Sprintf(`{"spec":{"template":{"spec":%s}}}`, specPatch)
	}
	workloads := []workload{
		{
			name: "Deployment",
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().Deployments(ns.Name).Create(ctx, &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Spec: appsv1.DeploymentSpec{
						Selector: &metav1.LabelSelector{MatchLabels: labels},
						Template: template(spec, v1.RestartPolicyAlways),
					},
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().Deployments(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(wrapStandardTemplatePatch(specPatch)), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
		},
		{
			name: "StatefulSet",
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().StatefulSets(ns.Name).Create(ctx, &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Spec: appsv1.StatefulSetSpec{
						Selector:    &metav1.LabelSelector{MatchLabels: labels},
						ServiceName: "headless",
						Template:    template(spec, v1.RestartPolicyAlways),
					},
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().StatefulSets(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(wrapStandardTemplatePatch(specPatch)), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
		},
		{
			name: "DaemonSet",
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().DaemonSets(ns.Name).Create(ctx, &appsv1.DaemonSet{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Spec: appsv1.DaemonSetSpec{
						Selector: &metav1.LabelSelector{MatchLabels: labels},
						Template: template(spec, v1.RestartPolicyAlways),
					},
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().DaemonSets(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(wrapStandardTemplatePatch(specPatch)), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
		},
		{
			name: "ReplicaSet",
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().ReplicaSets(ns.Name).Create(ctx, &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Spec: appsv1.ReplicaSetSpec{
						Selector: &metav1.LabelSelector{MatchLabels: labels},
						Template: template(spec, v1.RestartPolicyAlways),
					},
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				obj, err := client.AppsV1().ReplicaSets(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(wrapStandardTemplatePatch(specPatch)), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
		},
		{
			name: "ReplicationController",
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				tmpl := template(spec, v1.RestartPolicyAlways)
				obj, err := client.CoreV1().ReplicationControllers(ns.Name).Create(ctx, &v1.ReplicationController{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Spec: v1.ReplicationControllerSpec{
						Selector: labels,
						Template: &tmpl,
					},
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				obj, err := client.CoreV1().ReplicationControllers(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(wrapStandardTemplatePatch(specPatch)), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
		},
		{
			name:              "Job",
			immutableTemplate: true,
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				obj, err := client.BatchV1().Jobs(ns.Name).Create(ctx, &batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Spec:       batchv1.JobSpec{Template: template(spec, v1.RestartPolicyNever)},
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				obj, err := client.BatchV1().Jobs(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(wrapStandardTemplatePatch(specPatch)), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.Template.Spec, nil
			},
		},
		{
			name: "CronJob",
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				obj, err := client.BatchV1().CronJobs(ns.Name).Create(ctx, &batchv1.CronJob{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Spec: batchv1.CronJobSpec{
						Schedule:    "* * * * *",
						JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: template(spec, v1.RestartPolicyNever)}},
					},
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.JobTemplate.Spec.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				patchBody := fmt.Sprintf(`{"spec":{"jobTemplate":{"spec":{"template":{"spec":%s}}}}}`, specPatch)
				obj, err := client.BatchV1().CronJobs(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(patchBody), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Spec.JobTemplate.Spec.Template.Spec, nil
			},
		},
		{
			name: "PodTemplate",
			create: func(name string, spec v1.PodSpec) (*v1.PodSpec, error) {
				obj, err := client.CoreV1().PodTemplates(ns.Name).Create(ctx, &v1.PodTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: name},
					Template:   template(spec, v1.RestartPolicyAlways),
				}, metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Template.Spec, nil
			},
			patch: func(name, specPatch string) (*v1.PodSpec, error) {
				patchBody := fmt.Sprintf(`{"template":{"spec":%s}}`, specPatch)
				obj, err := client.CoreV1().PodTemplates(ns.Name).Patch(ctx, name, types.StrategicMergePatchType, []byte(patchBody), metav1.PatchOptions{})
				if err != nil {
					return nil, err
				}
				return &obj.Template.Spec, nil
			},
		},
	}

	httpProbe := &v1.Probe{ProbeHandler: v1.ProbeHandler{HTTPGet: &v1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080)}}}

	createCases := []struct {
		name                       string
		suffix                     string
		spec                       v1.PodSpec
		wantErr                    string
		wantTemplateDefaultNetwork *v1.PodDefaultNetwork
		wantPodDefaultNetwork      v1.PodDefaultNetwork
		wantHostNetwork            bool
		wantPodHostPort            int32
		wantDNSPolicy              v1.DNSPolicy
		wantTemplateServiceLinks   *bool
		wantPodServiceLinks        bool
	}{
		{
			name:                       "unset stays unset on template and defaults to Pod on Pod",
			suffix:                     "unset",
			spec:                       v1.PodSpec{},
			wantTemplateDefaultNetwork: nil,
			wantPodDefaultNetwork:      v1.PodDefaultNetworkPod,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantTemplateServiceLinks:   nil,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "unset with hostNetwork stays unset on template and defaults to Host on Pod",
			suffix:                     "unset-hostnet",
			spec:                       v1.PodSpec{HostNetwork: true},
			wantTemplateDefaultNetwork: nil,
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantTemplateServiceLinks:   nil,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "Pod stays Pod on template and Pod",
			suffix:                     "pod",
			spec:                       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkPod)},
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkPod),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkPod,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantTemplateServiceLinks:   nil,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "Pod with hostNetwork becomes Host on template and Pod",
			suffix:                     "pod-hostnet",
			spec:                       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkPod), HostNetwork: true},
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantTemplateServiceLinks:   nil,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "Host mirrors to hostNetwork on template and defaults hostPort on Pod",
			suffix:                     "host",
			spec:                       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkHost)},
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantTemplateServiceLinks:   nil,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "Host with hostNetwork on template and Pod",
			suffix:                     "host-hostnet",
			spec:                       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkHost), HostNetwork: true},
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantTemplateServiceLinks:   nil,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "None defaults dnsPolicy on template and enableServiceLinks on Pod",
			suffix:                     "none",
			spec:                       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone)},
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkNone,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSNone,
			wantTemplateServiceLinks:   nil,
			wantPodServiceLinks:        false,
		},
		{
			name:   "None keeps explicit dnsPolicy and enableServiceLinks on template and Pod",
			suffix: "none-explicit",
			spec: v1.PodSpec{
				DefaultNetwork:     ptr.To(v1.PodDefaultNetworkNone),
				DNSPolicy:          v1.DNSClusterFirst,
				EnableServiceLinks: ptr.To(true),
			},
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkNone,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantTemplateServiceLinks:   ptr.To(true),
			wantPodServiceLinks:        true,
		},
		{
			name:    "None with hostNetwork is rejected on template",
			suffix:  "err-none-hostnet",
			spec:    v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone), HostNetwork: true},
			wantErr: `must not be "None" when hostNetwork is true`,
		},
		{
			name:   "None with HTTP probe is rejected on template",
			suffix: "err-none-probe",
			spec: v1.PodSpec{
				DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
				Containers:     []v1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10", LivenessProbe: httpProbe}},
			},
			wantErr: `livenessProbe.httpGet: Forbidden: may not be set when defaultNetwork is "None"`,
		},
		{
			name:   "None with hostPort is rejected on template",
			suffix: "err-none-hostport",
			spec: v1.PodSpec{
				DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
				Containers: []v1.Container{{
					Name: "c", Image: "registry.k8s.io/pause:3.10",
					Ports: []v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080, Protocol: v1.ProtocolTCP}},
				}},
			},
			wantErr: `ports[0].hostPort: Forbidden: may not be set when defaultNetwork is "None"`,
		},
		{
			name:    "unsupported value is rejected on template",
			suffix:  "err-unsupported",
			spec:    v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetwork("Bridge"))},
			wantErr: `defaultNetwork: Unsupported value: "Bridge"`,
		},
	}

	patchCases := []struct {
		name                       string
		suffix                     string
		initialSpec                v1.PodSpec
		patchJSON                  string
		wantErr                    string
		wantTemplateDefaultNetwork *v1.PodDefaultNetwork
		wantPodDefaultNetwork      v1.PodDefaultNetwork
		wantHostNetwork            bool
		wantPodHostPort            int32
		wantDNSPolicy              v1.DNSPolicy
		wantPodServiceLinks        bool
	}{
		{
			name:                       "unset patched with hostNetwork true",
			suffix:                     "patch-unset-to-hostnet",
			initialSpec:                v1.PodSpec{},
			patchJSON:                  `{"hostNetwork":true}`,
			wantTemplateDefaultNetwork: nil,
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "unset patched with defaultNetwork Pod",
			suffix:                     "patch-unset-to-pod",
			initialSpec:                v1.PodSpec{},
			patchJSON:                  `{"defaultNetwork":"Pod"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkPod),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkPod,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "unset patched with defaultNetwork Host",
			suffix:                     "patch-unset-to-host",
			initialSpec:                v1.PodSpec{},
			patchJSON:                  `{"defaultNetwork":"Host"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "unset patched with defaultNetwork None",
			suffix:                     "patch-unset-to-none",
			initialSpec:                v1.PodSpec{},
			patchJSON:                  `{"defaultNetwork":"None"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkNone,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			// dnsPolicy was already defaulted to ClusterFirst when the template was created.
			wantDNSPolicy:       v1.DNSClusterFirst,
			wantPodServiceLinks: false,
		},
		{
			name:                       "hostNetwork template patched with hostNetwork false",
			suffix:                     "patch-hostnet-to-false",
			initialSpec:                v1.PodSpec{HostNetwork: true},
			patchJSON:                  `{"hostNetwork":false}`,
			wantTemplateDefaultNetwork: nil,
			wantPodDefaultNetwork:      v1.PodDefaultNetworkPod,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "hostNetwork template patched with defaultNetwork Host",
			suffix:                     "patch-hostnet-to-host",
			initialSpec:                v1.PodSpec{HostNetwork: true},
			patchJSON:                  `{"defaultNetwork":"Host"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "hostNetwork template patched with defaultNetwork Pod and hostNetwork false",
			suffix:                     "patch-hostnet-to-pod",
			initialSpec:                v1.PodSpec{HostNetwork: true},
			patchJSON:                  `{"defaultNetwork":"Pod","hostNetwork":false}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkPod),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkPod,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "hostNetwork template patched with defaultNetwork None and hostNetwork false",
			suffix:                     "patch-hostnet-to-none",
			initialSpec:                v1.PodSpec{HostNetwork: true},
			patchJSON:                  `{"defaultNetwork":"None","hostNetwork":false}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkNone,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        false,
		},
		{
			name:                       "old client patches hostNetwork into a Pod template",
			suffix:                     "patch-pod-to-hostnet",
			initialSpec:                v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkPod)},
			patchJSON:                  `{"hostNetwork":true}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "Pod template patched with defaultNetwork Host",
			suffix:                     "patch-pod-to-host",
			initialSpec:                v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkPod)},
			patchJSON:                  `{"defaultNetwork":"Host"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "Pod template patched with defaultNetwork None",
			suffix:                     "patch-pod-to-none",
			initialSpec:                v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkPod)},
			patchJSON:                  `{"defaultNetwork":"None"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkNone,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        false,
		},
		{
			name:                       "Host template patched with defaultNetwork Pod and hostNetwork false",
			suffix:                     "patch-host-to-pod",
			initialSpec:                v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkHost)},
			patchJSON:                  `{"defaultNetwork":"Pod","hostNetwork":false}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkPod),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkPod,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "Host template patched with defaultNetwork None and hostNetwork false",
			suffix:                     "patch-host-to-none",
			initialSpec:                v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkHost)},
			patchJSON:                  `{"defaultNetwork":"None","hostNetwork":false}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkNone),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkNone,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        false,
		},
		{
			name:                       "None template patched with defaultNetwork Pod and dnsPolicy ClusterFirst",
			suffix:                     "patch-none-to-pod",
			initialSpec:                v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone)},
			patchJSON:                  `{"defaultNetwork":"Pod","dnsPolicy":"ClusterFirst"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkPod),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkPod,
			wantHostNetwork:            false,
			wantPodHostPort:            0,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:                       "None template patched with defaultNetwork Host and dnsPolicy ClusterFirst",
			suffix:                     "patch-none-to-host",
			initialSpec:                v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone)},
			patchJSON:                  `{"defaultNetwork":"Host","dnsPolicy":"ClusterFirst"}`,
			wantTemplateDefaultNetwork: ptr.To(v1.PodDefaultNetworkHost),
			wantPodDefaultNetwork:      v1.PodDefaultNetworkHost,
			wantHostNetwork:            true,
			wantPodHostPort:            8080,
			wantDNSPolicy:              v1.DNSClusterFirst,
			wantPodServiceLinks:        true,
		},
		{
			name:        "None template patched with hostNetwork is rejected",
			suffix:      "patch-none-to-hostnet-err",
			initialSpec: v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone)},
			patchJSON:   `{"hostNetwork":true}`,
			wantErr:     `must not be "None" when hostNetwork is true`,
		},
	}

	verifyTemplateAndPod := func(t *testing.T, podName string, spec *v1.PodSpec, wantTemplateDefaultNetwork *v1.PodDefaultNetwork, wantPodDefaultNetwork v1.PodDefaultNetwork, wantHostNetwork bool, wantPodHostPort int32, wantDNSPolicy v1.DNSPolicy, wantTemplateServiceLinks *bool, wantPodServiceLinks bool) {
		t.Helper()
		if !ptr.Equal(spec.DefaultNetwork, wantTemplateDefaultNetwork) {
			t.Errorf("template defaultNetwork = %v, want %v", ptr.Deref(spec.DefaultNetwork, "<nil>"), ptr.Deref(wantTemplateDefaultNetwork, "<nil>"))
		}
		if spec.HostNetwork != wantHostNetwork {
			t.Errorf("template hostNetwork = %v, want %v", spec.HostNetwork, wantHostNetwork)
		}
		if len(spec.Containers) > 0 && len(spec.Containers[0].Ports) > 0 && spec.Containers[0].Ports[0].HostPort != 0 {
			t.Errorf("template hostPort = %d, want 0 (defaulted on Pods only)", spec.Containers[0].Ports[0].HostPort)
		}
		if spec.DNSPolicy != wantDNSPolicy {
			t.Errorf("template dnsPolicy = %q, want %q", spec.DNSPolicy, wantDNSPolicy)
		}
		if !ptr.Equal(spec.EnableServiceLinks, wantTemplateServiceLinks) {
			t.Errorf("template enableServiceLinks = %v, want %v", spec.EnableServiceLinks, wantTemplateServiceLinks)
		}

		// Instantiate a Pod from the stored template spec, just as a workload controller does.
		createdPod, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: ns.Name},
			Spec:       *spec.DeepCopy(),
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create pod from template: %v", err)
		}
		if got := ptr.Deref(createdPod.Spec.DefaultNetwork, ""); got != wantPodDefaultNetwork {
			t.Errorf("pod from template defaultNetwork = %q, want %q", got, wantPodDefaultNetwork)
		}
		if createdPod.Spec.HostNetwork != wantHostNetwork {
			t.Errorf("pod from template hostNetwork = %v, want %v", createdPod.Spec.HostNetwork, wantHostNetwork)
		}
		if len(createdPod.Spec.Containers) > 0 && len(createdPod.Spec.Containers[0].Ports) > 0 && createdPod.Spec.Containers[0].Ports[0].HostPort != wantPodHostPort {
			t.Errorf("pod from template hostPort = %d, want %d", createdPod.Spec.Containers[0].Ports[0].HostPort, wantPodHostPort)
		}
		if createdPod.Spec.DNSPolicy != wantDNSPolicy {
			t.Errorf("pod from template dnsPolicy = %q, want %q", createdPod.Spec.DNSPolicy, wantDNSPolicy)
		}
		if got := ptr.Deref(createdPod.Spec.EnableServiceLinks, false); got != wantPodServiceLinks {
			t.Errorf("pod from template enableServiceLinks = %v, want %v", got, wantPodServiceLinks)
		}
	}

	for _, w := range workloads {
		prefix := strings.ToLower(w.name)
		t.Run(w.name, func(t *testing.T) {
			for _, tc := range createCases {
				t.Run(tc.name, func(t *testing.T) {
					objName := prefix + "-" + tc.suffix
					spec, err := w.create(objName, *tc.spec.DeepCopy())
					if tc.wantErr != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
							t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("create: %v", err)
					}
					verifyTemplateAndPod(t, objName+"-pod", spec, tc.wantTemplateDefaultNetwork, tc.wantPodDefaultNetwork, tc.wantHostNetwork, tc.wantPodHostPort, tc.wantDNSPolicy, tc.wantTemplateServiceLinks, tc.wantPodServiceLinks)
				})
			}

			for _, tc := range patchCases {
				t.Run(tc.name, func(t *testing.T) {
					if w.immutableTemplate {
						t.Skip("pod template is immutable")
					}
					objName := prefix + "-" + tc.suffix
					if _, err := w.create(objName, *tc.initialSpec.DeepCopy()); err != nil {
						t.Fatalf("create: %v", err)
					}
					spec, err := w.patch(objName, tc.patchJSON)
					if tc.wantErr != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
							t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("patch: %v", err)
					}
					verifyTemplateAndPod(t, objName+"-pod", spec, tc.wantTemplateDefaultNetwork, tc.wantPodDefaultNetwork, tc.wantHostNetwork, tc.wantPodHostPort, tc.wantDNSPolicy, nil, tc.wantPodServiceLinks)
				})
			}
		})
	}
}

// TestPodDefaultNetworkGateDisabled covers the field being dropped on write
// while the gate is disabled, kept on objects that already carry it, and
// PodTemplate to Pod instantiation across gate transitions.
func TestPodDefaultNetworkGateDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodDefaultNetwork, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()
	client := clientset.NewForConfigOrDie(server.ClientConfig)
	ns := framework.CreateNamespaceOrDie(client, "pod-default-network-gate", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)
	ctx := context.Background()

	containerWithPort := func() v1.Container {
		c := isolatedContainer()
		c.Ports = []v1.ContainerPort{{ContainerPort: 8080, Protocol: v1.ProtocolTCP}}
		return c
	}

	// Objects created while the gate is enabled.
	isolated, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "isolated", Namespace: ns.Name},
		Spec:       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone), Containers: []v1.Container{isolatedContainer()}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	templatePodNet, err := client.CoreV1().PodTemplates(ns.Name).Create(ctx, &v1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-pod-enabled", Namespace: ns.Name},
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkPod), Containers: []v1.Container{containerWithPort()}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create Pod template: %v", err)
	}
	templateHostNet, err := client.CoreV1().PodTemplates(ns.Name).Create(ctx, &v1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-host-enabled", Namespace: ns.Name},
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkHost), Containers: []v1.Container{containerWithPort()}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create Host template: %v", err)
	}
	templateNoneNet, err := client.CoreV1().PodTemplates(ns.Name).Create(ctx, &v1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-none-enabled", Namespace: ns.Name},
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone), Containers: []v1.Container{containerWithPort()}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create None template: %v", err)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodDefaultNetwork, false)

	t.Run("field is dropped on pod and template create", func(t *testing.T) {
		created, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "dropped", Namespace: ns.Name},
			Spec:       v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone), Containers: []v1.Container{isolatedContainer()}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}
		if created.Spec.DefaultNetwork != nil {
			t.Errorf("spec.defaultNetwork = %q, want dropped", *created.Spec.DefaultNetwork)
		}
		if created.Spec.DNSPolicy != v1.DNSClusterFirst {
			t.Errorf("spec.dnsPolicy = %q, want %q", created.Spec.DNSPolicy, v1.DNSClusterFirst)
		}
		if !ptr.Deref(created.Spec.EnableServiceLinks, false) {
			t.Errorf("spec.enableServiceLinks = %v, want true", created.Spec.EnableServiceLinks)
		}

		createdTmpl, err := client.CoreV1().PodTemplates(ns.Name).Create(ctx, &v1.PodTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "tmpl-dropped", Namespace: ns.Name},
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{DefaultNetwork: ptr.To(v1.PodDefaultNetworkNone), Containers: []v1.Container{containerWithPort()}},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create template: %v", err)
		}
		if createdTmpl.Template.Spec.DefaultNetwork != nil {
			t.Errorf("template.spec.defaultNetwork = %q, want dropped", *createdTmpl.Template.Spec.DefaultNetwork)
		}
		if createdTmpl.Template.Spec.DNSPolicy != v1.DNSClusterFirst {
			t.Errorf("template.spec.dnsPolicy = %q, want %q", createdTmpl.Template.Spec.DNSPolicy, v1.DNSClusterFirst)
		}
	})

	t.Run("hostNetwork is not mirrored", func(t *testing.T) {
		created, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "hostnet", Namespace: ns.Name},
			Spec:       v1.PodSpec{HostNetwork: true, Containers: []v1.Container{isolatedContainer()}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}
		if created.Spec.DefaultNetwork != nil {
			t.Errorf("spec.defaultNetwork = %q, want unset", *created.Spec.DefaultNetwork)
		}
	})

	t.Run("stored field is kept on pod and template and still validated", func(t *testing.T) {
		got, err := client.CoreV1().Pods(ns.Name).Get(ctx, isolated.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if ptr.Deref(got.Spec.DefaultNetwork, "") != v1.PodDefaultNetworkNone {
			t.Fatalf("spec.defaultNetwork = %v, want None", got.Spec.DefaultNetwork)
		}
		// Label update keeps the field.
		got.Labels = map[string]string{"updated": "true"}
		updated, err := client.CoreV1().Pods(ns.Name).Update(ctx, got, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("failed to update pod: %v", err)
		}
		if ptr.Deref(updated.Spec.DefaultNetwork, "") != v1.PodDefaultNetworkNone {
			t.Errorf("spec.defaultNetwork = %v after update, want None", updated.Spec.DefaultNetwork)
		}
		// The status invariant does not depend on the gate.
		updated.Status.PodIP = "10.0.0.1"
		updated.Status.PodIPs = []v1.PodIP{{IP: "10.0.0.1"}}
		if _, err := client.CoreV1().Pods(ns.Name).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err == nil {
			t.Errorf("expected status update with a pod IP to be rejected")
		}

		// Updating a stored template while the gate is disabled also preserves the field.
		gotTmpl, err := client.CoreV1().PodTemplates(ns.Name).Get(ctx, templateNoneNet.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get template: %v", err)
		}
		gotTmpl.Labels = map[string]string{"updated": "true"}
		updatedTmpl, err := client.CoreV1().PodTemplates(ns.Name).Update(ctx, gotTmpl, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("failed to update template: %v", err)
		}
		if ptr.Deref(updatedTmpl.Template.Spec.DefaultNetwork, "") != v1.PodDefaultNetworkNone {
			t.Errorf("template.spec.defaultNetwork = %v after update, want None", updatedTmpl.Template.Spec.DefaultNetwork)
		}
	})

	t.Run("creating Pods from templates stored while the gate was enabled", func(t *testing.T) {
		// "Pod" template -> new Pod has defaultNetwork dropped and runs on pod network.
		podFromPodTmpl, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "from-pod-tmpl-disabled", Namespace: ns.Name},
			Spec:       *templatePodNet.Template.Spec.DeepCopy(),
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create pod from Pod template: %v", err)
		}
		if podFromPodTmpl.Spec.DefaultNetwork != nil {
			t.Errorf("spec.defaultNetwork = %q, want dropped", *podFromPodTmpl.Spec.DefaultNetwork)
		}
		if podFromPodTmpl.Spec.HostNetwork {
			t.Errorf("spec.hostNetwork = true, want false")
		}

		// "Host" template had hostNetwork: true mirrored on creation -> new Pod
		// drops defaultNetwork but keeps hostNetwork: true and defaults hostPort.
		podFromHostTmpl, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "from-host-tmpl-disabled", Namespace: ns.Name},
			Spec:       *templateHostNet.Template.Spec.DeepCopy(),
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create pod from Host template: %v", err)
		}
		if podFromHostTmpl.Spec.DefaultNetwork != nil {
			t.Errorf("spec.defaultNetwork = %q, want dropped", *podFromHostTmpl.Spec.DefaultNetwork)
		}
		if !podFromHostTmpl.Spec.HostNetwork {
			t.Errorf("spec.hostNetwork = false, want true")
		}
		if podFromHostTmpl.Spec.Containers[0].Ports[0].HostPort != 8080 {
			t.Errorf("spec.containers[0].ports[0].hostPort = %d, want 8080", podFromHostTmpl.Spec.Containers[0].Ports[0].HostPort)
		}

		// "None" template had dnsPolicy defaulted to None without dnsConfig ->
		// when the gate is disabled, defaultNetwork is dropped on the new Pod
		// and validation rejects the Pod rather than running it on the pod network.
		_, err = client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "from-none-tmpl-disabled", Namespace: ns.Name},
			Spec:       *templateNoneNet.Template.Spec.DeepCopy(),
		}, metav1.CreateOptions{})
		if err == nil || !strings.Contains(err.Error(), "must provide `dnsConfig` when `dnsPolicy` is None") {
			t.Fatalf("expected DNSConfig validation error when creating pod from isolated template with gate disabled, got: %v", err)
		}
	})

	t.Run("creating Pods after re-enabling the gate from templates stored while disabled", func(t *testing.T) {
		tmplUnsetDisabled, err := client.CoreV1().PodTemplates(ns.Name).Create(ctx, &v1.PodTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "tmpl-unset-disabled", Namespace: ns.Name},
			Template:   v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{containerWithPort()}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create unset template: %v", err)
		}
		tmplHostDisabled, err := client.CoreV1().PodTemplates(ns.Name).Create(ctx, &v1.PodTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "tmpl-hostnet-disabled", Namespace: ns.Name},
			Template:   v1.PodTemplateSpec{Spec: v1.PodSpec{HostNetwork: true, Containers: []v1.Container{containerWithPort()}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create hostNetwork template: %v", err)
		}

		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodDefaultNetwork, true)

		podFromUnset, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "from-unset-tmpl-reenabled", Namespace: ns.Name},
			Spec:       *tmplUnsetDisabled.Template.Spec.DeepCopy(),
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create pod from unset template: %v", err)
		}
		if got := ptr.Deref(podFromUnset.Spec.DefaultNetwork, ""); got != v1.PodDefaultNetworkPod {
			t.Errorf("spec.defaultNetwork = %q, want %q", got, v1.PodDefaultNetworkPod)
		}
		if podFromUnset.Spec.HostNetwork {
			t.Errorf("spec.hostNetwork = true, want false")
		}

		podFromHost, err := client.CoreV1().Pods(ns.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "from-hostnet-tmpl-reenabled", Namespace: ns.Name},
			Spec:       *tmplHostDisabled.Template.Spec.DeepCopy(),
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create pod from hostNetwork template: %v", err)
		}
		if got := ptr.Deref(podFromHost.Spec.DefaultNetwork, ""); got != v1.PodDefaultNetworkHost {
			t.Errorf("spec.defaultNetwork = %q, want %q", got, v1.PodDefaultNetworkHost)
		}
		if !podFromHost.Spec.HostNetwork {
			t.Errorf("spec.hostNetwork = false, want true")
		}
		if podFromHost.Spec.Containers[0].Ports[0].HostPort != 8080 {
			t.Errorf("spec.containers[0].ports[0].hostPort = %d, want 8080", podFromHost.Spec.Containers[0].Ports[0].HostPort)
		}
	})
}

