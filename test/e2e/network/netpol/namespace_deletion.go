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

package netpol

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	networkingv1 "k8s.io/api/networking/v1"

	"github.com/onsi/ginkgo/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	"k8s.io/kubernetes/test/e2e/network/common"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = common.SIGDescribe("Netpol", func() {
	f := framework.NewDefaultFramework("netpol-deletion")
	f.SkipNamespaceCreation = true // we create our own 2 test namespaces, we don't need the default one
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	f.It("should enforce network policies on namespace deletion", feature.NetworkPolicy, func(ctx context.Context) {
		ginkgo.By("Creating testing namespaces")
		ns, err := f.CreateNamespace(ctx, "netpol-prober", nil)
		framework.ExpectNoError(err, "failed to create namespace for prober")
		nsProber := ns.Name
		ns, err = f.CreateNamespace(ctx, "netpol-victim", nil)
		framework.ExpectNoError(err, "failed to create namespace for victim")
		nsVictim := ns.Name

		ginkgo.By("Creating 1 webserver pod able to serve traffic")
		webserverPod := e2epod.NewAgnhostPod(nsVictim, "webserver-pod", nil, nil, nil, "netexec", "--http-port=80")
		webserverPod = e2epod.PodClientNS(f, nsVictim).CreateSync(ctx, webserverPod)

		ginkgo.By("Creating 1 client pod that will try to connect to the webserver")
		// the client Pod will capture the TERM signal and wait for the grace period so
		// we can expect the network policy will be deleted first
		gracePeriod := int64(60)
		pausePod := e2epod.NewAgnhostPod(nsProber, "pause-pod", nil, nil, nil, "netexec", "--http-port=80", fmt.Sprintf("--delay-shutdown=%d", gracePeriod))
		pausePod.Spec.TerminationGracePeriodSeconds = &gracePeriod
		pausePod = e2epod.PodClientNS(f, nsProber).CreateSync(ctx, pausePod)

		ginkgo.By("Try to connect to the webserver")
		// Wait until we are able to connect to the Pod
		targetAddr := net.JoinHostPort(webserverPod.Status.PodIP, strconv.Itoa(80))
		framework.Logf("Waiting up to %v to get response from %s", 30*time.Second, targetAddr)
		cmd := fmt.Sprintf(`curl -q -s --max-time 5 %s/hostname`, targetAddr)
		err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 30*time.Second, true, func(context.Context) (bool, error) {
			stdout, err := e2eoutput.RunHostCmd(pausePod.Namespace, pausePod.Name, cmd)
			if err != nil {
				framework.Logf("got err: %v, retry until timeout", err)
				return false, nil
			}
			// Need to check output because it might omit in case of error.
			if strings.TrimSpace(stdout) == "" {
				framework.Logf("got empty stdout, retry until timeout")
				return false, nil
			}
			// Ensure we're comparing hostnames and not FQDNs
			hostname := strings.TrimSpace(strings.Split(stdout, ".")[0])
			if hostname == webserverPod.Name {
				return true, nil
			}
			framework.Logf("expected: %s got: %s", webserverPod.Name, hostname)
			return false, nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating one network policy so the prober can never reach the victim")
		netpol := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "default-deny-egress", Namespace: nsProber},
			Spec: networkingv1.NetworkPolicySpec{
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			},
		}
		_, err = f.ClientSet.NetworkingV1().NetworkPolicies(nsProber).Create(ctx, netpol, metav1.CreateOptions{})
		framework.ExpectNoError(err, "unexpected error creating Network Policy")

		err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 30*time.Second, true, func(context.Context) (bool, error) {
			stdout, err := e2eoutput.RunHostCmd(pausePod.Namespace, pausePod.Name, cmd)
			if err != nil {
				return true, nil
			}
			framework.Logf("connection succeeded, retry until is blocked by the network policy: %s", stdout)
			return false, nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the  namespace should not allow the Pod to reach the victim")
		err = f.ClientSet.CoreV1().Namespaces().Delete(ctx, nsProber, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "unexpected error deleting prober namespace")
		framework.Logf("namespace : %v api call to delete is complete ", nsProber)

		// grace period is 60 seconds
		err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 100*time.Second, true, func(context.Context) (bool, error) {
			// Stop once namespace has been deleted
			_, err := f.ClientSet.CoreV1().Namespaces().Get(ctx, nsProber, metav1.GetOptions{})
			if err != nil {
				return true, nil
			}

			stdout, err := e2eoutput.RunHostCmd(pausePod.Namespace, pausePod.Name, cmd)
			if err != nil {
				framework.Logf("retry until namespace is deleted: %v", err)
				return false, nil
			}
			return false, fmt.Errorf("connection unexpectedly succeded: %s", stdout)
		})
		framework.ExpectNoError(err)
	})
})
