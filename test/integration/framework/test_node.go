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

package framework

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	_ "k8s.io/component-base/metrics/prometheus/restclient" // for client metric registration
	_ "k8s.io/component-base/metrics/prometheus/version"    // for version metric registration
	internalapi "k8s.io/cri-api/pkg/apis"
	"k8s.io/klog/v2"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	cadvisortest "k8s.io/kubernetes/pkg/kubelet/cadvisor/testing"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/cri/remote"
	fakeremote "k8s.io/kubernetes/pkg/kubelet/cri/remote/fake"
	probetest "k8s.io/kubernetes/pkg/kubelet/prober/testing"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/cephfs"
	"k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/downwardapi"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	"k8s.io/kubernetes/pkg/volume/fc"
	"k8s.io/kubernetes/pkg/volume/git_repo"
	"k8s.io/kubernetes/pkg/volume/hostpath"
	"k8s.io/kubernetes/pkg/volume/iscsi"
	"k8s.io/kubernetes/pkg/volume/local"
	"k8s.io/kubernetes/pkg/volume/nfs"
	"k8s.io/kubernetes/pkg/volume/portworx"
	"k8s.io/kubernetes/pkg/volume/projected"
	"k8s.io/kubernetes/pkg/volume/rbd"
	"k8s.io/kubernetes/pkg/volume/secret"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/kubernetes/test/utils"
	"k8s.io/mount-utils"
)

// Adaptation of the pkg/kubemark/hollow_kubelet.go code

// TestNodeSetup holds configuration information for a kube-apiserver test server.
type TestNodeSetup struct {
	ModifyNodeRunOptions   func(*kubeletoptions.KubeletFlags)
	ModifyNodeConfig       func(*kubeletconfig.KubeletConfiguration)
	ModifyNodeDependencies func(*kubelet.Dependencies)
}

func volumePlugins() []volume.VolumePlugin {
	allPlugins := []volume.VolumePlugin{}
	allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, git_repo.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, hostpath.FakeProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, nfs.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, secret.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, iscsi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, rbd.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, cephfs.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, downwardapi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, fc.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, configmap.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, projected.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, portworx.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, local.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, csi.ProbeVolumePlugins()...)
	return allPlugins
}

// StartTestNode runs a kubelet, optionally calling out to the setup.ModifyNodeRunOptions and setup.ModifyNodeConfig functions
func StartTestNode(ctx context.Context, t testing.TB, setup TestNodeSetup, config *rest.Config) TearDownFunc {
	var client *clientset.Clientset
	var err error
	nodeName := "integration-kubelet"
	ctx, cancel := context.WithCancel(ctx)

	// check if is not a standalone kubelet
	if config != nil {
		kubeletConfig := rest.CopyConfig(config)
		kubeletConfig.UserAgent = "node-integration"
		client, err = clientset.NewForConfig(kubeletConfig)
		if err != nil {
			t.Fatalf("error creating clientset %v", err)
		}
	}

	testRootDir := utils.MakeTempDirOrDie("integration-kubelet", "")
	klog.Infof("Using %s as root dir for integration-kubelet", testRootDir)

	// Flags struct
	f := options.NewKubeletFlags()
	f.RootDirectory = testRootDir
	f.MinimumGCAge = metav1.Duration{Duration: 1 * time.Minute}
	f.MaxContainerCount = 100
	f.MaxPerPodContainerCount = 2
	f.RegisterSchedulable = true
	f.HostnameOverride = nodeName

	if setup.ModifyNodeRunOptions != nil {
		setup.ModifyNodeRunOptions(f)
	}

	// Config struct
	c, err := kubeletoptions.NewKubeletConfiguration()
	if err != nil {
		t.Fatalf("error obtaining new kubelet configuration %v", err)
	}

	c.ImageServiceEndpoint = "unix:///run/containerd/containerd.sock"
	c.StaticPodURL = ""
	c.EnableServer = true
	c.Address = "0.0.0.0" /* bind address */

	if setup.ModifyNodeRunOptions != nil {
		setup.ModifyNodeConfig(c)
	}

	cadvisorInterface := &cadvisortest.Fake{
		NodeName: nodeName,
	}

	var containerManager cm.ContainerManager
	containerManager = cm.NewStubContainerManager()

	endpoint, err := fakeremote.GenerateEndpoint()
	if err != nil {
		t.Fatalf("Failed to generate fake endpoint, error: %w", err)
	}
	fakeRemoteRuntime := fakeremote.NewFakeRemoteRuntime()
	if err = fakeRemoteRuntime.Start(endpoint); err != nil {
		t.Fatalf("Failed to start fake runtime, error: %w", err)
	}
	defer fakeRemoteRuntime.Stop()
	runtimeService, err := remote.NewRemoteRuntimeService(endpoint, 15*time.Second, oteltrace.NewNoopTracerProvider())
	if err != nil {
		t.Fatalf("Failed to init runtime service, error: %w", err)
	}

	var imageService internalapi.ImageManagerService = fakeRemoteRuntime.ImageService
	imageService, err = remote.NewRemoteImageService(c.ImageServiceEndpoint, 15*time.Second, oteltrace.NewNoopTracerProvider())
	if err != nil {
		t.Fatalf("Failed to init image service, error: %w", err)
	}

	d := &kubelet.Dependencies{
		KubeClient:                client,
		HeartbeatClient:           client,
		ProbeManager:              probetest.FakeManager{},
		RemoteRuntimeService:      runtimeService,
		RemoteImageService:        imageService,
		CAdvisorInterface:         cadvisorInterface,
		Cloud:                     nil,
		OSInterface:               &containertest.FakeOS{},
		ContainerManager:          containerManager,
		VolumePlugins:             volumePlugins(),
		TLSOptions:                nil,
		OOMAdjuster:               oom.NewFakeOOMAdjuster(),
		Mounter:                   &mount.FakeMounter{},
		Subpather:                 &subpath.FakeSubpath{},
		HostUtil:                  hostutil.NewFakeHostUtil(nil),
		PodStartupLatencyTracker:  kubeletutil.NewPodStartupLatencyTracker(),
		NodeStartupLatencyTracker: kubeletutil.NewNodeStartupLatencyTracker(),
		TracerProvider:            trace.NewNoopTracerProvider(),
		Recorder:                  &record.FakeRecorder{}, // With real recorder we attempt to read /dev/kmsg.
	}

	if setup.ModifyNodeDependencies != nil {
		setup.ModifyNodeDependencies(d)
	}

	if err := kubeletapp.RunKubelet(ctx, &options.KubeletServer{
		KubeletFlags:         *f,
		KubeletConfiguration: *c,
	}, d, false); err != nil {
		t.Fatalf("Failed to run HollowKubelet: %v. Exiting.", err)
	}

	return TearDownFunc(cancel)
}
