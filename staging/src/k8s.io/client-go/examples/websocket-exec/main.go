/*
Copyright 2025 The Kubernetes Authors.

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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"

	restclient "k8s.io/client-go/rest"

	"golang.org/x/net/websocket"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
	//
	// Uncomment to load all auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// Or uncomment to load specific auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

var (
	podName       string
	podNamespace  string
	containerName string
	kubeconfig    string
)

func init() {
	flag.StringVar(&podName, "pod-name", "", "pod name")
	flag.StringVar(&podNamespace, "pod-namespace", "default", "pod namespace")
	if home := homedir.HomeDir(); home != "" {
		flag.StringVar(&kubeconfig, "kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	}

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: kexec [options]\n\n")
		flag.PrintDefaults()
	}
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	klog.InitFlags(nil)
	flag.Parse()

	if podName == "" {
		klog.Fatal("missing target pod name")
	}

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		klog.Fatal(err)
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("can not create client-go client: %v", err)
	}

	pod, err := clientset.CoreV1().Pods(podNamespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		klog.Fatalf("error trying to get pod %s/%s : %v", podNamespace, podName, err)
	}
	if containerName == "" {
		containerName = pod.Spec.Containers[0].Name
	}

	req := clientset.CoreV1().RESTClient().Get().
		Namespace(podNamespace).
		Resource("pods").
		Name(podName).
		Suffix("exec").
		Param("stderr", "1").
		Param("stdout", "1").
		Param("container", containerName).
		Param("command", "echo").
		Param("command", "hola")

	url := req.URL()
	ws, err := OpenWebSocketForURL(url, config, []string{"v5.channel.k8s.io"})
	if err != nil {
		klog.Fatalf("Failed to open websocket to %s: %v", url.String(), err)
	}
	defer ws.Close()

	done := make(chan struct{})
	channelStreams := make(map[byte]chan []byte)

	go func() {
		defer close(done)
		for {
			var message []byte
			err := websocket.Message.Receive(ws, &message)
			if err != nil {
				log.Println("read:", err)
				return
			}

			if len(message) < 1 {
				log.Println("Message too short")
				continue
			}

			channel := message[0]
			data := message[1:]

			log.Printf("recv channel: %d, message: %x", channel, data)

			stream, ok := channelStreams[channel]
			if !ok {
				stream = make(chan []byte, 10)
				channelStreams[channel] = stream
				go processChannel(channel, stream)
			}

			stream <- data
		}
	}()

	for {
		select {
		case <-done:
			return
		case <-interrupt:
			log.Println("interrupt")

			// Cleanly close the connection.
			ws.Close()
			return
		}
	}
}

func processChannel(channel byte, stream chan []byte) {
	log.Printf("Starting processing channel: %d", channel)
	for data := range stream {
		fmt.Printf("Channel %d received: %x\n", channel, data)
	}
	log.Printf("Channel %d processing stopped", channel)
}

func sendMessage(ws *websocket.Conn, channel byte, data []byte) error {
	message := append([]byte{channel}, data...)
	return websocket.Message.Send(ws, message)
}

// OpenWebSocketForURL constructs a websocket connection to the provided URL, using the client
// config, with the specified protocols.
func OpenWebSocketForURL(url *url.URL, config *restclient.Config, protocols []string) (*websocket.Conn, error) {
	tlsConfig, err := restclient.TLSConfigFor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create tls config: %v", err)
	}
	if url.Scheme == "https" {
		url.Scheme = "wss"
	} else {
		url.Scheme = "ws"
	}

	cfg, err := websocket.NewConfig(url.String(), "http://localhost")
	if err != nil {
		return nil, fmt.Errorf("failed to create websocket config: %v", err)
	}
	cfg.TlsConfig = tlsConfig
	cfg.Protocol = protocols
	cfg.Dialer = &net.Dialer{}
	return websocket.DialConfig(cfg)
}
