/*
Copyright 2014 The Kubernetes Authors.

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

package auth

// This file tests authentication and (soon) authorization of HTTP requests to an API server object.
// It does not use the client in pkg/client/... because authentication and authorization needs
// to work for any client of the HTTP interface.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/group"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestWebhookTokenAuthenticatorHTTP2(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	t.Setenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS", "10")
	t.Setenv("HTTP2_PING_TIMEOUT_SECONDS", "5")
	t.Setenv("DISABLE_HTTP2", "")

	ts := httptest.NewUnstartedServer(http.HandlerFunc(newWebhookTokenAuthHandler))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("failed to parse URL from %q: %v", ts.URL, err)
	}
	lb := newLB(t, u.Host)
	defer lb.ln.Close()
	stopCh := make(chan struct{})
	go lb.serve(stopCh)
	transport, ok := ts.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("failed to assert *http.Transport")
	}
	config := &rest.Config{
		Host:      "https://" + lb.ln.Addr().String(),
		Transport: utilnet.SetTransportDefaults(transport),
		// These fields are required to create a REST client.
		ContentConfig: rest.ContentConfig{
			GroupVersion:         &schema.GroupVersion{},
			NegotiatedSerializer: &serializer.CodecFactory{},
		},
	}

	var authenticator authenticator.Request
	var err error

	authenticator, err = getTestWebhookTokenAuth(authServer.URL, nil)

	if err != nil {
		t.Fatalf("error starting webhook token authenticator server: %v", err)
	}

	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(ctx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// Disable ServiceAccount admission plugin as we don't have serviceaccount controller running.
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authorization.Modes = []string{"ABAC"}
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			config.GenericConfig.Authentication.Authenticator = group.NewAuthenticatedGroupAdder(authenticator)
			// Disable checking API audiences that is set by testserver by default.
			config.GenericConfig.Authentication.APIAudiences = nil
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-webhook-token-transport", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Expect Bob's requests to all fail.
	token := BobToken
	bodyBytes := bytes.NewReader([]byte(r.body))
	req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	func() {
		resp, err := transport.RoundTrip(req)
		if err != nil {
			t.Logf("case %v", r)
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		// Expect all of Bob's actions to return Forbidden
		if resp.StatusCode != http.StatusForbidden {
			t.Logf("case %v", r)
			t.Errorf("Expected http.Forbidden, but got %s", resp.Status)
		}
	}()
	// Expect Alice's requests to succeed.
	bodyBytes = bytes.NewReader([]byte(r.body))
	req, err = http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	func() {
		resp, err := transport.RoundTrip(req)
		if err != nil {
			t.Logf("case %v", r)
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		// Expect all of Alice's actions to at least get past authn/authz.
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			t.Logf("case %v", r)
			t.Errorf("Expected something other than Unauthorized/Forbidden, but got %s", resp.Status)
		}
	}()

}

func newWebhookTokenAuthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var review authenticationv1.TokenReview
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
			http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)
			return
		}
		type userInfo struct {
			Username string   `json:"username"`
			UID      string   `json:"uid"`
			Groups   []string `json:"groups"`
		}
		type status struct {
			Authenticated bool     `json:"authenticated"`
			User          userInfo `json:"user"`
		}
		var username, uid string
		authenticated := false
		if review.Spec.Token == AliceToken {
			authenticated, username, uid = true, "alice", "1"
		} else if review.Spec.Token == BobToken {
			authenticated, username, uid = true, "bob", "2"
		}

		resp := struct {
			APIVersion string `json:"apiVersion"`
			Status     status `json:"status"`
		}{
			APIVersion: authenticationv1.SchemeGroupVersion.String(),
			Status: status{
				authenticated,
				userInfo{
					Username: username,
					UID:      uid,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}

}

type tcpLB struct {
	t         *testing.T
	ln        net.Listener
	serverURL string
	dials     int32
}

func (lb *tcpLB) handleConnection(in net.Conn, stopCh chan struct{}) {
	out, err := net.Dial("tcp", lb.serverURL)
	if err != nil {
		lb.t.Log(err)
		return
	}
	go io.Copy(out, in)
	go io.Copy(in, out)
	<-stopCh
	if err := out.Close(); err != nil {
		lb.t.Fatalf("failed to close connection: %v", err)
	}
}

func (lb *tcpLB) serve(stopCh chan struct{}) {
	conn, err := lb.ln.Accept()
	if err != nil {
		lb.t.Fatalf("failed to accept: %v", err)
	}
	atomic.AddInt32(&lb.dials, 1)
	go lb.handleConnection(conn, stopCh)
}

func newLB(t *testing.T, serverURL string) *tcpLB {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}
	lb := tcpLB{
		serverURL: serverURL,
		ln:        ln,
		t:         t,
	}
	return &lb
}
