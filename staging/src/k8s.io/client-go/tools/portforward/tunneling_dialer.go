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

package portforward

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	gwebsocket "github.com/gorilla/websocket"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	constants "k8s.io/apimachinery/pkg/util/portforward"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	"k8s.io/klog/v2"
)

const (
	// PingPeriod defines how often a heartbeat "ping" message is sent.
	PingPeriod = 10 * time.Second
	// size of buffers for the websocket connection.
	dataBufferSize = 32 * 1024
)

var (
	statusScheme = runtime.NewScheme()
	statusCodecs = serializer.NewCodecFactory(statusScheme)
)

func init() {
	statusScheme.AddUnversionedTypes(metav1.SchemeGroupVersion,
		&metav1.Status{},
	)
}

// tunnelingDialer implements "httpstream.Dial" interface
type tunnelingDialer struct {
	url      *url.URL
	wsDialer gwebsocket.Dialer
}

// NewTunnelingDialer creates and returns the tunnelingDialer structure which implemements the "httpstream.Dialer"
// interface. The dialer can upgrade a websocket request, creating a websocket connection. This function
// returns an error if one occurs.
func NewSPDYOverWebsocketDialer(url *url.URL, config *restclient.Config) (httpstream.Dialer, error) {
	transportCfg, err := config.TransportConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating websocket transports: %v", err)
	}
	tlsConfig, err := transport.TLSConfigFor(transportCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating websocket transports: %v", err)
	}
	proxy := config.Proxy
	if proxy == nil {
		proxy = utilnet.NewProxierWithNoProxyCIDR(http.ProxyFromEnvironment)
	}
	dialer := gwebsocket.Dialer{
		Proxy:           proxy,
		TLSClientConfig: tlsConfig,
		ReadBufferSize:  dataBufferSize + 1024, // add space for the protocol byte indicating which channel the data is for
		WriteBufferSize: dataBufferSize + 1024, // add space for the protocol byte indicating which channel the data is for
	}

	return &tunnelingDialer{
		url:      url,
		wsDialer: dialer,
	}, nil
}

// Dial upgrades to a tunneling streaming connection, returning a SPDY connection
// containing a WebSockets connection (which implements "net.Conn"). Also
// returns the protocol negotiated, or an error.
func (d *tunnelingDialer) Dial(protocols ...string) (httpstream.Connection, string, error) {
	// There is no passed context, so skip the context when creating request for now.
	// Websockets requires "GET" method: RFC 6455 Sec. 4.1 (page 17).
	req, err := http.NewRequest("GET", d.url.String(), nil)
	if err != nil {
		return nil, "", err
	}
	switch req.URL.Scheme {
	case "https":
		req.URL.Scheme = "wss"
	case "http":
		req.URL.Scheme = "ws"
	default:
		return nil, "", fmt.Errorf("unknown url scheme: %s", req.URL.Scheme)
	}
	// Add the spdy tunneling prefix to the requested protocols. The tunneling
	// handler will know how to negotiate these protocols.
	for _, protocol := range protocols {
		d.wsDialer.Subprotocols = append(d.wsDialer.Subprotocols,
			constants.WebsocketsSPDYTunnelingPrefix+protocol)
	}
	klog.V(4).Infoln("Before WebSocket Upgrade Connection...")

	wsConn, resp, err := d.wsDialer.DialContext(req.Context(), req.URL.String(), req.Header)
	if err != nil {
		// BadHandshake error becomes an "UpgradeFailureError" (used for streaming fallback).
		if errors.Is(err, gwebsocket.ErrBadHandshake) {
			cause := err
			// Enhance the error message with the error response if possible.
			if resp != nil && len(resp.Status) > 0 {
				defer resp.Body.Close()                         //nolint:errcheck
				cause = fmt.Errorf("%w (%s)", err, resp.Status) // Always add the response status
				responseError := ""
				responseErrorBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
				if readErr != nil {
					cause = fmt.Errorf("%w: unable to read error from server response", cause)
				} else {
					// If returned error can be decoded as "metav1.Status", return a "StatusError".
					responseError = strings.TrimSpace(string(responseErrorBytes))
					if len(responseError) > 0 {
						if obj, _, decodeErr := statusCodecs.UniversalDecoder().Decode(responseErrorBytes, nil, &metav1.Status{}); decodeErr == nil {
							if status, ok := obj.(*metav1.Status); ok {
								cause = &apierrors.StatusError{ErrStatus: *status}
							}
						} else {
							// Otherwise, append the responseError string.
							cause = fmt.Errorf("%w: %s", cause, responseError)
						}
					}
				}
			}
			return nil, "", &httpstream.UpgradeFailureError{Cause: cause}
		}
		return nil, "", err
	}

	// Ensure we got back a protocol we understand
	foundProtocol := false
	for _, protocolVersion := range d.wsDialer.Subprotocols {
		if protocolVersion == wsConn.Subprotocol() {
			foundProtocol = true
			break
		}
	}
	if !foundProtocol {
		wsConn.Close() // nolint:errcheck
		return nil, "", &httpstream.UpgradeFailureError{Cause: fmt.Errorf("invalid protocol, expected one of %q, got %q", d.wsDialer.Subprotocols, wsConn.Subprotocol())}
	}

	if wsConn == nil {
		return nil, "", fmt.Errorf("negotiated websocket connection is nil")
	}
	defer wsConn.Close()
	protocol := wsConn.Subprotocol()
	protocol = strings.TrimPrefix(protocol, constants.WebsocketsSPDYTunnelingPrefix)
	klog.V(4).Infof("negotiated protocol: %s", protocol)

	// Wrap the websocket connection which implements "net.Conn".
	tConn := NewTunnelingConnection("client", wsConn)
	// Create SPDY connection injecting the previously created tunneling connection.
	spdyConn, err := spdy.NewClientConnectionWithPings(tConn, PingPeriod)

	return spdyConn, protocol, err
}
