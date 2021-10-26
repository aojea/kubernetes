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

package transport

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

type altSvcRoundTripper struct {
	mu    sync.Mutex
	cache map[string]bool // keyed by host[:port], value indicates if the entry is local
	last  string

	rt http.RoundTripper
}

func NewAltSvcRoundTripper(rt http.RoundTripper) http.RoundTripper {
	return &altSvcRoundTripper{
		cache: make(map[string]bool),
		rt:    rt,
	}
}

func (rt *altSvcRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = utilnet.CloneRequest(req)
	// Alternative Services can be exploited to hijack service
	// only use them with https.
	if req.URL.Scheme != "https" {
		return rt.rt.RoundTrip(req)
	}
	hostOrig := req.URL.Host
	// check if there is a preferred alternative service and use it
	host := rt.getAltSvc(hostOrig)
	if host != hostOrig {
		// replace the destination host
		req.URL.Host = host
		// set the host header to the original one
		req.Host = hostOrig
		// RFC7838 Section 5
		req.Header.Set("Alt-Used", host)
	}

	resp, err := rt.rt.RoundTrip(req)
	// remove the host from the cache if it fails
	// TODO: should we do that only on some errors? there is is some retry logic on top
	if err != nil {
		rt.removeHost(host)
		return resp, err
	}

	// process the alt-svc header if exist to update the cache
	altSvc := resp.Header.Get("Alt-Svc")
	if len(altSvc) > 0 {
		rt.handleAltSvcHeader(altSvc, host)
	}
	return resp, err
}

func (rt *altSvcRoundTripper) CancelRequest(req *http.Request) {
	tryCancelRequest(rt.WrappedRoundTripper(), req)
}

func (rt *altSvcRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.rt }

// removeHost removes the specified host from the cache
func (rt *altSvcRoundTripper) removeHost(host string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.last == host {
		rt.last = ""
	}
	delete(rt.cache, host)
}

// updateCache updates the cache with a new list of alternative services
// checking if they are local
func (rt *altSvcRoundTripper) updateCache(hosts []string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.cache = map[string]bool{}
	for _, h := range hosts {
		rt.cache[h] = isLocal(h)
	}
}

func (rt *altSvcRoundTripper) handleAltSvcHeader(altSvcString, host string) {
	if len(altSvcString) == 0 {
		return
	}

	altSvcs, err := parseAltSvcHeader(altSvcString, host)
	if err != nil {
		klog.Infof("Error parsing Alt-Svc header %s: %v", altSvcString, err)
		return
	}

	rt.updateCache(altSvcs)
}

// getAltSvc returns an alternative host, if exist, otherwise it returns the same host
func (rt *altSvcRoundTripper) getAltSvc(host string) string {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// no hosts on the cache, reset and exit
	if len(rt.cache) == 0 {
		rt.last = ""
		return host
	}
	// known devil is better
	if len(rt.last) > 0 {
		return rt.last
	}
	// get one host from the cache, prefer local ones
	hosts := []string{}
	for k, v := range rt.cache {
		if v {
			rt.last = k
			return k
		}
		hosts = append(hosts, k)
	}
	if len(hosts) > 0 {
		rt.last = hosts[0]
		return hosts[0]
	}
	return host
}

// Given a string of the form "host", "host:port", or "[ipv6::address]:port",
// return true if the string includes a port.
func hasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

// is local checks if the host is local
func isLocal(urlHost string) bool {
	var host string
	var err error
	if hasPort(urlHost) {
		host, _, err = net.SplitHostPort(urlHost)
		if err != nil {
			return false
		}
	} else {
		host = urlHost
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}
	localIPs := getLocalAddressSet()
	for _, ip := range ips {
		if localIPs.Has(ip) {
			return true
		}
	}
	return false
}

// getLocalAddrs returns a set with all network addresses on the local system
func getLocalAddressSet() netutils.IPSet {
	localAddrs := netutils.IPSet{}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		klog.InfoS("Error getting local addresses", "error", err)
		return localAddrs
	}

	for _, addr := range addrs {
		ip, _, err := netutils.ParseCIDRSloppy(addr.String())
		if err != nil {
			klog.InfoS("Error getting local addresses", "address", addr.String(), "error", err)
			continue
		}
		localAddrs.Insert(ip)
	}
	return localAddrs
}

// parseAltSvcHeader parses an Alt-Svc header and returns the list of alternative services in format host[:port]
// Alt-Svc
// RFC 7838
//    Alt-Svc       = clear / 1#alt-value
// clear         = %s"clear"; "clear", case-sensitive
// alt-value     = alternative *( OWS ";" OWS parameter )
// alternative   = protocol-id "=" alt-authority
// protocol-id   = token ; percent-encoded ALPN protocol name
// alt-authority = quoted-string ; containing [ uri-host ] ":" port
// parameter     = token "=" ( token / quoted-string )
// Caching parameters:
// ma 			 = delta-seconds
// persist       = not clear on network changes
func parseAltSvcHeader(header, origHost string) ([]string, error) {
	// tolerate whitespaces
	header = strings.TrimSpace(header)

	if header == "clear" {
		return []string{}, nil
	}

	var errors []error
	// comma separated list of alternative services
	alternatives := strings.Split(header, ",")
	hosts := make([]string, len(alternatives))
	for i, a := range alternatives {
		// semi colon separated list of options per alternative service
		alternative := strings.Split(a, ";")
		if len(alternative) == 0 {
			errors = append(errors, fmt.Errorf("no alternative service present"))
			continue
		}
		// Process first entry
		// alternative   = protocol-id "=" alt-authority
		h := strings.Split(strings.TrimSpace(alternative[0]), "=")
		if len(h) != 2 {
			errors = append(errors, fmt.Errorf("error parsing alternative service %s", alternative))
			continue
		}
		// only support http2
		if h[0] != "h2" {
			errors = append(errors, fmt.Errorf("unsupported protocol %s", h[0]))
			continue
		}

		// validate alt-authority (it is a quoted string)
		authority := strings.Trim(h[1], "\"")
		host, port, err := net.SplitHostPort(authority)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		// Alt-Svc returns an empty host for the service we are connecting against
		if host == "" {
			host = origHost
		}
		hosts[i] = net.JoinHostPort(host, port)
		// TODO the rest of the options in an Alt-Svc header are related to caching
		// that doesn't really apply in this environment
	}
	if len(errors) > 0 {
		return hosts, utilerrors.NewAggregate(errors)
	}
	return hosts, nil
}
