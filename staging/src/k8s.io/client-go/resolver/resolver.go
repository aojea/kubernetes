package resolver

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/resolver/dns"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// ResolverOption defines the functional option type for resolver
type ResolverOption func(*resolver) *resolver

func WithTimeout(timeout time.Duration) func(*resolver) {
	return func(r *resolver) {
		r.timeout = timeout
	}
}

// resolver to store API server IP addresses
type resolver struct {
	mu          sync.Mutex
	refreshing  int32     // only access with atomic. Non zero means refresh operation in progress
	cache       []net.IP  // API server IP addresses
	lastContact time.Time // store the time the resolved is contacted once the cache has already been populated
	host        string    // API server configured hostname
	port        string    // API server configured port

	// time after the cache entries are considered stale and pruned
	timeout time.Duration
	// RESTclient configuration
	client *kubernetes.Clientset
}

// NewClientsetWithResolver returns a Kubernetes Clientset with an in memory
// net.Resolver that resolves the API server.
// Host name with the addresses obtained from the API server published Endpoints
// resources.
// The resolver polls periodically the API server to refresh the local cache.
// The resolver will fall back to the default golang resolver if:
// - Is not able to obtain the API server Endpoints.
// - The configured API server host name is not resolvable via DNS, per example,
//   is not an IP address or is resolved via /etc/hosts.
// - The configured API server URL has a different port
//   than the one used in the Endpoints.
func NewClientsetWithResolver(c *rest.Config, options ...ResolverOption) (*kubernetes.Clientset, error) {
	config := *c
	host, port, err := getHostPort(config.Host)
	if err != nil {
		return nil, err
	}

	if netutils.ParseIPSloppy(host) != nil {
		return nil, fmt.Errorf("APIServerResolver only works for domain names")
	}

	// defaulting
	r := &resolver{
		host:    host,
		port:    port,
		timeout: 100 * time.Second,
	}

	// options
	for _, o := range options {
		o(r)
	}

	f := &dns.MemResolver{
		LookupIP: func(ctx context.Context, network, host string) ([]net.IP, error) {
			return r.lookupIP(ctx, network, host)
		},
	}

	resolver := dns.NewMemoryResolver(f)
	config.Dial = (&net.Dialer{
		Resolver: resolver,
	}).DialContext

	client, err := kubernetes.NewForConfig(&config)
	if err != nil {
		return nil, err
	}
	r.client = client
	return r.client, nil
}

func (r *resolver) lookupIP(ctx context.Context, network, host string) ([]net.IP, error) {
	// Use the default resolver if is not trying to resolve the configured API server hostname
	if !strings.HasPrefix(host, r.host) {
		klog.V(7).Infof("Resolver Trace: use default resolver for host %s, different than API server hostname %s", host, r.host)
		return net.DefaultResolver.LookupIP(ctx, network, host)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Use the default resolver if the cache is empty and try to renew the cache.
	if len(r.cache) == 0 {
		klog.V(7).Infof("Resolver Trace: use default resolver for host %s, cache is empty", host)
		addrs, err := net.DefaultResolver.LookupIP(ctx, network, host)
		if err != nil {
			return addrs, err
		}
		// we were able to resolve the IP with the default resolver
		// try to update the cache, but check this is not a recursive call
		if len(addrs) > 0 && atomic.LoadInt32(&r.refreshing) == 0 {
			klog.V(7).Infof("Resolver Trace: refreshing cache for first time for host %s", host)
			go func() {
				// avoid multiple refresh queries in parallel
				atomic.StoreInt32(&r.refreshing, 1)
				defer atomic.StoreInt32(&r.refreshing, 0)
				r.refreshCache()
			}()
		}
		return addrs, nil
	}

	// Check if is the first time we query the cache and record the time, it is a fresh cache
	// so don't try to refresh it again, if something goes wrong next time will be refreshed.
	// If is not it the first time, it means that previous connection didn't work out
	// so we return the IPs in a different order to reach a different API server.
	if r.lastContact.IsZero() {
		r.lastContact = time.Now()
	} else {
		rand.Shuffle(len(r.cache), func(i, j int) {
			r.cache[i], r.cache[j] = r.cache[j], r.cache[i]
		})
		if atomic.LoadInt32(&r.refreshing) == 0 {
			klog.V(7).Infof("Resolver Trace: refreshing cache for host %s", host)
			go func() {
				// avoid multiple refresh queries in parallel
				atomic.StoreInt32(&r.refreshing, 1)
				defer atomic.StoreInt32(&r.refreshing, 0)
				r.refreshCache()
			}()
		}
	}

	// Return the IP addresses from the cache.
	ips := make([]net.IP, len(r.cache))
	copy(ips, r.cache)
	klog.V(7).Infof("Resolver Trace: host %s resolves to %v", host, ips)
	return ips, nil
}

// refreshCache refresh the cache with the API server IP addresses.
// If it can not refresh the IPs because of network errors during
// a predefined time, it falls back to the default resolver.
// If it can not refresh the IPs because of other type of errors, per
// example, it is able to connect to the API server but this is not ready
// or is not able to reply, it shuffles the IPs so it will retry randomly.
// If there are no errors, local IP addresses are returned first, so it
// favors direct connectivity.
func (r *resolver) refreshCache() {
	// Kubernetes conformance clusters require: The cluster MUST have a service
	// named "kubernetes" on the default namespace referencing the API servers.
	// The "kubernetes.default" service MUST have Endpoints and EndpointSlices
	// pointing to each API server instance.
	// Endpoints managed by API servers are removed if they API server is not ready.
	// Initialize cache
	b, err := r.client.RESTClient().Get().AbsPath("/healthz").Do(context.TODO()).Raw()
	if err != nil {
		klog.Errorf("error checking /healthz: %v\n%s\n", err, string(b))
		return
	}

	endpoint, err := r.client.CoreV1().Endpoints("default").Get(context.TODO(), "kubernetes", metav1.GetOptions{})
	if err != nil {
		klog.V(7).Infof("Resolver Trace: error getting apiserver addresses from Endpoints: %v", err)
		stale := false
		if !r.lastContact.IsZero() {
			stale = time.Now().After(r.lastContact.Add(r.timeout))
		}
		// give up if there are errors and we could not renew the entries during the specified timeout
		if stale {
			klog.V(7).Infof("Resolver Trace: falling back to default resolver, too many errors to connect to %s:%s on %v : %v", r.host, r.port, r.cache, err)
			// clean the cache and exit
			r.mu.Lock()
			r.populateCache([]net.IP{}, false)
			r.mu.Unlock()
		}
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Get IPs from the Endpoint.
	ips := []net.IP{}
	for _, ss := range endpoint.Subsets {
		for _, e := range ss.Addresses {
			ips = append(ips, netutils.ParseIPSloppy(e.IP))
		}
		// Unsupported configurations:
		// - API Server with multiple endpoints
		// - Configured URL and Endpoints with different ports
		if len(ss.Ports) != 1 || strconv.Itoa(int(ss.Ports[0].Port)) != r.port {
			// clean the cache and exit
			r.populateCache([]net.IP{}, true)
			return
		}
	}
	r.populateCache(ips, true)

	return
}

// populate cache inserts the IPs in the cache
// assuming the lock is held
func (r *resolver) populateCache(ips []net.IP, preferLocal bool) {
	klog.V(7).Infof("Resolver Trace: Update cache with %v", ips)
	// clear the pool so next connection uses the cache
	defer r.resetConnection()
	// reset the lastContact timestamp since we are going to refresh the cache
	r.lastContact = time.Time{}

	// Clear the cache
	if len(ips) == 0 {
		r.cache = []net.IP{}
		return
	}

	// Update the cache and exit (optimize for one IP)
	if len(ips) == 1 {
		r.cache = []net.IP{ips[0]}
		return
	}

	// Shuffle the ips so different clients don't end in the same API server.
	rand.Shuffle(len(ips), func(i, j int) {
		ips[i], ips[j] = ips[j], ips[i]
	})

	if preferLocal {
		// Favor local, returning it first because dialParallel races two copies of
		// dialSerial, giving the first a head start. It returns the first
		// established connection and closes the others.
		localAddresses := getLocalAddressSet()
		for _, ip := range ips {
			if localAddresses.Has(ip) {
				moveToFront(ip, ips)
				break
			}
		}
	}

	r.cache = make([]net.IP, len(ips))
	copy(r.cache, ips)
	return
}

// resetConnection clear the Transport Idle connections from the pool
// forcing the subsequent connection to query DNS and use the cache addresses
func (r *resolver) resetConnection() {
	// This is tricky, we need access to the transport (that is shared by all the consumers)
	t, ok := r.client.RESTClient().(*rest.RESTClient)
	if !ok {
		klog.Errorf("Unable to get restClient from transport")
	}
	// Closing all idle connections we "force" the next connection to query the DNS
	// that at this point will have the entries with the apiserver IPs
	// consequently, next connection will use the cache to reach the apiserver
	utilnet.CloseIdleConnectionsFor(t.Client.Transport)
}

// getHostPort returns the host and port from an URL defaulting http and https ports
func getHostPort(h string) (string, string, error) {
	url, err := url.Parse(h)
	if err != nil {
		return "", "", err
	}

	port := url.Port()
	if port == "" {
		switch url.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return "", "", fmt.Errorf("Unsupported URL scheme")
		}
	}
	return url.Hostname(), port, nil
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

// https://github.com/golang/go/wiki/SliceTricks#move-to-front-or-prepend-if-not-present-in-place-if-possible
// moveToFront moves needle to the front of haystack, in place if possible.
func moveToFront(needle net.IP, haystack []net.IP) []net.IP {
	if len(haystack) != 0 && haystack[0].Equal(needle) {
		return haystack
	}
	prev := needle
	for i, elem := range haystack {
		switch {
		case i == 0:
			haystack[0] = needle
			prev = elem
		case elem.Equal(needle):
			haystack[i] = prev
			return haystack
		default:
			haystack[i] = prev
			prev = elem
		}
	}
	return append(haystack, prev)
}
