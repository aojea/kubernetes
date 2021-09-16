package resolver

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// cache to store API server IP addresses
type cache struct {
	mu     sync.Mutex
	cache  []net.IP // API server IP addresses
	host   string   // URL host from the apiserver
	port   string   // port
	client *rest.RESTClient
}

// APIServerResolver returns a golang resolver that resolves the apiserver IPs
// based on the published endpoints. If not information is available it falls back
// to the golang default resolver
func APIServerResolver(ctx context.Context, c *rest.Config) (*net.Resolver, error) {
	config := *c
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}
	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}
	url, err := url.Parse(config.Host)
	if err != nil {
		return nil, err
	}

	r := &cache{
		client: client,
		host:   url.Hostname(),
		port:   url.Port(),
	}

	r.Start(ctx)

	f := &MemResolver{
		LookupIP: func(ctx context.Context, network, host string) ([]net.IP, error) {
			return r.lookupIP(ctx, network, host)
		},
	}

	return NewMemoryResolver(f), nil
}

func setConfigDefaults(config *rest.Config) error {
	gv := v1.SchemeGroupVersion
	config.GroupVersion = &gv
	config.APIPath = "/api"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	return nil
}

func (r *cache) lookupIP(ctx context.Context, network, host string) ([]net.IP, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	wantName := host
	// fqdn appends a dot
	if netutils.ParseIPSloppy(host) == nil {
		wantName = strings.TrimSuffix(host, ".")
	}
	// Use the default resolver if:
	// - cache is empty
	// - is not trying to resolve the apiserver
	if len(r.cache) == 0 ||
		wantName != r.host ||
		wantName != "kubernetes.default" {
		return net.DefaultResolver.LookupIP(ctx, network, host)
	}

	// Favor local, any local IP is fine
	localAddresses := getLocalAddressSet()
	for _, ip := range r.cache {
		if localAddresses.Has(ip) {
			return []net.IP{ip}, nil
		}
	}
	// Let the golang dialer choose one
	result := make([]net.IP, len(r.cache))
	copy(result, r.cache)
	return result, nil
}

// start starts a loop to get the apiserver endpoints from the apiserver
// so the dialer can connect to the registered apiservers in the cluster.
// This is the tricky part, since the resolver uses the same dialer it feeds
// so it will benefit from the resilience it provides.
func (r *cache) Start(ctx context.Context) {
	// run a goroutine updating the apiserver hosts in the dialer
	// this handle cluster resizing and renumbering
	klog.Info("Starting apiserver resolver ...")
	go func() {
		// Refresh the cache periodically
		tick := time.Tick(30 * time.Second)
		for {
			select {
			case <-tick:
				// apiservers are registered as endpoints of the kubernetes.default service
				// they are unregistered when they are not healthy or when shutting down
				// so we don't have to check the healthz or readyz endpoints
				endpoint := &v1.Endpoints{}
				err := r.client.Get().
					Resource("endpoints").
					Namespace("default").
					Name("kubernetes").
					Do(ctx).
					Into(endpoint)
				if err != nil {
					klog.InfoS("Error getting apiserver addresses", "error", err)
					continue
				}
				// obtain apiserver IPs from the Endpoint object
				// and refresh the cache with the new IPs
				r.mu.Lock()
				ips := []net.IP{}
				for _, ss := range endpoint.Subsets {
					for _, e := range ss.Addresses {
						ips = append(ips, netutils.ParseIPSloppy(e.IP))
					}
					if len(ss.Ports) != 1 {
						klog.Fatalf("Unsupported api servers endpoints with multiple ports")
					} else if strconv.Itoa(int(ss.Ports[0].Port)) != r.port {
						klog.Fatalf("Unsupported api servers host with different port")
					}
				}
				// if there are no ips we keep previous ones in the cache
				// if something fails we can still retry against the old ones
				if len(ips) > 0 {
					klog.Infof("Got apiserver addresses: %v", ips)
					r.cache = ips
				}
				r.mu.Unlock()

			case <-ctx.Done():
				return
			}
		}
	}()
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
