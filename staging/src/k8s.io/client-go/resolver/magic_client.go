package resolver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

const (
	magicName = "kubernetes.default"
)

// cache to store API server IP addresses
type cache struct {
	client *kubernetes.Clientset
	config *rest.Config

	host string // URL host from the apiserver
	port string // port

	mu    sync.Mutex
	cache []net.IP // API server IP addresses

	preferEndpoints bool
}

// APIServerResolver returns a golang resolver that resolves the apiserver IPs
// based on the published endpoints. If not information is available it falls back
// to the golang default resolver
func MagicClient(ctx context.Context, c *rest.Config) (*kubernetes.Clientset, error) {
	config := *c

	// Get configuration Host and Port for the apiserver
	host, port, err := getHostPort(config.Host)
	if err != nil {
		return nil, err
	}

	// Create a resolver that uses the apiserver to resolve the api server endpoints
	r := &cache{
		host: host,
		port: port,
	}

	// use our magic name to force the dialer to use our custom resolver
	u, err := url.Parse(c.Host)
	if err != nil {
		klog.Fatalf("Error parsing configuration Host URL: %v", err)
	}
	u.Host = magicName + ":" + port
	config.Host = u.String()

	f := &MemResolver{
		LookupIP: func(ctx context.Context, network, host string) ([]net.IP, error) {
			return r.lookupIP(ctx, network, host)
		},
	}

	// use our resolver on the RESTClient, when the rest client tries to resolve config.Host
	// it will be resolved by in memory resolver with the apiserver endpoints
	config.Resolver = NewMemoryResolver(f)
	r.config = &config

	// Create the clientset using the custom resolver
	client, err := kubernetes.NewForConfig(&config)
	if err != nil {
		return nil, err
	}
	r.client = client

	r.preferEndpoints = true
	r.Start(ctx)

	return r.client, nil
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

	klog.Infof("DEBUG lookupIP network %s host %s", network, host)
	wantName := strings.TrimSuffix(host, ".")
	// Use the default resolver if we are not using magic
	if wantName != magicName {
		return net.DefaultResolver.LookupIP(ctx, network, host)
	}

	// bootstrap problem, magic name will not work if there is nothing in the cache
	// so we use the original host to resolve and initialize the cache
	if len(r.cache) == 0 {
		addr, err := net.DefaultResolver.LookupIP(ctx, network, r.host)
		if err != nil {
			return nil, err
		}
		r.cache = addr
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
		tick := time.Tick(5 * time.Second)
		errCount := 0
		for {
			select {
			case <-tick:
				// apiservers are registered as endpoints of the kubernetes.default service
				// they are unregistered when they are not healthy or when shutting down
				// so we don't have to check the healthz or readyz endpoints
				endpoint, err := r.client.CoreV1().Endpoints("default").Get(ctx, "kubernetes", metav1.GetOptions{})
				if err != nil {
					if _, ok := err.(net.Error); ok {
						klog.InfoS("Network error trying to connect to %s:%s %v", r.host, r.port, err)
						errCount++
						if errCount > 3 {
							r.restoreClienset()
							// clean cache
							r.mu.Lock()
							r.cache = []net.IP{}
							r.mu.Unlock()
						}
					}
					klog.InfoS("Error getting apiserver addresses", "error", err)
					continue
				}
				// obtain apiserver IPs from the Endpoint object
				// and refresh the cache with the new IPs
				r.mu.Lock()
				ips := []net.IP{}
				recreatePort := 0
				for _, ss := range endpoint.Subsets {
					for _, e := range ss.Addresses {
						ips = append(ips, netutils.ParseIPSloppy(e.IP))
					}
					if len(ss.Ports) != 1 {
						klog.Fatalf("Unsupported API servers endpoints with multiple ports")
					}
					if strconv.Itoa(int(ss.Ports[0].Port)) != r.port {
						if r.preferEndpoints {
							recreatePort = int(ss.Ports[0].Port)
						} else {
							klog.Infof("API servers host with different port than endpoints and preferEndpoints disabled" +
								"the client will not fallback to endpoints")
							return
						}
					}
				}
				// if there are no ips we keep previous ones in the cache
				// if something fails we can still retry against the old ones
				// this is weird since this means we are connected to one apiserver
				if len(ips) > 0 {
					klog.Infof("Got apiserver addresses: %v", ips)
					r.cache = ips
				}
				if recreatePort > 0 {
					r.recreateClienset(recreatePort)
				}
				r.mu.Unlock()

			case <-ctx.Done():
				return
			}
		}
	}()
}

func (r *cache) recreateClienset(port int) {
	klog.Infof("Modifying clienset base URL to use port %d", port)
	// Shallow copy to modify the URL Port
	c := *r.config
	u, err := url.Parse(c.Host)
	if err != nil {
		klog.Fatalf("Error parsing configuration Host URL: %v", err)
	}
	p := strconv.Itoa(port)
	u.Host = magicName + ":" + p
	c.Host = u.String()
	client, err := kubernetes.NewForConfig(&c)
	if err != nil {
		klog.Fatalf("Error restoring original clientset: %v", err)
	}
	r.port = p
	r.client = client
}

func (r *cache) restoreClienset() {
	klog.Infof("Restoring original client configuration")
	// Get configuration Host and Port for the apiserver
	_, port, err := getHostPort(r.config.Host)
	if err != nil {
		klog.Fatalf("Error restoring original clientset: %v", err)
	}
	r.port = port

	client, err := kubernetes.NewForConfig(r.config)
	if err != nil {
		klog.Fatalf("Error restoring original clientset: %v", err)
	}
	r.client = client
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
