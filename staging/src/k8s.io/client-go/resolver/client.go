package resolver

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// MagicClient creates a resilient client-go that, in case of connection failures,
// tries to connect to all the available apiservers in the cluster.
func MagicClient(ctx context.Context, config *rest.Config) (*kubernetes.Clientset, error) {
	// create an apiserver resolver
	r, err := APIServerResolver(ctx, config)
	if err != nil {
		return nil, err
	}
	// inject the API server resolver into the client
	if config.Dial != nil {
		return nil, fmt.Errorf("APIServer resolver doesn't support custom dialers")
	}
	config.Resolver = r

	// create the clientset with our own resolver
	return kubernetes.NewForConfig(config)
}
