/*
Copyright 2026 The Kubernetes Authors.

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

package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
	"k8s.io/apimachinery/pkg/runtime"
)

// NewBoltDBStore creates a Store backed by BoltDB.
func NewBoltDBStore(path string, exampleObject runtime.Object, keyFunc KeyFunc) (Store, error) {
	return NewBoltDBIndexer(path, exampleObject, keyFunc, Indexers{})
}

// NewBoltDBIndexer creates an Indexer backed by BoltDB.
func NewBoltDBIndexer(path string, exampleObject runtime.Object, keyFunc KeyFunc, indexers Indexers) (Indexer, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating directories for BoltDB: %w", err)
	}

	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening BoltDB at %q: %w", path, err)
	}

	threadSafeStore, err := NewBoltThreadSafeStore(db, exampleObject, indexers, Indices{}, 0, 0)
	if err != nil {
		db.Close()
		return nil, err
	}

	// We wrap the db.Close() so that closing the Store/Indexer closes the DB handle if owned.
	return &boltDBIndexerWrapper{
		Indexer: &cache{
			cacheStorage: threadSafeStore,
			keyFunc:      keyFunc,
		},
		db: db,
	}, nil
}

// NewBoltDBIndexerWithDB creates an Indexer backed by an existing BoltDB instance.
func NewBoltDBIndexerWithDB(db *bbolt.DB, exampleObject runtime.Object, keyFunc KeyFunc, indexers Indexers) (Indexer, error) {
	threadSafeStore, err := NewBoltThreadSafeStore(db, exampleObject, indexers, Indices{}, 0, 0)
	if err != nil {
		return nil, err
	}

	return &cache{
		cacheStorage: threadSafeStore,
		keyFunc:      keyFunc,
	}, nil
}

// boltDBIndexerWrapper wraps standard cache indexer to close the owned DB file when closed.
type boltDBIndexerWrapper struct {
	Indexer
	db *bbolt.DB
}

func (w *boltDBIndexerWrapper) Close() error {
	return w.db.Close()
}

func (w *boltDBIndexerWrapper) RunCompactor(ctx context.Context, interval time.Duration, threshold float64) {
	if comp, ok := w.Indexer.(interface {
		RunCompactor(ctx context.Context, interval time.Duration, fragmentationThreshold float64)
	}); ok {
		comp.RunCompactor(ctx, interval, threshold)
	}
}
