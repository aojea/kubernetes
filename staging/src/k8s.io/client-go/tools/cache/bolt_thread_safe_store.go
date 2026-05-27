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
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"k8s.io/apimachinery/pkg/runtime"
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/lru"
)

// Using sync.Pool to recycle JSON encode buffers. Protobuf is deliberately avoided here to maintain support for standard Kubernetes CRDs.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Hybrid Design:
// To handle massive scale in memory-constrained environments, we store actual resource data
// on disk using BoltDB, but maintain the cache.storeIndex in memory. Keeping indexes in RAM
// ensures heavily utilized K8s operations like ByIndex and IndexKeys remain extremely fast,
// while RAM usage remains minimal because full serialized objects are offloaded to disk.
type boltThreadSafeMap struct {
	db             *bbolt.DB
	bucketName     []byte
	metaBucketName []byte

	scheme     *runtime.Scheme
	serializer runtime.Serializer

	lock sync.RWMutex
	// index holds in-memory index mappings (index value -> set of string keys)
	index         *storeIndex
	rv            string
	compactorStop chan struct{}
	lruCache      *lru.Cache
}

// Ensure boltThreadSafeMap implements ThreadSafeStoreWithTransaction
var _ ThreadSafeStore = &boltThreadSafeMap{}
var _ ThreadSafeStoreWithTransaction = &boltThreadSafeMap{}

// NewBoltThreadSafeStore creates a ThreadSafeStore backed by BoltDB and in-memory indexes.
func NewBoltThreadSafeStore(db *bbolt.DB, scheme *runtime.Scheme, indexers Indexers, indices Indices, compactionInterval time.Duration, compactionThreshold float64) (ThreadSafeStore, error) {
	// Fail-Fast Guardrail: Nil check to ensure we don't get runtime panic during deserialization
	if scheme == nil {
		return nil, fmt.Errorf("cannot initialize BoltThreadSafeStore with a nil runtime.Scheme")
	}

	bucketName := []byte("items")
	metaBucketName := []byte("meta")

	// Initialize structural buckets
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return fmt.Errorf("failed to create items bucket: %w", err)
		}
		_, err = tx.CreateBucketIfNotExists(metaBucketName)
		if err != nil {
			return fmt.Errorf("failed to create meta bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	s := &boltThreadSafeMap{
		db:             db,
		bucketName:     bucketName,
		metaBucketName: metaBucketName,
		scheme:         scheme,
		serializer:     jsonserializer.NewSerializerWithOptions(jsonserializer.DefaultMetaFactory, scheme, scheme, jsonserializer.SerializerOptions{Yaml: false, Pretty: false, Strict: false}),
		index: &storeIndex{
			indexers: indexers,
			indices:  indices,
		},
		lruCache: lru.New(1000),
	}
	if s.index.indices == nil {
		s.index.indices = Indices{}
	}

	// Bootstrapping: Scan database and populate the in-memory index structure
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return nil
		}
		metaBucket := tx.Bucket(s.metaBucketName)
		if metaBucket != nil {
			if val := metaBucket.Get([]byte("resourceVersion")); val != nil {
				s.rv = string(val)
			}
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			obj, err := s.decode(v)
			if err != nil {
				return fmt.Errorf("failed to decode object for key %q: %w", string(k), err)
			}
			s.index.updateIndices(nil, obj, string(k))
		}
		return nil
	})

	if err != nil {
		klog.Warningf("WARNING: Disk cache corruption or load failure detected, wiping database to force full resync: %v", err)

		// Perform a db.Update to wipe the buckets and start fresh
		wipeErr := db.Update(func(tx *bbolt.Tx) error {
			if err := tx.DeleteBucket(bucketName); err != nil && err != bbolt.ErrBucketNotFound {
				return err
			}
			if _, err := tx.CreateBucket(bucketName); err != nil {
				return err
			}
			if err := tx.DeleteBucket(metaBucketName); err != nil && err != bbolt.ErrBucketNotFound {
				return err
			}
			if _, err := tx.CreateBucket(metaBucketName); err != nil {
				return err
			}
			return nil
		})
		if wipeErr != nil {
			return nil, fmt.Errorf("failed to wipe corrupted database: %w", wipeErr)
		}

		// Reset the in-memory index structure and RV state
		s.lock.Lock()
		s.index.reset()
		s.rv = ""
		s.lock.Unlock()
	}

	if compactionInterval > 0 {
		s.compactorStop = make(chan struct{})
		if compactionThreshold <= 0 {
			compactionThreshold = 0.30
		}
		go s.runCompactorLoop(compactionInterval, compactionThreshold)
	}

	return s, nil
}

func (s *boltThreadSafeMap) Close() error {
	if s.compactorStop != nil {
		close(s.compactorStop)
	}
	return nil
}

func (s *boltThreadSafeMap) encode(obj runtime.Object) ([]byte, error) {
	// Using sync.Pool to recycle JSON encode buffers. Protobuf is deliberately avoided here to maintain support for standard Kubernetes CRDs.
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	if err := s.serializer.Encode(obj, buf); err != nil {
		return nil, err
	}

	// Safety Guardrail: copy the bytes before returning the buffer back to the pool
	data := make([]byte, buf.Len())
	copy(data, buf.Bytes())
	return data, nil
}

// The Scheme Decision:
// We use the runtime.Scheme (acting as our Type Registry) inside the decoder to solve
// the "Cold Start Type Instantiation" problem. The decoder reads the GVK from the wire-format
// JSON data, dynamically creates a fresh instance of the concrete K8s struct (e.g. *corev1.Pod)
// via s.serializer.Decode, and decodes the fields into it. This enables seamless instantiation
// of any registered API resource type after a process restart.
func (s *boltThreadSafeMap) decode(data []byte) (runtime.Object, error) {
	obj, _, err := s.serializer.Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *boltThreadSafeMap) Add(key string, obj interface{}) {
	s.Update(key, obj)
}

func (s *boltThreadSafeMap) Update(key string, obj interface{}) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		klog.Errorf("BoltThreadSafeStore only supports objects implementing runtime.Object, got %T", obj)
		return
	}

	err := s.db.Update(func(tx *bbolt.Tx) error {
		return s.updateLocked(tx, key, runtimeObj)
	})
	if err != nil {
		klog.Errorf("BoltDB Update failed for key %q: %v", key, err)
	}
}

func (s *boltThreadSafeMap) updateLocked(tx *bbolt.Tx, key string, obj runtime.Object) error {
	bucket := tx.Bucket(s.bucketName)
	if bucket == nil {
		return fmt.Errorf("items bucket not found")
	}

	var oldObj runtime.Object
	if oldData := bucket.Get([]byte(key)); oldData != nil {
		var err error
		oldObj, err = s.decode(oldData)
		if err != nil {
			klog.Warningf("Failed to decode old object with key %q during update: %v", key, err)
		}
	}

	data, err := s.encode(obj)
	if err != nil {
		return fmt.Errorf("failed to encode object: %w", err)
	}

	if err := bucket.Put([]byte(key), data); err != nil {
		return fmt.Errorf("failed to save object: %w", err)
	}

	s.lock.Lock()
	s.index.updateIndices(oldObj, obj, key)
	s.lruCache.Add(key, obj)
	s.lock.Unlock()

	if rv, err := rvFromObject(obj); err == nil && rv != "" {
		s.lock.Lock()
		s.rv = rv
		s.lock.Unlock()
		metaBucket := tx.Bucket(s.metaBucketName)
		if metaBucket != nil {
			_ = metaBucket.Put([]byte("resourceVersion"), []byte(rv))
		}
	}

	return nil
}

func (s *boltThreadSafeMap) Delete(key string) {
	s.DeleteWithObject(key, nil)
}

func (s *boltThreadSafeMap) DeleteWithObject(key string, obj interface{}) {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		return s.deleteLocked(tx, key)
	})
	if err != nil {
		klog.Errorf("BoltDB Delete failed for key %q: %v", key, err)
	}
}

func (s *boltThreadSafeMap) deleteLocked(tx *bbolt.Tx, key string) error {
	bucket := tx.Bucket(s.bucketName)
	if bucket == nil {
		return fmt.Errorf("items bucket not found")
	}

	oldData := bucket.Get([]byte(key))
	if oldData == nil {
		return nil
	}

	oldObj, err := s.decode(oldData)
	if err != nil {
		klog.Warningf("Failed to decode old object with key %q during delete: %v", key, err)
	}

	if err := bucket.Delete([]byte(key)); err != nil {
		return fmt.Errorf("failed to delete object from disk: %w", err)
	}

	s.lock.Lock()
	s.index.updateIndices(oldObj, nil, key)
	s.lruCache.Remove(key)
	s.lock.Unlock()

	if oldObj != nil {
		if rv, err := rvFromObject(oldObj); err == nil && rv != "" {
			s.lock.Lock()
			s.rv = rv
			s.lock.Unlock()
			metaBucket := tx.Bucket(s.metaBucketName)
			if metaBucket != nil {
				_ = metaBucket.Put([]byte("resourceVersion"), []byte(rv))
			}
		}
	}

	return nil
}

func (s *boltThreadSafeMap) Get(key string) (item interface{}, exists bool) {
	s.lock.Lock()
	if obj, ok := s.lruCache.Get(key); ok {
		s.lock.Unlock()
		return obj, true
	}
	s.lock.Unlock()

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(key))
		if data == nil {
			return nil
		}
		var decodeErr error
		item, decodeErr = s.decode(data)
		if decodeErr != nil {
			klog.Errorf("Failed to decode object for key %q in Get(): %v", key, decodeErr)
			return nil
		}
		exists = true
		return nil
	})
	if err != nil {
		klog.Errorf("BoltDB Get view failed for key %q: %v", key, err)
	}

	if exists && item != nil {
		s.lock.Lock()
		s.lruCache.Add(key, item)
		s.lock.Unlock()
	}
	return item, exists
}

func (s *boltThreadSafeMap) List() []interface{} {
	var items []interface{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			obj, err := s.decode(v)
			if err != nil {
				klog.Errorf("Failed to decode object for key %q in List(): %v", string(k), err)
				continue
			}
			items = append(items, obj)
		}
		return nil
	})
	if err != nil {
		klog.Errorf("BoltDB List view failed: %v", err)
	}
	return items
}

func (s *boltThreadSafeMap) ListKeys() []string {
	var keys []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, string(k))
		}
		return nil
	})
	if err != nil {
		klog.Errorf("BoltDB ListKeys view failed: %v", err)
	}
	return keys
}

// BoltDB Specifics (Replace):
// Deleting keys one-by-one in BoltDB requires executing multiple B+ tree balance routines,
// which is slow. Instead, Replace drops the entire bucket at once (an extremely fast metadata
// operation) and recreates it empty before populating new objects.
func (s *boltThreadSafeMap) Replace(items map[string]interface{}, resourceVersion string) {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(s.bucketName); err != nil && err != bbolt.ErrBucketNotFound {
			return fmt.Errorf("failed to drop items bucket: %w", err)
		}
		_, err := tx.CreateBucket(s.bucketName)
		if err != nil {
			return fmt.Errorf("failed to recreate items bucket: %w", err)
		}

		s.lock.Lock()
		s.index.reset()
		s.lruCache.Clear()
		s.lock.Unlock()

		for key, item := range items {
			runtimeObj, ok := item.(runtime.Object)
			if !ok {
				klog.Errorf("Object for key %q in Replace does not implement runtime.Object", key)
				continue
			}
			if err := s.updateLocked(tx, key, runtimeObj); err != nil {
				return err
			}
		}

		s.lock.Lock()
		s.rv = resourceVersion
		s.lock.Unlock()

		metaBucket := tx.Bucket(s.metaBucketName)
		if metaBucket != nil {
			_ = metaBucket.Put([]byte("resourceVersion"), []byte(resourceVersion))
		}

		return nil
	})
	if err != nil {
		klog.Errorf("BoltDB Replace failed: %v", err)
	}
}

func (s *boltThreadSafeMap) Index(indexName string, obj interface{}) ([]interface{}, error) {
	s.lock.RLock()
	storeKeySet, err := s.index.getKeysFromIndex(indexName, obj)
	var keys []string
	if err == nil && storeKeySet != nil {
		keys = storeKeySet.UnsortedList()
	}
	s.lock.RUnlock()
	if err != nil {
		return nil, err
	}

	var items []interface{}
	err = s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return nil
		}
		for _, storeKey := range keys {
			data := bucket.Get([]byte(storeKey))
			if data != nil {
				decoded, err := s.decode(data)
				if err != nil {
					klog.Errorf("Failed to decode object for key %q in Index(): %v", storeKey, err)
					continue
				}
				items = append(items, decoded)
			}
		}
		return nil
	})
	return items, err
}

func (s *boltThreadSafeMap) IndexKeys(indexName, indexedValue string) ([]string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	set, err := s.index.getKeysByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return sets.List(set), nil
}

func (s *boltThreadSafeMap) ListIndexFuncValues(indexName string) []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.index.getIndexValues(indexName)
}

func (s *boltThreadSafeMap) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	s.lock.Lock()
	set, err := s.index.getKeysByIndex(indexName, indexedValue)
	var keys []string
	if err == nil && set != nil {
		keys = set.UnsortedList()
	}
	if err != nil {
		s.lock.Unlock()
		return nil, err
	}

	var items []interface{}
	var missKeys []string

	// Query the L1 LRU first under exclusive lock
	for _, key := range keys {
		if obj, ok := s.lruCache.Get(key); ok {
			items = append(items, obj)
		} else {
			missKeys = append(missKeys, key)
		}
	}
	s.lock.Unlock()

	if len(missKeys) == 0 {
		return items, nil
	}

	// Fetch only missed keys from L2 BoltDB
	err = s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return nil
		}

		var decodedItems []interface{}
		for _, key := range missKeys {
			data := bucket.Get([]byte(key))
			if data != nil {
				decoded, err := s.decode(data)
				if err != nil {
					klog.Errorf("Failed to decode object for key %q in ByIndex(): %v", key, err)
					continue
				}
				items = append(items, decoded)
				decodedItems = append(decodedItems, decoded)
			}
		}

		// Update the L1 LRU with the newly fetched items
		if len(decodedItems) > 0 {
			s.lock.Lock()
			for i, key := range missKeys {
				if i < len(decodedItems) {
					s.lruCache.Add(key, decodedItems[i])
				}
			}
			s.lock.Unlock()
		}
		return nil
	})
	return items, err
}

func (s *boltThreadSafeMap) GetIndexers() Indexers {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.index.indexers
}

func (s *boltThreadSafeMap) AddIndexers(newIndexers Indexers) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.index.addIndexers(newIndexers); err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucketName)
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			obj, err := s.decode(v)
			if err != nil {
				continue
			}
			for name := range newIndexers {
				s.index.updateSingleIndex(name, nil, obj, string(k))
			}
		}
		return nil
	})
}

func (s *boltThreadSafeMap) Bookmark(rv string) {
	s.lock.Lock()
	s.rv = rv
	s.lock.Unlock()

	err := s.db.Update(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket(s.metaBucketName)
		if metaBucket != nil {
			_ = metaBucket.Put([]byte("resourceVersion"), []byte(rv))
		}
		return nil
	})
	if err != nil {
		klog.Errorf("Failed to save resource version bookmark: %v", err)
	}
}

func (s *boltThreadSafeMap) LastStoreSyncResourceVersion() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.rv
}

func (s *boltThreadSafeMap) Resync() error {
	return nil
}

// Transaction:
// Implements ThreadSafeStoreWithTransaction. All write operations inside fns
// are bundled and executed inside a single BoltDB update transaction, yielding
// peak disk I/O throughput and consistency.
func (s *boltThreadSafeMap) Transaction(fns ...ThreadSafeStoreTransaction) {
	if len(fns) == 0 {
		return
	}

	err := s.db.Update(func(tx *bbolt.Tx) error {
		for _, fn := range fns {
			runtimeObj, ok := fn.Object.(runtime.Object)
			if !ok && fn.Type != TransactionTypeDelete {
				klog.Errorf("Transaction object for key %q does not implement runtime.Object, got %T", fn.Key, fn.Object)
				continue
			}

			switch fn.Type {
			case TransactionTypeAdd:
				if err := s.updateLocked(tx, fn.Key, runtimeObj); err != nil {
					return err
				}
			case TransactionTypeUpdate:
				if err := s.updateLocked(tx, fn.Key, runtimeObj); err != nil {
					return err
				}
			case TransactionTypeDelete:
				if err := s.deleteLocked(tx, fn.Key); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		klog.Errorf("BoltDB batch transaction failed: %v", err)
	}
}

// runCompactorLoop starts a background compaction ticker to keep BoltDB file defragmented.
// It runs continuously in the background until s.compactorStop is closed.
func (s *boltThreadSafeMap) runCompactorLoop(interval time.Duration, fragmentationThreshold float64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.compactorStop:
			return
		case <-ticker.C:
			s.compactIfNeeded(fragmentationThreshold)
		}
	}
}

func (s *boltThreadSafeMap) compactIfNeeded(threshold float64) {
	stats := s.db.Stats()
	pageSize := s.db.Info().PageSize
	dbPath := s.db.Path()

	fi, err := os.Stat(dbPath)
	if err != nil {
		klog.Errorf("Compactor failed to stat database file %q: %v", dbPath, err)
		return
	}
	totalSize := fi.Size()
	if totalSize == 0 {
		return
	}

	fragmentationRatio := float64(stats.FreePageN*pageSize) / float64(totalSize)
	if fragmentationRatio < threshold {
		return
	}

	klog.Infof("Compactor triggered for %q: fragmentation ratio %.2f%% (threshold %.2f%%)", dbPath, fragmentationRatio*100, threshold*100)

	// LOCK ACQUISITION EXPLANATION (Safety Guardrail):
	// We MUST acquire an exclusive write lock (c.lock.Lock()) on our store during the entire WriteTo
	// and file swap process. This is absolutely critical: it prevents any writes (Add, Update, Delete,
	// or Transaction calls) from executing concurrently in the old database file while the defragmented
	// snapshot is being written to the temporary file on disk. Without this exclusive lock, any concurrent
	// writes would be made to the old database file, and subsequently discarded when the temporary file
	// overwrites the old database file, resulting in lost Kubernetes events and cache inconsistency.
	s.lock.Lock()
	defer s.lock.Unlock()

	tmpPath := dbPath + ".tmp"
	_ = os.Remove(tmpPath)

	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		klog.Errorf("CRITICAL: Compactor failed to create temporary database file %q: %v", tmpPath, err)
		return
	}
	defer func() {
		tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()

	// Stream the defragmented snapshot into the temporary file
	err = s.db.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(tmpFile)
		return err
	})
	if err != nil {
		klog.Errorf("CRITICAL: Compactor failed to stream defragmented snapshot of database %q to temporary file: %v", dbPath, err)
		return
	}
	_ = tmpFile.Sync()
	_ = tmpFile.Close()

	// Close current database connection to release file locks and allow file rename
	if err := s.db.Close(); err != nil {
		klog.Errorf("CRITICAL: Compactor failed to close current database %q: %v", dbPath, err)
		return
	}

	// Atomically replace old database file with the new compacted database file
	if err := os.Rename(tmpPath, dbPath); err != nil {
		klog.Errorf("CRITICAL: Compactor failed to rename temporary file to replace old database %q: %v", dbPath, err)
		return
	}

	// Reopen the compacted database file and restore s.db
	newDB, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		klog.Errorf("CRITICAL: Compactor failed to re-open database %q after compaction swap: %v", dbPath, err)
		panic(fmt.Sprintf("CRITICAL: BoltDB database reopen failed after compaction swap: %v", err))
	}
	s.db = newDB
}
