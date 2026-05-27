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
	"fmt"
	"path/filepath"
	stdruntime "runtime"
	"sync"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestObject implements runtime.Object for testing Scheme-based serialization
type TestObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Value             string `json:"value"`
	Payload           string `json:"payload,omitempty"`
}

func (t *TestObject) DeepCopyObject() runtime.Object {
	return &TestObject{
		TypeMeta:   t.TypeMeta,
		ObjectMeta: t.ObjectMeta,
		Value:      t.Value,
		Payload:    t.Payload,
	}
}

func setupTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{Group: "test", Version: "v1", Kind: "TestObject"}
	scheme.AddKnownTypeWithName(gvk, &TestObject{})
	return scheme
}

func TestNewBoltThreadSafeStoreFailFast(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Fail-Fast Check: passing nil scheme must fail immediately
	_, err = NewBoltThreadSafeStore(db, nil, Indexers{}, Indices{}, 0, 0)
	if err == nil {
		t.Fatal("Expected constructor to return error when scheme is nil, but it succeeded")
	}
}

func TestBoltThreadSafeStoreLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "lifecycle.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_value": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}

	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	obj1 := &TestObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "obj1", Namespace: "ns1"},
		Value:      "val-a",
	}
	obj2 := &TestObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "obj2", Namespace: "ns2"},
		Value:      "val-b",
	}

	// 1. Add objects
	store.Add("ns1/obj1", obj1)
	store.Add("ns2/obj2", obj2)

	// 2. Get objects
	item1, exists := store.Get("ns1/obj1")
	if !exists {
		t.Fatal("Expected obj1 to exist")
	}
	if item1.(*TestObject).Value != "val-a" {
		t.Errorf("Expected value 'val-a', got %q", item1.(*TestObject).Value)
	}

	// 3. List Keys
	keys := store.ListKeys()
	keysSet := sets.New(keys...)
	if !keysSet.HasAll("ns1/obj1", "ns2/obj2") || len(keysSet) != 2 {
		t.Errorf("Unexpected keys: %v", keys)
	}

	// 4. Index query (ByIndex)
	results, err := store.ByIndex("by_value", "val-a")
	if err != nil {
		t.Fatalf("ByIndex failed: %v", err)
	}
	if len(results) != 1 || results[0].(*TestObject).Name != "obj1" {
		t.Errorf("Unexpected index query results: %v", results)
	}

	// 5. Update object
	obj1.Value = "val-updated"
	store.Update("ns1/obj1", obj1)

	// Verify updated index
	results, _ = store.ByIndex("by_value", "val-a")
	if len(results) != 0 {
		t.Errorf("Expected 0 results for old index value, got %d", len(results))
	}
	results, _ = store.ByIndex("by_value", "val-updated")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for updated index value, got %d", len(results))
	}

	// 6. Delete object
	store.Delete("ns2/obj2")
	if _, exists := store.Get("ns2/obj2"); exists {
		t.Error("Expected deleted obj2 to be missing")
	}
}

func TestBoltThreadSafeStoreBootstrapAndReplace(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "bootstrap.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_value": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}

	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	obj1 := &TestObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "obj1", Namespace: "ns1"},
		Value:      "val-a",
	}
	store.Add("ns1/obj1", obj1)

	// Close and reopen to test Cold Start / Bootstrapping index reconstruction
	store = nil
	db.Close()

	db, err = bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to reopen db: %v", err)
	}

	store, err = NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to reconstruct store on restart: %v", err)
	}

	// Index should have been fully rebuilt on startup!
	results, err := store.ByIndex("by_value", "val-a")
	if err != nil {
		t.Fatalf("ByIndex on reopened store failed: %v", err)
	}
	if len(results) != 1 || results[0].(*TestObject).Name != "obj1" {
		t.Errorf("Index reconstruction failed, expected obj1, got: %v", results)
	}

	// Test Replace
	newItems := map[string]interface{}{
		"ns3/obj3": &TestObject{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
			ObjectMeta: metav1.ObjectMeta{Name: "obj3", Namespace: "ns3"},
			Value:      "val-c",
		},
	}
	store.Replace(newItems, "100")

	// Old item should be gone
	if _, exists := store.Get("ns1/obj1"); exists {
		t.Error("Expected old item to be dropped on Replace")
	}

	// New item should exist and be indexed
	if item, exists := store.Get("ns3/obj3"); !exists || item.(*TestObject).Value != "val-c" {
		t.Error("Expected new item to be successfully added during Replace")
	}
}

func TestBoltThreadSafeStoreTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "txn.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_value": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}

	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	obj1 := &TestObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "obj1", Namespace: "ns1"},
		Value:      "val-a",
	}
	obj2 := &TestObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "obj2", Namespace: "ns2"},
		Value:      "val-b",
	}

	txnStore := store.(ThreadSafeStoreWithTransaction)
	txnStore.Transaction(
		ThreadSafeStoreTransaction{
			Key: "ns1/obj1",
			Transaction: Transaction{
				Type:   TransactionTypeAdd,
				Object: obj1,
			},
		},
		ThreadSafeStoreTransaction{
			Key: "ns2/obj2",
			Transaction: Transaction{
				Type:   TransactionTypeAdd,
				Object: obj2,
			},
		},
	)

	// Verify both exist and indexes are populated
	if _, exists := store.Get("ns1/obj1"); !exists {
		t.Error("Expected obj1 to be created by transaction")
	}
	if _, exists := store.Get("ns2/obj2"); !exists {
		t.Error("Expected obj2 to be created by transaction")
	}

	results, _ := store.ByIndex("by_value", "val-a")
	if len(results) != 1 {
		t.Errorf("Expected val-a to be indexed, got %d results", len(results))
	}
}

func TestBoltThreadSafeStoreCorruptionRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "corruption.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_value": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}

	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	obj1 := &TestObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "obj1", Namespace: "ns1"},
		Value:      "val-a",
	}
	store.Add("ns1/obj1", obj1)
	db.Close()

	// Reopen DB and deliberately corrupt the bytes in the items bucket
	db, err = bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to reopen db: %v", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("items"))
		if bucket == nil {
			t.Fatal("items bucket not found")
		}
		// Write corrupted non-JSON bytes
		return bucket.Put([]byte("ns1/obj1"), []byte("invalid-non-json-corrupt-data-!!!"))
	})
	if err != nil {
		t.Fatalf("Failed to write corrupt bytes: %v", err)
	}

	// Start store again with the corrupted DB. The constructor MUST catch the decode error,
	// wipe the database, reset the state, and start fresh successfully!
	store, err = NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Expected store to successfully recover and initialize, but got error: %v", err)
	}
	defer db.Close()

	// Database should have been wiped clean
	keys := store.ListKeys()
	if len(keys) != 0 {
		t.Errorf("Expected database to be wiped clean, but found keys: %v", keys)
	}

	// Index should be empty
	results, _ := store.ByIndex("by_value", "val-a")
	if len(results) != 0 {
		t.Errorf("Expected index to be empty, but found results: %v", results)
	}
}

func TestBoltThreadSafeStoreCompactor(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "compaction.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_value": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}

	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// 1. Insert 200 objects to grow the database file
	for i := 0; i < 200; i++ {
		obj := &TestObject{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
			ObjectMeta: metav1.ObjectMeta{Name: "obj", Namespace: "ns"},
			Value:      "val-a",
		}
		store.Add(fmt.Sprintf("ns/obj-%d", i), obj)
	}

	// 2. Delete 180 of them to create fragmentation (free pages)
	for i := 0; i < 180; i++ {
		store.Delete(fmt.Sprintf("ns/obj-%d", i))
	}

	// 3. Run compaction explicitly with a low threshold
	// This will execute the STW (Stop-The-World) snapshot & swap
	store.(*boltThreadSafeMap).compactIfNeeded(0.01)

	// 4. Verify remaining objects still exist and are completely correct!
	for i := 180; i < 200; i++ {
		item, exists := store.Get(fmt.Sprintf("ns/obj-%d", i))
		if !exists {
			t.Fatalf("Expected obj-%d to exist after compaction swap", i)
		}
		if item.(*TestObject).Value != "val-a" {
			t.Errorf("Expected value 'val-a', got %q", item.(*TestObject).Value)
		}
	}

	// 5. Verify index is intact and correct
	results, err := store.ByIndex("by_value", "val-a")
	if err != nil {
		t.Fatalf("ByIndex query failed: %v", err)
	}
	if len(results) != 20 {
		t.Errorf("Expected 20 objects in index after compaction, got %d", len(results))
	}
}

func TestBoltThreadSafeStoreConcurrencyStress(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "stress.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_value": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}

	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start 10 writer goroutines
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("ns/obj-%d-%d", workerId, i)
					obj := &TestObject{
						TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
						ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("obj-%d", i), Namespace: "ns"},
						Value:      fmt.Sprintf("val-%d", i%5),
					}
					store.Add(key, obj)
					if i%3 == 0 {
						store.Delete(key)
					}
					i++
				}
			}
		}(w)
	}

	// Start 10 reader goroutines
	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					_ = store.List()
					_ = store.ListKeys()
					_, _ = store.ByIndex("by_value", "val-2")
				}
			}
		}(r)
	}

	// Run stress test for 500 milliseconds
	time.Sleep(500 * time.Millisecond)
	close(stopCh)
	wg.Wait()
}

func BenchmarkStoreAdd_InMemory(b *testing.B) {
	store := NewThreadSafeStore(Indexers{}, Indices{})
	obj := &TestObject{
		TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		Value:    "bar",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Add("key", obj)
	}
}

func BenchmarkStoreAdd_BoltDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		b.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	store, err := NewBoltThreadSafeStore(db, scheme, Indexers{}, Indices{}, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer store.(*boltThreadSafeMap).Close()

	obj := &TestObject{
		TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		Value:    "bar",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Add("key", obj)
	}
}

func BenchmarkStoreGet_InMemory(b *testing.B) {
	store := NewThreadSafeStore(Indexers{}, Indices{})
	obj := &TestObject{
		TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		Value:    "bar",
	}
	store.Add("key", obj)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get("key")
	}
}

func BenchmarkStoreGet_BoltDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		b.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	store, err := NewBoltThreadSafeStore(db, scheme, Indexers{}, Indices{}, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer store.(*boltThreadSafeMap).Close()

	obj := &TestObject{
		TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		Value:    "bar",
	}
	store.Add("key", obj)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get("key")
	}
}

func BenchmarkStoreList_InMemory(b *testing.B) {
	store := NewThreadSafeStore(Indexers{}, Indices{})
	for i := 0; i < 100; i++ {
		store.Add(fmt.Sprintf("key-%d", i), &TestObject{Value: "bar"})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.List()
	}
}

func BenchmarkStoreList_BoltDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		b.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	store, err := NewBoltThreadSafeStore(db, scheme, Indexers{}, Indices{}, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer store.(*boltThreadSafeMap).Close()

	for i := 0; i < 100; i++ {
		store.Add(fmt.Sprintf("key-%d", i), &TestObject{
			TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
			Value:    "bar",
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.List()
	}
}

func BenchmarkStoreByIndex_InMemory(b *testing.B) {
	indexers := Indexers{
		"by_val": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}
	store := NewThreadSafeStore(indexers, Indices{})
	for i := 0; i < 100; i++ {
		store.Add(fmt.Sprintf("key-%d", i), &TestObject{Value: "bar"})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.ByIndex("by_val", "bar")
	}
}

func BenchmarkStoreByIndex_BoltDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		b.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_val": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}
	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create BoltDB store: %v", err)
	}
	defer store.(*boltThreadSafeMap).Close()

	for i := 0; i < 100; i++ {
		store.Add(fmt.Sprintf("key-%d", i), &TestObject{
			TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
			Value:    "bar",
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.ByIndex("by_val", "bar")
	}
}

func TestBoltThreadSafeStoreMemorySavings(t *testing.T) {
	// 1. Run standard in-memory test inside a separate anonymous function so its stack/variables are fully closed and freed!
	var heapAllocBefore uint64
	func() {
		largePayload := ""
		for i := 0; i < 1000; i++ {
			largePayload += "abcd" // 4KB
		}

		inMemoryStore := NewThreadSafeStore(Indexers{}, Indices{})
		for i := 0; i < 10000; i++ {
			// Generate unique string allocation
			uniquePayload := fmt.Sprintf("%s-%d", largePayload, i)
			obj := &TestObject{
				TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("obj-%d", i), Namespace: "default"},
				Value:      fmt.Sprintf("val-%d", i%10),
				Payload:    uniquePayload,
			}
			inMemoryStore.Add(fmt.Sprintf("default/obj-%d", i), obj)
		}
		stdruntime.GC()
		var memBefore stdruntime.MemStats
		stdruntime.ReadMemStats(&memBefore)
		heapAllocBefore = memBefore.HeapAlloc

		// Keep store alive until stats are read!
		_ = inMemoryStore.ListKeys()
	}()

	// Force GC after anonymous function exits to completely clean the heap of standard store objects
	stdruntime.GC()

	// 2. Run BoltDB store test in a separate anonymous function
	var heapAllocAfter uint64
	func() {
		largePayload := ""
		for i := 0; i < 1000; i++ {
			largePayload += "abcd" // 4KB
		}

		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "mem.db")
		db, err := bbolt.Open(dbPath, 0600, nil)
		if err != nil {
			t.Fatalf("Failed to open db: %v", err)
		}
		defer db.Close()

		scheme := setupTestScheme()
		boltStore, err := NewBoltThreadSafeStore(db, scheme, Indexers{}, Indices{}, 0, 0)
		if err != nil {
			t.Fatalf("Failed to create BoltDB store: %v", err)
		}
		defer boltStore.(*boltThreadSafeMap).Close()

		for i := 0; i < 10000; i++ {
			// Generate unique string allocation
			uniquePayload := fmt.Sprintf("%s-%d", largePayload, i)
			obj := &TestObject{
				TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("obj-%d", i), Namespace: "default"},
				Value:      fmt.Sprintf("val-%d", i%10),
				Payload:    uniquePayload,
			}
			boltStore.Add(fmt.Sprintf("default/obj-%d", i), obj)
		}

		stdruntime.GC()
		var memAfter stdruntime.MemStats
		stdruntime.ReadMemStats(&memAfter)
		heapAllocAfter = memAfter.HeapAlloc

		// Keep store alive until stats are read!
		_ = boltStore.ListKeys()
	}()

	// Force GC again
	stdruntime.GC()

	// 3. Calculate and assert memory savings
	diff := int64(heapAllocBefore) - int64(heapAllocAfter)
	savingsPercent := (float64(diff) / float64(heapAllocBefore)) * 100

	t.Logf("Heap Allocations (In-Memory Store): %.2f MB", float64(heapAllocBefore)/(1024*1024))
	t.Logf("Heap Allocations (BoltDB Store):    %.2f MB", float64(heapAllocAfter)/(1024*1024))
	t.Logf("RAM Savings Percentage:             %.2f%%", savingsPercent)

	if savingsPercent < 75 {
		t.Errorf("Expected heap RAM savings to be at least 75%%, but got %.2f%%", savingsPercent)
	}
}

func TestBoltThreadSafeStoreL1Cache(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "l1cache.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	scheme := setupTestScheme()
	indexers := Indexers{
		"by_value": func(obj interface{}) ([]string, error) {
			return []string{obj.(*TestObject).Value}, nil
		},
	}

	store, err := NewBoltThreadSafeStore(db, scheme, indexers, Indices{}, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	obj1 := &TestObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "obj1", Namespace: "default"},
		Value:      "val-a",
	}

	// 1. Write to L2 should automatically populate L1 (Update-in-Place)
	store.Add("default/obj1", obj1)

	// Verify that the object exists in L1 LRU cache directly!
	store.(*boltThreadSafeMap).lock.Lock()
	cachedVal, ok := store.(*boltThreadSafeMap).lruCache.Get("default/obj1")
	store.(*boltThreadSafeMap).lock.Unlock()

	if !ok {
		t.Fatal("Expected obj1 to be populated in L1 LRU cache automatically after Add()")
	}
	if cachedVal.(*TestObject).Value != "val-a" {
		t.Errorf("Expected value 'val-a' in cache, got %q", cachedVal.(*TestObject).Value)
	}

	// 2. Read (Get) should hit L1 cache
	// We close the DB file handle temporarily to PROVE that the read is answered entirely
	// from L1 cache without touching the BoltDB on-disk database!
	db.Close()

	item, exists := store.Get("default/obj1")
	if !exists {
		t.Fatal("Expected item to exist via L1 cache lookup")
	}
	if item.(*TestObject).Value != "val-a" {
		t.Errorf("Expected 'val-a' from L1 cache, got %q", item.(*TestObject).Value)
	}

	// Reopen DB for subsequent write operations
	db, err = bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to reopen db: %v", err)
	}
	// Re-assign the newly opened DB handle to the store
	store.(*boltThreadSafeMap).db = db

	// 3. Invalidation on Update (Update-in-Place)
	obj1.Value = "val-updated"
	store.Update("default/obj1", obj1)

	// Verify L1 contains the updated value
	store.(*boltThreadSafeMap).lock.Lock()
	cachedVal, ok = store.(*boltThreadSafeMap).lruCache.Get("default/obj1")
	store.(*boltThreadSafeMap).lock.Unlock()
	if !ok || cachedVal.(*TestObject).Value != "val-updated" {
		t.Errorf("Expected L1 cache to be synchronously updated to 'val-updated'")
	}

	// 4. Invalidation on Delete (Strict Eviction)
	store.Delete("default/obj1")

	// Verify L1 is evicted
	store.(*boltThreadSafeMap).lock.Lock()
	_, ok = store.(*boltThreadSafeMap).lruCache.Get("default/obj1")
	store.(*boltThreadSafeMap).lock.Unlock()
	if ok {
		t.Error("Expected L1 cache to be strictly evicted after Delete()")
	}
}
