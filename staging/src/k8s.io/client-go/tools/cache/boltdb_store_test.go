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
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBoltDBStoreNew(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	testKeyFunc := func(obj interface{}) (string, error) {
		return obj.(*TestObject).Value, nil
	}

	scheme := setupTestScheme()
	store, err := NewBoltDBStore(dbPath, scheme, testKeyFunc)
	if err != nil {
		t.Fatalf("Failed to create BoltDBStore: %v", err)
	}
	defer store.(interface{ Close() error }).Close()

	// Custom object that is registered in the scheme
	mkObj := func(id string, val string) *TestObject {
		return &TestObject{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "test/v1",
				Kind:       "TestObject",
			},
			Value: val,
		}
	}

	err = store.Add(mkObj("foo", "bar"))
	if err != nil {
		t.Fatalf("Failed to Add: %v", err)
	}

	item, ok, _ := store.Get(mkObj("foo", "bar"))
	if !ok {
		t.Fatal("Expected item to exist")
	}
	if item.(*TestObject).Value != "bar" {
		t.Errorf("Expected 'bar', got %q", item.(*TestObject).Value)
	}
}

func BenchmarkStoreAddBoltDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")

	benchKeyFunc := func(obj interface{}) (string, error) {
		return obj.(*TestObject).Value, nil
	}

	scheme := setupTestScheme()
	store, err := NewBoltDBStore(dbPath, scheme, benchKeyFunc)
	if err != nil {
		b.Fatalf("Failed to create BoltDBStore: %v", err)
	}
	defer store.(interface{ Close() error }).Close()

	obj := &TestObject{
		TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		Value:    "bar",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Add(obj)
	}
}

func BenchmarkStoreGetBoltDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")

	benchKeyFunc := func(obj interface{}) (string, error) {
		return obj.(*TestObject).Value, nil
	}

	scheme := setupTestScheme()
	store, err := NewBoltDBStore(dbPath, scheme, benchKeyFunc)
	if err != nil {
		b.Fatalf("Failed to create BoltDBStore: %v", err)
	}
	defer store.(interface{ Close() error }).Close()

	obj := &TestObject{
		TypeMeta: metav1.TypeMeta{APIVersion: "test/v1", Kind: "TestObject"},
		Value:    "bar",
	}
	_ = store.Add(obj)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = store.Get(obj)
	}
}
