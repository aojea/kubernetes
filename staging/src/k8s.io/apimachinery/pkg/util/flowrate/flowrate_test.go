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

// Package flowrate implements a token based rate limited Reader and Writer
package flowrate

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestReader(t *testing.T) {
	dataSize := 100
	rate := int64(100) // bytes per second
	randomData := make([]byte, dataSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}

	b := make([]byte, dataSize)
	r := NewReader(bytes.NewReader(randomData), rate)
	start := time.Now()

	if n, err := r.Read(b); n != 100 || err != nil {
		t.Fatalf("r.Read(b) expected 100 (<nil>); got %v (%v)", n, err)
	}
	if rt := time.Since(start); rt < 1*time.Second || rt > 1100*time.Millisecond {
		t.Fatalf("r.Read(b) not rate limited (%v)", rt)
	}

	if !bytes.Equal(b, randomData) {
		t.Errorf("r.Read() input doesn't match output")
	}
}

func TestReaderLimited(t *testing.T) {
	dataSize := 100
	rate := int64(10) // bytes per second
	randomData := make([]byte, dataSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}

	b := make([]byte, dataSize)
	r := NewReader(bytes.NewReader(randomData), rate)
	start := time.Now()
	time.Sleep(1 * time.Second)
	// at 10 bytes per second we should have only 10 bytes after one second :)
	if n, err := r.Read(b); n != 10 || err != nil {
		t.Fatalf("r.Read(b) expected 10 (<nil>); got %d (%v)", n, err)
	}
	if rt := time.Since(start); rt < 1*time.Second || rt > 1100*time.Millisecond {
		t.Fatalf("r.Read(b) not rate limited (%v)", rt)
	}

	if !bytes.Equal(b[:10], randomData[:10]) {
		t.Errorf("r.Read() input doesn't match output")
	}
}

func TestReaderBlocksUntilClose(t *testing.T) {
	dataSize := 100
	rate := int64(1) // bytes per second
	randomData := make([]byte, dataSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}

	// Try to read 10 bytes at 1 bps it should hang for 10 seconds
	b := make([]byte, 10)
	n := 0
	r := NewReader(bytes.NewReader(randomData), rate)

	// 10 bytes at 1 byte per second it should take 10 seconds
	closeCh := make(chan struct{})
	go func() {
		defer close(closeCh)
		var err error
		n, err = r.Read(b)
		if err == nil {
			t.Errorf("expected cancel error reading")
		}
	}()
	select {
	case <-closeCh:
		t.Fatalf("Read should be blocking")
	case <-time.After(1 * time.Second):
		r.Close()
	}
	<-closeCh
	if !bytes.Equal(b[:n], randomData[:n]) {
		t.Errorf("r.Read() input doesn't match output")
	}
}

func TestWriter(t *testing.T) {
	dataSize := 100
	rate := int64(100) // bytes per second
	randomData := make([]byte, dataSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}

	b := new(bytes.Buffer)
	// read 100 bytes at 10 bps
	w := NewWriter(b, rate)
	start := time.Now()

	if n, err := w.Write(randomData); n != 100 || b.Len() != 100 || err != nil {
		t.Fatalf("w.Write(b) expected 100 (<nil>); got %v (%v)", n, err)
	}
	if rt := time.Since(start); rt < 1*time.Second || rt > 1100*time.Millisecond {
		t.Fatalf("w.Write(b) not rate limited (%v)", rt)
	}

	if !bytes.Equal(b.Bytes(), randomData) {
		t.Errorf("w.Write() input doesn't match output: %v", cmp.Diff(b, randomData))
	}
}
