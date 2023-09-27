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
	"context"
	"fmt"
	"io"

	"golang.org/x/time/rate"
)

// NewReader restricts all Read operations on r to limit bytes per second.
// If there are more bytes than the allowed limit, it blocks until the limit
// is met.
func NewReader(r io.Reader, bps int64) io.ReadCloser {
	limiter := rate.NewLimiter(rate.Limit(bps), 0)
	// initialize limiter
	limiter.Tokens()
	ctx, cancel := context.WithCancel(context.Background())
	return &reader{
		r:        r,
		l:        limiter,
		ctx:      ctx,
		cancelFn: cancel,
	}
}

// reader implements io.ReadCloser with a restriction on the rate of data
// transfer.
type reader struct {
	r        io.Reader       // Data source
	l        *rate.Limiter   // Token bucket rate limiter
	ctx      context.Context // used for Close
	cancelFn context.CancelFunc
}

// Read reads up to len(p) bytes into p without exceeding the current transfer
// rate limit.
func (r *reader) Read(p []byte) (int, error) {
	n := len(p)
	available := int(r.l.Tokens())
	fmt.Println("Available", available, "n", n)
	// just read as much as we can without exceeding the rate limit
	if available > 0 && available < n {
		n = int(available)
	}
	// read all of them in one shot
	r.l.SetBurst(n)
	err := r.l.WaitN(r.ctx, n)
	if err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func (r *reader) Close() error {
	r.cancelFn()
	if c, ok := r.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// NewWriter restricts all Write operations on r to limit bytes per second.
// If there are more bytes than the allowed limit, it blocks until the limit
// is met.
func NewWriter(w io.Writer, bps int64) io.WriteCloser {
	limiter := rate.NewLimiter(rate.Limit(bps), 0)
	limiter.Tokens()
	ctx, cancel := context.WithCancel(context.Background())
	return &writer{
		w:        w,
		l:        limiter,
		ctx:      ctx,
		cancelFn: cancel,
	}
}

// writer implements io.WriteCloser with a restriction on the rate of data
// transfer.
type writer struct {
	w        io.Writer       // Data source
	l        *rate.Limiter   // Token bucket rate limiter
	ctx      context.Context // used for Close
	cancelFn context.CancelFunc
}

// Write writes up to len(p) bytes into p without exceeding the current transfer
// rate limit. It returns (0, nil) immediately if r is non-blocking and no new
// bytes can be read at this time.
func (w *writer) Write(p []byte) (int, error) {
	n := len(p)
	available := int(w.l.Tokens())
	// just write as much as we can without exceeding the rate limit
	if available > 0 && available < n {
		n = available
	}
	// write all of them in one shot
	w.l.SetBurst(n)
	err := w.l.WaitN(w.ctx, n)
	if err != nil {
		return 0, err
	}
	return w.w.Write(p[:n])
}

func (w *writer) Close() error {
	w.cancelFn()
	if c, ok := w.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
