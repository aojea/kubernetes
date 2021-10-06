// Based on golang tests
// ref: https://github.com/golang/net/blob/master/nettest/conntest.go

package dns

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

var (
	aLongTimeAgo = time.Unix(233431200, 0)
	neverTimeout = time.Time{}
)

// TestBasicIO tests that the data sent on c is properly received back on c.
func TestPacketBasicIO(t *testing.T) {
	ph := func(b []byte) []byte {
		if bytes.Equal(b, []byte{0x00}) {
			return nil
		}
		return b
	}
	c := PacketHairpin(ph)
	want := make([]byte, 1<<20)
	rand.New(rand.NewSource(0)).Read(want)
	dataCh := make(chan []byte)
	go func() {
		rd := bytes.NewReader(want)
		if err := chunkedCopy(c, rd); err != nil {
			t.Errorf("unexpected buffer Write error: %v", err)
		}
		// we can't close the connection directly until the reader has finished
		// so we indicate the server to close it after he has processed all the packets
		c.Write([]byte{0x00})
	}()
	go func() {
		wr := new(bytes.Buffer)
		if err := chunkedCopy(wr, c); err != nil {
			t.Errorf("unexpected buffer Read error: %v", err)
		}
		dataCh <- wr.Bytes()
	}()
	if got := <-dataCh; !bytes.Equal(got, want) {
		t.Errorf("transmitted data differs, got: %d bytes want: %d bytes", len(got), len(want))
	}
}

// testPingPong tests that the two endpoints can synchronously send data to
// each other in a typical request-response pattern.
func TestPacketPingPong(t *testing.T) {
	var prev uint64
	ph := func(buf []byte) []byte {
		v := binary.LittleEndian.Uint64(buf)
		binary.LittleEndian.PutUint64(buf, v+1)
		if prev != 0 && prev+2 != v {
			t.Errorf("mismatching value: got %d, want %d", v, prev+2)
		}
		prev = v
		// stop processing once we get 1000 pings
		if v == 1000 {
			return nil
		}
		return buf
	}
	c := PacketHairpin(ph)
	var wg sync.WaitGroup
	defer wg.Wait()
	pingPonger := func(c net.Conn) {
		defer wg.Done()
		buf := make([]byte, 8)
		var prev uint64
		for {
			if _, err := io.ReadFull(c, buf); err != nil {
				if err == io.EOF {
					break
				}
				t.Errorf("unexpected Read error: %v", err)
			}
			v := binary.LittleEndian.Uint64(buf)
			binary.LittleEndian.PutUint64(buf, v+1)
			if prev != 0 && prev+2 != v {
				t.Errorf("mismatching value: got %d, want %d", v, prev+2)
			}
			prev = v
			if v == 1001 {
				break
			}
			if _, err := c.Write(buf); err != nil {
				t.Logf("unexpected Write error: %v", err)
			}
		}
		if err := c.Close(); err != nil {
			t.Errorf("unexpected Close error: %v", err)
		}
	}
	wg.Add(1)
	go pingPonger(c)
	// Start off the chain reaction.
	if _, err := c.Write(make([]byte, 8)); err != nil {
		t.Errorf("unexpected c.Write error: %v", err)
	}
}

// TestRacyRead tests that it is safe to mutate the input Read buffer
// immediately after cancelation has occurred.
func TestPacketRacyRead(t *testing.T) {
	c := PacketHairpin(nil)
	go chunkedCopy(c, rand.New(rand.NewSource(0)))
	var wg sync.WaitGroup
	defer wg.Wait()
	c.SetReadDeadline(time.Now().Add(time.Millisecond))
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b1 := make([]byte, 1024)
			b2 := make([]byte, 1024)
			for j := 0; j < 100; j++ {
				_, err := c.Read(b1)
				copy(b1, b2) // Mutate b1 to trigger potential race
				if err != nil {
					checkForTimeoutError(t, err)
					c.SetReadDeadline(time.Now().Add(time.Millisecond))
				}
			}
		}()
	}
}

// TestRacyWrite tests that it is safe to mutate the input Write buffer
// immediately after cancelation has occurred.
func TestPacketRacyWrite(t *testing.T) {
	c := PacketHairpin(nil)
	go chunkedCopy(ioutil.Discard, c)
	var wg sync.WaitGroup
	defer wg.Wait()
	c.SetWriteDeadline(time.Now().Add(time.Millisecond))
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b1 := make([]byte, 1024)
			b2 := make([]byte, 1024)
			for j := 0; j < 100; j++ {
				_, err := c.Write(b1)
				copy(b1, b2) // Mutate b1 to trigger potential race
				if err != nil {
					checkForTimeoutError(t, err)
					c.SetWriteDeadline(time.Now().Add(time.Millisecond))
				}
			}
		}()
	}
}

// testReadTimeout tests that Read timeouts do not affect Write.
func TestPacketReadTimeout(t *testing.T) {
	c := PacketHairpin(nil)
	go chunkedCopy(ioutil.Discard, c)
	c.SetReadDeadline(aLongTimeAgo)
	_, err := c.Read(make([]byte, 1024))
	checkForTimeoutError(t, err)
	if _, err := c.Write(make([]byte, 1024)); err != nil {
		t.Errorf("unexpected Write error: %v", err)
	}
}

// testPastTimeout tests that a deadline set in the past immediately times out
// Read and Write requests.
func TestPacketPastTimeout(t *testing.T) {
	c := PacketHairpin(nil)
	go chunkedCopy(c, c)
	testRoundtrip(t, c)
	c.SetDeadline(aLongTimeAgo)
	n, err := c.Write(make([]byte, 1024))
	if n != 0 {
		t.Errorf("unexpected Write count: got %d, want 0", n)
	}
	checkForTimeoutError(t, err)
	n, err = c.Read(make([]byte, 1024))
	if n != 0 {
		t.Errorf("unexpected Read count: got %d, want 0", n)
	}
	checkForTimeoutError(t, err)
	testRoundtrip(t, c)
}

// testPresentTimeout tests that a past deadline set while there are pending
// Read and Write operations immediately times out those operations.
func TestPacketPresentTimeout(t *testing.T) {
	ph := func(b []byte) []byte {
		// block until deadline is set
		time.Sleep(300 * time.Millisecond)
		return b
	}
	c := Hairpin(ph)
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(3)
	deadlineSet := make(chan bool, 1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		deadlineSet <- true
		c.SetReadDeadline(aLongTimeAgo)
		c.SetWriteDeadline(aLongTimeAgo)
	}()
	go func() {
		defer wg.Done()
		n, err := c.Read(make([]byte, 1024))
		if n != 0 {
			t.Errorf("unexpected Read count: got %d, want 0", n)
		}
		checkForTimeoutError(t, err)
		if len(deadlineSet) == 0 {
			t.Error("Read timed out before deadline is set")
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		for err == nil {
			_, err = c.Write(make([]byte, 1024))
		}
		checkForTimeoutError(t, err)
		if len(deadlineSet) == 0 {
			t.Error("Write timed out before deadline is set")
		}
	}()
}

// testFutureTimeout tests that a future deadline will eventually time out
// Read and Write operations.
func TestPacketFutureTimeout(t *testing.T) {
	ph := func(b []byte) []byte {
		// block until deadline is set
		time.Sleep(300 * time.Millisecond)
		return b
	}
	c := PacketHairpin(ph)
	var wg sync.WaitGroup
	wg.Add(2)
	c.SetDeadline(time.Now().Add(100 * time.Millisecond))
	go func() {
		defer wg.Done()
		_, err := c.Read(make([]byte, 1024))
		checkForTimeoutError(t, err)
	}()
	go func() {
		defer wg.Done()
		var err error
		for err == nil {
			_, err = c.Write(make([]byte, 1024))
		}
		checkForTimeoutError(t, err)
	}()
	wg.Wait()
	go chunkedCopy(c, c)
	resyncConn(t, c)
	testRoundtrip(t, c)
}

// testCloseTimeout tests that calling Close immediately times out pending
// Read and Write operations.
func TestPacketCloseTimeout(t *testing.T) {
	ph := func(b []byte) []byte {
		return b
	}
	c := PacketHairpin(ph)
	go chunkedCopy(c, c)
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(3)
	// Test for cancelation upon connection closure.
	c.SetDeadline(neverTimeout)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		c.Close()
	}()
	go func() {
		defer wg.Done()
		var err error
		buf := make([]byte, 1024)
		for err == nil {
			_, err = c.Read(buf)
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		buf := make([]byte, 1024)
		for err == nil {
			_, err = c.Write(buf)
		}
	}()
}

// testConcurrentMethods tests that the methods of net.Conn can safely
// be called concurrently.
func TestPacketConcurrentMethods(t *testing.T) {
	ph := func(b []byte) []byte {
		return b
	}
	c := PacketHairpin(ph)
	if runtime.GOOS == "plan9" {
		t.Skip("skipping on plan9; see https://golang.org/issue/20489")
	}
	go chunkedCopy(c, c)
	// The results of the calls may be nonsensical, but this should
	// not trigger a race detector warning.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(7)
		go func() {
			defer wg.Done()
			c.Read(make([]byte, 1024))
		}()
		go func() {
			defer wg.Done()
			c.Write(make([]byte, 1024))
		}()
		go func() {
			defer wg.Done()
			c.SetDeadline(time.Now().Add(10 * time.Millisecond))
		}()
		go func() {
			defer wg.Done()
			c.SetReadDeadline(aLongTimeAgo)
		}()
		go func() {
			defer wg.Done()
			c.SetWriteDeadline(aLongTimeAgo)
		}()
		go func() {
			defer wg.Done()
			c.LocalAddr()
		}()
		go func() {
			defer wg.Done()
			c.RemoteAddr()
		}()
	}
	wg.Wait() // At worst, the deadline is set 10ms into the future
	resyncConn(t, c)
	testRoundtrip(t, c)
}
