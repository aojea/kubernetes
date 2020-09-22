/*
Copyright 2018 The Kubernetes Authors.
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

package badtcpseqserver

import (
	"net"
	"reflect"
	"testing"
)

func TestTCPEncode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc string
		tcp  []byte
		src  net.IP
		dest net.IP
		data []byte
		want []byte
	}{
		{
			desc: "localhost IPv4",
			// Transmission Control Protocol, Src Port: 55912, Dst Port: 2379, Seq: 1, Ack: 1, Len: 0
			tcp: []byte{
				0xda, 0x68, 0x09, 0x4b, 0xe2,
				0x8c, 0xad, 0xa0, 0x64, 0xf1,
				0xf0, 0x68, 0x50, 0x10, 0x5f,
				0xed, 0x7a, 0x05, 0x00, 0x00,
			},
			src:  net.ParseIP("127.0.0.1"),
			dest: net.ParseIP("127.0.0.1"),
			data: []byte("boom"),
			want: []byte{
				0xda, 0x68, 0x09, 0x4b, 0xe2,
				0x8c, 0xad, 0xa0, 0x64, 0xf1,
				0xf0, 0x68, 0x80, 0x10, 0x5f,
				0xed, 0x7a, 0x05, 0x00, 0x00,
			},
		},
		{
			desc: "localhost IPv6",
			// Transmission Control Protocol, Src Port: 55912, Dst Port: 2379, Seq: 1, Ack: 1, Len: 0
			tcp: []byte{
				0xda, 0x68, 0x09, 0x4b, 0xe2,
				0x8c, 0xad, 0xa0, 0x64, 0xf1,
				0xf0, 0x68, 0x50, 0x10, 0x5f,
				0xed, 0x7a, 0x05, 0x00, 0x00,
			},
			src:  net.ParseIP("::1"),
			dest: net.ParseIP("::1"),
			data: []byte("boom"),
			want: []byte{
				0xda, 0x68, 0x09, 0x4b, 0xe2,
				0x8c, 0xad, 0xa0, 0x64, 0xf1,
				0xf0, 0x68, 0x80, 0x10, 0x5f,
				0xed, 0x7a, 0x05, 0x00, 0x00,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			pkt := make([]byte, tcpHeaderSize)
			tcpPkt := &tcpPacket{}
			_, err := tcpPkt.decode(tc.tcp)
			if err != nil {
				t.Errorf("tcp packet parse error: %v", err)
			}

			// create packet with new data
			pkt = tcpPkt.encode(tc.src, tc.dest, tc.data)
			if !reflect.DeepEqual(tc.want, pkt) {
				t.Errorf("tcp.Encode() = %v, want %v; tcp = %+v, flags = %v", pkt, tc.want, tcpPkt, tcpPkt.FlagString())
			}
			if string(pkt[tcpPkt.DataOffset():]) != string(tc.data) {
				t.Errorf("tcp.Encode() want %v data = %v", string(tc.data), string(pkt[tcpPkt.DataOffset():]))
			}

		})
	}
}

func TestTCPChecksummer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc string
		data [][]byte
		want uint16
	}{
		{
			desc: "empty",
			data: [][]byte{},
			want: 0xffff,
		},
		{
			desc: "1 byte",
			data: [][]byte{[]byte{0x55}},
			want: 0xffaa,
		},
		{
			desc: "2 bytes",
			data: [][]byte{[]byte{0x55, 0x88}},
			want: 0x77aa,
		},
		{
			desc: "3 bytes",
			data: [][]byte{[]byte{0x55, 0x88, 0x99}},
			want: 0x7711,
		},
		{
			desc: "3 bytes / 1 at a time",
			data: [][]byte{[]byte{0x55}, []byte{0x88}, []byte{0x99}},
			want: 0x7711,
		},
		{
			desc: "3 bytes / 2 1",
			data: [][]byte{[]byte{0x55, 0x88}, []byte{0x99}},
			want: 0x7711,
		},
		{
			desc: "simple packet",
			data: [][]byte{
				[]byte{
					0x7f, 0x00, 0x00, 0x01, // 127.0.0.1
					0x7f, 0x00, 0x00, 0x01, // 127.0.0.1
					0x00, 0x06, // TCP proto 6
					0x00, 0x14, // Size = 20 bytes
					0x00, 0x50, 0x1f, 0x90, 0x00,
					0x00, 0x00, 0x01, 0x00, 0x00,
					0x00, 0x00, 0x50, 0x02, 0x00,
					0x00, 0x00, 0x00, 0x00, 0x00,
				},
			},
			want: 0xff91,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			c := &tcpChecksummer{}
			for _, b := range tc.data {
				c.add(b)
			}
			got := c.finalize()
			if got != tc.want {
				t.Errorf("c.finalize() = %x, want %x; bytes: %v", got, tc.want, tc.data)
			}
		})
	}
}
