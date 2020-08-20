/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"
	"log"
	"net"
	"time"

	"github.com/spf13/cobra"
)

// CmdBadTCPSeqServer is used by agnhost Cobra.
var CmdBadTCPSeqServer = &cobra.Command{
	Use:   "bad-tcp-seq-server",
	Short: "Starts a TCP server and injects TCP packets with wrong sequence numbers",
	Long:  "Starts a TCP server and injects TCP packets with wrong sequence numbers on the given --port. It panics if a RST associated to the connection is received.",
	Args:  cobra.MaximumNArgs(0),
	Run:   main,
}

var (
	port int
)

func init() {
	CmdBadTCPSeqServer.Flags().IntVar(&port, "port", 9000, "Port number.")
}

func main(cmd *cobra.Command, args []string) {
	// get local IP addresses
	ipv4, ipv6 := getIPs()
	if ipv4 == nil && ipv6 == nil {
		panic("Not valid IP found")
	}

	if ipv4 != nil {
		log.Printf("external ip: %v", ipv4.String())
		// listen TCP packets to inject the out of order TCP packets
		go func() {
			if err := probe(ipv4.String()); err != nil {
				panic(err.Error())
			}
		}()
	}

	if ipv6 != nil {
		log.Printf("external ip: %v", ipv6.String())
		// listen TCP packets to inject the out of order TCP packets
		go func() {
			if err := probe(ipv6.String()); err != nil {
				panic(err.Error())
			}
		}()
	}

	log.Printf("listen on %v:%d", "0.0.0.0", port)

	// open a server listening to establish the TCP connections
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go func(conn net.Conn) {
			// Close the connection after 10 secs
			time.Sleep(10 * time.Second)
			conn.Close()
		}(conn)
	}
}

func probe(ip string) error {
	log.Printf("probing %v", ip)

	ipAddr, err := net.ResolveIPAddr("ip:tcp", ip)
	if err != nil {
		return err
	}

	conn, err := net.ListenIP("ip:tcp", ipAddr)
	if err != nil {
		return err
	}

	// track the pending packets
	pending := make(map[string]uint32)

	var buffer [4096]byte
	for {
		n, addr, err := conn.ReadFrom(buffer[:])
		if err != nil {
			log.Printf("conn.ReadFrom() error: %v", err)
			continue
		}

		// decode the TCP packet
		pkt := &tcpPacket{}
		data, err := pkt.decode(buffer[:n])
		if err != nil {
			log.Printf("tcp packet parse error: %v", err)
			continue
		}

		// skip unrelated packets
		if pkt.DestPort != uint16(port) {
			continue
		}

		log.Printf("tcp packet: %+v, flag: %v, data: %v, addr: %v", pkt, pkt.FlagString(), data, addr)

		// if is not the first packet increment the sequence
		if pkt.Flags&SYN != 0 {
			pending[addr.String()] = pkt.Seq + 1
			continue
		}

		// we should not receive a RST because
		// invalid conntrack packets must be dropped
		if pkt.Flags&RST != 0 {
			return fmt.Errorf("RST received")
		}

		// if we are here it means the connection was established
		if pkt.Flags&ACK != 0 {
			if seq, ok := pending[addr.String()]; ok {
				log.Println("connection established")
				delete(pending, addr.String())

				badPkt := &tcpPacket{
					SrcPort:    pkt.DestPort,
					DestPort:   pkt.SrcPort,
					Ack:        seq,
					Seq:        pkt.Ack - 100000,      // Bad: seq out-of-window
					Flags:      (5 << 12) | PSH | ACK, // Offset and Flags  oooo000F FFFFFFFF (o:offset, F:flags)
					WindowSize: pkt.WindowSize,
				}

				data := []byte("boom!!!")
				remoteIP := net.ParseIP(addr.String())
				localIP := net.ParseIP(conn.LocalAddr().String())
				_, err := conn.WriteTo(badPkt.encode(localIP, remoteIP, data[:]), addr)
				if err != nil {
					log.Printf("conn.WriteTo() error: %v", err)
				}
				log.Printf("boom packet: %+v, flag: %v, data: %v, addr: %v", badPkt, badPkt.FlagString(), data, addr)

			}
		}
	}
}

// getIPs return one IPv4 and IPv6 global ip address
func getIPs() (ipv4, ipv6 net.IP) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, nil
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, nil
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			// if is a Global Unicast address check the IP family
			// and return if we got one per family
			if ip.IsGlobalUnicast() {
				if ip.To4() == nil && ipv6 == nil {
					ipv6 = ip
				} else if ip.To4() != nil && ipv4 == nil {
					ipv4 = ip
				}
				if ipv4 != nil && ipv6 != nil {
					return
				}
			}
		}
	}
	return
}
