/*
Copyright 2016 The Kubernetes Authors.

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

package cidrset

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"math/bits"
	"net"
	"sync"
)

// Interface manages the allocation of items out of a range. Interface
// should be threadsafe.
type Interface interface {
	Occupy(*net.IPNet) error
	AllocateNext() (*net.IPNet, error)
	Release(*net.IPNet) error
}

// CidrSet manages a set of CIDR ranges from which blocks of IPs can
// be allocated from.
type CidrSet struct {
	sync.Mutex
	clusterCIDR     *net.IPNet
	clusterIP       net.IP
	clusterMaskSize int
	maxCIDRs        int
	nextCandidate   int
	used            big.Int
	subNetMaskSize  int
}

var _ Interface = &CidrSet{}

const (
	// The subnet mask size cannot be greater than 16 more than the cluster mask size
	// TODO: https://github.com/kubernetes/kubernetes/issues/44918
	// clusterSubnetMaxDiff limited to 16 due to the uncompressed bitmap
	// Due to this limitation the subnet mask for IPv6 cluster cidr needs to be >= 48
	// as default mask size for IPv6 is 64.
	clusterSubnetMaxDiff = 16
	// halfIPv6Len is the half of the IPv6 length
	halfIPv6Len = net.IPv6len / 2
	// Using the map CIDRset we can have until 2^64 ranges
	clusterMapSubnetMaxDiff = 64
)

var (
	// ErrCIDRRangeNoCIDRsRemaining occurs when there is no more space
	// to allocate CIDR ranges.
	ErrCIDRRangeNoCIDRsRemaining = errors.New(
		"CIDR allocation failed; there are no remaining CIDRs left to allocate in the accepted range")
	// ErrCIDRSetSubNetTooBig occurs when the subnet mask size is too
	// big compared to the CIDR mask size.
	ErrCIDRSetSubNetTooBig = errors.New(
		"New CIDR set failed; the node CIDR size is too big")
)

// NewCIDRSet creates a new CidrSet.
func NewCIDRSet(clusterCIDR *net.IPNet, subNetMaskSize int) (*CidrSet, error) {
	clusterMask := clusterCIDR.Mask
	clusterMaskSize, _ := clusterMask.Size()

	var maxCIDRs int
	if (clusterCIDR.IP.To4() == nil) && (subNetMaskSize-clusterMaskSize > clusterSubnetMaxDiff) {
		return nil, ErrCIDRSetSubNetTooBig
	}
	maxCIDRs = 1 << uint32(subNetMaskSize-clusterMaskSize)
	return &CidrSet{
		clusterCIDR:     clusterCIDR,
		clusterIP:       clusterCIDR.IP,
		clusterMaskSize: clusterMaskSize,
		maxCIDRs:        maxCIDRs,
		subNetMaskSize:  subNetMaskSize,
	}, nil
}

func (s *CidrSet) indexToCIDRBlock(index int) *net.IPNet {
	var ip []byte
	var mask int
	switch /*v4 or v6*/ {
	case s.clusterIP.To4() != nil:
		{
			j := uint32(index) << uint32(32-s.subNetMaskSize)
			ipInt := (binary.BigEndian.Uint32(s.clusterIP)) | j
			ip = make([]byte, 4)
			binary.BigEndian.PutUint32(ip, ipInt)
			mask = 32

		}
	case s.clusterIP.To16() != nil:
		{
			// leftClusterIP      |     rightClusterIP
			// 2001:0DB8:1234:0000:0000:0000:0000:0000
			const v6NBits = 128
			const halfV6NBits = v6NBits / 2
			leftClusterIP := binary.BigEndian.Uint64(s.clusterIP[:halfIPv6Len])
			rightClusterIP := binary.BigEndian.Uint64(s.clusterIP[halfIPv6Len:])

			leftIP, rightIP := make([]byte, halfIPv6Len), make([]byte, halfIPv6Len)

			if s.subNetMaskSize <= halfV6NBits {
				// We only care about left side IP
				leftClusterIP |= uint64(index) << uint(halfV6NBits-s.subNetMaskSize)
			} else {
				if s.clusterMaskSize < halfV6NBits {
					// see how many bits are needed to reach the left side
					btl := uint(s.subNetMaskSize - halfV6NBits)
					indexMaxBit := uint(64 - bits.LeadingZeros64(uint64(index)))
					if indexMaxBit > btl {
						leftClusterIP |= uint64(index) >> btl
					}
				}
				// the right side will be calculated the same way either the
				// subNetMaskSize affects both left and right sides
				rightClusterIP |= uint64(index) << uint(v6NBits-s.subNetMaskSize)
			}
			binary.BigEndian.PutUint64(leftIP, leftClusterIP)
			binary.BigEndian.PutUint64(rightIP, rightClusterIP)

			ip = append(leftIP, rightIP...)
			mask = 128
		}
	}
	return &net.IPNet{
		IP:   ip,
		Mask: net.CIDRMask(s.subNetMaskSize, mask),
	}
}

// AllocateNext allocates the next free CIDR range. This will set the range
// as occupied and return the allocated range.
func (s *CidrSet) AllocateNext() (*net.IPNet, error) {
	s.Lock()
	defer s.Unlock()

	nextUnused := -1
	for i := 0; i < s.maxCIDRs; i++ {
		candidate := (i + s.nextCandidate) % s.maxCIDRs
		if s.used.Bit(candidate) == 0 {
			nextUnused = candidate
			break
		}
	}
	if nextUnused == -1 {
		return nil, ErrCIDRRangeNoCIDRsRemaining
	}
	s.nextCandidate = (nextUnused + 1) % s.maxCIDRs

	s.used.SetBit(&s.used, nextUnused, 1)

	return s.indexToCIDRBlock(nextUnused), nil
}

func (s *CidrSet) getBeginingAndEndIndices(cidr *net.IPNet) (begin, end int, err error) {
	if cidr == nil {
		return -1, -1, fmt.Errorf("error getting indices for cluster cidr %v, cidr is nil", s.clusterCIDR)
	}
	begin, end = 0, s.maxCIDRs-1
	cidrMask := cidr.Mask
	maskSize, _ := cidrMask.Size()
	var ipSize int

	if !s.clusterCIDR.Contains(cidr.IP.Mask(s.clusterCIDR.Mask)) && !cidr.Contains(s.clusterCIDR.IP.Mask(cidr.Mask)) {
		return -1, -1, fmt.Errorf("cidr %v is out the range of cluster cidr %v", cidr, s.clusterCIDR)
	}

	if s.clusterMaskSize < maskSize {

		ipSize = net.IPv4len
		if cidr.IP.To4() == nil {
			ipSize = net.IPv6len
		}
		subNetMask := net.CIDRMask(s.subNetMaskSize, ipSize*8)
		begin, err = s.getIndexForCIDR(&net.IPNet{
			IP:   cidr.IP.Mask(subNetMask),
			Mask: subNetMask,
		})
		if err != nil {
			return -1, -1, err
		}
		ip := make([]byte, ipSize)
		if cidr.IP.To4() != nil {
			ipInt := binary.BigEndian.Uint32(cidr.IP) | (^binary.BigEndian.Uint32(cidr.Mask))
			binary.BigEndian.PutUint32(ip, ipInt)
		} else {
			// ipIntLeft          |         ipIntRight
			// 2001:0DB8:1234:0000:0000:0000:0000:0000
			ipIntLeft := binary.BigEndian.Uint64(cidr.IP[:net.IPv6len/2]) | (^binary.BigEndian.Uint64(cidr.Mask[:net.IPv6len/2]))
			ipIntRight := binary.BigEndian.Uint64(cidr.IP[net.IPv6len/2:]) | (^binary.BigEndian.Uint64(cidr.Mask[net.IPv6len/2:]))
			binary.BigEndian.PutUint64(ip[:net.IPv6len/2], ipIntLeft)
			binary.BigEndian.PutUint64(ip[net.IPv6len/2:], ipIntRight)
		}
		end, err = s.getIndexForCIDR(&net.IPNet{
			IP:   net.IP(ip).Mask(subNetMask),
			Mask: subNetMask,
		})
		if err != nil {
			return -1, -1, err
		}
	}
	return begin, end, nil
}

// Release releases the given CIDR range.
func (s *CidrSet) Release(cidr *net.IPNet) error {
	begin, end, err := s.getBeginingAndEndIndices(cidr)
	if err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	for i := begin; i <= end; i++ {
		s.used.SetBit(&s.used, i, 0)
	}
	return nil
}

// Occupy marks the given CIDR range as used. Occupy does not check if the CIDR
// range was previously used.
func (s *CidrSet) Occupy(cidr *net.IPNet) (err error) {
	begin, end, err := s.getBeginingAndEndIndices(cidr)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()
	for i := begin; i <= end; i++ {
		s.used.SetBit(&s.used, i, 1)
	}

	return nil
}

func (s *CidrSet) getIndexForCIDR(cidr *net.IPNet) (int, error) {
	return s.getIndexForIP(cidr.IP)
}

func (s *CidrSet) getIndexForIP(ip net.IP) (int, error) {
	if ip.To4() != nil {
		cidrIndex := (binary.BigEndian.Uint32(s.clusterIP) ^ binary.BigEndian.Uint32(ip.To4())) >> uint32(32-s.subNetMaskSize)
		if cidrIndex >= uint32(s.maxCIDRs) {
			return 0, fmt.Errorf("CIDR: %v/%v is out of the range of CIDR allocator", ip, s.subNetMaskSize)
		}
		return int(cidrIndex), nil
	}
	if ip.To16() != nil {
		bigIP := big.NewInt(0).SetBytes(s.clusterIP)
		bigIP = bigIP.Xor(bigIP, big.NewInt(0).SetBytes(ip))
		cidrIndexBig := bigIP.Rsh(bigIP, uint(net.IPv6len*8-s.subNetMaskSize))
		cidrIndex := cidrIndexBig.Uint64()
		if cidrIndex >= uint64(s.maxCIDRs) {
			return 0, fmt.Errorf("CIDR: %v/%v is out of the range of CIDR allocator", ip, s.subNetMaskSize)
		}
		return int(cidrIndex), nil
	}

	return 0, fmt.Errorf("invalid IP: %v", ip)
}

// Map manages a set of CIDR ranges from which blocks of IPs can
// be allocated from.
type Map struct {
	sync.Mutex
	clusterCIDR   *net.IPNet
	nodeMask      net.IPMask
	clusterIP     net.IP
	maxCIDRs      uint64
	nextCandidate uint64
	used          map[uint64]bool
}

var _ Interface = &Map{}

// NewMapCIDRSet creates a new CidrSet based on a map[uint64]bool.
func NewMapCIDRSet(clusterCIDR *net.IPNet, subNetMaskSize int) (*Map, error) {
	clusterMask := clusterCIDR.Mask
	clusterMaskSize, clusterMaskLen := clusterMask.Size()
	// Normalize the cluster IP
	clusterCIDR.IP.Mask(clusterCIDR.Mask)
	// Find the maximum number
	var maxCIDRs uint64
	if (clusterCIDR.IP.To4() == nil) && (subNetMaskSize-clusterMaskSize >= clusterMapSubnetMaxDiff) {
		return nil, ErrCIDRSetSubNetTooBig
	}
	maxCIDRs = 1 << uint64(subNetMaskSize-clusterMaskSize)
	return &Map{
		clusterCIDR: clusterCIDR,
		maxCIDRs:    maxCIDRs,
		nodeMask:    net.CIDRMask(subNetMaskSize, clusterMaskLen),
		used:        make(map[uint64]bool),
	}, nil
}

// Occupy marks the given CIDR range as used.
func (m *Map) Occupy(cidr *net.IPNet) error {
	m.Lock()
	defer m.Unlock()
	// validate that CIDR belongs to the Cluster CIDR ranges
	if err := m.validate(cidr); err != nil {
		return err
	}
	// obtain the IP key
	ipKey := m.calculateIPOffset(cidr.IP)

	if _, ok := m.used[ipKey]; ok {
		return fmt.Errorf("CIDR %v already in use", cidr)
	}
	m.used[ipKey] = true
	m.nextCandidate = (m.nextCandidate + 1) % m.maxCIDRs
	return nil
}

// Release marks the given CIDR range as free.
func (m *Map) Release(cidr *net.IPNet) error {
	m.Lock()
	defer m.Unlock()
	// validate that CIDR belongs to the Cluster CIDR ranges
	if err := m.validate(cidr); err != nil {
		return err
	}
	// obtain the IP key
	ipKey := m.calculateIPOffset(cidr.IP.Mask(m.nodeMask))

	if _, ok := m.used[ipKey]; ok {
		delete(m.used, ipKey)
		return nil
	}
	return fmt.Errorf("CIDR %v was already released", cidr)
}

// AllocateNext allocates a free CIDR range and returns it.
func (m *Map) AllocateNext() (*net.IPNet, error) {
	m.Lock()
	defer m.Unlock()
	if uint64(len(m.used)) >= m.maxCIDRs {
		return nil, ErrCIDRRangeNoCIDRsRemaining
	}

	var i uint64
	for i = 0; i < m.maxCIDRs; i++ {
		candidate := (i + m.nextCandidate) % m.maxCIDRs
		if _, ok := m.used[candidate]; !ok {
			m.used[candidate] = true
			m.nextCandidate = (candidate + 1) % m.maxCIDRs
			cidr := &net.IPNet{IP: m.addIPOffset(candidate), Mask: m.nodeMask}
			return cidr, nil
		}

	}
	return nil, ErrCIDRRangeNoCIDRsRemaining

}

func (m *Map) validate(cidr *net.IPNet) error {
	if !m.clusterCIDR.Contains(cidr.IP.Mask(m.clusterCIDR.Mask)) && !cidr.Contains(m.clusterCIDR.IP.Mask(cidr.Mask)) {
		return fmt.Errorf("cidr %v is out the range of cluster cidr %v", cidr, m.clusterCIDR)
	}
	return nil

}

// addIPOffset adds the provided integer offset to a base big.Int representing a
// net.IP
func (m *Map) addIPOffset(offset uint64) net.IP {
	base := big.NewInt(0).SetBytes(m.clusterCIDR.IP.To16())
	return net.IP(big.NewInt(0).Add(base, big.NewInt(int64(offset))).Bytes())
}

// calculateIPOffset calculates the integer offset of ip from base such that
// base + offset = ip. It requires ip >= base.
func (m *Map) calculateIPOffset(ip net.IP) uint64 {
	base := big.NewInt(0).SetBytes(m.clusterCIDR.IP.To16())
	bigOffset := big.NewInt(0).SetBytes(ip.To16())
	return bigOffset.Sub(bigOffset, base).Uint64()
}
