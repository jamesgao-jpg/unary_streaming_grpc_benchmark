//go:build !linux

// This file provides the unsupported TCP_INFO implementation for non-Linux builds.
package main

import "net"

// tcpCounterSnapshot preserves the cross-platform transport snapshot shape.
type tcpCounterSnapshot struct {
	bytesReceived uint64
	bytesSent     uint64
	bytesAcked    uint64
	notSentBytes  uint64
	available     bool
}

// readTCPInfo reports TCP_INFO as unavailable outside Linux.
func readTCPInfo(net.Conn) (tcpCounterSnapshot, bool) {
	return tcpCounterSnapshot{}, false
}
