//go:build linux

// This file reads Linux TCP_INFO counters from tracked benchmark sockets.
package main

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// tcpCounterSnapshot contains the TCP byte state exposed by the Linux kernel.
type tcpCounterSnapshot struct {
	bytesReceived uint64
	bytesSent     uint64
	bytesAcked    uint64
	notSentBytes  uint64
	available     bool
}

// readTCPInfo reads cumulative and pending byte counters from a TCP socket.
func readTCPInfo(connection net.Conn) (tcpCounterSnapshot, bool) {
	syscallConnection, ok := connection.(syscall.Conn)
	if !ok {
		return tcpCounterSnapshot{}, false
	}
	rawConnection, err := syscallConnection.SyscallConn()
	if err != nil {
		return tcpCounterSnapshot{}, false
	}
	var info *unix.TCPInfo
	var controlError error
	if err := rawConnection.Control(func(fd uintptr) {
		info, controlError = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	}); err != nil || controlError != nil || info == nil {
		return tcpCounterSnapshot{}, false
	}
	return tcpCounterSnapshot{
		bytesReceived: info.Bytes_received,
		bytesSent:     info.Bytes_sent,
		bytesAcked:    info.Bytes_acked,
		notSentBytes:  uint64(info.Notsent_bytes),
		available:     true,
	}, true
}
