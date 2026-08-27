// This file defines tracked network connections and cumulative transport counter snapshots.
package main

import (
	"net"
	"sync"
	"sync/atomic"
	"syscall"
)

// transportCounterSnapshot captures connection I/O and optional Linux TCP counters.
type transportCounterSnapshot struct {
	connectionReadBytes  uint64
	connectionWriteBytes uint64
	tcpBytesReceived     uint64
	tcpBytesSent         uint64
	tcpBytesAcked        uint64
	tcpNotSentBytes      uint64
	tcpInfoAvailable     bool
}

// since returns cumulative counter deltas and the ending TCP_NOTSENT gauge.
func (end transportCounterSnapshot) since(start transportCounterSnapshot) transportCounterSnapshot {
	delta := transportCounterSnapshot{
		connectionReadBytes:  end.connectionReadBytes - start.connectionReadBytes,
		connectionWriteBytes: end.connectionWriteBytes - start.connectionWriteBytes,
		// TCP_NOTSENT is a point-in-time gauge, not a cumulative byte counter.
		tcpNotSentBytes: end.tcpNotSentBytes,
	}
	// A missing TCP_INFO endpoint would make subtraction misleading or underflow.
	if !start.tcpInfoAvailable || !end.tcpInfoAvailable {
		return delta
	}
	delta.tcpBytesReceived = end.tcpBytesReceived - start.tcpBytesReceived
	delta.tcpBytesSent = end.tcpBytesSent - start.tcpBytesSent
	delta.tcpBytesAcked = end.tcpBytesAcked - start.tcpBytesAcked
	delta.tcpInfoAvailable = true
	return delta
}

// add accumulates numeric counters from another connection snapshot.
func (s *transportCounterSnapshot) add(other transportCounterSnapshot) {
	s.connectionReadBytes += other.connectionReadBytes
	s.connectionWriteBytes += other.connectionWriteBytes
	s.tcpBytesReceived += other.tcpBytesReceived
	s.tcpBytesSent += other.tcpBytesSent
	s.tcpBytesAcked += other.tcpBytesAcked
	s.tcpNotSentBytes += other.tcpNotSentBytes
}

// trackedConn counts bytes crossing the net.Conn boundary and exposes TCP_INFO.
type trackedConn struct {
	net.Conn
	readBytes  atomic.Uint64
	writeBytes atomic.Uint64
	closeOnce  sync.Once
	closeMu    sync.Mutex
	finalTCP   tcpCounterSnapshot
}

// Read delegates to the connection and records bytes returned to gRPC.
func (c *trackedConn) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	c.readBytes.Add(uint64(n))
	return n, err
}

// Write delegates to the connection and records bytes accepted from gRPC.
func (c *trackedConn) Write(buffer []byte) (int, error) {
	n, err := c.Conn.Write(buffer)
	c.writeBytes.Add(uint64(n))
	return n, err
}

// Close preserves final TCP counters before closing the underlying socket.
func (c *trackedConn) Close() error {
	c.closeOnce.Do(func() {
		// Preserve the final kernel counters because TCP_INFO is unavailable after Close.
		c.closeMu.Lock()
		c.finalTCP, _ = readTCPInfo(c.Conn)
		c.closeMu.Unlock()
	})
	return c.Conn.Close()
}

// SyscallConn exposes the socket descriptor required for Linux TCP_INFO.
func (c *trackedConn) SyscallConn() (syscall.RawConn, error) {
	connection, ok := c.Conn.(syscall.Conn)
	if !ok {
		return nil, syscall.EINVAL
	}
	return connection.SyscallConn()
}

// snapshot reads cumulative connection bytes and the latest available TCP state.
func (c *trackedConn) snapshot() transportCounterSnapshot {
	tcp, available := readTCPInfo(c.Conn)
	if !available {
		c.closeMu.Lock()
		tcp = c.finalTCP
		c.closeMu.Unlock()
		available = tcp.available
	}
	return transportCounterSnapshot{
		connectionReadBytes:  c.readBytes.Load(),
		connectionWriteBytes: c.writeBytes.Load(),
		tcpBytesReceived:     tcp.bytesReceived,
		tcpBytesSent:         tcp.bytesSent,
		tcpBytesAcked:        tcp.bytesAcked,
		tcpNotSentBytes:      tcp.notSentBytes,
		tcpInfoAvailable:     available,
	}
}

// connectionTracker retains all connections grouped by resolver or peer address.
type connectionTracker struct {
	mu       sync.Mutex
	byRemote map[string][]*trackedConn
}

// newConnectionTracker creates an empty address-indexed connection tracker.
func newConnectionTracker() *connectionTracker {
	return &connectionTracker{byRemote: make(map[string][]*trackedConn)}
}

// track wraps a connection and indexes it by its reported remote address.
func (t *connectionTracker) track(conn net.Conn) net.Conn {
	return t.trackAs(conn.RemoteAddr().String(), conn)
}

// trackAs wraps a connection under a stable caller-provided address key.
func (t *connectionTracker) trackAs(remoteAddress string, conn net.Conn) net.Conn {
	tracked := &trackedConn{Conn: conn}
	t.mu.Lock()
	t.byRemote[remoteAddress] = append(t.byRemote[remoteAddress], tracked)
	t.mu.Unlock()
	return tracked
}

// snapshot aggregates every current or closed connection for one address.
func (t *connectionTracker) snapshot(remoteAddress string) transportCounterSnapshot {
	t.mu.Lock()
	connections := append([]*trackedConn(nil), t.byRemote[remoteAddress]...)
	t.mu.Unlock()
	var snapshot transportCounterSnapshot
	snapshot.tcpInfoAvailable = len(connections) > 0
	for _, connection := range connections {
		connectionSnapshot := connection.snapshot()
		snapshot.add(connectionSnapshot)
		snapshot.tcpInfoAvailable = snapshot.tcpInfoAvailable && connectionSnapshot.tcpInfoAvailable
	}
	return snapshot
}

// snapshotAll aggregates every connection known to the tracker.
func (t *connectionTracker) snapshotAll() transportCounterSnapshot {
	t.mu.Lock()
	connections := make([]*trackedConn, 0)
	for _, remoteConnections := range t.byRemote {
		connections = append(connections, remoteConnections...)
	}
	t.mu.Unlock()
	var snapshot transportCounterSnapshot
	snapshot.tcpInfoAvailable = len(connections) > 0
	for _, connection := range connections {
		connectionSnapshot := connection.snapshot()
		snapshot.add(connectionSnapshot)
		snapshot.tcpInfoAvailable = snapshot.tcpInfoAvailable && connectionSnapshot.tcpInfoAvailable
	}
	return snapshot
}

// trackedListener wraps every accepted child-server connection for measurement.
type trackedListener struct {
	net.Listener
	tracker *connectionTracker
}

// Accept tracks a newly accepted connection before returning it to gRPC.
func (l trackedListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return l.tracker.track(connection), nil
}
