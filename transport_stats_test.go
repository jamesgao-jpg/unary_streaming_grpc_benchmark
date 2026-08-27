// This file verifies byte accounting at the wrapped net.Conn boundary.
package main

import (
	"io"
	"net"
	"testing"
)

// TestConnectionTrackerCountsReadAndWriteBoundaries verifies delegated I/O byte totals.
func TestConnectionTrackerCountsReadAndWriteBoundaries(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	tracker := newConnectionTracker()
	tracked := tracker.trackAs("child", left)
	writeDone := make(chan error, 1)
	go func() {
		_, err := right.Write([]byte("read"))
		writeDone <- err
	}()
	if _, err := io.ReadFull(tracked, make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	go func() {
		_, err := tracked.Write([]byte("write"))
		writeDone <- err
	}()
	if _, err := io.ReadFull(right, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	snapshot := tracker.snapshot("child")
	if snapshot.connectionReadBytes != 4 || snapshot.connectionWriteBytes != 5 {
		t.Fatalf("snapshot %+v, expected 4 read bytes and 5 write bytes", snapshot)
	}
}
