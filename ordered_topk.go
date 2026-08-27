// This file defines ordered Units, child Chunk Buffers, and unary or streaming topK reduction.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
)

const (
	fullTransferWorkflow      = "full_transfer"
	orderedTopKWorkflow       = "ordered_topk"
	interleavedDistribution   = "interleaved"
	dominantChildDistribution = "dominant_child"
	orderedUnitHeaderBytes    = 16
	orderedHashOffset         = uint64(14695981039346656037)
	orderedHashPrime          = uint64(1099511628211)
)

// orderedUnit is the smallest ranked result consumed by one reduction step.
type orderedUnit struct {
	rank uint64
	id   uint64
}

// generateOrderedPayload creates one deterministic, sorted child result payload.
func generateOrderedPayload(cfg benchmarkConfig, childIndex int) []byte {
	unitsPerChild := cfg.TotalPayloadBytesPerChild / cfg.PerUnitBytes
	payload := make([]byte, cfg.TotalPayloadBytesPerChild)
	for localIndex := 0; localIndex < unitsPerChild; localIndex++ {
		unit := expectedOrderedUnit(cfg, childIndex, localIndex)
		offset := localIndex * cfg.PerUnitBytes
		binary.LittleEndian.PutUint64(payload[offset:], unit.rank)
		binary.LittleEndian.PutUint64(payload[offset+8:], unit.id)
		for i := offset + orderedUnitHeaderBytes; i < offset+cfg.PerUnitBytes; i++ {
			payload[i] = byte(childIndex%251 + 1)
		}
	}
	return payload
}

// expectedOrderedUnit returns the rank and ID assigned to one child-local Unit.
func expectedOrderedUnit(cfg benchmarkConfig, childIndex, localIndex int) orderedUnit {
	unitsPerChild := cfg.TotalPayloadBytesPerChild / cfg.PerUnitBytes
	rank := uint64(childIndex*unitsPerChild + localIndex)
	if cfg.ResultDistribution == interleavedDistribution {
		rank = uint64(localIndex*cfg.ChildProcesses + childIndex)
	}
	return orderedUnit{
		rank: rank,
		id:   uint64(childIndex*unitsPerChild + localIndex),
	}
}

// verifyOrderedPayload checks every encoded Unit against its configured rank distribution.
func verifyOrderedPayload(payload []byte, cfg benchmarkConfig, childIndex int) error {
	if len(payload) != cfg.TotalPayloadBytesPerChild {
		return fmt.Errorf("child %d returned %d bytes, expected %d", childIndex, len(payload), cfg.TotalPayloadBytesPerChild)
	}
	for localIndex := 0; localIndex < len(payload)/cfg.PerUnitBytes; localIndex++ {
		offset := localIndex * cfg.PerUnitBytes
		actual := decodeOrderedUnit(payload[offset:])
		expected := expectedOrderedUnit(cfg, childIndex, localIndex)
		if actual != expected {
			return fmt.Errorf("child %d Unit %d is %+v, expected %+v", childIndex, localIndex, actual, expected)
		}
	}
	return nil
}

// decodeOrderedUnit reads one rank and ID from an encoded Unit.
func decodeOrderedUnit(data []byte) orderedUnit {
	return orderedUnit{
		rank: binary.LittleEndian.Uint64(data),
		id:   binary.LittleEndian.Uint64(data[8:]),
	}
}

// childChunkBuffer retains one child's current Chunk and next unread Unit.
type childChunkBuffer struct {
	chunk     []byte
	offset    int
	unitBytes int
}

// accept installs a non-empty, Unit-aligned Chunk into an empty Buffer.
func (b *childChunkBuffer) accept(chunk []byte) error {
	if b.hasUnit() {
		return errors.New("child Chunk Buffer still contains Units")
	}
	if len(chunk) == 0 || len(chunk)%b.unitBytes != 0 {
		return fmt.Errorf("child Chunk contains %d bytes, which is not a positive multiple of Unit size %d", len(chunk), b.unitBytes)
	}
	b.chunk = chunk
	b.offset = 0
	return nil
}

// hasUnit reports whether the Buffer has an unread Unit.
func (b *childChunkBuffer) hasUnit() bool {
	return b.offset < len(b.chunk)
}

// head returns the next Unit without consuming it.
func (b *childChunkBuffer) head() orderedUnit {
	return decodeOrderedUnit(b.chunk[b.offset:])
}

// pop consumes and returns the next Unit, releasing an exhausted Chunk.
func (b *childChunkBuffer) pop() orderedUnit {
	unit := b.head()
	b.offset += b.unitBytes
	if b.offset == len(b.chunk) {
		b.chunk = nil
		b.offset = 0
	}
	return unit
}

// reduceOrderedTopK performs a k-way merge and refills only the selected empty Buffer.
func reduceOrderedTopK(buffers []childChunkBuffer, topK int, refill func(int) error) (int, uint64, error) {
	emitted := 0
	hash := orderedHashOffset
	for emitted < topK {
		winner := -1
		for i := range buffers {
			if !buffers[i].hasUnit() {
				continue
			}
			if winner == -1 || ranksBefore(buffers[i].head(), buffers[winner].head()) {
				winner = i
			}
		}
		if winner == -1 {
			return 0, 0, fmt.Errorf("child results ended after %d of %d requested Units", emitted, topK)
		}

		unit := buffers[winner].pop()
		hash ^= unit.rank
		hash *= orderedHashPrime
		hash ^= unit.id
		hash *= orderedHashPrime
		emitted++
		if emitted < topK && !buffers[winner].hasUnit() && refill != nil {
			if err := refill(winner); err != nil {
				return 0, 0, err
			}
		}
	}
	return emitted, hash, nil
}

// ranksBefore applies the deterministic rank-then-ID ordering.
func ranksBefore(candidate, selected orderedUnit) bool {
	return candidate.rank < selected.rank || candidate.rank == selected.rank && candidate.id < selected.id
}

// addReceivedMessage records one parent-visible child message in a transfer result.
func addReceivedMessage(result *transferResult, childIndex int, payload []byte, unitBytes int) {
	result.bytes += len(payload)
	result.protobufBytes += protobufBytesValueSize(len(payload))
	result.responseMessages++
	result.receivedUnits += len(payload) / unitBytes
	result.perChild[childIndex].messages++
	result.perChild[childIndex].bytes += len(payload)
}

// orderedTopKUnary materializes all child results before performing ordered topK reduction.
func orderedTopKUnary(ctx context.Context, cfg benchmarkConfig, clients []*benchmarkClient, verify bool) (transferResult, error) {
	started := time.Now()
	type childResult struct {
		index         int
		payload       []byte
		firstResponse time.Duration
		err           error
	}
	results := make(chan childResult, len(clients))
	for _, client := range clients {
		client := client
		go func() {
			payload, firstResponse, err := client.unaryPayload(ctx, started)
			results <- childResult{index: client.childIndex, payload: payload, firstResponse: firstResponse, err: err}
		}()
	}

	buffers := make([]childChunkBuffer, len(clients))
	result := transferResult{perChild: make([]childReceiveCounters, len(clients))}
	for range clients {
		child := <-results
		if child.err != nil {
			return transferResult{}, child.err
		}
		if verify {
			if err := verifyOrderedPayload(child.payload, cfg, child.index); err != nil {
				return transferResult{}, err
			}
		}
		buffers[child.index].unitBytes = cfg.PerUnitBytes
		if err := buffers[child.index].accept(child.payload); err != nil {
			return transferResult{}, err
		}
		addReceivedMessage(&result, child.index, child.payload, cfg.PerUnitBytes)
		if result.firstResponse == 0 || child.firstResponse < result.firstResponse {
			result.firstResponse = child.firstResponse
		}
	}

	emitted, hash, err := reduceOrderedTopK(buffers, cfg.GlobalTopK, nil)
	if err != nil {
		return transferResult{}, err
	}
	result.emittedUnits = emitted
	result.outputHash = hash
	return result, nil
}

// orderedTopKStreaming receives initial Chunks and refills only Buffers exhausted by reduction.
func orderedTopKStreaming(ctx context.Context, cfg benchmarkConfig, clients []*benchmarkClient) (transferResult, error) {
	// Open every child stream concurrently and receive the first Chunk required for comparison.
	streamContext, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	type childResult struct {
		index         int
		stream        grpc.ClientStream
		chunk         []byte
		firstResponse time.Duration
		err           error
	}
	results := make(chan childResult, len(clients))
	for _, client := range clients {
		client := client
		go func() {
			stream, err := client.openStreaming(streamContext)
			if err != nil {
				results <- childResult{index: client.childIndex, err: err}
				return
			}
			chunk, err := receiveStreamingChunk(stream)
			results <- childResult{
				index:         client.childIndex,
				stream:        stream,
				chunk:         chunk,
				firstResponse: time.Since(started),
				err:           err,
			}
		}()
	}

	// Install one initial Chunk per child so every current head participates in ordering.
	streams := make([]grpc.ClientStream, len(clients))
	buffers := make([]childChunkBuffer, len(clients))
	result := transferResult{perChild: make([]childReceiveCounters, len(clients))}
	for range clients {
		child := <-results
		if child.err != nil {
			return transferResult{}, child.err
		}
		streams[child.index] = child.stream
		buffers[child.index].unitBytes = cfg.PerUnitBytes
		if err := buffers[child.index].accept(child.chunk); err != nil {
			return transferResult{}, err
		}
		addReceivedMessage(&result, child.index, child.chunk, cfg.PerUnitBytes)
		if result.firstResponse == 0 || child.firstResponse < result.firstResponse {
			result.firstResponse = child.firstResponse
		}
	}

	// Refill only the winning child's exhausted Buffer until topK has been emitted.
	refill := func(childIndex int) error {
		chunk, err := receiveStreamingChunk(streams[childIndex])
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("child stream %d Recv failed: %w", childIndex, err)
		}
		if err := buffers[childIndex].accept(chunk); err != nil {
			return fmt.Errorf("child stream %d returned an invalid Chunk: %w", childIndex, err)
		}
		addReceivedMessage(&result, childIndex, chunk, cfg.PerUnitBytes)
		return nil
	}
	emitted, hash, err := reduceOrderedTopK(buffers, cfg.GlobalTopK, refill)
	if err != nil {
		return transferResult{}, err
	}
	result.emittedUnits = emitted
	result.outputHash = hash
	return result, nil
}
