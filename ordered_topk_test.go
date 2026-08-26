package main

import "testing"

func TestGenerateOrderedPayload(t *testing.T) {
	cfg := orderedTestConfig(interleavedDistribution, 13)
	for childIndex := 0; childIndex < cfg.ChildProcesses; childIndex++ {
		payload := generateOrderedPayload(cfg, childIndex)
		if err := verifyOrderedPayload(payload, cfg, childIndex); err != nil {
			t.Fatalf("child %d: %v", childIndex, err)
		}
	}
}

func TestChunkedOrderedTopKMatchesCompleteResults(t *testing.T) {
	tests := []struct {
		name          string
		distribution  string
		topK          int
		expectedBytes int
	}{
		{name: "interleaved", distribution: interleavedDistribution, topK: 13, expectedBytes: 256},
		{name: "dominant child", distribution: dominantChildDistribution, topK: 5, expectedBytes: 192},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := orderedTestConfig(test.distribution, test.topK)
			payloads := make([][]byte, cfg.ChildProcesses)
			completeBuffers := make([]childChunkBuffer, cfg.ChildProcesses)
			for childIndex := range payloads {
				payloads[childIndex] = generateOrderedPayload(cfg, childIndex)
				completeBuffers[childIndex].unitBytes = cfg.PerUnitBytes
				if err := completeBuffers[childIndex].accept(payloads[childIndex]); err != nil {
					t.Fatal(err)
				}
			}
			completeCount, completeHash, err := reduceOrderedTopK(completeBuffers, cfg.GlobalTopK, nil)
			if err != nil {
				t.Fatal(err)
			}

			chunkBuffers := make([]childChunkBuffer, cfg.ChildProcesses)
			nextOffsets := make([]int, cfg.ChildProcesses)
			receivedBytes := 0
			for childIndex := range chunkBuffers {
				chunkBuffers[childIndex].unitBytes = cfg.PerUnitBytes
				nextOffsets[childIndex] = cfg.StreamChunkBytes
				if err := chunkBuffers[childIndex].accept(payloads[childIndex][:cfg.StreamChunkBytes]); err != nil {
					t.Fatal(err)
				}
				receivedBytes += cfg.StreamChunkBytes
			}
			refill := func(childIndex int) error {
				start := nextOffsets[childIndex]
				if start == len(payloads[childIndex]) {
					return nil
				}
				end := min(start+cfg.StreamChunkBytes, len(payloads[childIndex]))
				nextOffsets[childIndex] = end
				receivedBytes += end - start
				return chunkBuffers[childIndex].accept(payloads[childIndex][start:end])
			}
			chunkedCount, chunkedHash, err := reduceOrderedTopK(chunkBuffers, cfg.GlobalTopK, refill)
			if err != nil {
				t.Fatal(err)
			}

			if completeCount != chunkedCount || completeHash != chunkedHash {
				t.Fatalf("complete=(%d,%x), chunked=(%d,%x)", completeCount, completeHash, chunkedCount, chunkedHash)
			}
			if receivedBytes != test.expectedBytes {
				t.Fatalf("received %d bytes, expected %d", receivedBytes, test.expectedBytes)
			}
		})
	}
}

func TestChildChunkBufferRejectsMisalignedChunk(t *testing.T) {
	buffer := childChunkBuffer{unitBytes: 16}
	if err := buffer.accept(make([]byte, 17)); err == nil {
		t.Fatal("expected a misaligned Chunk error")
	}
}

func orderedTestConfig(distribution string, topK int) benchmarkConfig {
	return benchmarkConfig{
		ChildProcesses:            4,
		Workflow:                  orderedTopKWorkflow,
		TotalPayloadBytesPerChild: 128,
		StreamChunkBytes:          32,
		PerUnitBytes:              16,
		GlobalTopK:                topK,
		ResultDistribution:        distribution,
	}
}
