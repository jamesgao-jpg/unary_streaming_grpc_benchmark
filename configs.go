package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type config struct {
	GRPC      grpcConfig      `yaml:"grpc"`
	Benchmark benchmarkConfig `yaml:"benchmark"`
}

type grpcConfig struct {
	CompressionEnabled bool             `yaml:"compression_enabled"`
	InternalTLSEnabled bool             `yaml:"internal_tls_enabled"`
	Client             grpcClientConfig `yaml:"client"`
	Server             grpcServerConfig `yaml:"server"`
}

type grpcClientConfig struct {
	MaxSendBytes        int     `yaml:"max_send_bytes"`
	MaxReceiveBytes     int     `yaml:"max_receive_bytes"`
	DialTimeoutMS       int     `yaml:"dial_timeout_ms"`
	KeepaliveTimeMS     int     `yaml:"keepalive_time_ms"`
	KeepaliveTimeoutMS  int     `yaml:"keepalive_timeout_ms"`
	PermitWithoutStream bool    `yaml:"permit_without_stream"`
	BackoffBaseDelayMS  int     `yaml:"backoff_base_delay_ms"`
	BackoffMultiplier   float64 `yaml:"backoff_multiplier"`
	BackoffJitter       float64 `yaml:"backoff_jitter"`
	BackoffMaxDelayMS   int     `yaml:"backoff_max_delay_ms"`
}

type grpcServerConfig struct {
	MaxSendBytes          int  `yaml:"max_send_bytes"`
	MaxReceiveBytes       int  `yaml:"max_receive_bytes"`
	KeepaliveTimeMS       int  `yaml:"keepalive_time_ms"`
	KeepaliveTimeoutMS    int  `yaml:"keepalive_timeout_ms"`
	MinimumPingIntervalMS int  `yaml:"minimum_ping_interval_ms"`
	PermitWithoutStream   bool `yaml:"permit_without_stream"`
	GracefulStopTimeoutMS int  `yaml:"graceful_stop_timeout_ms"`
}

type benchmarkConfig struct {
	ChildProcesses            int    `yaml:"child_processes"`
	Host                      string `yaml:"host"`
	BasePort                  int    `yaml:"base_port"`
	TotalPayloadBytesPerChild int    `yaml:"total_payload_bytes_per_child"`
	StreamChunkBytes          int    `yaml:"stream_chunk_bytes"`
	Concurrency               int    `yaml:"concurrency"`
	WarmupRequests            int    `yaml:"warmup_requests"`
	MeasuredRequests          int    `yaml:"measured_requests"`
	RequestTimeoutMS          int    `yaml:"request_timeout_ms"`
	StartupTimeoutMS          int    `yaml:"startup_timeout_ms"`
}

func loadConfig(path string) (config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return config{}, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	var cfg config
	if err := decoder.Decode(&cfg); err != nil {
		return config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (c config) validate() error {
	if c.GRPC.CompressionEnabled {
		return errors.New("compression_enabled must remain false to match the current Milvus setting")
	}
	if c.GRPC.InternalTLSEnabled {
		return errors.New("internal_tls_enabled must remain false to match the current Milvus setting")
	}
	positive := map[string]int{
		"grpc.client.max_send_bytes":              c.GRPC.Client.MaxSendBytes,
		"grpc.client.max_receive_bytes":           c.GRPC.Client.MaxReceiveBytes,
		"grpc.client.dial_timeout_ms":             c.GRPC.Client.DialTimeoutMS,
		"grpc.client.keepalive_time_ms":           c.GRPC.Client.KeepaliveTimeMS,
		"grpc.client.keepalive_timeout_ms":        c.GRPC.Client.KeepaliveTimeoutMS,
		"grpc.client.backoff_base_delay_ms":       c.GRPC.Client.BackoffBaseDelayMS,
		"grpc.client.backoff_max_delay_ms":        c.GRPC.Client.BackoffMaxDelayMS,
		"grpc.server.max_send_bytes":              c.GRPC.Server.MaxSendBytes,
		"grpc.server.max_receive_bytes":           c.GRPC.Server.MaxReceiveBytes,
		"grpc.server.keepalive_time_ms":           c.GRPC.Server.KeepaliveTimeMS,
		"grpc.server.keepalive_timeout_ms":        c.GRPC.Server.KeepaliveTimeoutMS,
		"grpc.server.minimum_ping_interval_ms":    c.GRPC.Server.MinimumPingIntervalMS,
		"grpc.server.graceful_stop_timeout_ms":    c.GRPC.Server.GracefulStopTimeoutMS,
		"benchmark.child_processes":               c.Benchmark.ChildProcesses,
		"benchmark.total_payload_bytes_per_child": c.Benchmark.TotalPayloadBytesPerChild,
		"benchmark.stream_chunk_bytes":            c.Benchmark.StreamChunkBytes,
		"benchmark.concurrency":                   c.Benchmark.Concurrency,
		"benchmark.measured_requests":             c.Benchmark.MeasuredRequests,
		"benchmark.request_timeout_ms":            c.Benchmark.RequestTimeoutMS,
		"benchmark.startup_timeout_ms":            c.Benchmark.StartupTimeoutMS,
	}
	for name, value := range positive {
		if value <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.Benchmark.WarmupRequests < 0 {
		return errors.New("benchmark.warmup_requests cannot be negative")
	}
	if c.Benchmark.Host == "" {
		return errors.New("benchmark.host is required")
	}
	if c.Benchmark.BasePort <= 0 || c.Benchmark.BasePort+c.Benchmark.ChildProcesses-1 > 65535 {
		return errors.New("benchmark child port range is invalid")
	}
	if c.GRPC.Client.BackoffMultiplier <= 0 || c.GRPC.Client.BackoffJitter < 0 {
		return errors.New("gRPC backoff multiplier and jitter are invalid")
	}

	effectiveResponseLimit := min(c.GRPC.Client.MaxReceiveBytes, c.GRPC.Server.MaxSendBytes)
	unaryMessageBytes := protobufBytesValueSize(c.Benchmark.TotalPayloadBytesPerChild)
	streamMessageBytes := protobufBytesValueSize(min(c.Benchmark.TotalPayloadBytesPerChild, c.Benchmark.StreamChunkBytes))
	if unaryMessageBytes > effectiveResponseLimit {
		return fmt.Errorf("unary protobuf response is %d bytes, exceeding the effective %d-byte response limit", unaryMessageBytes, effectiveResponseLimit)
	}
	if streamMessageBytes > effectiveResponseLimit {
		return fmt.Errorf("streaming protobuf response is %d bytes, exceeding the effective %d-byte response limit", streamMessageBytes, effectiveResponseLimit)
	}
	return nil
}

func protobufBytesValueSize(payloadBytes int) int {
	value := payloadBytes
	lengthBytes := 1
	for value >= 128 {
		value >>= 7
		lengthBytes++
	}
	return 1 + lengthBytes + payloadBytes
}
