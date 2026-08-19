package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"gopkg.in/yaml.v3"
)

const (
	unaryMethod     = "/benchmark.BenchmarkService/Unary"
	streamingMethod = "/benchmark.BenchmarkService/Streaming"
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

type benchmarkServiceServer interface {
	Unary(context.Context, *emptypb.Empty) (*wrapperspb.BytesValue, error)
	Streaming(grpc.ServerStream) error
}

type transferService struct {
	payload    []byte
	chunkBytes int
}

func (s *transferService) Unary(context.Context, *emptypb.Empty) (*wrapperspb.BytesValue, error) {
	return wrapperspb.Bytes(s.payload), nil
}

func (s *transferService) Streaming(stream grpc.ServerStream) error {
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		return err
	}
	for offset := 0; offset < len(s.payload); offset += s.chunkBytes {
		end := min(offset+s.chunkBytes, len(s.payload))
		if err := stream.SendMsg(wrapperspb.Bytes(s.payload[offset:end])); err != nil {
			return err
		}
	}
	return nil
}

func unaryHandler(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := &emptypb.Empty{}
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(benchmarkServiceServer).Unary(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: unaryMethod}
	handler := func(ctx context.Context, request any) (any, error) {
		return srv.(benchmarkServiceServer).Unary(ctx, request.(*emptypb.Empty))
	}
	return interceptor(ctx, request, info, handler)
}

func streamingHandler(srv any, stream grpc.ServerStream) error {
	return srv.(benchmarkServiceServer).Streaming(stream)
}

var serviceDescription = grpc.ServiceDesc{
	ServiceName: "benchmark.BenchmarkService",
	HandlerType: (*benchmarkServiceServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Unary",
		Handler:    unaryHandler,
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Streaming",
		Handler:       streamingHandler,
		ServerStreams: true,
		ClientStreams: true,
	}},
	Metadata: "benchmark.proto",
}

type benchmarkClient struct {
	connection *grpc.ClientConn
	expected   byte
}

func (c *benchmarkClient) unary(ctx context.Context, verify bool, started time.Time) (transferResult, error) {
	response := &wrapperspb.BytesValue{}
	if err := c.connection.Invoke(ctx, unaryMethod, &emptypb.Empty{}, response); err != nil {
		return transferResult{}, err
	}
	if verify {
		if err := verifyPayload(response.Value, c.expected); err != nil {
			return transferResult{}, err
		}
	}
	return transferResult{bytes: len(response.Value), firstResponse: time.Since(started)}, nil
}

var clientStreamingDescription = grpc.StreamDesc{
	StreamName:    "Streaming",
	ServerStreams: true,
	ClientStreams: true,
}

func (c *benchmarkClient) streaming(ctx context.Context, verify bool, started time.Time) (transferResult, error) {
	stream, err := c.connection.NewStream(ctx, &clientStreamingDescription, streamingMethod)
	if err != nil {
		return transferResult{}, err
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		return transferResult{}, err
	}
	if err := stream.CloseSend(); err != nil {
		return transferResult{}, err
	}

	result := transferResult{}
	for {
		response := &wrapperspb.BytesValue{}
		err := stream.RecvMsg(response)
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return transferResult{}, err
		}
		if result.firstResponse == 0 {
			result.firstResponse = time.Since(started)
		}
		if verify {
			if err := verifyPayload(response.Value, c.expected); err != nil {
				return transferResult{}, err
			}
		}
		result.bytes += len(response.Value)
	}
}

func verifyPayload(payload []byte, expected byte) error {
	for i, value := range payload {
		if value != expected {
			return fmt.Errorf("payload byte %d is %d, expected %d", i, value, expected)
		}
	}
	return nil
}

type childProcess struct {
	command *exec.Cmd
	done    chan error
}

// launches the benchmark’s simulated QN/SN workers as separate OS processes.
func startChildren(configPath string, cfg config) ([]childProcess, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	children := make([]childProcess, 0, cfg.Benchmark.ChildProcesses)
	for i := 0; i < cfg.Benchmark.ChildProcesses; i++ {
		address := net.JoinHostPort(cfg.Benchmark.Host, strconv.Itoa(cfg.Benchmark.BasePort+i))
		command := exec.Command(executable,
			"--child",
			"--config", configPath,
			"--child-index", strconv.Itoa(i),
			"--listen", address,
		)
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			stopChildren(children, time.Duration(cfg.GRPC.Server.GracefulStopTimeoutMS)*time.Millisecond)
			return nil, err
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		children = append(children, childProcess{command: command, done: done})
	}
	return children, nil
}

func stopChildren(children []childProcess, timeout time.Duration) {
	for _, child := range children {
		if child.command.Process != nil {
			_ = child.command.Process.Signal(os.Interrupt)
		}
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for _, child := range children {
		select {
		case <-child.done:
		case <-deadline.C:
			for _, remaining := range children {
				if remaining.command.Process != nil {
					_ = remaining.command.Process.Kill()
				}
			}
			return
		}
	}
}

// creates the client side of the benchmark topology after startChildren() launches the server processes.
func dialChildren(ctx context.Context, cfg config) ([]*benchmarkClient, error) {
	clients := make([]*benchmarkClient, 0, cfg.Benchmark.ChildProcesses)
	for i := 0; i < cfg.Benchmark.ChildProcesses; i++ {
		address := net.JoinHostPort(cfg.Benchmark.Host, strconv.Itoa(cfg.Benchmark.BasePort+i))
		connection, err := dialChild(ctx, address, cfg.GRPC.Client, cfg.Benchmark.StartupTimeoutMS)
		if err != nil {
			closeClients(clients)
			return nil, fmt.Errorf("dial child %d at %s: %w", i, address, err)
		}
		clients = append(clients, &benchmarkClient{
			connection: connection,
			expected:   byte(i%251 + 1),
		})
	}
	return clients, nil
}

func dialChild(parent context.Context, address string, cfg grpcClientConfig, startupTimeoutMS int) (*grpc.ClientConn, error) {
	startupCtx, cancelStartup := context.WithTimeout(parent, time.Duration(startupTimeoutMS)*time.Millisecond)
	defer cancelStartup()
	for {
		dialCtx, cancelDial := context.WithTimeout(startupCtx, time.Duration(cfg.DialTimeoutMS)*time.Millisecond)
		connection, err := grpc.DialContext(
			dialCtx,
			address,
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(cfg.MaxReceiveBytes),
				grpc.MaxCallSendMsgSize(cfg.MaxSendBytes),
			),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Duration(cfg.KeepaliveTimeMS) * time.Millisecond,
				Timeout:             time.Duration(cfg.KeepaliveTimeoutMS) * time.Millisecond,
				PermitWithoutStream: cfg.PermitWithoutStream,
			}),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Duration(cfg.BackoffBaseDelayMS) * time.Millisecond,
					Multiplier: cfg.BackoffMultiplier,
					Jitter:     cfg.BackoffJitter,
					MaxDelay:   time.Duration(cfg.BackoffMaxDelayMS) * time.Millisecond,
				},
				MinConnectTimeout: time.Duration(cfg.DialTimeoutMS) * time.Millisecond,
			}),
		)
		cancelDial()
		if err == nil {
			return connection, nil
		}
		if startupCtx.Err() != nil {
			return nil, startupCtx.Err()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func closeClients(clients []*benchmarkClient) {
	for _, client := range clients {
		_ = client.connection.Close()
	}
}

type transferMode string

const (
	unaryMode     transferMode = "unary"
	streamingMode transferMode = "streaming"
)

type transferResult struct {
	bytes         int
	firstResponse time.Duration
}

func transferAll(ctx context.Context, clients []*benchmarkClient, mode transferMode, verify bool) (transferResult, error) {
	started := time.Now()
	results := make(chan struct {
		result transferResult
		err    error
	}, len(clients))
	for _, client := range clients {
		client := client
		go func() {
			var result transferResult
			var err error
			if mode == unaryMode {
				result, err = client.unary(ctx, verify, started)
			} else {
				result, err = client.streaming(ctx, verify, started)
			}
			results <- struct {
				result transferResult
				err    error
			}{result: result, err: err}
		}()
	}

	combined := transferResult{}
	for range clients {
		child := <-results
		if child.err != nil {
			return transferResult{}, child.err
		}
		combined.bytes += child.result.bytes
		if combined.firstResponse == 0 || child.result.firstResponse < combined.firstResponse {
			combined.firstResponse = child.result.firstResponse
		}
	}
	return combined, nil
}

type workflowReport struct {
	mode           transferMode
	duration       time.Duration
	successes      int
	errors         int
	bytes          int64
	latencies      []time.Duration
	firstResponses []time.Duration
}

func runWorkflow(parent context.Context, cfg benchmarkConfig, clients []*benchmarkClient, mode transferMode) (workflowReport, error) {
	for i := 0; i < cfg.WarmupRequests; i++ {
		ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.RequestTimeoutMS)*time.Millisecond)
		_, err := transferAll(ctx, clients, mode, false)
		cancel()
		if err != nil {
			return workflowReport{}, fmt.Errorf("%s warm-up request %d: %w", mode, i, err)
		}
	}

	report := workflowReport{
		mode:           mode,
		latencies:      make([]time.Duration, cfg.MeasuredRequests),
		firstResponses: make([]time.Duration, cfg.MeasuredRequests),
	}
	errorsByRequest := make([]error, cfg.MeasuredRequests)
	bytesByRequest := make([]int, cfg.MeasuredRequests)
	jobs := make(chan int)
	var workers sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for requestIndex := range jobs {
				ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.RequestTimeoutMS)*time.Millisecond)
				started := time.Now()
				result, err := transferAll(ctx, clients, mode, false)
				report.latencies[requestIndex] = time.Since(started)
				cancel()
				errorsByRequest[requestIndex] = err
				bytesByRequest[requestIndex] = result.bytes
				report.firstResponses[requestIndex] = result.firstResponse
			}
		}()
	}

	started := time.Now()
	for requestIndex := 0; requestIndex < cfg.MeasuredRequests; requestIndex++ {
		jobs <- requestIndex
	}
	close(jobs)
	workers.Wait()
	report.duration = time.Since(started)

	validLatencies := report.latencies[:0]
	validFirstResponses := report.firstResponses[:0]
	for i, err := range errorsByRequest {
		if err != nil {
			report.errors++
			continue
		}
		report.successes++
		report.bytes += int64(bytesByRequest[i])
		validLatencies = append(validLatencies, report.latencies[i])
		if report.firstResponses[i] > 0 {
			validFirstResponses = append(validFirstResponses, report.firstResponses[i])
		}
	}
	report.latencies = validLatencies
	report.firstResponses = validFirstResponses
	if report.errors > 0 {
		return report, fmt.Errorf("%s completed with %d failed requests", mode, report.errors)
	}
	return report, nil
}

func runChild(ctx context.Context, cfg config, childIndex int, address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	payload := bytes.Repeat([]byte{byte(childIndex%251 + 1)}, cfg.Benchmark.TotalPayloadBytesPerChild)
	serverCfg := cfg.GRPC.Server
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(serverCfg.MaxReceiveBytes),
		grpc.MaxSendMsgSize(serverCfg.MaxSendBytes),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Duration(serverCfg.MinimumPingIntervalMS) * time.Millisecond,
			PermitWithoutStream: serverCfg.PermitWithoutStream,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Duration(serverCfg.KeepaliveTimeMS) * time.Millisecond,
			Timeout: time.Duration(serverCfg.KeepaliveTimeoutMS) * time.Millisecond,
		}),
	)
	server.RegisterService(&serviceDescription, &transferService{
		payload:    payload,
		chunkBytes: cfg.Benchmark.StreamChunkBytes,
	})

	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(listener) }()
	select {
	case err := <-serveError:
		return err
	case <-ctx.Done():
	}

	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Duration(serverCfg.GracefulStopTimeoutMS) * time.Millisecond):
		server.Stop()
	}
	return nil
}

func runBenchmark(ctx context.Context, configPath string, cfg config) error {
	absoluteConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return err
	}
	children, err := startChildren(absoluteConfigPath, cfg)
	if err != nil {
		return err
	}
	defer stopChildren(children, time.Duration(cfg.GRPC.Server.GracefulStopTimeoutMS)*time.Millisecond)

	clients, err := dialChildren(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeClients(clients)

	expectedBytes := cfg.Benchmark.ChildProcesses * cfg.Benchmark.TotalPayloadBytesPerChild
	for _, mode := range []transferMode{unaryMode, streamingMode} {
		requestCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Benchmark.RequestTimeoutMS)*time.Millisecond)
		result, err := transferAll(requestCtx, clients, mode, true)
		cancel()
		if err != nil {
			return fmt.Errorf("verify %s: %w", mode, err)
		}
		if result.bytes != expectedBytes {
			return fmt.Errorf("verify %s: received %d bytes, expected %d", mode, result.bytes, expectedBytes)
		}
	}

	fmt.Printf("children=%d payload_per_child=%d stream_chunk=%d concurrency=%d\n",
		cfg.Benchmark.ChildProcesses,
		cfg.Benchmark.TotalPayloadBytesPerChild,
		cfg.Benchmark.StreamChunkBytes,
		cfg.Benchmark.Concurrency,
	)
	fmt.Printf("%-10s %8s %8s %10s %12s %12s %12s %12s %14s\n",
		"MODE", "SUCCESS", "ERROR", "QPS", "MiB/S", "P50", "P95", "P99", "FIRST_P50")
	for _, mode := range []transferMode{unaryMode, streamingMode} {
		report, runErr := runWorkflow(ctx, cfg.Benchmark, clients, mode)
		printReport(report)
		if runErr != nil {
			return runErr
		}
	}
	return nil
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to benchmark YAML")
	child := flag.Bool("child", false, "run as a child gRPC server")
	childIndex := flag.Int("child-index", 0, "child process index")
	listenAddress := flag.String("listen", "", "child server listen address")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *child {
		if *listenAddress == "" {
			fmt.Fprintln(os.Stderr, "--listen is required in child mode")
			os.Exit(1)
		}
		err = runChild(ctx, cfg, *childIndex, *listenAddress)
	} else {
		err = runBenchmark(ctx, *configPath, cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
