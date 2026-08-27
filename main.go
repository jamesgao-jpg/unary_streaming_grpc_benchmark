// This file orchestrates child processes, gRPC connections, benchmark workflows, and shutdown.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
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
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// childProcess tracks one simulated QN/SN process and its completion signal.
type childProcess struct {
	command *exec.Cmd
	done    chan error
}

// startChildren launches the simulated QN/SN workers as separate OS processes.
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

// stopChildren requests graceful child shutdown and kills processes that exceed the deadline.
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

// dialChildren creates child-specific clients over one resolver-backed ClientConn.
func dialChildren(ctx context.Context, cfg config) ([]*benchmarkClient, error) {
	childResolver := manual.NewBuilderWithScheme(childResolverScheme)
	transport := newConnectionTracker()
	grpcReceives := newGRPCReceiveTracker(cfg.Benchmark.ChildProcesses)
	addresses := make([]resolver.Address, cfg.Benchmark.ChildProcesses)
	for i := range addresses {
		addresses[i] = resolver.Address{
			Addr:       net.JoinHostPort(cfg.Benchmark.Host, strconv.Itoa(cfg.Benchmark.BasePort+i)),
			Attributes: attributes.New(childAddressIndexKey{}, i),
		}
	}
	childResolver.InitialState(resolver.State{Addresses: addresses})
	connection, err := dialChildrenChannel(ctx, cfg.GRPC.Client, cfg.Benchmark.StartupTimeoutMS, childResolver, transport, grpcReceives)
	if err != nil {
		return nil, err
	}

	clients := make([]*benchmarkClient, 0, cfg.Benchmark.ChildProcesses)
	for i := 0; i < cfg.Benchmark.ChildProcesses; i++ {
		clients = append(clients, &benchmarkClient{
			connection:   connection,
			childIndex:   i,
			expected:     byte(i%251 + 1),
			address:      addresses[i].Addr,
			transport:    transport,
			grpcReceives: grpcReceives,
		})
	}
	return clients, nil
}

// dialChildrenChannel creates one logical gRPC channel whose SubConns reach all children.
func dialChildrenChannel(parent context.Context, cfg grpcClientConfig, startupTimeoutMS int, childResolver *manual.Resolver, transport *connectionTracker, grpcReceives *grpcReceiveTracker) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(startupTimeoutMS)*time.Millisecond)
	defer cancel()
	dialOptions := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
		grpc.WithResolvers(childResolver),
		grpc.WithDefaultServiceConfig(childServiceConfig),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(parentStatsHandler{receives: grpcReceives}),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if err != nil {
				return nil, err
			}
			// Use the resolver address as the key; RemoteAddr may normalize localhost or IPv6.
			return transport.trackAs(address, connection), nil
		}),
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
	}
	if cfg.StaticWindowBytes > 0 {
		dialOptions = append(dialOptions,
			grpc.WithStaticStreamWindowSize(cfg.StaticWindowBytes),
			grpc.WithStaticConnWindowSize(cfg.StaticWindowBytes),
		)
	}
	return grpc.DialContext(ctx, childResolver.Scheme()+":///children", dialOptions...)
}

// closeClients closes the shared ClientConn held by all benchmark clients.
func closeClients(clients []*benchmarkClient) {
	if len(clients) > 0 {
		_ = clients[0].connection.Close()
	}
}

// transferMode identifies the unary or streaming transport under measurement.
type transferMode string

const (
	unaryMode     transferMode = "unary"
	streamingMode transferMode = "streaming"
)

// transferResult contains one fan-in operation's payload, timing, and correctness data.
type transferResult struct {
	bytes            int
	protobufBytes    int
	responseMessages int
	receivedUnits    int
	emittedUnits     int
	outputHash       uint64
	firstResponse    time.Duration
	perChild         []childReceiveCounters
}

// childReceiveCounters records application-level messages and bytes received from one child.
type childReceiveCounters struct {
	messages int
	bytes    int
}

// transferAll receives each child's complete payload without ordered reduction.
func transferAll(ctx context.Context, clients []*benchmarkClient, mode transferMode, verify bool) (transferResult, error) {
	started := time.Now()
	results := make(chan struct {
		childIndex int
		result     transferResult
		err        error
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
				childIndex int
				result     transferResult
				err        error
			}{childIndex: client.childIndex, result: result, err: err}
		}()
	}

	combined := transferResult{perChild: make([]childReceiveCounters, len(clients))}
	for range clients {
		child := <-results
		if child.err != nil {
			return transferResult{}, child.err
		}
		combined.bytes += child.result.bytes
		combined.protobufBytes += child.result.protobufBytes
		combined.responseMessages += child.result.responseMessages
		combined.receivedUnits += child.result.receivedUnits
		combined.emittedUnits += child.result.emittedUnits
		combined.perChild[child.childIndex].messages += child.result.responseMessages
		combined.perChild[child.childIndex].bytes += child.result.bytes
		if combined.firstResponse == 0 || child.result.firstResponse < combined.firstResponse {
			combined.firstResponse = child.result.firstResponse
		}
	}
	return combined, nil
}

// executeFanIn dispatches one operation to the configured workflow and transport mode.
func executeFanIn(ctx context.Context, cfg benchmarkConfig, clients []*benchmarkClient, mode transferMode, verify bool) (transferResult, error) {
	if cfg.Workflow == orderedTopKWorkflow {
		if mode == unaryMode {
			return orderedTopKUnary(ctx, cfg, clients, verify)
		}
		return orderedTopKStreaming(ctx, cfg, clients)
	}
	return transferAll(ctx, clients, mode, verify)
}

// runWorkflow warms up and measures one transport mode across concurrent fan-in operations.
func runWorkflow(parent context.Context, cfg benchmarkConfig, clients []*benchmarkClient, mode transferMode) (workflowReport, error) {
	// Warm connections, then establish clean application and transport counter baselines.
	for i := 0; i < cfg.WarmupRequests; i++ {
		ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.RequestTimeoutMS)*time.Millisecond)
		_, err := executeFanIn(ctx, cfg, clients, mode, false)
		cancel()
		if err != nil {
			return workflowReport{}, fmt.Errorf("%s warm-up request %d: %w", mode, i, err)
		}
	}
	if err := resetChildSendCounters(parent, cfg.RequestTimeoutMS, clients); err != nil {
		return workflowReport{}, fmt.Errorf("reset %s child send counters: %w", mode, err)
	}
	clients[0].grpcReceives.reset()
	parentTransportStart := make([]transportCounterSnapshot, len(clients))
	for childIndex, client := range clients {
		parentTransportStart[childIndex] = client.transport.snapshot(client.address)
	}

	// Run a fixed worker pool so both modes use the same concurrency model.
	type measuredResult struct {
		result  transferResult
		latency time.Duration
		err     error
	}

	report := workflowReport{
		mode:     mode,
		perChild: make([]childWorkflowCounters, len(clients)),
	}
	jobs := make(chan struct{})
	results := make(chan measuredResult, cfg.Concurrency)
	var workers sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range jobs {
				ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.RequestTimeoutMS)*time.Millisecond)
				started := time.Now()
				result, err := executeFanIn(ctx, cfg, clients, mode, false)
				cancel()
				results <- measuredResult{
					result:  result,
					latency: time.Since(started),
					err:     err,
				}
			}
		}()
	}

	// Submit requests until both the request-count and duration requirements are met.
	started := time.Now()
	minimumDuration := time.Duration(cfg.MinimumMeasurementMS) * time.Millisecond
	submitted := 0
	for i := 0; i < cfg.Concurrency; i++ {
		jobs <- struct{}{}
		submitted++
	}

	for completed := 0; completed < submitted; completed++ {
		result := <-results
		if result.err != nil {
			report.errors++
		} else {
			report.successes++
			report.bytes += int64(result.result.bytes)
			report.protobufBytes += int64(result.result.protobufBytes)
			report.responseMessages += int64(result.result.responseMessages)
			report.receivedUnits += int64(result.result.receivedUnits)
			report.emittedUnits += int64(result.result.emittedUnits)
			for childIndex, counters := range result.result.perChild {
				report.perChild[childIndex].receivedMessages += int64(counters.messages)
				report.perChild[childIndex].receivedBytes += int64(counters.bytes)
			}
			report.latencies = append(report.latencies, result.latency)
			if result.result.firstResponse > 0 {
				report.firstResponses = append(report.firstResponses, result.result.firstResponse)
			}
		}
		if completed+1 < cfg.MeasuredRequests || time.Since(started) < minimumDuration {
			jobs <- struct{}{}
			submitted++
		}
	}
	close(jobs)
	workers.Wait()
	report.duration = time.Since(started)

	// Capture parent counters before GetStats adds reporting control traffic.
	grpcReceives := clients[0].grpcReceives.snapshot()
	for childIndex, client := range clients {
		report.perChild[childIndex].grpcReceive = grpcReceives[childIndex]
		report.perChild[childIndex].parentTransport = client.transport.snapshot(client.address).since(parentTransportStart[childIndex])
	}
	sendCounters, err := collectChildSendCounters(parent, cfg.RequestTimeoutMS, clients)
	if err != nil {
		return report, fmt.Errorf("collect %s child send counters: %w", mode, err)
	}
	// Child transport deltas include the small ResetStats response and GetStats
	// request because those control messages share the measured connection. The
	// application send counters still contain benchmark responses only.
	for childIndex, counters := range sendCounters {
		report.perChild[childIndex].send = counters
	}

	if report.errors > 0 {
		return report, fmt.Errorf("%s completed with %d failed requests", mode, report.errors)
	}
	return report, nil
}

// runChild serves one simulated QN/SN process until cancellation and then shuts it down.
func runChild(ctx context.Context, cfg config, childIndex int, address string) error {
	// Build this child's deterministic payload and instrumented gRPC server.
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	payload := bytes.Repeat([]byte{byte(childIndex%251 + 1)}, cfg.Benchmark.TotalPayloadBytesPerChild)
	if cfg.Benchmark.Workflow == orderedTopKWorkflow {
		payload = generateOrderedPayload(cfg.Benchmark, childIndex)
	}
	serverCfg := cfg.GRPC.Server
	transport := newConnectionTracker()
	tracker := &sendCounterTracker{transport: transport}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(serverCfg.MaxReceiveBytes),
		grpc.MaxSendMsgSize(serverCfg.MaxSendBytes),
		grpc.StatsHandler(benchmarkStatsHandler{tracker: tracker}),
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
		tracker:    tracker,
	})

	// Serve requests until the server fails or the parent cancels the process.
	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(trackedListener{Listener: listener, tracker: transport}) }()
	select {
	case err := <-serveError:
		return err
	case <-ctx.Done():
	}

	// Prefer graceful shutdown, with a bounded forced-stop fallback.
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

// runBenchmark builds the process topology, verifies both modes, and executes measurements.
func runBenchmark(ctx context.Context, configPath string, cfg config) error {
	// Launch the configured child processes and connect one logical parent channel.
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

	// Run one correctness operation per mode before collecting performance results.
	expectedTotalBytesAcrossChildren := cfg.Benchmark.ChildProcesses * cfg.Benchmark.TotalPayloadBytesPerChild
	verification := make(map[transferMode]transferResult, 2)
	for _, mode := range []transferMode{unaryMode, streamingMode} {
		requestCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Benchmark.RequestTimeoutMS)*time.Millisecond)
		result, err := executeFanIn(requestCtx, cfg.Benchmark, clients, mode, true)
		cancel()
		if err != nil {
			return fmt.Errorf("verify %s: %w", mode, err)
		}
		if cfg.Benchmark.Workflow == fullTransferWorkflow && result.bytes != expectedTotalBytesAcrossChildren {
			return fmt.Errorf("verify %s: received %d total bytes across children, expected %d", mode, result.bytes, expectedTotalBytesAcrossChildren)
		}
		if cfg.Benchmark.Workflow == orderedTopKWorkflow {
			if result.emittedUnits != cfg.Benchmark.GlobalTopK {
				return fmt.Errorf("verify %s: emitted %d Units, expected %d", mode, result.emittedUnits, cfg.Benchmark.GlobalTopK)
			}
			if mode == unaryMode && result.bytes != expectedTotalBytesAcrossChildren {
				return fmt.Errorf("verify unary: received %d total bytes across children, expected %d", result.bytes, expectedTotalBytesAcrossChildren)
			}
			if result.bytes > expectedTotalBytesAcrossChildren {
				return fmt.Errorf("verify %s: received %d bytes, exceeding %d potential bytes", mode, result.bytes, expectedTotalBytesAcrossChildren)
			}
			verification[mode] = result
		}
	}
	if cfg.Benchmark.Workflow == orderedTopKWorkflow && verification[unaryMode].outputHash != verification[streamingMode].outputHash {
		return fmt.Errorf("verify ordered topK: unary hash %x does not match streaming hash %x", verification[unaryMode].outputHash, verification[streamingMode].outputHash)
	}

	// Print the workload identity, then measure modes in the configured order.
	fmt.Printf("workflow=%s children=%d logical_channels=1 total_payload_bytes_per_child=%d stream_chunk_bytes=%d concurrency=%d mode_order=%s static_window_bytes=%d",
		cfg.Benchmark.Workflow,
		cfg.Benchmark.ChildProcesses,
		cfg.Benchmark.TotalPayloadBytesPerChild,
		cfg.Benchmark.StreamChunkBytes,
		cfg.Benchmark.Concurrency,
		cfg.Benchmark.ModeOrder,
		cfg.GRPC.Client.StaticWindowBytes,
	)
	if cfg.Benchmark.Workflow == orderedTopKWorkflow {
		fmt.Printf(" per_unit_bytes=%d global_topk=%d result_distribution=%s",
			cfg.Benchmark.PerUnitBytes,
			cfg.Benchmark.GlobalTopK,
			cfg.Benchmark.ResultDistribution,
		)
	}
	fmt.Println()
	fmt.Printf("%-10s %8s %8s %10s %12s %12s %12s %12s %14s\n",
		"MODE", "SUCCESS", "ERROR", "QPS", "MiB/S", "P50", "P95", "P99", "FIRST_P50")
	modes := []transferMode{unaryMode, streamingMode}
	if cfg.Benchmark.ModeOrder == "streaming_first" {
		modes[0], modes[1] = modes[1], modes[0]
	}
	for _, mode := range modes {
		report, runErr := runWorkflow(ctx, cfg.Benchmark, clients, mode)
		printReport(report, cfg.Benchmark, expectedTotalBytesAcrossChildren)
		if runErr != nil {
			return runErr
		}
	}
	return nil
}

// main loads configuration and selects parent benchmark or child-server mode.
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
