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

// dialChildren creates one resolver-backed ClientConn for all child addresses.
// The parent opens each RPC stream and the child picker routes it to the intended
// child; child processes only accept the server-side stream.
func dialChildren(ctx context.Context, cfg config) ([]*benchmarkClient, error) {
	childResolver := manual.NewBuilderWithScheme(childResolverScheme)
	addresses := make([]resolver.Address, cfg.Benchmark.ChildProcesses)
	for i := range addresses {
		addresses[i] = resolver.Address{
			Addr:       net.JoinHostPort(cfg.Benchmark.Host, strconv.Itoa(cfg.Benchmark.BasePort+i)),
			Attributes: attributes.New(childAddressIndexKey{}, i),
		}
	}
	childResolver.InitialState(resolver.State{Addresses: addresses})
	connection, err := dialChildrenChannel(ctx, cfg.GRPC.Client, cfg.Benchmark.StartupTimeoutMS, childResolver)
	if err != nil {
		return nil, err
	}

	clients := make([]*benchmarkClient, 0, cfg.Benchmark.ChildProcesses)
	for i := 0; i < cfg.Benchmark.ChildProcesses; i++ {
		clients = append(clients, &benchmarkClient{
			connection: connection,
			childIndex: i,
			expected:   byte(i%251 + 1),
		})
	}
	return clients, nil
}

func dialChildrenChannel(parent context.Context, cfg grpcClientConfig, startupTimeoutMS int, childResolver *manual.Resolver) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(startupTimeoutMS)*time.Millisecond)
	defer cancel()
	return grpc.DialContext(
		ctx,
		childResolver.Scheme()+":///children",
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
		grpc.WithResolvers(childResolver),
		grpc.WithDefaultServiceConfig(childServiceConfig),
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
}

func closeClients(clients []*benchmarkClient) {
	if len(clients) > 0 {
		_ = clients[0].connection.Close()
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

// transferAll starts one RPC per child for one fan-in operation. Streaming mode
// creates one stream per child; all payload Chunks are messages within that
// stream, not additional streams.
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

// steps:
//
//  1. start a bunch of children of same processes with known tcp addresses
//
//  2. for each started child, runChild is called which starts server with known service
//     descriptions (and methods) to listen to clients to corresponding addresses
//
//  3. calls dialChildren which creates grpc connection and wrap them into benchmarkClient
//
// 4. requests are sent through clients which triggers server method and return correspondingly
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

	expectedTotalBytesAcrossChildren := cfg.Benchmark.ChildProcesses * cfg.Benchmark.TotalPayloadBytesPerChild
	for _, mode := range []transferMode{unaryMode, streamingMode} {
		requestCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Benchmark.RequestTimeoutMS)*time.Millisecond)
		result, err := transferAll(requestCtx, clients, mode, true)
		cancel()
		if err != nil {
			return fmt.Errorf("verify %s: %w", mode, err)
		}
		if result.bytes != expectedTotalBytesAcrossChildren {
			return fmt.Errorf("verify %s: received %d total bytes across children, expected %d", mode, result.bytes, expectedTotalBytesAcrossChildren)
		}
	}

	fmt.Printf("children=%d logical_channels=1 total_payload_bytes_per_child=%d stream_chunk_bytes=%d concurrency=%d\n",
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
