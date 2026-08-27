// This file defines deterministic gRPC routing from each logical child index to its connection.
package main

import (
	"context"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC invokes the registered picker for Invoke and NewStream calls; benchmark
// code selects a child by tagging the RPC context.

const (
	childResolverScheme = "benchmark-children"
	childBalancerName   = "benchmark-child-picker"
	childServiceConfig  = `{"loadBalancingConfig":[{"benchmark-child-picker":{}}]}`
)

// childAddressIndexKey associates a resolver address with its child index.
type childAddressIndexKey struct{}

// childContextIndexKey associates an RPC context with its target child index.
type childContextIndexKey struct{}

// init registers the benchmark's child-index-aware gRPC picker.
func init() {
	balancer.Register(base.NewBalancerBuilder(childBalancerName, childPickerBuilder{}, base.Config{}))
}

// childPickerBuilder builds a picker from ready child connections.
type childPickerBuilder struct{}

// Build indexes each ready gRPC SubConn by the child address attribute.
func (childPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	subConns := make(map[int]balancer.SubConn, len(info.ReadySCs))
	for subConn, subConnInfo := range info.ReadySCs {
		index, ok := subConnInfo.Address.Attributes.Value(childAddressIndexKey{}).(int)
		if ok {
			subConns[index] = subConn
		}
	}
	return childPicker{subConns: subConns}
}

// childPicker routes an RPC to the SubConn selected in its context.
type childPicker struct {
	subConns map[int]balancer.SubConn
}

// Pick returns the ready SubConn for the RPC's requested child index.
func (p childPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	index, ok := info.Ctx.Value(childContextIndexKey{}).(int)
	if !ok {
		return balancer.PickResult{}, status.Error(codes.Internal, "RPC is missing child index")
	}
	subConn, ok := p.subConns[index]
	if !ok {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	return balancer.PickResult{SubConn: subConn}, nil
}

// withChildIndex tags an RPC context so the picker can route it deterministically.
func withChildIndex(ctx context.Context, index int) context.Context {
	return context.WithValue(ctx, childContextIndexKey{}, index)
}
