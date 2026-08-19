package main

import (
	"context"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	childResolverScheme = "benchmark-children"
	childBalancerName   = "benchmark-child-picker"
	childServiceConfig  = `{"loadBalancingConfig":[{"benchmark-child-picker":{}}]}`
)

type childAddressIndexKey struct{}
type childContextIndexKey struct{}

func init() {
	balancer.Register(base.NewBalancerBuilder(childBalancerName, childPickerBuilder{}, base.Config{}))
}

type childPickerBuilder struct{}

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

type childPicker struct {
	subConns map[int]balancer.SubConn
}

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

func withChildIndex(ctx context.Context, index int) context.Context {
	return context.WithValue(ctx, childContextIndexKey{}, index)
}
