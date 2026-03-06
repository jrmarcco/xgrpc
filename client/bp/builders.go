package bp

import (
	"fmt"

	"github.com/jrmarcco/jit/bean/option"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const (
	PolicyRoundRobin      = "xgrpc_round_robin"
	PolicyRandom          = "xgrpc_random"
	PolicyWeight          = "xgrpc_weight"
	PolicyWeightRandom    = "xgrpc_weight_random"
	PolicyDynamicWeight   = "xgrpc_dynamic_weight"
	PolicyReadWriteWeight = "xgrpc_read_write_weight"
	PolicyHash            = "xgrpc_hash"
	PolicyConsistentHash  = "xgrpc_consistent_hash"
)

// NewRoundRobin 创建 round-robin 负载均衡器 builder。
func NewRoundRobin() balancer.Builder {
	return base.NewBalancerBuilder(PolicyRoundRobin, &roundRobinPickerBuilder{}, base.Config{HealthCheck: true})
}

// NewRandom 创建随机负载均衡器 builder。
func NewRandom() balancer.Builder {
	return base.NewBalancerBuilder(PolicyRandom, &randomPickerBuilder{}, base.Config{HealthCheck: true})
}

// NewWeight 创建平滑权重轮询负载均衡器 builder。
func NewWeight() balancer.Builder {
	return base.NewBalancerBuilder(PolicyWeight, &weightPickerBuilder{}, base.Config{HealthCheck: true})
}

// NewWeightRandom 创建权重随机负载均衡器 builder。
func NewWeightRandom() balancer.Builder {
	return base.NewBalancerBuilder(PolicyWeightRandom, &weightRandomPickerBuilder{}, base.Config{HealthCheck: true})
}

// NewDynamicWeight 创建自适应权重负载均衡器 builder。
func NewDynamicWeight() balancer.Builder {
	return base.NewBalancerBuilder(PolicyDynamicWeight, &dynamicWeightPickerBuilder{}, base.Config{HealthCheck: true})
}

// NewReadWriteWeight 创建读写权重负载均衡器 builder。
func NewReadWriteWeight() balancer.Builder {
	return base.NewBalancerBuilder(PolicyReadWriteWeight, &readWriteWeightPickerBuilder{}, base.Config{HealthCheck: true})
}

// NewHash 创建哈希负载均衡器 builder。
func NewHash() balancer.Builder {
	return base.NewBalancerBuilder(PolicyHash, &hashPickerBuilder{}, base.Config{HealthCheck: true})
}

// WithCHVirtualNodeCnt 配置一致哈希虚拟节点数量。
func WithCHVirtualNodeCnt(cnt int) option.Opt[consistentHashPickerBuilder] {
	return func(b *consistentHashPickerBuilder) {
		b.virtualNodeCnt = cnt
	}
}

// NewConsistentHash 创建一致哈希负载均衡器 builder。
func NewConsistentHash(opts ...option.Opt[consistentHashPickerBuilder]) (balancer.Builder, error) {
	builder := &consistentHashPickerBuilder{virtualNodeCnt: defaultVirtualNodeCnt}
	option.Apply(builder, opts...)

	if builder.virtualNodeCnt <= 0 {
		return nil, fmt.Errorf("[consistent-hash-balancer] virtual node count must be greater than 0")
	}

	return base.NewBalancerBuilder(
		PolicyConsistentHash,
		builder,
		base.Config{HealthCheck: true},
	), nil
}
