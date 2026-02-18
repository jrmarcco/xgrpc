package br

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/jrmarcco/jit/xslice"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"

	"github.com/jrmarcco/xgrpc/client"
)

var _ base.PickerBuilder = (*RwWeightBalancerBuilder)(nil)

type RwWeightBalancerBuilder struct {
	mu sync.Mutex

	picker *RwWeightBalancer
}

func (b *RwWeightBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.picker == nil {
		b.picker = &RwWeightBalancer{
			nodes: make(map[string]*rwWeightServiceNode, len(info.ReadySCs)),
		}
	}

	readySCs := make(map[string]*rwWeightServiceNode, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		if addr == "" {
			continue
		}
		readWeight, ok := scInfo.Address.Attributes.Value(client.AttrNameReadWeight).(uint32)
		if !ok {
			continue
		}
		writeWeight, ok := scInfo.Address.Attributes.Value(client.AttrNameWriteWeight).(uint32)
		if !ok {
			continue
		}
		groupName, ok := scInfo.Address.Attributes.Value(client.AttrNameGroup).(string)
		if !ok {
			continue
		}
		nodeName, ok := scInfo.Address.Attributes.Value(client.AttrNameNode).(string)
		if !ok {
			continue
		}
		readySCs[nodeName] = newRwWeightServiceNode(sc, readWeight, writeWeight, groupName)
	}

	b.picker.syncReadySCs(readySCs)
	return b.picker
}

func NewRwWeightBalancerBuilder() *RwWeightBalancerBuilder {
	return &RwWeightBalancerBuilder{}
}

var _ balancer.Picker = (*RwWeightBalancer)(nil)

type RwWeightBalancer struct {
	mu sync.RWMutex

	list  []*rwWeightServiceNode
	nodes map[string]*rwWeightServiceNode
}

func (b *RwWeightBalancer) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	b.mu.RLock()
	nodes := b.list
	b.mu.RUnlock()

	if len(nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	// 获取候选节点。
	ctx := info.Ctx
	group, hasGroup := client.ContextGroup(ctx)

	var candidateNodes []*rwWeightServiceNode
	if !hasGroup {
		// ctx 中没有 group 字段，不进行筛选，返回所有节点。
		candidateNodes = nodes
	} else {
		// ctx 中有 group 字段，进行筛选。
		candidateNodes = xslice.FilterMap(nodes, func(_ int, src *rwWeightServiceNode) (*rwWeightServiceNode, bool) {
			src.mu.RLock()
			nodeGroup := src.group
			src.mu.RUnlock()

			return src, group == nodeGroup
		})
	}

	if len(candidateNodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight uint32
	var selectedNode *rwWeightServiceNode

	isWriteReq := b.isWriteReq(ctx)

	// 权重计算。
	for _, node := range candidateNodes {
		node.mu.Lock()
		if isWriteReq {
			totalWeight += node.efficientWriteWeight
			node.currentWriteWeight += node.efficientWriteWeight
			if selectedNode == nil || selectedNode.currentWriteWeight < node.currentWriteWeight {
				selectedNode = node
			}
			node.mu.Unlock()
			continue
		}

		totalWeight += node.efficientReadWeight
		node.currentReadWeight += node.efficientReadWeight
		if selectedNode == nil || selectedNode.currentReadWeight < node.currentReadWeight {
			selectedNode = node
		}
		node.mu.Unlock()
	}

	if selectedNode == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	selectedNode.mu.Lock()
	if isWriteReq {
		selectedNode.currentWriteWeight -= totalWeight
	} else {
		selectedNode.currentReadWeight -= totalWeight
	}
	selectedNode.mu.Unlock()

	return balancer.PickResult{
		SubConn: selectedNode.sc,
		Done: func(info balancer.DoneInfo) {
			b.recalculateWeight(isWriteReq, selectedNode, info)
		},
	}, nil
}

// recalculateWeight 重新计算被选中节点的权重。
func (b *RwWeightBalancer) recalculateWeight(isWriteReq bool, node *rwWeightServiceNode, info balancer.DoneInfo) {
	node.mu.Lock()
	defer node.mu.Unlock()

	isDecrementErr := info.Err != nil && (errors.Is(info.Err, context.DeadlineExceeded) || errors.Is(info.Err, io.EOF))
	const twice = 2
	if isWriteReq {
		if info.Err == nil {
			node.efficientWriteWeight++
			node.currentWriteWeight = max(node.efficientWriteWeight, node.writeWeight*twice)
			return
		}
		if isDecrementErr && node.efficientWriteWeight > 1 {
			node.efficientWriteWeight--
		}
		return
	}

	if info.Err == nil {
		node.efficientReadWeight++
		node.currentReadWeight = max(node.efficientReadWeight, node.readWeight*twice)
		return
	}

	if isDecrementErr && node.efficientReadWeight > 1 {
		node.efficientReadWeight--
	}
}

func (b *RwWeightBalancer) syncReadySCs(readySCs map[string]*rwWeightServiceNode) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for nodeName := range b.nodes {
		if _, ok := readySCs[nodeName]; ok {
			continue
		}
		delete(b.nodes, nodeName)
	}

	for nodeName, nextNode := range readySCs {
		oldNode, ok := b.nodes[nodeName]
		if !ok {
			b.nodes[nodeName] = nextNode
			continue
		}

		oldNode.mu.Lock()
		weightChanged := oldNode.readWeight != nextNode.readWeight || oldNode.writeWeight != nextNode.writeWeight
		if !weightChanged {
			oldNode.sc = nextNode.sc
			oldNode.group = nextNode.group
			oldNode.mu.Unlock()
			continue
		}
		oldNode.mu.Unlock()

		b.nodes[nodeName] = nextNode
	}

	b.list = b.list[:0]
	for _, node := range b.nodes {
		b.list = append(b.list, node)
	}
}

func (b *RwWeightBalancer) isWriteReq(ctx context.Context) bool {
	if reqType, ok := client.ContextReqType(ctx); ok {
		return reqType == 1
	}
	return false
}

type rwWeightServiceNode struct {
	mu sync.RWMutex

	sc balancer.SubConn

	readWeight          uint32
	currentReadWeight   uint32
	efficientReadWeight uint32

	writeWeight          uint32
	currentWriteWeight   uint32
	efficientWriteWeight uint32

	group string
}

func newRwWeightServiceNode(sc balancer.SubConn, readWeight, writeWeight uint32, group string) *rwWeightServiceNode {
	return &rwWeightServiceNode{
		sc: sc,

		readWeight:          readWeight,
		currentReadWeight:   readWeight,
		efficientReadWeight: readWeight,

		writeWeight:          writeWeight,
		currentWriteWeight:   writeWeight,
		efficientWriteWeight: writeWeight,

		group: group,
	}
}
