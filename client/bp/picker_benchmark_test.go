package bp

import (
	"context"
	"testing"

	"github.com/jrmarcco/xgrpc/client"
	"google.golang.org/grpc/balancer"
)

func benchmarkPick(b *testing.B, p balancer.Picker, pickInfo balancer.PickInfo, callDone bool) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := p.Pick(pickInfo)
			if err != nil {
				panic(err)
			}
			if callDone && res.Done != nil {
				res.Done(balancer.DoneInfo{})
			}
		}
	})
}

func BenchmarkRoundRobinPickerPick(b *testing.B) {
	builder := &roundRobinPickerBuilder{}
	info, _ := buildInfoFromAddrs(
		"10.0.0.1:8080",
		"10.0.0.2:8080",
		"10.0.0.3:8080",
		"10.0.0.4:8080",
	)
	p := builder.Build(info)
	benchmarkPick(b, p, balancer.PickInfo{}, false)
}

func BenchmarkRandomPickerPick(b *testing.B) {
	builder := &randomPickerBuilder{}
	info, _ := buildInfoFromAddrs(
		"10.0.1.1:8080",
		"10.0.1.2:8080",
		"10.0.1.3:8080",
		"10.0.1.4:8080",
	)
	p := builder.Build(info)
	benchmarkPick(b, p, balancer.PickInfo{}, false)
}

func BenchmarkWeightRandomPickerPick(b *testing.B) {
	builder := &weightRandomPickerBuilder{}
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.2.1:8080": 10,
		"10.0.2.2:8080": 20,
		"10.0.2.3:8080": 30,
		"10.0.2.4:8080": 40,
	})
	p := builder.Build(info)
	benchmarkPick(b, p, balancer.PickInfo{}, false)
}

func BenchmarkWeightPickerPick(b *testing.B) {
	builder := &weightPickerBuilder{}
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.3.1:8080": 10,
		"10.0.3.2:8080": 20,
		"10.0.3.3:8080": 30,
		"10.0.3.4:8080": 40,
	})
	p := builder.Build(info)
	benchmarkPick(b, p, balancer.PickInfo{}, true)
}

func BenchmarkDynamicWeightPickerPick(b *testing.B) {
	builder := &dynamicWeightPickerBuilder{}
	info, _ := buildInfoFromAddrWeights(map[string]uint32{
		"10.0.4.1:8080": 10,
		"10.0.4.2:8080": 20,
		"10.0.4.3:8080": 30,
		"10.0.4.4:8080": 40,
	})
	p := builder.Build(info)
	benchmarkPick(b, p, balancer.PickInfo{}, true)
}

func BenchmarkReadWriteWeightPickerPickRead(b *testing.B) {
	builder := &readWriteWeightPickerBuilder{}
	info, _ := buildInfoFromRwNodes([]rwNodeSpec{
		{addr: "10.0.5.1:8080", nodeName: "n1", group: "g1", readWeight: 10, writeWeight: 5},
		{addr: "10.0.5.2:8080", nodeName: "n2", group: "g1", readWeight: 20, writeWeight: 10},
		{addr: "10.0.5.3:8080", nodeName: "n3", group: "g2", readWeight: 30, writeWeight: 15},
		{addr: "10.0.5.4:8080", nodeName: "n4", group: "g2", readWeight: 40, writeWeight: 20},
	})
	p := builder.Build(info)

	ctx := client.ContextWithReqType(context.Background(), 0)
	benchmarkPick(b, p, balancer.PickInfo{Ctx: ctx}, true)
}

func BenchmarkReadWriteWeightPickerPickWrite(b *testing.B) {
	builder := &readWriteWeightPickerBuilder{}
	info, _ := buildInfoFromRwNodes([]rwNodeSpec{
		{addr: "10.0.6.1:8080", nodeName: "n1", group: "g1", readWeight: 10, writeWeight: 5},
		{addr: "10.0.6.2:8080", nodeName: "n2", group: "g1", readWeight: 20, writeWeight: 10},
		{addr: "10.0.6.3:8080", nodeName: "n3", group: "g2", readWeight: 30, writeWeight: 15},
		{addr: "10.0.6.4:8080", nodeName: "n4", group: "g2", readWeight: 40, writeWeight: 20},
	})
	p := builder.Build(info)

	ctx := client.ContextWithReqType(context.Background(), 1)
	benchmarkPick(b, p, balancer.PickInfo{Ctx: ctx}, true)
}

func BenchmarkHashPickerPick(b *testing.B) {
	builder := &hashPickerBuilder{}
	info, _ := buildInfoFromAddrs(
		"10.0.7.1:8080",
		"10.0.7.2:8080",
		"10.0.7.3:8080",
		"10.0.7.4:8080",
	)
	p := builder.Build(info)

	ctx := client.ContextWithBizId(context.Background(), 20260306)
	benchmarkPick(b, p, balancer.PickInfo{Ctx: ctx}, false)
}

func BenchmarkConsistentHashPickerPick(b *testing.B) {
	builder := &consistentHashPickerBuilder{virtualNodeCnt: 64}
	info, _ := buildInfoFromAddrs(
		"10.0.8.1:8080",
		"10.0.8.2:8080",
		"10.0.8.3:8080",
		"10.0.8.4:8080",
	)
	p := builder.Build(info)

	ctx := client.ContextWithBizId(context.Background(), 20260306)
	benchmarkPick(b, p, balancer.PickInfo{Ctx: ctx}, false)
}
