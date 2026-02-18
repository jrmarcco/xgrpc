package register

import (
	"context"
	"io"
)

// Registry 是服务注册器，用于服务注册发现。
type Registry interface {
	// 注册服务实例。
	Register(ctx context.Context, si ServiceInstance) error

	// 注销服务实例。
	Unregister(ctx context.Context, si ServiceInstance) error

	// 获取服务实例列表。
	ListServices(ctx context.Context, serviceName string) ([]ServiceInstance, error)

	// 订阅服务实例变更。
	Subscribe(serviceName string) <-chan struct{}

	// 关闭注册器。
	io.Closer
}

// ServiceInstance 是服务实例。
type ServiceInstance struct {
	Name        string // 服务名
	Addr        string // 服务地址
	Group       string // 服务组信息 ( 可选 )
	ReadWeight  uint32 // 读权重
	WriteWeight uint32 // 写权重
}
