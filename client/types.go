package client

import "context"

const (
	AttrNameWeight      = "attr_weight"
	AttrNameReadWeight  = "attr_read_weight"
	AttrNameWriteWeight = "attr_write_weight"
	AttrNameGroup       = "attr_group"
	AttrNameNode        = "attr_node"
)

type contextKeyGroup struct{}

// ContextWithGroup 在 context.Context 内写入 group 信息。
func ContextWithGroup(ctx context.Context, group string) context.Context {
	return context.WithValue(ctx, contextKeyGroup{}, group)
}

func ContextGroup(ctx context.Context) (string, bool) {
	val := ctx.Value(contextKeyGroup{})
	group, ok := val.(string)
	return group, ok
}

type contextKeyReqType struct{}

// ContextWithReqType 在 context.Context 内写入 request type 信息。
// - read request  = 0
// - write request = 1
func ContextWithReqType(ctx context.Context, reqType uint8) context.Context {
	return context.WithValue(ctx, contextKeyReqType{}, reqType)
}

// ContextReqType 从 context.Context 获取 request type。
func ContextReqType(ctx context.Context) (uint8, bool) {
	val := ctx.Value(contextKeyReqType{})
	reqType, ok := val.(uint8)
	return reqType, ok
}

type contextKeyBizId struct{}

// ContextWithBizId 在 context.Context 内写入 business id。
func ContextWithBizId(ctx context.Context, bizId uint64) context.Context {
	return context.WithValue(ctx, contextKeyBizId{}, bizId)
}

// ContextBizId 从 context.Context 获取 business id。
func ContextBizId(ctx context.Context) (uint64, bool) {
	val := ctx.Value(contextKeyBizId{})
	bizId, ok := val.(uint64)
	return bizId, ok
}
