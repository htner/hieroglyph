// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.6
// source: lake_service.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// LakeClient is the client API for Lake service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type LakeClient interface {
	Start(ctx context.Context, in *StartTransactionRequest, opts ...grpc.CallOption) (*StartTransactionResponse, error)
	Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error)
	Rollback(ctx context.Context, in *RollbackRequest, opts ...grpc.CallOption) (*RollbackResponse, error)
	PrepareInsertFiles(ctx context.Context, in *PrepareInsertFilesRequest, opts ...grpc.CallOption) (*PrepareInsertFilesResponse, error)
	UpdateFiles(ctx context.Context, in *UpdateFilesRequest, opts ...grpc.CallOption) (*UpdateFilesResponse, error)
}

type lakeClient struct {
	cc grpc.ClientConnInterface
}

func NewLakeClient(cc grpc.ClientConnInterface) LakeClient {
	return &lakeClient{cc}
}

func (c *lakeClient) Start(ctx context.Context, in *StartTransactionRequest, opts ...grpc.CallOption) (*StartTransactionResponse, error) {
	out := new(StartTransactionResponse)
	err := c.cc.Invoke(ctx, "/lake.Lake/Start", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lakeClient) Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error) {
	out := new(CommitResponse)
	err := c.cc.Invoke(ctx, "/lake.Lake/Commit", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lakeClient) Rollback(ctx context.Context, in *RollbackRequest, opts ...grpc.CallOption) (*RollbackResponse, error) {
	out := new(RollbackResponse)
	err := c.cc.Invoke(ctx, "/lake.Lake/Rollback", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lakeClient) PrepareInsertFiles(ctx context.Context, in *PrepareInsertFilesRequest, opts ...grpc.CallOption) (*PrepareInsertFilesResponse, error) {
	out := new(PrepareInsertFilesResponse)
	err := c.cc.Invoke(ctx, "/lake.Lake/PrepareInsertFiles", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lakeClient) UpdateFiles(ctx context.Context, in *UpdateFilesRequest, opts ...grpc.CallOption) (*UpdateFilesResponse, error) {
	out := new(UpdateFilesResponse)
	err := c.cc.Invoke(ctx, "/lake.Lake/UpdateFiles", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LakeServer is the server API for Lake service.
// All implementations must embed UnimplementedLakeServer
// for forward compatibility
type LakeServer interface {
	Start(context.Context, *StartTransactionRequest) (*StartTransactionResponse, error)
	Commit(context.Context, *CommitRequest) (*CommitResponse, error)
	Rollback(context.Context, *RollbackRequest) (*RollbackResponse, error)
	PrepareInsertFiles(context.Context, *PrepareInsertFilesRequest) (*PrepareInsertFilesResponse, error)
	UpdateFiles(context.Context, *UpdateFilesRequest) (*UpdateFilesResponse, error)
	mustEmbedUnimplementedLakeServer()
}

// UnimplementedLakeServer must be embedded to have forward compatible implementations.
type UnimplementedLakeServer struct {
}

func (UnimplementedLakeServer) Start(context.Context, *StartTransactionRequest) (*StartTransactionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Start not implemented")
}
func (UnimplementedLakeServer) Commit(context.Context, *CommitRequest) (*CommitResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Commit not implemented")
}
func (UnimplementedLakeServer) Rollback(context.Context, *RollbackRequest) (*RollbackResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Rollback not implemented")
}
func (UnimplementedLakeServer) PrepareInsertFiles(context.Context, *PrepareInsertFilesRequest) (*PrepareInsertFilesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PrepareInsertFiles not implemented")
}
func (UnimplementedLakeServer) UpdateFiles(context.Context, *UpdateFilesRequest) (*UpdateFilesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateFiles not implemented")
}
func (UnimplementedLakeServer) mustEmbedUnimplementedLakeServer() {}

// UnsafeLakeServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to LakeServer will
// result in compilation errors.
type UnsafeLakeServer interface {
	mustEmbedUnimplementedLakeServer()
}

func RegisterLakeServer(s grpc.ServiceRegistrar, srv LakeServer) {
	s.RegisterService(&Lake_ServiceDesc, srv)
}

func _Lake_Start_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartTransactionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LakeServer).Start(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lake.Lake/Start",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LakeServer).Start(ctx, req.(*StartTransactionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lake_Commit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CommitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LakeServer).Commit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lake.Lake/Commit",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LakeServer).Commit(ctx, req.(*CommitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lake_Rollback_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RollbackRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LakeServer).Rollback(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lake.Lake/Rollback",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LakeServer).Rollback(ctx, req.(*RollbackRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lake_PrepareInsertFiles_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PrepareInsertFilesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LakeServer).PrepareInsertFiles(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lake.Lake/PrepareInsertFiles",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LakeServer).PrepareInsertFiles(ctx, req.(*PrepareInsertFilesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lake_UpdateFiles_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateFilesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LakeServer).UpdateFiles(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lake.Lake/UpdateFiles",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LakeServer).UpdateFiles(ctx, req.(*UpdateFilesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Lake_ServiceDesc is the grpc.ServiceDesc for Lake service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Lake_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "lake.Lake",
	HandlerType: (*LakeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Start",
			Handler:    _Lake_Start_Handler,
		},
		{
			MethodName: "Commit",
			Handler:    _Lake_Commit_Handler,
		},
		{
			MethodName: "Rollback",
			Handler:    _Lake_Rollback_Handler,
		},
		{
			MethodName: "PrepareInsertFiles",
			Handler:    _Lake_PrepareInsertFiles_Handler,
		},
		{
			MethodName: "UpdateFiles",
			Handler:    _Lake_UpdateFiles_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "lake_service.proto",
}
