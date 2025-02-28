package proxy

import "google.golang.org/grpc"

func NewServer(dialer Director, opts ...grpc.ServerOption) *grpc.Server {
	handler := NewHandler(dialer)
	opts = append(opts, grpc.UnknownServiceHandler(handler))
	grpcServer := grpc.NewServer(
		opts...,
	)
	return grpcServer
}
