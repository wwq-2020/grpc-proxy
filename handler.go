package proxy

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Director func(ctx context.Context, method string) (context.Context, *grpc.ClientConn, error)

type handler struct {
	director Director
}

func NewHandler(director Director) grpc.StreamHandler {
	handler := &handler{
		director: director,
	}
	return handler.ServeStream
}

var (
	proxyDesc = &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
)

func (h *handler) ServeStream(srv interface{}, srcStream grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(srcStream)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "method not foun")
	}
	destCtx, destConn, err := h.director(srcStream.Context(), method)
	if err != nil {
		return err
	}
	defer destConn.Close()

	destCtx, dstCancel := context.WithCancel(destCtx)
	defer dstCancel()
	destStream, err := grpc.NewClientStream(destCtx, proxyDesc, destConn, method)
	if err != nil {
		return err
	}
	defer destStream.CloseSend()

	wg := &sync.WaitGroup{}
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		var err error
		defer func() {
			if err == io.EOF {
				destStream.CloseSend()
				return
			}
			dstCancel()
			errCh <- err
		}()
		for {
			msg := &emptypb.Empty{}
			if err = srcStream.RecvMsg(msg); err != nil {
				return
			}
			if err := destStream.SendMsg(msg); err != nil {
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		var err error
		defer func() {
			srcStream.SetTrailer(destStream.Trailer())
			if err == io.EOF {
				return
			}
			errCh <- err
		}()
		count := 0
		for {
			msg := &emptypb.Empty{}
			if err := destStream.RecvMsg(msg); err != nil {
				return
			}
			if count == 0 {
				md, err := destStream.Header()
				if err != nil {
					return
				}
				if err := srcStream.SendHeader(md); err != nil {
					return
				}
			}
			if err := srcStream.SendMsg(msg); err != nil {
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return status.Errorf(codes.Internal, "failed to proxy,err:%v", err)
		}
	}
	return nil
}
