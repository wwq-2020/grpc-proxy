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
	hook     Hook
}

type noopHook struct {
}

func (noopHook) OnStart(ctx context.Context, method string) context.Context {
	return ctx
}

func (noopHook) OnFinish(ctx context.Context, method string, err error) {

}

func (noopHook) OnClientMsg(ctx context.Context, method string) {

}

func (noopHook) OnBackendMsg(ctx context.Context, method string) {

}

func (noopHook) OnProxyToBackendFinish(ctx context.Context, method string, err error) {

}

func (noopHook) OnProxyToClientFinish(ctx context.Context, method string, err error) {

}

func (noopHook) OnDirect(ctx context.Context, method string) {

}

func (noopHook) OnNewClientStream(ctx context.Context, method string) {

}

var (
	_ Hook = &noopHook{}
)

type Hook interface {
	OnStart(ctx context.Context, method string) context.Context
	OnFinish(ctx context.Context, method string, err error)
	OnClientMsg(ctx context.Context, method string)
	OnBackendMsg(ctx context.Context, method string)
	OnProxyToBackendFinish(ctx context.Context, method string, err error)
	OnProxyToClientFinish(ctx context.Context, method string, err error)
	OnDirect(ctx context.Context, method string)
	OnNewClientStream(ctx context.Context, method string)
}

type Option func(*handler)

func WithHook(hook Hook) Option {
	return func(h *handler) {
		if hook != nil {
			h.hook = hook
		}
	}
}

func NewHandler(director Director, options ...Option) grpc.StreamHandler {
	handler := &handler{
		director: director,
		hook:     &noopHook{},
	}
	for _, option := range options {
		option(handler)
	}
	return handler.ServeStream
}

var (
	proxyDesc = &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
)

func (h *handler) ServeStream(srv interface{}, srcStream grpc.ServerStream) (err error) {
	method, ok := grpc.MethodFromServerStream(srcStream)

	hookCtx := h.hook.OnStart(srcStream.Context(), method)
	defer h.hook.OnFinish(hookCtx, method, err)

	if !ok {
		return status.Errorf(codes.InvalidArgument, "method not foun")
	}
	destCtx, destConn, err := h.director(hookCtx, method)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to director,err:%v", err)
	}
	h.hook.OnDirect(hookCtx, method)

	defer destConn.Close()

	destCtx, dstCancel := context.WithCancel(destCtx)
	defer dstCancel()

	destStream, err := grpc.NewClientStream(destCtx, proxyDesc, destConn, method)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to NewClientStream,err:%v", err)
	}
	h.hook.OnNewClientStream(hookCtx, method)

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
			h.hook.OnProxyToBackendFinish(hookCtx, method, err)
			dstCancel()
			errCh <- err
		}()
		msg := &emptypb.Empty{}

		for {
			if err = srcStream.RecvMsg(msg); err != nil {
				return
			}
			h.hook.OnClientMsg(hookCtx, method)

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
			h.hook.OnProxyToClientFinish(hookCtx, method, err)
			errCh <- err
		}()
		count := 0
		msg := &emptypb.Empty{}

		for {
			if err := destStream.RecvMsg(msg); err != nil {
				return
			}
			h.hook.OnBackendMsg(hookCtx, method)

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
