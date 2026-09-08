// Package grpctest provides client-side RPC inspection for transport tests.
// It complements emulator tests without implementing Spanner server behavior.
package grpctest

import (
	"context"

	"google.golang.org/grpc"
)

// Inspect calls inspect before each unary request or stream message is sent.
// Returning a non-nil gRPC status error prevents that message from being sent.
// The callback may run concurrently; it must not mutate or retain the message
// without copying it. Returning nil preserves the actual server response.
func Inspect(inspect func(method string, request any) error) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if err := inspect(method, req); err != nil {
				return err
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx, cancel := context.WithCancel(ctx)
			stream, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil {
				cancel()
				return nil, err
			}
			return &inspectingStream{ClientStream: stream, inspect: inspect, method: method, cancel: cancel}, nil
		}),
	}
}

type inspectingStream struct {
	grpc.ClientStream
	inspect func(string, any) error
	method  string
	cancel  context.CancelFunc
}

func (s *inspectingStream) SendMsg(m any) error {
	if err := s.inspect(s.method, m); err != nil {
		s.cancel()
		return err
	}
	// Preserve transport errors (including io.EOF); RecvMsg may still need
	// to retrieve the server status.
	return s.ClientStream.SendMsg(m)
}

func (s *inspectingStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.cancel()
	}
	return err
}
