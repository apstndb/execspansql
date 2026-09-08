package grpctest

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type transportStream struct {
	grpc.ClientStream
	sent    any
	sendErr error
	recvErr error
}

func (s *transportStream) SendMsg(m any) error { s.sent = m; return s.sendErr }
func (s *transportStream) RecvMsg(any) error   { return s.recvErr }

func TestInspectingStreamRejectsBeforeSending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &transportStream{}
	rejected := status.Error(codes.InvalidArgument, "captured")
	stream := &inspectingStream{ClientStream: transport, method: "/test/Query", cancel: cancel,
		inspect: func(method string, req any) error {
			if method != "/test/Query" || req != "request" {
				t.Fatalf("inspection = %s %v", method, req)
			}
			return rejected
		}}
	if err := stream.SendMsg("request"); err != rejected {
		t.Fatalf("error = %v", err)
	}
	if transport.sent != nil {
		t.Fatal("rejected request reached transport")
	}
	if ctx.Err() == nil {
		t.Fatal("rejected stream was not cancelled")
	}
}

func TestInspectingStreamPreservesServerStatusAfterSendEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErr := status.Error(codes.PermissionDenied, "denied")
	transport := &transportStream{sendErr: io.EOF, recvErr: serverErr}
	stream := &inspectingStream{ClientStream: transport, cancel: cancel,
		inspect: func(string, any) error { return nil }}
	if err := stream.SendMsg("request"); err != io.EOF {
		t.Fatalf("send error = %v", err)
	}
	if transport.sent != "request" {
		t.Fatal("request was not forwarded")
	}
	if ctx.Err() != nil {
		t.Fatal("cancelled before receiving server status")
	}
	if err := stream.RecvMsg(nil); err != serverErr {
		t.Fatalf("receive error = %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("finished stream was not cancelled")
	}
}
