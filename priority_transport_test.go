package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type priorityRecordingSpannerServer struct {
	sppb.UnimplementedSpannerServer
	requests chan *sppb.ExecuteSqlRequest
}

func (s *priorityRecordingSpannerServer) CreateSession(_ context.Context, req *sppb.CreateSessionRequest) (*sppb.Session, error) {
	return &sppb.Session{Name: req.Database + "/sessions/priority-test"}, nil
}

func (*priorityRecordingSpannerServer) BeginTransaction(context.Context, *sppb.BeginTransactionRequest) (*sppb.Transaction, error) {
	return &sppb.Transaction{Id: []byte("priority-test")}, nil
}

func (s *priorityRecordingSpannerServer) ExecuteStreamingSql(req *sppb.ExecuteSqlRequest, _ sppb.Spanner_ExecuteStreamingSqlServer) error {
	s.requests <- req
	return status.Error(codes.InvalidArgument, "priority test capture")
}

func (s *priorityRecordingSpannerServer) ExecuteSql(_ context.Context, req *sppb.ExecuteSqlRequest) (*sppb.ResultSet, error) {
	s.requests <- req
	return nil, status.Error(codes.InvalidArgument, "priority test capture")
}

func startPriorityRecordingSpannerServer(t *testing.T) (*priorityRecordingSpannerServer, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	recorder := &priorityRecordingSpannerServer{requests: make(chan *sppb.ExecuteSqlRequest, 1)}
	sppb.RegisterSpannerServer(server, recorder)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return recorder, listener.Addr().String()
}

func TestMainSendsPriorityOnExecuteSQL(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		args     []string
		priority sppb.RequestOptions_Priority
	}{
		{name: "json_eager", args: []string{"--format", "json", "--priority", "high"}, priority: sppb.RequestOptions_PRIORITY_HIGH},
		{name: "json_lazy", args: []string{"--format", "json", "--jq-input-mode", "lazy", "--priority", "low"}, priority: sppb.RequestOptions_PRIORITY_LOW},
		{name: "csv", args: []string{"--format", "experimental_csv", "--priority", "medium"}, priority: sppb.RequestOptions_PRIORITY_MEDIUM},
		{name: "dml", sql: "UPDATE T SET V=1", args: []string{"--priority", "high"}, priority: sppb.RequestOptions_PRIORITY_HIGH},
		{name: "partitioned_dml", sql: "UPDATE T SET V=1", args: []string{"--enable-partitioned-dml", "--priority", "low"}, priority: sppb.RequestOptions_PRIORITY_LOW},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder, host := startPriorityRecordingSpannerServer(t)
			t.Setenv("SPANNER_EMULATOR_HOST", host)

			sql := tt.sql
			if sql == "" {
				sql = "SELECT 1"
			}
			args := []string{"database", "--project", "project", "--instance", "instance", "--sql", sql, "--timeout", "5s"}
			args = append(args, tt.args...)
			err := runMain(t, args)
			if err == nil || !strings.Contains(err.Error(), "priority test capture") {
				t.Fatalf("_main() error = %v, want priority test capture", err)
			}

			select {
			case req := <-recorder.requests:
				if got := req.GetRequestOptions().GetPriority(); got != tt.priority {
					t.Fatalf("request priority = %v, want %v", got, tt.priority)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("did not receive ExecuteStreamingSql request")
			}
		})
	}
}
