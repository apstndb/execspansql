package main

import (
	"context"
	"net"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/alecthomas/kong"
	"google.golang.org/grpc"
)

type databaseRoleSpannerServer struct {
	sppb.UnimplementedSpannerServer
	createSession chan *sppb.CreateSessionRequest
}

func (s *databaseRoleSpannerServer) CreateSession(_ context.Context, req *sppb.CreateSessionRequest) (*sppb.Session, error) {
	s.createSession <- req
	return &sppb.Session{
		Name:        req.Database + "/sessions/test",
		CreatorRole: req.GetSession().GetCreatorRole(),
		Multiplexed: true,
	}, nil
}

func TestDatabaseRoleFlag(t *testing.T) {
	t.Parallel()

	var got opts
	parser, err := kong.New(&got, kong.Name("execspansql"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.Parse([]string{
		"db", "--project", "p", "--instance", "i", "--sql", "SELECT 1",
		"--database-role", "report_reader",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.DatabaseRole != "report_reader" {
		t.Fatalf("DatabaseRole = %q, want %q", got.DatabaseRole, "report_reader")
	}
}

func TestNewClientSendsDatabaseRoleWhenCreatingSession(t *testing.T) {
	for _, tt := range []struct {
		name string
		role string
	}{
		{name: "set", role: "report_reader"},
		{name: "omitted"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := &databaseRoleSpannerServer{createSession: make(chan *sppb.CreateSessionRequest, 1)}
			grpcServer := grpc.NewServer()
			sppb.RegisterSpannerServer(grpcServer, server)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				grpcServer.Stop()
				_ = listener.Close()
			})
			go func() { _ = grpcServer.Serve(listener) }()
			t.Setenv("SPANNER_EMULATOR_HOST", listener.Addr().String())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := newClient(ctx, "p", "i", "d", tt.role, logGrpcModeOff, false)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			select {
			case req := <-server.createSession:
				if got := req.GetSession().GetCreatorRole(); got != tt.role {
					t.Fatalf("session CreatorRole = %q, want %q", got, tt.role)
				}
			case <-ctx.Done():
				t.Fatalf("CreateSession was not called: %v", ctx.Err())
			}
		})
	}
}
