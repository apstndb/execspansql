package main

import (
	"context"

	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/alecthomas/kong"
	"github.com/apstndb/execspansql/internal/grpctest"
	"github.com/apstndb/spanemuboost"
	"google.golang.org/api/option"
)

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
	env, err := spanemuboost.RunEmulatorWithClients(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close() //nolint:errcheck
	t.Setenv("SPANNER_EMULATOR_HOST", env.Emulator().URI())

	for _, tt := range []struct {
		name string
		role string
	}{
		{name: "set", role: "report_reader"},
		{name: "omitted"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			roles := make(chan string, 1)
			dialOptions := grpctest.Inspect(func(_ string, req any) error {
				if req, ok := req.(*sppb.CreateSessionRequest); ok {
					select {
					case roles <- req.GetSession().GetCreatorRole():
					default:
					}
				}
				return nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := newClient(ctx, env.ProjectID, env.InstanceID, env.DatabaseID, tt.role, logGrpcModeOff, false, option.WithGRPCDialOption(dialOptions[0]), option.WithGRPCDialOption(dialOptions[1]))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			select {
			case got := <-roles:
				if got != tt.role {
					t.Fatalf("session CreatorRole = %q, want %q", got, tt.role)
				}
			case <-ctx.Done():
				t.Fatalf("CreateSession was not called: %v", ctx.Err())
			}
		})
	}
}
