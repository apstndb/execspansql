package main

import (
	"context"

	"strings"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/internal/grpctest"
	"github.com/apstndb/spanemuboost"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMainSendsPriorityOnExecuteSQL(t *testing.T) {
	env, err := spanemuboost.RunEmulatorWithClients(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close() //nolint:errcheck
	t.Setenv("SPANNER_EMULATOR_HOST", env.Emulator().URI())

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
			requests := make(chan *sppb.ExecuteSqlRequest, 1)
			dialOptions := grpctest.Inspect(func(_ string, req any) error {
				if req, ok := req.(*sppb.ExecuteSqlRequest); ok {
					select {
					case requests <- proto.Clone(req).(*sppb.ExecuteSqlRequest):
					default:
					}
					return status.Error(codes.InvalidArgument, "priority test capture")
				}
				return nil
			})

			sql := tt.sql
			if sql == "" {
				sql = "SELECT 1"
			}
			args := []string{env.DatabaseID, "--project", env.ProjectID, "--instance", env.InstanceID, "--sql", sql, "--timeout", "5s"}
			args = append(args, tt.args...)
			err := runMain(t, args, option.WithGRPCDialOption(dialOptions[0]), option.WithGRPCDialOption(dialOptions[1]))
			if err == nil || !strings.Contains(err.Error(), "priority test capture") {
				t.Fatalf("_main() error = %v, want priority test capture", err)
			}

			select {
			case req := <-requests:
				if got := req.GetRequestOptions().GetPriority(); got != tt.priority {
					t.Fatalf("request priority = %v, want %v", got, tt.priority)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("did not receive ExecuteStreamingSql request")
			}
		})
	}
}
