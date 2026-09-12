package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This transport fixture exercises the real SDK retry and output boundary.
// Database semantics remain covered by the emulator integration suite.
type executionServer struct {
	queryStatsModeServer
	executes   atomic.Int32
	commits    atomic.Int32
	retry      bool
	failCommit bool
}

func (s *executionServer) BeginTransaction(context.Context, *sppb.BeginTransactionRequest) (*sppb.Transaction, error) {
	return &sppb.Transaction{Id: []byte("test-transaction")}, nil
}

func (s *executionServer) ExecuteStreamingSql(_ *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
	attempt := s.executes.Add(1)
	return stream.Send(&sppb.PartialResultSet{
		Metadata: &sppb.ResultSetMetadata{
			Transaction: &sppb.Transaction{Id: []byte("test-transaction")},
			RowType:     &sppb.StructType{Fields: []*sppb.StructType_Field{{Name: "value", Type: &sppb.Type{Code: sppb.TypeCode_STRING}}}},
		},
		Values: []*structpb.Value{structpb.NewStringValue(fmt.Sprintf("attempt-%d", attempt))},
		Stats:  &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 1}},
	})
}

func (s *executionServer) Commit(context.Context, *sppb.CommitRequest) (*sppb.CommitResponse, error) {
	attempt := s.commits.Add(1)
	if s.failCommit {
		return nil, status.Error(codes.FailedPrecondition, "test commit failure")
	}
	if s.retry && attempt == 1 {
		return nil, status.Error(codes.Aborted, "retry transaction")
	}
	return &sppb.CommitResponse{CommitTimestamp: timestamppb.Now()}, nil
}

func (*executionServer) Rollback(context.Context, *sppb.RollbackRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func TestDMLResultPublication(t *testing.T) {
	for _, format := range []string{"json", "yaml", "experimental_csv"} {
		for _, scenario := range []string{"retry", "commit_failure", "output_failure"} {
			t.Run(format+"/"+scenario, func(t *testing.T) {
				server := &executionServer{retry: scenario == "retry", failCommit: scenario == "commit_failure"}
				startQueryStatsModeServer(t, server)
				path := filepath.Join(t.TempDir(), "result")
				if scenario == "output_failure" {
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
					t.Fatal(err)
				}
				err := runCLI(t.Context(), []string{"db", "--project", "p", "--instance", "i",
					"--sql", "UPDATE T SET V=1 THEN RETURN V", "--format", format, "--output", path, "--timeout", "5s"})
				switch scenario {
				case "retry":
					if err != nil {
						t.Fatal(err)
					}
					output, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(output), "attempt-1") || strings.Count(string(output), "attempt-2") != 1 {
						t.Fatalf("output must contain only the committed attempt: %s", output)
					}
					if got := server.executes.Load(); got != 2 {
						t.Fatalf("executions=%d, want 2", got)
					}
				case "commit_failure":
					if err == nil || !strings.Contains(err.Error(), "test commit failure") || strings.Contains(err.Error(), "statement was committed") {
						t.Fatalf("commit error = %v", err)
					}
					output, readErr := os.ReadFile(path)
					if readErr != nil {
						t.Fatal(readErr)
					}
					if string(output) != "original" {
						t.Fatalf("failed commit published %q", output)
					}
				case "output_failure":
					if err == nil || !strings.Contains(err.Error(), "statement was committed") {
						t.Fatalf("output error = %v", err)
					}
					if got := server.executes.Load(); got != 1 {
						t.Fatalf("output failure replayed SQL: executions=%d", got)
					}
					if got := server.commits.Load(); got != 1 {
						t.Fatalf("commits=%d, want 1", got)
					}
				}
			})
		}
	}
}
