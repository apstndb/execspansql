package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// A resume token makes the client deliver the first row before the stream
// error. Without it the SDK buffers that row and the failure looks early.
type lateQueryErrorServer struct {
	queryStatsModeServer
	withPlan bool
}

func (s *lateQueryErrorServer) ExecuteStreamingSql(_ *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
	if err := stream.Send(&sppb.PartialResultSet{
		Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{
			Name: "value",
			Type: &sppb.Type{Code: sppb.TypeCode_STRING},
		}}}},
		Values:      []*structpb.Value{structpb.NewStringValue("partial")},
		ResumeToken: []byte("review-checkpoint"),
	}); err != nil {
		return err
	}
	if !s.withPlan {
		return status.Error(codes.FailedPrecondition, "late query failure")
	}
	return stream.Send(&sppb.PartialResultSet{Stats: &sppb.ResultSetStats{
		QueryStats: &structpb.Struct{Fields: map[string]*structpb.Value{
			"summary": structpb.NewStringValue("done"),
		}},
	}})
}

func TestLateQueryFailurePreservesPrimaryOutput(t *testing.T) {
	startQueryStatsModeServer(t, &lateQueryErrorServer{})
	dir := t.TempDir()
	path := filepath.Join(dir, "rows.json")
	plan := filepath.Join(dir, "plan.json")
	if err := os.WriteFile(path, []byte("ORIGINAL"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=SELECT value FROM T",
		"--jq-input-mode=lazy", "--filter=first(.rows[])",
		"--output=" + path, "--plan-output=" + plan, "--query-mode=PROFILE", "--timeout=5s",
	})
	out, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err == nil {
		t.Fatal("query error was swallowed")
	}
	if string(out) != "ORIGINAL" {
		t.Fatalf("query failure replaced original output with %q (err=%v)", out, err)
	}
	if _, statErr := os.Stat(plan); !os.IsNotExist(statErr) {
		t.Fatalf("plan published after query failure: %v", statErr)
	}
}

func TestPlanRenderFailureAfterSuccessfulDrainPublishesPrimary(t *testing.T) {
	startQueryStatsModeServer(t, &lateQueryErrorServer{withPlan: true})
	dir := t.TempDir()
	path := filepath.Join(dir, "rows.json")
	plan := filepath.Join(dir, "plan.json")
	if err := os.WriteFile(path, []byte("ORIGINAL"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=SELECT value FROM T",
		"--jq-input-mode=lazy", "--filter=first(.rows[])",
		"--output=" + path, "--plan-output=" + plan, "--query-mode=PROFILE", "--timeout=5s",
	})
	out, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err == nil || !strings.Contains(err.Error(), "primary output written") {
		t.Fatalf("error = %v, want primary publication after render failure", err)
	}
	if string(out) == "ORIGINAL" || len(out) == 0 {
		t.Fatalf("primary was not published: %q", out)
	}
	if _, statErr := os.Stat(plan); !os.IsNotExist(statErr) {
		t.Fatalf("plan published without nodes: %v", statErr)
	}
}
