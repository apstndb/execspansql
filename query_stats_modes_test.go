package main

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAdditionalQueryStatsModesRejectPartitionedDMLBeforeClient(t *testing.T) {
	for _, queryMode := range []string{"WITH_PLAN_AND_STATS", "WITH_STATS"} {
		queryMode := queryMode
		t.Run(queryMode, func(t *testing.T) {
			err := runMain(t, []string{
				"database", "--project", "unused-project", "--instance", "unused-instance",
				"--sql", "UPDATE T SET V=1", "--enable-partitioned-dml", "--query-mode", queryMode,
			})
			want := "--query-mode=" + queryMode + " cannot be combined with --enable-partitioned-dml"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("_main() error = %v, want %q", err, want)
			}
		})
	}
}

func TestQueryStatsModesPreserveDMLCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		mode            sppb.ExecuteSqlRequest_QueryMode
		wantDMLRowCount bool
	}{
		{name: "normal", mode: sppb.ExecuteSqlRequest_NORMAL, wantDMLRowCount: true},
		{name: "plan", mode: sppb.ExecuteSqlRequest_PLAN},
		{name: "profile", mode: sppb.ExecuteSqlRequest_PROFILE, wantDMLRowCount: true},
		{name: "with_plan_and_stats", mode: sppb.ExecuteSqlRequest_WITH_PLAN_AND_STATS, wantDMLRowCount: true},
		{name: "with_stats", mode: sppb.ExecuteSqlRequest_WITH_STATS, wantDMLRowCount: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := spanner.QueryOptions{Mode: tt.mode.Enum()}
			if got := dmlRowCountForMode(readWrite{}, opts); got != tt.wantDMLRowCount {
				t.Errorf("dmlRowCountForMode() = %v, want %v", got, tt.wantDMLRowCount)
			}
		})
	}
}

func TestAdditionalQueryStatsModesReachSpannerAndProduceStats(t *testing.T) {
	server := &queryStatsModeServer{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	sppb.RegisterSpannerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	t.Setenv("SPANNER_EMULATOR_HOST", listener.Addr().String())

	for _, queryMode := range []string{"WITH_PLAN_AND_STATS", "WITH_STATS"} {
		for _, tc := range []struct {
			name   string
			format string
			lazy   bool
		}{
			{name: "json_eager", format: "json"},
			{name: "yaml_eager", format: "yaml"},
			{name: "json_lazy", format: "json", lazy: true},
			{name: "yaml_lazy", format: "yaml", lazy: true},
		} {
			queryMode := queryMode
			tc := tc
			t.Run(queryMode+"_"+tc.name, func(t *testing.T) {
				server.resetModes()
				args := []string{
					"database", "--project", "project", "--instance", "instance", "--sql", "SELECT 'value'",
					"--query-mode", queryMode, "--format", tc.format, "--timeout", "5s",
				}
				if tc.lazy {
					args = append(args, "--jq-input-mode", "lazy", "--filter", ".stats.queryStats.mode")
				}
				out, err := captureStdout(t, func() error { return runMain(t, args) })
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(out, queryMode) {
					t.Fatalf("output = %q, want query stats containing %q", out, queryMode)
				}
				if !tc.lazy {
					if !strings.Contains(out, "value") {
						t.Fatalf("eager output = %q, want row value", out)
					}
					if queryMode == "WITH_PLAN_AND_STATS" && !strings.Contains(out, "Fake Scan") {
						t.Fatalf("WITH_PLAN_AND_STATS output = %q, want fake query plan", out)
					}
					if queryMode == "WITH_STATS" && strings.Contains(out, "Fake Scan") {
						t.Fatalf("WITH_STATS output = %q, got unexpected query plan", out)
					}
				}
				modes := server.modes()
				if len(modes) != 1 || modes[0].String() != queryMode {
					t.Fatalf("received query modes = %v, want [%s]", modes, queryMode)
				}
			})
		}
	}
}

type queryStatsModeServer struct {
	sppb.UnimplementedSpannerServer

	mu            sync.Mutex
	receivedModes []sppb.ExecuteSqlRequest_QueryMode
}

func (s *queryStatsModeServer) CreateSession(_ context.Context, req *sppb.CreateSessionRequest) (*sppb.Session, error) {
	return &sppb.Session{Name: req.GetDatabase() + "/sessions/test"}, nil
}

func (s *queryStatsModeServer) ExecuteStreamingSql(req *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
	s.mu.Lock()
	s.receivedModes = append(s.receivedModes, req.GetQueryMode())
	s.mu.Unlock()

	if err := stream.Send(&sppb.PartialResultSet{
		Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{
			Name: "value",
			Type: &sppb.Type{Code: sppb.TypeCode_STRING},
		}}}},
		Values: []*structpb.Value{structpb.NewStringValue("value")},
	}); err != nil {
		return err
	}

	stats := &sppb.ResultSetStats{
		QueryStats: &structpb.Struct{Fields: map[string]*structpb.Value{
			"mode": structpb.NewStringValue(req.GetQueryMode().String()),
		}},
	}
	if req.GetQueryMode() == sppb.ExecuteSqlRequest_WITH_PLAN_AND_STATS {
		stats.QueryPlan = &sppb.QueryPlan{PlanNodes: []*sppb.PlanNode{{DisplayName: "Fake Scan"}}}
	}
	return stream.Send(&sppb.PartialResultSet{Stats: stats})
}

func (s *queryStatsModeServer) resetModes() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receivedModes = nil
}

func (s *queryStatsModeServer) modes() []sppb.ExecuteSqlRequest_QueryMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sppb.ExecuteSqlRequest_QueryMode(nil), s.receivedModes...)
}
