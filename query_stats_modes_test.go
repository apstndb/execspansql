package main

import (
	"context"
	"encoding/csv"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/internal/grpctest"
	"github.com/apstndb/spanemuboost"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
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

func TestQueryStatsResponseReachesOutput(t *testing.T) {
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

	// Two smoke cases cover the distinct eager and lazy SDK-to-output paths.
	// Plan/stat combinations are covered without a server in jqresult tests.
	for _, lazy := range []bool{false, true} {
		name := "eager"
		if lazy {
			name = "lazy"
		}
		t.Run(name, func(t *testing.T) {
			args := []string{
				"database", "--project", "project", "--instance", "instance", "--sql", "SELECT 'value'",
				"--query-mode", "WITH_PLAN_AND_STATS", "--format", "json", "--timeout", "5s",
			}
			if lazy {
				args = append(args, "--jq-input-mode", "lazy", "--filter", ".stats")
			}
			out, err := captureStdout(t, func() error { return runMain(t, args) })
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"test stats", "Fake Scan"} {
				if !strings.Contains(out, want) {
					t.Fatalf("output = %q, want %q", out, want)
				}
			}
			if !lazy && !strings.Contains(out, "value") {
				t.Fatalf("eager output = %q, want row value", out)
			}
		})
	}
}

type queryStatsModeServer struct {
	sppb.UnimplementedSpannerServer
	omitValues bool
}

func (s *queryStatsModeServer) CreateSession(_ context.Context, req *sppb.CreateSessionRequest) (*sppb.Session, error) {
	return &sppb.Session{Name: req.GetDatabase() + "/sessions/test"}, nil
}

func (s *queryStatsModeServer) ExecuteStreamingSql(_ *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
	first := &sppb.PartialResultSet{
		Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{{
			Name: "value",
			Type: &sppb.Type{Code: sppb.TypeCode_STRING},
		}}}},
	}
	if !s.omitValues {
		first.Values = []*structpb.Value{structpb.NewStringValue("value")}
	}
	if err := stream.Send(first); err != nil {
		return err
	}

	stats := &sppb.ResultSetStats{
		QueryStats: &structpb.Struct{Fields: map[string]*structpb.Value{
			"summary": structpb.NewStringValue("test stats"),
		}},
	}
	stats.QueryPlan = &sppb.QueryPlan{PlanNodes: []*sppb.PlanNode{{DisplayName: "Fake Scan"}}}
	return stream.Send(&sppb.PartialResultSet{Stats: stats})
}

func startQueryStatsModeServer(t *testing.T, server sppb.SpannerServer) {
	t.Helper()
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
}

func assertPlanEnvelopeFile(t *testing.T, planPath string) {
	t.Helper()
	b, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	var rs sppb.ResultSet
	if err := protojson.Unmarshal(b, &rs); err != nil {
		t.Fatalf("unmarshal plan: %v\n%s", err, b)
	}
	if rs.GetMetadata() == nil || rs.GetMetadata().GetRowType() == nil {
		t.Fatalf("plan missing metadata: %s", b)
	}
	if len(rs.GetRows()) != 0 {
		t.Fatalf("plan artifact has rows: %#v", rs.GetRows())
	}
	if len(rs.GetStats().GetQueryPlan().GetPlanNodes()) == 0 {
		t.Fatalf("plan missing queryPlan.planNodes: %s", b)
	}
}

func TestSplitPlanOutputCLI(t *testing.T) {
	startQueryStatsModeServer(t, &queryStatsModeServer{})

	t.Run("PROFILE csv", func(t *testing.T) {
		dir := t.TempDir()
		rowsPath := filepath.Join(dir, "rows.csv")
		planPath := filepath.Join(dir, "plan.json")
		err := runMain(t, []string{
			"database", "--project", "project", "--instance", "instance",
			"--sql", "SELECT 'value'",
			"--query-mode", "PROFILE",
			"--format", "experimental_csv",
			"--output", rowsPath,
			"--plan-output", planPath,
			"--timeout", "5s",
		})
		if err != nil {
			t.Fatal(err)
		}
		csvBytes, err := os.ReadFile(rowsPath)
		if err != nil {
			t.Fatal(err)
		}
		csvText := string(csvBytes)
		if strings.Contains(csvText, "queryPlan") || strings.Contains(csvText, "Fake Scan") {
			t.Fatalf("CSV contains plan: %s", csvText)
		}
		recs, err := csv.NewReader(strings.NewReader(csvText)).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 2 || recs[0][0] != "value" || recs[1][0] != "value" {
			t.Fatalf("csv records = %#v", recs)
		}
		assertPlanEnvelopeFile(t, planPath)
	})

	t.Run("discard-results", func(t *testing.T) {
		dir := t.TempDir()
		rowsPath := filepath.Join(dir, "rows.csv")
		planPath := filepath.Join(dir, "plan.json")
		err := runMain(t, []string{
			"database", "--project", "project", "--instance", "instance",
			"--sql", "SELECT 'value'",
			"--query-mode", "PROFILE",
			"--format", "experimental_csv",
			"--output", rowsPath,
			"--plan-output", planPath,
			"--discard-results",
			"--timeout", "5s",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(rowsPath); !os.IsNotExist(err) {
			t.Fatalf("primary file exists after --discard-results: %v", err)
		}
		assertPlanEnvelopeFile(t, planPath)
	})

	t.Run("lazy jq still writes plan after early stop", func(t *testing.T) {
		dir := t.TempDir()
		rowsPath := filepath.Join(dir, "rows.json")
		planPath := filepath.Join(dir, "plan.json")
		err := runMain(t, []string{
			"database", "--project", "project", "--instance", "instance",
			"--sql", "SELECT 'value'",
			"--query-mode", "PROFILE",
			"--jq-input-mode", "lazy",
			"--filter", ".rows[0]",
			"--output", rowsPath,
			"--plan-output", planPath,
			"--timeout", "5s",
		})
		if err != nil {
			t.Fatal(err)
		}
		assertPlanEnvelopeFile(t, planPath)
	})
}

func TestSplitPlanOutputZeroRowAndPLAN(t *testing.T) {
	startQueryStatsModeServer(t, &queryStatsModeServer{omitValues: true})

	for _, mode := range []string{"PROFILE", "PLAN"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			rowsPath := filepath.Join(dir, "rows.csv")
			planPath := filepath.Join(dir, "plan.json")
			err := runMain(t, []string{
				"database", "--project", "project", "--instance", "instance",
				"--sql", "SELECT 'value'",
				"--query-mode", mode,
				"--format", "experimental_csv",
				"--output", rowsPath,
				"--plan-output", planPath,
				"--timeout", "5s",
			})
			if err != nil {
				t.Fatal(err)
			}
			csvBytes, err := os.ReadFile(rowsPath)
			if err != nil {
				t.Fatal(err)
			}
			recs, err := csv.NewReader(strings.NewReader(string(csvBytes))).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			if len(recs) != 1 || recs[0][0] != "value" {
				t.Fatalf("csv records = %#v, want header only", recs)
			}
			assertPlanEnvelopeFile(t, planPath)
		})
	}
}

// TestMainSendsAdditionalQueryStatsModes stops at the request boundary because
// emulator support for these modes is independent of CLI option propagation.
func TestMainSendsAdditionalQueryStatsModes(t *testing.T) {
	env, err := spanemuboost.RunEmulatorWithClients(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close() //nolint:errcheck
	t.Setenv("SPANNER_EMULATOR_HOST", env.Emulator().URI())
	for _, mode := range []string{"WITH_PLAN_AND_STATS", "WITH_STATS"} {
		for _, args := range [][]string{
			{"--format", "json"},
			{"--format", "yaml", "--jq-input-mode", "lazy"},
			{"--format", "experimental_csv"},
		} {
			t.Run(mode+"/"+strings.Join(args, "_"), func(t *testing.T) {
				modes := make(chan string, 1)
				dialOptions := grpctest.Inspect(func(_ string, req any) error {
					if req, ok := req.(*sppb.ExecuteSqlRequest); ok {
						select {
						case modes <- req.GetQueryMode().String():
						default:
						}
						return status.Error(codes.InvalidArgument, "query mode test capture")
					}
					return nil
				})
				cli := []string{env.DatabaseID, "--project", env.ProjectID, "--instance", env.InstanceID, "--sql", "SELECT 1", "--query-mode", mode, "--timeout", "5s"}
				err := runMain(t, append(cli, args...), option.WithGRPCDialOption(dialOptions[0]), option.WithGRPCDialOption(dialOptions[1]))
				if err == nil || !strings.Contains(err.Error(), "query mode test capture") {
					t.Fatalf("error = %v, want capture error", err)
				}
				select {
				case got := <-modes:
					if got != mode {
						t.Fatalf("query mode = %s, want %s", got, mode)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("no ExecuteSql request")
				}
			})
		}
	}
}
