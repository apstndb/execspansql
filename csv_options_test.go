package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Use the actual SDK transport so both streaming reads and buffered DML pass
// through the CLI flags, rather than testing only the writer's option helpers.
type csvOptionsServer struct {
	executionServer
	omitRows bool
}

func (s *csvOptionsServer) ExecuteStreamingSql(_ *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
	record := csvGoldenFixtures()["struct_simple"]
	recordType := record.Metadata.RowType.Fields[0].Type
	recordValue := record.Rows[0].Values[0]
	var values []*structpb.Value
	if !s.omitRows {
		values = []*structpb.Value{
			structpb.NewStringValue("99.500000000"), recordValue,
			structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{recordValue}}),
		}
	}
	return stream.Send(&sppb.PartialResultSet{
		Metadata: &sppb.ResultSetMetadata{
			Transaction: &sppb.Transaction{Id: []byte("test-transaction")},
			RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{
				{Name: "n", Type: &sppb.Type{Code: sppb.TypeCode_NUMERIC}},
				{Name: "record", Type: recordType},
				{Name: "records", Type: &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: recordType}},
			}},
		},
		Values: values,
		Stats:  &sppb.ResultSetStats{},
	})
}

func TestCSVOptionsThroughCLI(t *testing.T) {
	const header = "n,record,records\n"
	const simple = "99.500000000,\"(7 AS i, x AS s)\",\"[(7 AS i, x AS s)]\"\n"
	const compatible = "99.5,\"[7, x]\",\"[[7, x]]\"\n"
	for _, query := range []string{"SELECT 1", "UPDATE T SET V=1 THEN RETURN V"} {
		t.Run(query, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				args     []string
				omitRows bool
				want     string
			}{
				{name: "default", want: header + simple},
				{name: "simple", args: []string{"--csv-format=simple"}, want: header + simple},
				{name: "spanner-cli", args: []string{"--csv-format=spanner-cli"}, want: header + compatible},
				{name: "no-header", args: []string{"--no-csv-header"}, want: simple},
				{name: "combined", args: []string{"--no-csv-header", "--csv-format=spanner-cli"}, want: compatible},
				{name: "zero-rows", omitRows: true, want: header},
				{name: "zero-rows-no-header", args: []string{"--no-csv-header"}, omitRows: true},
				{name: "redacted", args: []string{"--redact-rows"}, want: header},
				{name: "redacted-no-header", args: []string{"--redact-rows", "--no-csv-header"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					server := &csvOptionsServer{omitRows: tc.omitRows}
					startQueryStatsModeServer(t, server)
					path := filepath.Join(t.TempDir(), "rows.csv")
					args := []string{"db", "--project=p", "--instance=i", "--sql", query,
						"--format=experimental_csv", "--output", path, "--timeout=5s"}
					if err := runCLI(t.Context(), append(args, tc.args...)); err != nil {
						t.Fatal(err)
					}
					got, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if string(got) != tc.want {
						t.Fatalf("CSV = %q, want %q", got, tc.want)
					}
					var wantCommits int32
					if strings.HasPrefix(query, "UPDATE") {
						wantCommits = 1
					}
					if got := server.commits.Load(); got != wantCommits {
						t.Fatalf("commits = %d, want %d", got, wantCommits)
					}
				})
			}
		})
	}
}

func TestCSVOptionsRejectBeforeOutputOrClient(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"json-header", []string{"--no-csv-header"}, "require --format=experimental_csv"},
		{"json-simple", []string{"--csv-format=simple"}, "require --format=experimental_csv"},
		{"yaml-format", []string{"--format=yaml", "--csv-format=spanner-cli"}, "require --format=experimental_csv"},
		{"partition", []string{"--format=experimental_csv", "--try-partition-query", "--no-csv-header"}, "require primary CSV output"},
		{"discard", []string{"--format=experimental_csv", "--discard-results", "--csv-format=simple"}, "require primary CSV output"},
		{"invalid-format", []string{"--format=experimental_csv", "--csv-format=unknown"}, "--csv-format"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing", "output.csv")
			args := []string{"db", "--project=p", "--instance=i", "--sql=SELECT 1", "--output", path}
			err := runCLI(t.Context(), append(args, tc.args...))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("invalid arguments created output: %v", err)
			}
		})
	}
}

func TestCSVNoHeaderStillPublishesPlan(t *testing.T) {
	for _, omitRows := range []bool{false, true} {
		t.Run(map[bool]string{false: "redacted", true: "zero-rows"}[omitRows], func(t *testing.T) {
			startQueryStatsModeServer(t, &queryStatsModeServer{omitValues: omitRows})
			dir := t.TempDir()
			rowsPath, planPath := filepath.Join(dir, "rows.csv"), filepath.Join(dir, "plan.json")
			args := []string{"db", "--project=p", "--instance=i", "--sql=SELECT 1",
				"--format=experimental_csv", "--no-csv-header", "--query-mode=PROFILE",
				"--output", rowsPath, "--plan-output", planPath, "--timeout=5s"}
			if !omitRows {
				args = append(args, "--redact-rows")
			}
			if err := runCLI(t.Context(), args); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(rowsPath)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("CSV = %q, want no bytes", got)
			}
			assertPlanEnvelopeFile(t, planPath)
		})
	}
}
