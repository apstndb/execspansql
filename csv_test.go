package main

import (
	"bytes"
	"flag"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	svwriter "github.com/apstndb/spanvalue/writer"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

// TestExperimentalCsvSpanvalueContract ensures writeCsv stays aligned with
// spanvalue's DelimitedWriter (SimpleFormatConfig). When spanvalue is upgraded,
// update golden files if the change is intentional:
//
//	go test -update-golden -run TestExperimentalCsvGolden
func TestExperimentalCsvSpanvalueContract(t *testing.T) {
	t.Parallel()

	for name, rs := range csvFixtures() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var got, want bytes.Buffer
			if err := writeCsv(&got, rs); err != nil {
				t.Fatalf("writeCsv() error = %v", err)
			}
			if err := experimentalCsvViaSpanvalueWriter(&want, rs); err != nil {
				t.Fatalf("experimentalCsvViaSpanvalueWriter() error = %v", err)
			}
			if diff := cmp.Diff(want.String(), got.String()); diff != "" {
				t.Fatalf("writeCsv() output mismatch (-spanvalue writer +writeCsv):\n%s", diff)
			}
		})
	}
}

// TestExperimentalCsvBehavior covers execspansql-specific CSV behavior.
func TestExperimentalCsvBehavior(t *testing.T) {
	t.Parallel()

	t.Run("invalid_metadata", func(t *testing.T) {
		t.Parallel()
		if err := writeCsv(&bytes.Buffer{}, nil); err == nil {
			t.Fatal("writeCsv(nil) expected error")
		}
	})

	t.Run("row_value_mismatch", func(t *testing.T) {
		t.Parallel()
		rs := resultSet(
			[]string{"id", "name"},
			[]*sppb.Type{{Code: sppb.TypeCode_INT64}, {Code: sppb.TypeCode_STRING}},
			[][]*structpb.Value{{structpb.NewStringValue("1")}},
		)
		if err := writeCsv(&bytes.Buffer{}, rs); err == nil {
			t.Fatal("writeCsv() expected row value count mismatch error")
		}
	})

	t.Run("nil_row", func(t *testing.T) {
		t.Parallel()
		rs := resultSet(
			[]string{"id"},
			[]*sppb.Type{{Code: sppb.TypeCode_INT64}},
			nil,
		)
		rs.Rows = []*structpb.ListValue{nil}
		if err := writeCsv(&bytes.Buffer{}, rs); err == nil {
			t.Fatal("writeCsv() expected error for nil row")
		}
	})
}

// TestExperimentalCsvDumpFixtures logs CSV output for manual comparison across
// revisions (for example a second git worktree on origin/main):
//
//	go test -run TestExperimentalCsvDumpFixtures -v .
func TestExperimentalCsvDumpFixtures(t *testing.T) {
	fixtures := csvGoldenFixtures()
	for name, rs := range csvFixtures() {
		fixtures[name] = rs
	}
	for name, rs := range fixtures {
		var buf bytes.Buffer
		if err := writeCsv(&buf, rs); err != nil {
			t.Fatalf("%s: writeCsv() error = %v", name, err)
		}
		t.Logf("=== %s ===\n%s", name, buf.String())
	}
}

// experimentalCsvViaSpanvalueWriter is the reference path writeCsv is expected to follow.
func experimentalCsvViaSpanvalueWriter(out *bytes.Buffer, rs *sppb.ResultSet) error {
	csvWriter := svwriter.NewCSVWriter(out, svwriter.WithMetadata(rs.GetMetadata()))
	fields := rs.GetMetadata().GetRowType().GetFields()

	if len(rs.GetRows()) == 0 {
		if err := csvWriter.WriteHeader(); err != nil {
			return err
		}
		return csvWriter.Flush()
	}

	gcvs := make([]spanner.GenericColumnValue, len(fields))
	for _, row := range rs.GetRows() {
		values := row.GetValues()
		for i, field := range fields {
			gcvs[i] = spanner.GenericColumnValue{Type: field.GetType(), Value: values[i]}
		}
		if err := csvWriter.WriteGCVs(gcvs); err != nil {
			return err
		}
	}
	return csvWriter.Flush()
}

func resultSet(names []string, types []*sppb.Type, rows [][]*structpb.Value) *sppb.ResultSet {
	fields := make([]*sppb.StructType_Field, len(names))
	for i, name := range names {
		fields[i] = &sppb.StructType_Field{
			Name: name,
			Type: types[i],
		}
	}
	listRows := make([]*structpb.ListValue, len(rows))
	for i, values := range rows {
		listRows[i] = &structpb.ListValue{Values: values}
	}
	return &sppb.ResultSet{
		Metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{Fields: fields},
		},
		Rows: listRows,
	}
}
