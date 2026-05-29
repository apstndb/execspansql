package main

import (
	"bytes"
	"flag"
	"io"
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

// TestExperimentalCsvSpanvalueContract ensures writeCsvFromResultSet stays aligned with
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
			if err := writeCsvFromResultSet(&got, rs); err != nil {
				t.Fatalf("writeCsvFromResultSet() error = %v", err)
			}
			if err := experimentalCsvViaSpanvalueWriter(&want, rs); err != nil {
				t.Fatalf("experimentalCsvViaSpanvalueWriter() error = %v", err)
			}
			if diff := cmp.Diff(want.String(), got.String()); diff != "" {
				t.Fatalf("writeCsvFromResultSet() output mismatch (-spanvalue writer +writeCsvFromResultSet):\n%s", diff)
			}
		})
	}
}

// TestExperimentalCsvRowIterPathMatchesResultSet checks that the production RowIterator
// path (WriteRow) matches the ResultSet test helper (WriteStructValues).
func TestExperimentalCsvRowIterPathMatchesResultSet(t *testing.T) {
	t.Parallel()

	rs := resultSet(
		[]string{"id", "name"},
		[]*sppb.Type{{Code: sppb.TypeCode_INT64}, {Code: sppb.TypeCode_STRING}},
		[][]*structpb.Value{
			{structpb.NewStringValue("1"), structpb.NewStringValue("alice")},
			{structpb.NewStringValue("2"), structpb.NewStringValue("bob")},
		},
	)
	row1, err := spanner.NewRow([]string{"id", "name"}, []interface{}{int64(1), "alice"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}
	row2, err := spanner.NewRow([]string{"id", "name"}, []interface{}{int64(2), "bob"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	var fromResultSet, fromRowIter bytes.Buffer
	if err := writeCsvFromResultSet(&fromResultSet, rs); err != nil {
		t.Fatalf("writeCsvFromResultSet() error = %v", err)
	}
	if err := writeCsvFromPreparedRows(&fromRowIter, rs.GetMetadata(), []*spanner.Row{row1, row2}); err != nil {
		t.Fatalf("writeCsvFromPreparedRows() error = %v", err)
	}
	if diff := cmp.Diff(fromResultSet.String(), fromRowIter.String()); diff != "" {
		t.Fatalf("RowIterator path output mismatch (-ResultSet +RowIter):\n%s", diff)
	}
}

// TestExperimentalCsvBehavior covers execspansql-specific CSV behavior.
func TestExperimentalCsvBehavior(t *testing.T) {
	t.Parallel()

	t.Run("invalid_metadata", func(t *testing.T) {
		t.Parallel()
		if err := writeCsvFromResultSet(&bytes.Buffer{}, nil); err == nil {
			t.Fatal("writeCsvFromResultSet(nil) expected error")
		}
		if err := prepareCsvRowType(svwriter.NewCSVWriter(&bytes.Buffer{}), nil); err == nil {
			t.Fatal("prepareCsvRowType(nil metadata) expected error")
		}
	})

	t.Run("row_value_mismatch", func(t *testing.T) {
		t.Parallel()
		rs := resultSet(
			[]string{"id", "name"},
			[]*sppb.Type{{Code: sppb.TypeCode_INT64}, {Code: sppb.TypeCode_STRING}},
			[][]*structpb.Value{{structpb.NewStringValue("1")}},
		)
		if err := writeCsvFromResultSet(&bytes.Buffer{}, rs); err == nil {
			t.Fatal("writeCsvFromResultSet() expected row value count mismatch error")
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
		if err := writeCsvFromResultSet(&bytes.Buffer{}, rs); err == nil {
			t.Fatal("writeCsvFromResultSet() expected error for nil row")
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
		if err := writeCsvFromResultSet(&buf, rs); err != nil {
			t.Fatalf("%s: writeCsvFromResultSet() error = %v", name, err)
		}
		t.Logf("=== %s ===\n%s", name, buf.String())
	}
}

// experimentalCsvViaSpanvalueWriter is the reference path for ResultSet-shaped fixtures.
func experimentalCsvViaSpanvalueWriter(out *bytes.Buffer, rs *sppb.ResultSet) error {
	csvWriter := svwriter.NewCSVWriter(out, svwriter.WithMetadata(rs.GetMetadata()))
	for _, row := range rs.GetRows() {
		if err := csvWriter.WriteStructValues(row.GetValues()); err != nil {
			return err
		}
	}
	return csvWriter.Flush()
}

// writeCsvFromPreparedRows exercises the WriteRow path after PrepareRowType, matching production.
func writeCsvFromPreparedRows(w io.Writer, metadata *sppb.ResultSetMetadata, rows []*spanner.Row) error {
	csvWriter := svwriter.NewCSVWriter(w)
	if err := prepareCsvRowType(csvWriter, metadata); err != nil {
		return err
	}
	for _, row := range rows {
		if err := csvWriter.WriteRow(row); err != nil {
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
