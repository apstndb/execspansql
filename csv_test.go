package main

import (
	"bytes"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	svwriter "github.com/apstndb/spanvalue/writer"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/structpb"
	"slices"
	"spheric.cloud/xiter"
)

// csvFixtures are valid result sets used to guard experimental_csv output across
// spanvalue upgrades. Expected formatting is derived from spanvalue's CSV writer
// in TestExperimentalCsvSpanvalueContract, not from checked-in golden files.
func csvFixtures() map[string]*sppb.ResultSet {
	return map[string]*sppb.ResultSet{
		"int64_and_string": resultSet(
			[]string{"id", "name"},
			[]*sppb.Type{{Code: sppb.TypeCode_INT64}, {Code: sppb.TypeCode_STRING}},
			[][]*structpb.Value{
				{structpb.NewStringValue("1"), structpb.NewStringValue("alice")},
				{structpb.NewStringValue("2"), structpb.NewStringValue("bob")},
			},
		),
		"null_bool_float64": resultSet(
			[]string{"s", "b", "f"},
			[]*sppb.Type{
				{Code: sppb.TypeCode_STRING},
				{Code: sppb.TypeCode_BOOL},
				{Code: sppb.TypeCode_FLOAT64},
			},
			[][]*structpb.Value{
				{
					structpb.NewNullValue(),
					structpb.NewBoolValue(true),
					structpb.NewNumberValue(3.5),
				},
				{
					structpb.NewStringValue("ok"),
					structpb.NewBoolValue(false),
					structpb.NewNumberValue(0),
				},
			},
		),
		"csv_quoting": resultSet(
			[]string{"msg"},
			[]*sppb.Type{{Code: sppb.TypeCode_STRING}},
			[][]*structpb.Value{{structpb.NewStringValue(`say, "hi"`)}},
		),
		"bytes_date_timestamp_numeric": resultSet(
			[]string{"payload", "d", "ts", "n"},
			[]*sppb.Type{
				{Code: sppb.TypeCode_BYTES},
				{Code: sppb.TypeCode_DATE},
				{Code: sppb.TypeCode_TIMESTAMP},
				{Code: sppb.TypeCode_NUMERIC},
			},
			[][]*structpb.Value{{
				structpb.NewStringValue("YWJj"), // raw base64 for "abc"
				structpb.NewStringValue("2024-06-01"),
				structpb.NewStringValue("2024-06-01T12:34:56Z"),
				structpb.NewStringValue("99.5"),
			}},
		),
		"json": resultSet(
			[]string{"j"},
			[]*sppb.Type{{Code: sppb.TypeCode_JSON}},
			[][]*structpb.Value{{structpb.NewStringValue(`{"k":1}`)}},
		),
	}
}

// TestExperimentalCsvSpanvalueContract ensures writeCsv stays aligned with
// spanvalue's DelimitedWriter (SimpleFormatConfig). When spanvalue is upgraded,
// this test should reflect intentional formatting changes without golden files.
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

	t.Run("header_only", func(t *testing.T) {
		t.Parallel()
		rs := resultSet([]string{"id"}, []*sppb.Type{{Code: sppb.TypeCode_INT64}}, nil)
		var buf bytes.Buffer
		if err := writeCsv(&buf, rs); err != nil {
			t.Fatalf("writeCsv() error = %v", err)
		}
		if got := buf.String(); got != "id\n" {
			t.Fatalf("writeCsv() output:\n%q\nwant:\n%q", got, "id\n")
		}
	})

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
	for name, rs := range csvFixtures() {
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
	types := slices.Collect(xiter.Map(slices.Values(fields), (*sppb.StructType_Field).GetType))

	if len(rs.GetRows()) == 0 {
		if err := csvWriter.WriteHeader(); err != nil {
			return err
		}
		return csvWriter.Flush()
	}

	gcvs := make([]spanner.GenericColumnValue, len(types))
	for _, row := range rs.GetRows() {
		values := row.GetValues()
		for i, typ := range types {
			gcvs[i] = spanner.GenericColumnValue{Type: typ, Value: values[i]}
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
