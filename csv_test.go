package main

import (
	"bytes"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestExperimentalCsvOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rs   *sppb.ResultSet
		want string
	}{
		{
			name: "int64_and_string",
			rs: resultSet(
				[]string{"id", "name"},
				[]*sppb.Type{
					{Code: sppb.TypeCode_INT64},
					{Code: sppb.TypeCode_STRING},
				},
				[][]*structpb.Value{
					{structpb.NewStringValue("1"), structpb.NewStringValue("alice")},
					{structpb.NewStringValue("2"), structpb.NewStringValue("bob")},
				},
			),
			want: "id,name\n1,alice\n2,bob\n",
		},
		{
			name: "header_only",
			rs: resultSet(
				[]string{"id"},
				[]*sppb.Type{{Code: sppb.TypeCode_INT64}},
				nil,
			),
			want: "id\n",
		},
		{
			name: "null_bool_float64",
			rs: resultSet(
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
			want: "s,b,f\n<null>,true,3.5\nok,false,0\n",
		},
		{
			name: "csv_quoting",
			rs: resultSet(
				[]string{"msg"},
				[]*sppb.Type{{Code: sppb.TypeCode_STRING}},
				[][]*structpb.Value{
					{structpb.NewStringValue(`say, "hi"`)},
				},
			),
			want: "msg\n\"say, \"\"hi\"\"\"\n",
		},
	}

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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			if err := writeCsv(&buf, tt.rs); err != nil {
				t.Fatalf("writeCsv() error = %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Fatalf("writeCsv() output:\n%q\nwant:\n%q", got, tt.want)
			}
		})
	}
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
