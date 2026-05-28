package main

import (
	"bytes"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestWriteCsv(t *testing.T) {
	rs := &sppb.ResultSet{
		Metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{
				Fields: []*sppb.StructType_Field{
					{Name: "id", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
					{Name: "name", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
				},
			},
		},
		Rows: []*structpb.ListValue{
			{Values: []*structpb.Value{structpb.NewStringValue("1"), structpb.NewStringValue("alice")}},
			{Values: []*structpb.Value{structpb.NewStringValue("2"), structpb.NewStringValue("bob")}},
		},
	}

	var buf bytes.Buffer
	if err := writeCsv(&buf, rs); err != nil {
		t.Fatalf("writeCsv() error = %v", err)
	}

	got := strings.TrimSpace(buf.String())
	want := "id,name\n1,alice\n2,bob"
	if got != want {
		t.Fatalf("writeCsv() output:\n%q\nwant:\n%q", got, want)
	}
}
