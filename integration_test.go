package main

import (
	"context"
	_ "embed"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/gsqlsep"
	"github.com/apstndb/spanemuboost"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
	"spheric.cloud/xiter"

	"github.com/apstndb/execspansql/params"
)

//go:embed testdata/ddl.sql
var ddl string

//go:embed testdata/dml.sql
var dml string

func TestWithCloudSpannerEmulator(t *testing.T) {
	ctx := context.Background()

	_, clients, teardown, err := spanemuboost.NewEmulatorWithClients(ctx,
		spanemuboost.WithSetupDDLs(gsqlsep.SeparateInputString(ddl)),
		spanemuboost.WithSetupRawDMLs(gsqlsep.SeparateInputString(dml)),
	)
	if err != nil {
		return
	}
	defer teardown()

	client := clients.Client

	t.Run("PLAN with generateParams", func(t *testing.T) {
		paramStrMap := map[string]string{
			"i64":   "INT64",
			"f32":   "FLOAT32",
			"f64":   "FLOAT64",
			"s":     "STRING",
			"bs":    "BYTES",
			"bl":    "BOOL",
			"dt":    `DATE`,
			"ts":    `TIMESTAMP`,
			"n":     `NUMERIC`,
			"a_str": `ARRAY<STRUCT<int64_value INT64>>`,
			"a_s":   `ARRAY<STRING>`,
			"j":     `JSON`,
		}
		want := &sppb.StructType{
			Fields: []*sppb.StructType_Field{
				{Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
				{Type: &sppb.Type{Code: sppb.TypeCode_FLOAT32}},
				{Type: &sppb.Type{Code: sppb.TypeCode_FLOAT64}},
				{Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
				{Type: &sppb.Type{Code: sppb.TypeCode_BYTES}},
				{Type: &sppb.Type{Code: sppb.TypeCode_BOOL}},
				{Type: &sppb.Type{Code: sppb.TypeCode_DATE}},
				{Type: &sppb.Type{Code: sppb.TypeCode_TIMESTAMP}},
				{Type: &sppb.Type{Code: sppb.TypeCode_NUMERIC}},
				{Type: &sppb.Type{Code: sppb.TypeCode_ARRAY,
					ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{Fields: []*sppb.StructType_Field{
						{Name: "int64_value", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
					}}}}},
				{Type: &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRING}}},
				{Type: &sppb.Type{Code: sppb.TypeCode_JSON}},
			},
		}

		params, err := params.GenerateParams(paramStrMap, true)
		if err != nil {
			t.Fatal(err)
		}

		rowIter := clients.Client.Single().QueryWithOptions(ctx,
			spanner.Statement{SQL: "SELECT @i64, @f32, @f64, @s, @bs, @bl, @dt, @ts, @n, @a_str, @a_s, @j", Params: params},
			spanner.QueryOptions{Mode: sppb.ExecuteSqlRequest_PLAN.Enum()})
		defer rowIter.Stop()

		err = skipRowIter(rowIter)
		if err != nil {
			t.Fatal(err)
		}

		got := rowIter.Metadata.GetRowType()
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Error(diff)
			return
		}
	})

	t.Run("NORMAL with generateParams", func(t *testing.T) {
		for _, tcase := range []struct {
			desc  string
			input string
			want  spanner.GenericColumnValue
		}{
			{
				"INT64",
				"1",
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_INT64},
					Value: structpb.NewStringValue("1"),
				},
			},
			{
				"FLOAT64",
				"1.0",
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_FLOAT64},
					Value: structpb.NewNumberValue(1.0),
				},
			},
			{
				"STRING",
				`'foo'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_STRING},
					Value: structpb.NewStringValue("foo"),
				},
			},
			{
				"BYTES",
				`b'foo'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_BYTES},
					Value: structpb.NewStringValue("Zm9v"),
				},
			},
			{
				"DATE",
				`DATE '1970-01-01'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_DATE},
					Value: structpb.NewStringValue("1970-01-01"),
				},
			},
			{
				"TIMESTAMP",
				`TIMESTAMP '1970-01-01T00:00:00Z'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_TIMESTAMP},
					Value: structpb.NewStringValue("1970-01-01T00:00:00Z"),
				},
			},
			{
				"NUMERIC",
				`NUMERIC '1.0'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_NUMERIC},
					Value: structpb.NewStringValue("1"),
				},
			},
			{
				"ARRAY<STRUCT<int64_value INT64>>",
				`ARRAY<STRUCT<int64_value INT64>>[STRUCT(1)]`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{
						Code: sppb.TypeCode_ARRAY,
						ArrayElementType: &sppb.Type{
							Code: sppb.TypeCode_STRUCT,
							StructType: &sppb.StructType{
								Fields: []*sppb.StructType_Field{
									{Name: "int64_value", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
								},
							},
						},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewListValue(&structpb.ListValue{
								Values: []*structpb.Value{
									structpb.NewStringValue("1")}})}}),
				},
			},
			{
				"ARRAY<STRUCT<int64_value INT64>> verbose",
				`ARRAY<STRUCT<int64_value INT64>>[STRUCT<int64_value INT64>(1)]`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{
						Fields: []*sppb.StructType_Field{{Name: "int64_value", Type: &sppb.Type{Code: sppb.TypeCode_INT64}}}}},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewListValue(&structpb.ListValue{
								Values: []*structpb.Value{
									structpb.NewStringValue("1")}})}}),
				},
			},
			{
				"ARRAY<STRUCT<int64_value INT64>> typeless",
				`[STRUCT(1 AS int64_value)]`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{
						Fields: []*sppb.StructType_Field{{Name: "int64_value", Type: &sppb.Type{Code: sppb.TypeCode_INT64}}}}},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewListValue(&structpb.ListValue{
								Values: []*structpb.Value{
									structpb.NewStringValue("1")}})}}),
				},
			},
			{
				"ARRAY<STRUCT<INT64, STRING>> tuple",
				`[(1, "foo")]`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{
						Fields: []*sppb.StructType_Field{
							{Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
							{Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
						}}},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewListValue(&structpb.ListValue{
								Values: []*structpb.Value{
									structpb.NewStringValue("1"),
									structpb.NewStringValue("foo"),
								},
							})}}),
				},
			},
			{
				"ARRAY<STRING>",
				`['foo']`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{
						Code:             sppb.TypeCode_ARRAY,
						ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRING},
					},
					Value: structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("foo")}}),
				},
			},
			{
				"JSON",
				`JSON '{"foo": "bar"}'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_JSON},
					Value: structpb.NewStringValue(`{"foo":"bar"}`),
				},
			},
		} {
			t.Run(tcase.desc, func(t *testing.T) {
				params, err := params.GenerateParams(map[string]string{"v": tcase.input}, false)
				if err != nil {
					t.Fatal(err)
				}
				rowIter := client.Single().QueryWithOptions(ctx,
					spanner.Statement{SQL: "SELECT @v AS v", Params: params},
					spanner.QueryOptions{Mode: sppb.ExecuteSqlRequest_NORMAL.Enum()})
				defer rowIter.Stop()

				rows, err := xiter.TryCollect(rowIterSeq(rowIter))
				if err != nil {
					t.Error(err)
					return
				}

				if lenRows := len(rows); lenRows != 1 {
					t.Errorf("len(rows) must be 1, but: %v", lenRows)
					return
				}

				row := rows[0]
				if row.Size() != 1 {
					t.Errorf("row.Size() must be 1, but: %v", row.Size())
					return
				}

				var got spanner.GenericColumnValue
				err = row.ColumnByName("v", &got)
				if err != nil {
					t.Error(err)
					return
				}
				if diff := cmp.Diff(tcase.want, got, protocmp.Transform()); diff != "" {
					t.Error(diff)
					return
				}
			})
		}
	})
}
