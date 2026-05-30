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

	"github.com/apstndb/execspansql/jqresult"
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

	t.Run("lazy jq with RowIterator", func(t *testing.T) {
		runLazy := func(t *testing.T, filter string, redact bool, mode *sppb.ExecuteSqlRequest_QueryMode) []any {
			t.Helper()
			var opts spanner.QueryOptions
			if mode != nil {
				opts.Mode = mode
			}
			rowIter := client.Single().QueryWithOptions(ctx,
				spanner.Statement{SQL: "SELECT SingerId FROM Singers ORDER BY SingerId LIMIT 3"},
				opts,
			)
			code, err := jqresult.Compile(filter, jqresult.InputLazy)
			if err != nil {
				t.Fatal(err)
			}
			iter, cleanup, err := jqresult.Execute(code, jqresult.InputLazy, rowIter, nil, redact)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			var out []any
			for {
				v, ok := iter.Next()
				if !ok {
					return out
				}
				if err, isErr := v.(error); isErr {
					t.Fatal(err)
				}
				out = append(out, v)
			}
		}

		rows := runLazy(t, ".rows[]", false, sppb.ExecuteSqlRequest_NORMAL.Enum())
		if len(rows) != 3 {
			t.Fatalf("got %d row values, want 3", len(rows))
		}

		meta := runLazy(t, ".metadata.rowType.fields[0].name", false, sppb.ExecuteSqlRequest_NORMAL.Enum())
		if len(meta) != 1 || meta[0] != "SingerId" {
			t.Fatalf("metadata field name: got %v", meta)
		}

		stats := runLazy(t, ".stats.rowCount", false, sppb.ExecuteSqlRequest_PROFILE.Enum())
		if len(stats) != 1 {
			t.Fatalf("stats rowCount: got %v", stats)
		}

		lens := runLazy(t, "{alen: (.rows|length), blen: (.rows|length)}", false, sppb.ExecuteSqlRequest_NORMAL.Enum())
		if len(lens) != 1 {
			t.Fatalf("rows length output: got %v", lens)
		}
		lensObj, ok := lens[0].(map[string]any)
		if !ok {
			t.Fatalf("rows length type: %T", lens[0])
		}
		if lensObj["alen"] != 3 || lensObj["blen"] != 3 {
			t.Fatalf("rows lengths: got %#v", lensObj)
		}

		dupRows := runLazy(t, "{a: [.rows[]], b: [.rows[]]}", false, sppb.ExecuteSqlRequest_NORMAL.Enum())
		if len(dupRows) != 1 {
			t.Fatalf("duplicate rows output: got %v", dupRows)
		}
		dupObj, ok := dupRows[0].(map[string]any)
		if !ok {
			t.Fatalf("duplicate rows type: %T", dupRows[0])
		}
		for _, key := range []string{"a", "b"} {
			rowSlice, ok := dupObj[key].([]any)
			if !ok || len(rowSlice) != 3 {
				t.Fatalf("%s: got %v (%T)", key, dupObj[key], dupObj[key])
			}
		}

		objectRows := runLazy(t, "{rows: .rows, rc: .stats.rowCount}", false, sppb.ExecuteSqlRequest_PROFILE.Enum())
		if len(objectRows) != 1 {
			t.Fatalf("object rows output: got %v", objectRows)
		}
		objRows, ok := objectRows[0].(map[string]any)
		if !ok {
			t.Fatalf("object rows type: %T", objectRows[0])
		}
		normalized, err := jqresult.NormalizeForEncode(objRows["rows"])
		if err != nil {
			t.Fatal(err)
		}
		rowSlice, ok := normalized.([]any)
		if !ok || len(rowSlice) != 3 {
			t.Fatalf("normalized rows after stats: got %v (%T)", normalized, normalized)
		}

		combined := runLazy(t, "{rc: .stats.rowCount, ids: [.rows[] | .[0]]}", false, sppb.ExecuteSqlRequest_PROFILE.Enum())
		if len(combined) != 1 {
			t.Fatalf("combined output: got %v", combined)
		}
		obj, ok := combined[0].(map[string]any)
		if !ok {
			t.Fatalf("combined type: %T", combined[0])
		}
		ids, ok := obj["ids"].([]any)
		if !ok || len(ids) != 3 {
			t.Fatalf("combined ids: got %v", obj["ids"])
		}

		redacted := runLazy(t, ".stats.rowCount", true, sppb.ExecuteSqlRequest_PROFILE.Enum())
		if len(redacted) != 1 {
			t.Fatalf("redacted stats: got %v", redacted)
		}

		redactedRows := runLazy(t, ".rows[]", true, sppb.ExecuteSqlRequest_NORMAL.Enum())
		if len(redactedRows) != 0 {
			t.Fatalf("redacted .rows[]: got %d values, want 0", len(redactedRows))
		}
	})
}
