package main

import (
	"encoding/base64"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGenerateParams(t *testing.T) {
	tcases := []struct {
		Name         string
		Input        map[string]string
		PermitType   bool
		ExpectResult map[string]interface{}
		ExpectErr    bool
	}{
		{
			Name: "types",
			Input: map[string]string{
				"int64_type":        "INT64",
				"float64_type":      "FLOAT64",
				"float32_type":      "FLOAT32",
				"string_type":       `STRING`,
				"bytes_type":        `BYTES`,
				"bool_type":         "BOOL",
				"date_type":         `DATE`,
				"timestamp_type":    `TIMESTAMP`,
				"numeric_type":      `NUMERIC`,
				"struct_type":       `STRUCT<int64_value INT64>`,
				"string_array_type": `ARRAY<STRING>`,
				"json_type":         `JSON`,
			},
			PermitType: true,
			ExpectResult: map[string]interface{}{
				"int64_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
					Value: structpb.NewNullValue(),
				},
				"string_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
					Value: structpb.NewNullValue(),
				},
				"float64_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
					Value: structpb.NewNullValue(),
				},
				"float32_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT32},
					Value: structpb.NewNullValue(),
				},
				"bytes_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BYTES},
					Value: structpb.NewNullValue(),
				},
				"bool_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
					Value: structpb.NewNullValue(),
				},
				"date_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
					Value: structpb.NewNullValue(),
				},
				"timestamp_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
					Value: structpb.NewNullValue(),
				},
				"numeric_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC},
					Value: structpb.NewNullValue(),
				},
				"json_type": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_JSON},
					Value: structpb.NewNullValue(),
				},
				"struct_type": spanner.GenericColumnValue{
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_STRUCT,
						StructType: &spannerpb.StructType{Fields: []*spannerpb.StructType_Field{
							{
								Name: "int64_value",
								Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
							},
						}},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{structpb.NewNullValue()},
					}),
				},
				"string_array_type": spanner.GenericColumnValue{
					Type: &spannerpb.Type{
						Code:             spannerpb.TypeCode_ARRAY,
						ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
					},
					Value: structpb.NewNullValue(),
				},
			},
			ExpectErr: false,
		},
		{
			Name: "values",
			Input: map[string]string{
				"int_literal":          "42",
				"float_literal":        "3.14",
				"string_literal":       `"foo"`,
				"bytes_literal":        `b"bar"`,
				"true_literal":         "TRUE",
				"false_literal":        "FALSE",
				"date_literal":         `DATE "1970-01-01"`,
				"timestamp_literal":    `TIMESTAMP "1970-01-01T00:00:00Z"`,
				"numeric_literal":      `NUMERIC "3.14"`,
				"json_literal":         `JSON '{"number_value": 42}'`,
				"struct_literal":       `STRUCT<float64_value FLOAT64, string_value STRING>(3.14, "foo")`,
				"string_array_literal": `["foo", "bar", "baz"]`,
			},
			PermitType: false,
			ExpectResult: map[string]interface{}{
				"int_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
					Value: structpb.NewStringValue("42"),
				},
				"string_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
					Value: structpb.NewStringValue("foo"),
				},
				"float_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
					Value: structpb.NewNumberValue(3.14),
				},
				"bytes_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BYTES},
					Value: structpb.NewStringValue(base64.RawStdEncoding.EncodeToString([]byte("bar"))),
				},
				"true_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
					Value: structpb.NewBoolValue(true),
				},
				"false_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
					Value: structpb.NewBoolValue(false),
				},
				"date_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
					Value: structpb.NewStringValue("1970-01-01"),
				},
				"timestamp_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
					Value: structpb.NewStringValue("1970-01-01T00:00:00Z"),
				},
				"numeric_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC},
					Value: structpb.NewStringValue("3.14"),
				},
				"json_literal": spanner.GenericColumnValue{
					Type:  &spannerpb.Type{Code: spannerpb.TypeCode_JSON},
					Value: structpb.NewStringValue(`{"number_value": 42}`),
				},
				"struct_literal": spanner.GenericColumnValue{
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_STRUCT,
						StructType: &spannerpb.StructType{Fields: []*spannerpb.StructType_Field{
							{
								Name: "float64_value",
								Type: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
							},
							{
								Name: "string_value",
								Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
							},
						}},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{structpb.NewNumberValue(3.14), structpb.NewStringValue("foo")},
					}),
				},
				"string_array_literal": spanner.GenericColumnValue{
					Type: &spannerpb.Type{
						Code:             spannerpb.TypeCode_ARRAY,
						ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{structpb.NewStringValue("foo"), structpb.NewStringValue("bar"), structpb.NewStringValue("baz")},
					}),
				},
			},
			ExpectErr: false,
		},
	}
	for _, tt := range tcases {
		t.Run(tt.Name, func(t *testing.T) {
			params, err := generateParams(tt.Input, tt.PermitType)
			if !tt.ExpectErr && err != nil {
				t.Fatalf("error is not expected: %v", err)
			}
			if tt.ExpectErr && err == nil {
				t.Fatalf("error is expected, but pass")
			}
			if diff := cmp.Diff(tt.ExpectResult, params, protocmp.Transform()); diff != "" {
				t.Fatalf("(-got +want)\n%v", diff)
			}
		})
	}
}
