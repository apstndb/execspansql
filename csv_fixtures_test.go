package main

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// csvGoldenFixtures holds one-column (or small) result sets for golden-file tests.
// Each key becomes testdata/experimental_csv/<key>.golden.
func csvGoldenFixtures() map[string]*sppb.ResultSet {
	single := func(name string, typ *sppb.Type, value *structpb.Value) *sppb.ResultSet {
		return resultSet([]string{name}, []*sppb.Type{typ}, [][]*structpb.Value{{value}})
	}

	return map[string]*sppb.ResultSet{
		"bool":        single("v", &sppb.Type{Code: sppb.TypeCode_BOOL}, structpb.NewBoolValue(true)),
		"int64":       single("v", &sppb.Type{Code: sppb.TypeCode_INT64}, structpb.NewStringValue("42")),
		"float32":     single("v", &sppb.Type{Code: sppb.TypeCode_FLOAT32}, structpb.NewNumberValue(1.25)),
		"float64":     single("v", &sppb.Type{Code: sppb.TypeCode_FLOAT64}, structpb.NewNumberValue(3.5)),
		"string":      single("v", &sppb.Type{Code: sppb.TypeCode_STRING}, structpb.NewStringValue("hello")),
		"bytes":       single("v", &sppb.Type{Code: sppb.TypeCode_BYTES}, structpb.NewStringValue("YWJj")),
		"date":        single("v", &sppb.Type{Code: sppb.TypeCode_DATE}, structpb.NewStringValue("2024-06-01")),
		"timestamp":   single("v", &sppb.Type{Code: sppb.TypeCode_TIMESTAMP}, structpb.NewStringValue("2024-06-01T12:34:56Z")),
		"numeric":     single("v", &sppb.Type{Code: sppb.TypeCode_NUMERIC}, structpb.NewStringValue("99.5")),
		"json":        single("v", &sppb.Type{Code: sppb.TypeCode_JSON}, structpb.NewStringValue(`{"k":1}`)),
		"uuid":        single("v", &sppb.Type{Code: sppb.TypeCode_UUID}, structpb.NewStringValue("550e8400-e29b-41d4-a716-446655440000")),
		"interval":    single("v", &sppb.Type{Code: sppb.TypeCode_INTERVAL}, structpb.NewStringValue("P1Y2M3DT4H5M6.789S")),
		"null":        single("v", &sppb.Type{Code: sppb.TypeCode_STRING}, structpb.NewNullValue()),
		"csv_quoting": single("v", &sppb.Type{Code: sppb.TypeCode_STRING}, structpb.NewStringValue(`say, "hi"`)),
		"header_only": resultSet([]string{"id"}, []*sppb.Type{{Code: sppb.TypeCode_INT64}}, nil),
		"array_string": resultSet(
			[]string{"v"},
			[]*sppb.Type{{
				Code:             sppb.TypeCode_ARRAY,
				ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRING},
			}},
			[][]*structpb.Value{{
				structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("foo"),
						structpb.NewStringValue("bar"),
					},
				}),
			}},
		),
		"struct_simple": resultSet(
			[]string{"v"},
			[]*sppb.Type{{
				Code: sppb.TypeCode_STRUCT,
				StructType: &sppb.StructType{Fields: []*sppb.StructType_Field{
					{Name: "i", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
					{Name: "s", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
				}},
			}},
			[][]*structpb.Value{{
				structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("7"),
						structpb.NewStringValue("x"),
					},
				}),
			}},
		),
	}
}

// csvFixtures are multi-column sets for spanvalue contract tests.
func csvFixtures() map[string]*sppb.ResultSet {
	golden := csvGoldenFixtures()
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
		"csv_quoting": golden["csv_quoting"],
		"bytes_date_timestamp_numeric": resultSet(
			[]string{"payload", "d", "ts", "n"},
			[]*sppb.Type{
				{Code: sppb.TypeCode_BYTES},
				{Code: sppb.TypeCode_DATE},
				{Code: sppb.TypeCode_TIMESTAMP},
				{Code: sppb.TypeCode_NUMERIC},
			},
			[][]*structpb.Value{{
				structpb.NewStringValue("YWJj"),
				structpb.NewStringValue("2024-06-01"),
				structpb.NewStringValue("2024-06-01T12:34:56Z"),
				structpb.NewStringValue("99.5"),
			}},
		),
		"json": golden["json"],
	}
}
