package main

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// yamlGoldenCases maps fixture name to jq filter and ResultSet input.
// Golden files live under testdata/yaml_output/<name>.golden.
// PROFILE-shaped cases use testdata/profile/*.json instead (see profile_yaml_test.go).
func yamlGoldenCases() map[string]yamlGoldenCase {
	dcaAlbums := resultSet(
		[]string{"SingerId", "AlbumId", "AlbumTitle", "MarketingBudget"},
		[]*sppb.Type{
			{Code: sppb.TypeCode_INT64},
			{Code: sppb.TypeCode_INT64},
			{Code: sppb.TypeCode_STRING},
			{Code: sppb.TypeCode_INT64},
		},
		[][]*structpb.Value{
			{
				structpb.NewStringValue("2"), structpb.NewStringValue("5"),
				structpb.NewStringValue("Trackfringe"), structpb.NewStringValue("916353"),
			},
			{
				structpb.NewStringValue("3"), structpb.NewStringValue("17"),
				structpb.NewStringValue("Tigergeode"), structpb.NewStringValue("540557"),
			},
			{
				structpb.NewStringValue("4"), structpb.NewStringValue("26"),
				structpb.NewStringValue("Thieftime"), structpb.NewStringValue("926938"),
			},
		},
	)

	allTypes := csvFixtures()["bytes_date_timestamp_numeric"]
	multiScalar := csvFixtures()["null_bool_float64"]

	return map[string]yamlGoldenCase{
		"dca_albums_rowtype_rows": {
			filter: `{rowType: .metadata.rowType, rows: .rows}`,
			rs:     dcaAlbums,
		},
		"all_types_dot": {
			filter: `.`,
			rs:     allTypes,
		},
		"multi_scalar_rows": {
			filter: `.rows`,
			rs:     multiScalar,
		},
	}
}

type yamlGoldenCase struct {
	filter string
	rs     *sppb.ResultSet
}
