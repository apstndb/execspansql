package main

import (
	"os"
	"path/filepath"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"
)

func loadProfileJSONFixture(path string) (*sppb.ResultSet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rs sppb.ResultSet
	if err := protojson.Unmarshal(b, &rs); err != nil {
		return nil, err
	}
	return &rs, nil
}

type profileYAMLGoldenCase struct {
	jsonFile string
	filter   string
	golden   string
}

func profileYAMLGoldenCases() []profileYAMLGoldenCase {
	return []profileYAMLGoldenCase{
		{
			jsonFile: "testdata/profile/param_scalar.json",
			filter:   ".",
			golden:   "profile_param_scalar",
		},
		{
			jsonFile: "testdata/profile/albums_by_singer.json",
			filter:   ".",
			golden:   "profile_albums_by_singer",
		},
		{
			jsonFile: "testdata/profile/singers_limit3.json",
			filter:   ".",
			golden:   "profile_singers_limit3",
		},
		{
			jsonFile: "testdata/profile/singers_limit3.json",
			filter:   `{rowType: .metadata.rowType, rows: .rows}`,
			golden:   "profile_singers_limit3_rowtype_rows",
		},
		{
			jsonFile: "testdata/profile/singers_limit3.json",
			filter:   `.rows[]`,
			golden:   "profile_singers_limit3_rows_stream",
		},
		{
			jsonFile: "testdata/profile/singers_limit3.json",
			filter:   `.metadata.rowType.fields | map(.name)`,
			golden:   "profile_singers_limit3_field_names",
		},
	}
}

func TestProfileJSONToYamlGolden(t *testing.T) {
	for _, tc := range profileYAMLGoldenCases() {
		t.Run(tc.golden, func(t *testing.T) {
			rs, err := loadProfileJSONFixture(tc.jsonFile)
			if err != nil {
				t.Fatal(err)
			}
			if rs.GetMetadata() == nil || rs.GetMetadata().GetRowType() == nil {
				t.Fatal("missing metadata.rowType")
			}
			if rs.GetStats() == nil {
				t.Fatal("missing stats")
			}
			if rs.GetStats().GetQueryPlan() == nil && rs.GetStats().GetQueryStats() == nil {
				t.Fatal("missing query plan or query stats")
			}

			got, err := encodeResultSetYAML(tc.filter, rs)
			if err != nil {
				t.Fatalf("encodeResultSetYAML() error = %v", err)
			}

			goldenPath := filepath.Join("testdata", "yaml_output", tc.golden+".golden")
			if *updateGolden {
				if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
					t.Fatalf("MkdirAll() error = %v", err)
				}
				if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
				t.Logf("updated %s", goldenPath)
				return
			}

			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v (run: go test -update-golden -run TestProfileJSONToYamlGolden)", goldenPath, err)
			}
			if string(got) != string(want) {
				t.Fatalf("PROFILE JSON → YAML mismatch for %s\n\ngot:\n%s\n\nwant:\n%s", tc.golden, got, want)
			}
		})
	}
}
