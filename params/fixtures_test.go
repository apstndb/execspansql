package params

import (
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadParamFileFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		file string
		want map[string]string
	}{
		{
			file: "testdata/readme_example.yaml",
			want: map[string]string{
				"arr":   "ARRAY<STRING>",
				"names": `[STRUCT<FirstName STRING, LastName STRING>("John", "Doe"), ("Mary", "Sue")]`,
			},
		},
		{
			file: "testdata/spanner_literals.yml",
			want: map[string]string{
				"b":           "TRUE",
				"bs":          `b"foo"`,
				"i64":         "1",
				"f64":         "1.0",
				"f32":         "CAST(1.0 AS FLOAT32)",
				"n":           `NUMERIC "1"`,
				"s":           "'foo'",
				"js":          `JSON "{}"`,
				"ts":          `TIMESTAMP "2000-01-01T00:00:00Z"`,
				"ival_single": "INTERVAL 3 DAY",
				"n_b":         "CAST(NULL AS BOOL)",
			},
		},
		{
			file: "testdata/multiline_literal.yaml",
			want: map[string]string{
				"query": "SELECT SingerId, FirstName\nFROM Singers\nWHERE SingerId = @id\n",
				"id":    "42",
			},
		},
		{
			file: "testdata/large_int.yaml",
			want: map[string]string{
				"id": "1234567890123456789",
			},
		},
		{
			file: "testdata/scalars_extra.yaml",
			want: map[string]string{
				"id":         "123",
				"enabled":    "FALSE",
				"price":      "1.0",
				"ratio":      "0.25",
				"tag":        "yes",
				"label":      "ARRAY<INT64>",
				"created_at": `TIMESTAMP "2023-01-01T00:00:00Z"`,
			},
		},
		{
			file: "testdata/quoted_timestamp.yaml",
			want: map[string]string{
				"quoted_ts": "2023-01-01T00:00:00Z",
				"plain_ts":  `TIMESTAMP "2023-01-01T00:00:00Z"`,
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(filepath.Base(tc.file), func(t *testing.T) {
			t.Parallel()
			got, err := LoadParamFile(tc.file)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("(-want +got)\n%s", diff)
			}
		})
	}
}

func TestLoadParamFileFixtureErrors(t *testing.T) {
	t.Parallel()

	if _, err := LoadParamFile("testdata/nested_map.yaml"); err == nil {
		t.Fatal("expected error for nested map")
	}
	if _, err := LoadParamFile("testdata/nested_array.yaml"); err == nil {
		t.Fatal("expected error for nested array")
	}
}

func TestMergeParamsWithFixture(t *testing.T) {
	t.Parallel()

	file, err := LoadParamFile("testdata/readme_example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	got := MergeParams(file, map[string]string{"arr": "ARRAY<INT64>", "extra": "1"})
	want := map[string]string{
		"arr":   "ARRAY<INT64>",
		"names": `[STRUCT<FirstName STRING, LastName STRING>("John", "Doe"), ("Mary", "Sue")]`,
		"extra": "1",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("(-want +got)\n%s", diff)
	}
}
