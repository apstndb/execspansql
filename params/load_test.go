package params

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseParamFlags(t *testing.T) {
	t.Parallel()

	got, err := ParseParamFlags([]string{"arr=ARRAY<STRING>", "name=42"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"arr": "ARRAY<STRING>", "name": "42"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("(-want +got)\n%s", diff)
	}

	if _, err := ParseParamFlags([]string{"invalid"}); err == nil {
		t.Fatal("expected error for invalid param")
	}

	if _, err := ParseParamFlags([]string{"=value"}); err == nil {
		t.Fatal("expected error for empty parameter name")
	}

	got, err = ParseParamFlags([]string{`expr=a=b`})
	if err != nil {
		t.Fatal(err)
	}
	if got["expr"] != "a=b" {
		t.Fatalf("got expr=%q, want %q", got["expr"], "a=b")
	}

	got, err = ParseParamFlags([]string{"arr:ARRAY<STRING>", "legacy:42"})
	if err != nil {
		t.Fatal(err)
	}
	wantLegacy := map[string]string{"arr": "ARRAY<STRING>", "legacy": "42"}
	if diff := cmp.Diff(wantLegacy, got); diff != "" {
		t.Fatalf("(-want +got)\n%s", diff)
	}

	got, err = ParseParamFlags([]string{`key:val=ue`})
	if err != nil {
		t.Fatal(err)
	}
	if got["key"] != "val=ue" {
		t.Fatalf("got key=%q, want %q", got["key"], "val=ue")
	}

	if _, err := ParseParamFlags([]string{":value"}); err == nil {
		t.Fatal("expected error for empty parameter name")
	}
}

func TestLoadParamFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	jsonPath := filepath.Join(dir, "params.json")
	if err := os.WriteFile(jsonPath, []byte(`{"arr":"ARRAY<STRING>"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := LoadParamFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(map[string]string{"arr": "ARRAY<STRING>"}, got); diff != "" {
		t.Fatalf("(-want +got)\n%s", diff)
	}

	jsonLargeIntPath := filepath.Join(dir, "params-large-int.json")
	if err := os.WriteFile(jsonLargeIntPath, []byte(`{"id":1234567890123456789}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = LoadParamFile(jsonLargeIntPath)
	if err != nil {
		t.Fatal(err)
	}
	if got["id"] != "1234567890123456789" {
		t.Fatalf("got id=%q, want exact integer string", got["id"])
	}

	emptyPath := filepath.Join(dir, "params-empty.yaml")
	if err := os.WriteFile(emptyPath, []byte("   \n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = LoadParamFile(emptyPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map for empty file, got %v", got)
	}

	nullPath := filepath.Join(dir, "params-null.yaml")
	if err := os.WriteFile(nullPath, []byte("x: null\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadParamFile(nullPath); err == nil {
		t.Fatal("expected error for untyped null in param file")
	}
}

func TestLoadParamFileAcceptsSingleMappingDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		content  string
		want     map[string]string
	}{
		{
			name:     "JSON with trailing whitespace",
			filename: "params.json",
			content:  "{}  \n\t",
			want:     map[string]string{},
		},
		{
			name:     "YAML empty mapping",
			filename: "params.yaml",
			content:  "{}\n",
			want:     map[string]string{},
		},
		{
			name:     "YAML comments only",
			filename: "params.yaml",
			content:  "# no parameters yet\n",
			want:     map[string]string{},
		},
		{
			name:     "YAML empty document marker",
			filename: "params.yaml",
			content:  "---\n",
			want:     map[string]string{},
		},
		{
			name:     "YAML trailing empty document marker",
			filename: "params.yaml",
			content:  "x: INT64\n---\n",
			want:     map[string]string{"x": "INT64"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tc.filename)
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := LoadParamFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("LoadParamFile() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadParamFileRejectsInvalidDocuments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		content  string
	}{
		{
			name:     "JSON trailing non-whitespace content",
			filename: "params.json",
			content:  `{"x":1} trailing`,
		},
		{
			name:     "JSON second value",
			filename: "params.json",
			content:  `{"x":1} {"y":2}`,
		},
		{
			name:     "JSON top-level null",
			filename: "params.json",
			content:  `null`,
		},
		{
			name:     "YAML second document",
			filename: "params.yaml",
			content:  "x: 1\n---\ny: 2\n",
		},
		{
			name:     "YAML top-level null",
			filename: "params.yaml",
			content:  "null\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tc.filename)
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadParamFile(path); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestFormatParamFloat(t *testing.T) {
	t.Parallel()

	s, err := formatParamFloat(42)
	if err != nil {
		t.Fatal(err)
	}
	if s != "42.0" {
		t.Fatalf("got %q, want %q", s, "42.0")
	}
}

func TestFormatParamFloatRejectsNonFinite(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value float64
	}{
		{"NaN", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := formatParamFloat(tc.value); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLoadParamFileRejectsNonFiniteFloat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "params-nan.yaml")
	if err := os.WriteFile(path, []byte("x: .nan\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadParamFile(path); err == nil {
		t.Fatal("expected error for NaN in param file")
	}
}

func TestMergeParams(t *testing.T) {
	t.Parallel()

	got := MergeParams(
		map[string]string{"a": "1", "b": "2"},
		map[string]string{"b": "override"},
	)
	want := map[string]string{"a": "1", "b": "override"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("(-want +got)\n%s", diff)
	}
}
