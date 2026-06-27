package params

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseParamFlags(t *testing.T) {
	t.Parallel()

	got, err := ParseParamFlags([]string{"arr:ARRAY<STRING>", "name:42"})
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
}

func TestLoadParamFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	yamlPath := filepath.Join(dir, "params.yaml")
	if err := os.WriteFile(yamlPath, []byte("arr: ARRAY<STRING>\nname: \"42\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := LoadParamFile(yamlPath)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"arr": "ARRAY<STRING>", "name": "42"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("(-want +got)\n%s", diff)
	}

	jsonPath := filepath.Join(dir, "params.json")
	if err := os.WriteFile(jsonPath, []byte(`{"arr":"ARRAY<STRING>"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = LoadParamFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(map[string]string{"arr": "ARRAY<STRING>"}, got); diff != "" {
		t.Fatalf("(-want +got)\n%s", diff)
	}

	yamlScalarsPath := filepath.Join(dir, "params-scalars.yaml")
	if err := os.WriteFile(yamlScalarsPath, []byte("id: 123\nenabled: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = LoadParamFile(yamlScalarsPath)
	if err != nil {
		t.Fatal(err)
	}
	wantScalars := map[string]string{"id": "123", "enabled": "TRUE"}
	if diff := cmp.Diff(wantScalars, got); diff != "" {
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
