package params

import (
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
