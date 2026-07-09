package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/execspansql/jqresult"
)

func encodeResultSetYAML(filter string, rs *sppb.ResultSet) ([]byte, error) {
	code, err := jqresult.Compile(filter, jqresult.InputEager)
	if err != nil {
		return nil, err
	}
	iter, cleanup, err := jqresult.Execute(code, jqresult.InputEager, nil, rs, false)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var buf bytes.Buffer
	enc, err := newEncoder(&buf, "yaml", false, false)
	if err != nil {
		return nil, err
	}
	if err := jqresult.Print(enc, iter); err != nil {
		return nil, err
	}
	if closer, ok := enc.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func TestYamlOutputGolden(t *testing.T) {
	for name, tc := range yamlGoldenCases() {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			goldenPath := filepath.Join("testdata", "yaml_output", name+".golden")

			got, err := encodeResultSetYAML(tc.filter, tc.rs)
			if err != nil {
				t.Fatalf("encodeResultSetYAML() error = %v", err)
			}

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
				t.Fatalf("ReadFile(%q) error = %v (run: go test -update-golden -run TestYamlOutputGolden)", goldenPath, err)
			}
			if string(got) != string(want) {
				t.Fatalf("YAML output mismatch for %s\n\ngot:\n%s\n\nwant:\n%s", name, got, want)
			}
		})
	}
}

func TestNewEncoderUnknownFormat(t *testing.T) {
	t.Parallel()

	if _, err := newEncoder(&bytes.Buffer{}, "nope", false, false); err == nil {
		t.Fatal("expected error for unknown format")
	}
}
