package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/experimental_csv/*.golden files")

func TestExperimentalCsvGolden(t *testing.T) {
	for name, rs := range csvGoldenFixtures() {
		t.Run(name, func(t *testing.T) {
			goldenPath := filepath.Join("testdata", "experimental_csv", name+".golden")

			var buf bytes.Buffer
			if err := writeCsv(&buf, rs); err != nil {
				t.Fatalf("writeCsv() error = %v", err)
			}
			got := buf.Bytes()

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
				t.Fatalf("ReadFile(%q) error = %v (run: go test -update-golden -run TestExperimentalCsvGolden)", goldenPath, err)
			}
			if string(got) != string(want) {
				t.Fatalf("writeCsv() output mismatch for %s\n\ngot:\n%s\n\nwant:\n%s", name, got, want)
			}
		})
	}
}
