package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanemuboost"
)

const pdmlQueryModeAuditSQL = "UPDATE PdmlQueryModeAudit SET V=99 WHERE TRUE"

func TestPartitionedDMLQueryMode(t *testing.T) {
	ctx := context.Background()
	env, err := spanemuboost.RunEmulatorWithClients(ctx,
		spanemuboost.WithSetupDDLs([]string{
			"CREATE TABLE PdmlQueryModeAudit (Id INT64, V INT64) PRIMARY KEY (Id)",
		}),
		spanemuboost.WithSetupRawDMLs([]string{
			"INSERT INTO PdmlQueryModeAudit (Id, V) VALUES (1, 10)",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close() //nolint:errcheck

	t.Setenv("SPANNER_EMULATOR_HOST", env.Emulator().URI())

	cliBase := []string{
		env.DatabaseID,
		"--project", env.ProjectID,
		"--instance", env.InstanceID,
		"--sql", pdmlQueryModeAuditSQL,
		"--enable-partitioned-dml",
	}

	for _, queryMode := range []string{"PLAN", "PROFILE", "WITH_PLAN_AND_STATS", "WITH_STATS"} {
		for _, format := range []string{"json", "yaml", "experimental_csv"} {
			queryMode := queryMode
			format := format
			t.Run("rejects_"+queryMode+"_"+format, func(t *testing.T) {
				want := "--query-mode=" + queryMode + " cannot be combined with --enable-partitioned-dml"
				err := runMain(t, append(cliBase, "--query-mode", queryMode, "--format", format))
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("_main() error = %v, want %q", err, want)
				}
				if got := readPdmlQueryModeV(t, ctx, env.Client); got != 10 {
					t.Fatalf("stored V = %d, want 10 after %s %s rejection", got, queryMode, format)
				}
			})
		}
	}

	for _, format := range []string{"json", "yaml", "experimental_csv"} {
		format := format
		t.Run("NORMAL_"+format+"_accepts_partitioned_dml", func(t *testing.T) {
			setPdmlQueryModeV(t, ctx, env.Client, 10)
			_, err := captureStdout(t, func() error {
				return runMain(t, append(cliBase, "--query-mode", "NORMAL", "--format", format))
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := readPdmlQueryModeV(t, ctx, env.Client); got != 99 {
				t.Fatalf("stored V = %d, want 99 after NORMAL %s PDML", got, format)
			}
		})
	}
}

func runMain(t *testing.T, args []string) error {
	t.Helper()
	old := os.Args
	os.Args = append([]string{"execspansql"}, args...)
	defer func() { os.Args = old }()
	return _main()
}

func readPdmlQueryModeV(t *testing.T, ctx context.Context, client *spanner.Client) int64 {
	t.Helper()
	row, err := client.Single().ReadRow(ctx, "PdmlQueryModeAudit", spanner.Key{1}, []string{"V"})
	if err != nil {
		t.Fatal(err)
	}
	var v int64
	if err := row.Column(0, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func setPdmlQueryModeV(t *testing.T, ctx context.Context, client *spanner.Client, v int64) {
	t.Helper()
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{
			SQL:    "UPDATE PdmlQueryModeAudit SET V=@v WHERE Id=1",
			Params: map[string]any{"v": v},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fnErr := fn()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.String(), fnErr
}
