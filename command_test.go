package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apstndb/execspansql/jqresult"
)

func TestRunCLIInvalidArgumentsReturnsError(t *testing.T) {
	if err := runCLI(t.Context(), []string{"--unknown-option"}); err == nil {
		t.Fatal("expected argument error")
	}
}

func TestRunCLIHelpReturnsWithoutExecution(t *testing.T) {
	out, err := captureStdout(t, func() error { return runCLI(t.Context(), []string{"--help"}) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Usage: execspansql") {
		t.Fatalf("help = %q", out)
	}
}

func TestRunCLIVersionReturnsWithoutExecution(t *testing.T) {
	out, err := captureStdout(t, func() error { return runCLI(t.Context(), []string{"--version"}) })
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "dev" {
		t.Fatalf("version = %q, want dev", out)
	}
}

func TestProcessFlagsValidationErrorOmitsUsage(t *testing.T) {
	stderr, err := captureStderr(t, func() error {
		_, err := processFlags([]string{"db", "--instance", "i", "--sql", "SELECT 1"})
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "--project is required") {
		t.Fatalf("error = %v, want --project is required", err)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Fatalf("validation error dumped usage: %q", stderr)
	}
}

func TestProcessFlagsUnknownFlagOmitsUsage(t *testing.T) {
	stderr, err := captureStderr(t, func() error {
		return runCLI(t.Context(), []string{"--unknown-option"})
	})
	if err == nil {
		t.Fatal("expected argument error")
	}
	if strings.Contains(stderr, "Usage:") {
		t.Fatalf("parse error dumped usage: %q", stderr)
	}
}

func TestExitStatus(t *testing.T) {
	t.Parallel()

	if got := exitStatus(nil); got != 0 {
		t.Fatalf("nil = %d, want 0", got)
	}
	if got := exitStatus(errors.New("query failed")); got != exitFailure {
		t.Fatalf("generic = %d, want %d", got, exitFailure)
	}
	if got := exitStatus(wrapCommittedOutputError(errors.New("rename failed"))); got != exitOutputAfterCommit {
		t.Fatalf("after commit = %d, want %d", got, exitOutputAfterCommit)
	}

	_, err := processFlags([]string{"--unknown-option"})
	if err == nil {
		t.Fatal("expected parse error")
	}
	if got := exitStatus(err); got != exitUsage {
		t.Fatalf("parse = %d, want %d (%v)", got, exitUsage, err)
	}
}

func TestPrepareCommandFreezesInputs(t *testing.T) {
	dir := t.TempDir()
	sql, params, filter := filepath.Join(dir, "query.sql"), filepath.Join(dir, "params.json"), filepath.Join(dir, "filter.jq")
	for path, content := range map[string]string{sql: "SELECT @v", params: `{"v":1}`, filter: ".rows"} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	o, err := processFlags([]string{"db", "--project", "p", "--instance", "i", "--sql-file", sql, "--param-file", params, "--filter-file", filter})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareCommand(o)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{sql, params, filter} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	if prepared.statement.SQL != "SELECT @v" || len(prepared.statement.Params) != 1 || prepared.jqCode == nil {
		t.Fatalf("unresolved command: %+v", prepared)
	}
	value, ok := prepared.jqCode.Run(map[string]any{"rows": 7}).Next()
	if !ok || value != 7 {
		t.Fatalf("compiled filter result=%v, ok=%v", value, ok)
	}
}

func TestPrintJQHonorsCancellation(t *testing.T) {
	for _, mode := range []jqresult.InputMode{jqresult.InputEager, jqresult.InputLazy} {
		t.Run(string(mode), func(t *testing.T) {
			code, err := jqresult.Compile("def spin: spin; spin")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var out bytes.Buffer
			enc, err := newEncoder(&out, "json", false, false)
			if err != nil {
				t.Fatal(err)
			}
			var input any = map[string]any{}
			if mode == jqresult.InputLazy {
				input = jqresult.NewLazy(nil, false)
			}
			if err := printJQ(ctx, code, input, enc); !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v, want canceled", err)
			}
			if out.Len() != 0 {
				t.Fatalf("canceled filter emitted %q", out.String())
			}
		})
	}
}
