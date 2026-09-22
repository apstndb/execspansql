package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lazyJSONErrorServer struct {
	queryStatsModeServer
}

func (s *lazyJSONErrorServer) ExecuteStreamingSql(*sppb.ExecuteSqlRequest, sppb.Spanner_ExecuteStreamingSqlServer) error {
	return status.Error(codes.InvalidArgument, "invalid SQL from review fixture")
}

func TestLazyToJSONReturnsOriginalQueryError(t *testing.T) {
	startQueryStatsModeServer(t, &lazyJSONErrorServer{})
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=THIS IS INVALID SQL",
		"--jq-input-mode=lazy", "--filter=.rows | tojson",
		"--output=" + filepath.Join(t.TempDir(), "out"), "--timeout=5s",
	})
	if err == nil {
		t.Fatal("expected query error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, error = %v, want InvalidArgument", status.Code(err), err)
	}
	if !strings.Contains(err.Error(), "invalid SQL from review fixture") {
		t.Fatalf("error = %v, want original message", err)
	}
}

func TestLazyToJSONTryCatchSeesQueryError(t *testing.T) {
	startQueryStatsModeServer(t, &lazyJSONErrorServer{})
	path := filepath.Join(t.TempDir(), "out")
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=THIS IS INVALID SQL",
		"--jq-input-mode=lazy", `--filter=try (.rows | tojson) catch .`,
		"--output=" + path, "--timeout=5s",
	})
	if err != nil {
		t.Fatalf("try/catch returned %v", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(got), "invalid SQL from review fixture") {
		t.Fatalf("catch output = %q", got)
	}
}

func TestLazyAtJSONReturnsOriginalQueryError(t *testing.T) {
	startQueryStatsModeServer(t, &lazyJSONErrorServer{})
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=THIS IS INVALID SQL",
		"--jq-input-mode=lazy", "--filter=@json",
		"--output=" + filepath.Join(t.TempDir(), "out"), "--timeout=5s",
	})
	if err == nil {
		t.Fatal("expected query error")
	}
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "invalid SQL from review fixture") {
		t.Fatalf("error = %v, want InvalidArgument and the original message", err)
	}
}

func TestLazyUserDefinedToJSONWins(t *testing.T) {
	startQueryStatsModeServer(t, &queryStatsModeServer{})
	path := filepath.Join(t.TempDir(), "out")
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=SELECT value FROM T",
		"--jq-input-mode=lazy", `--filter=def tojson: "user-defined"; tojson`,
		"--output=" + path, "--timeout=5s",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(got), "user-defined") {
		t.Fatalf("output = %q, want the user definition", got)
	}
}

func TestLazyFormatJSONTryCatchSeesQueryError(t *testing.T) {
	startQueryStatsModeServer(t, &lazyJSONErrorServer{})
	path := filepath.Join(t.TempDir(), "out")
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=THIS IS INVALID SQL",
		"--jq-input-mode=lazy", `--filter=try format("json") catch .`,
		"--output=" + path, "--timeout=5s",
	})
	if err != nil {
		t.Fatalf("try/catch returned %v", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(got), "invalid SQL from review fixture") {
		t.Fatalf("catch output = %q", got)
	}
}

func TestLazyFormatJSONReturnsOriginalQueryError(t *testing.T) {
	startQueryStatsModeServer(t, &lazyJSONErrorServer{})
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=THIS IS INVALID SQL",
		"--jq-input-mode=lazy", `--filter=format("json")`,
		"--output=" + filepath.Join(t.TempDir(), "out"), "--timeout=5s",
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "invalid SQL from review fixture") {
		t.Fatalf("error = %v, want InvalidArgument and the original message", err)
	}
}

func TestLazyToJSONHealthyRows(t *testing.T) {
	startQueryStatsModeServer(t, &queryStatsModeServer{})
	path := filepath.Join(t.TempDir(), "out")
	err := runCLI(t.Context(), []string{
		"db", "--project=p", "--instance=i", "--sql=SELECT value FROM T",
		"--jq-input-mode=lazy", "--filter=.rows | tojson",
		"--output=" + path, "--timeout=5s",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(got), "value") {
		t.Fatalf("tojson = %q, want row contents", got)
	}
}
