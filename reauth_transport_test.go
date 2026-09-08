package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/auth"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/internal/grpctest"
	"github.com/apstndb/spanemuboost"
	"google.golang.org/api/option"
)

func TestReauthAutoSkipsPreflightWithInjectedClientOptions(t *testing.T) {
	env, err := spanemuboost.RunEmulatorWithClients(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close() //nolint:errcheck
	t.Setenv("SPANNER_EMULATOR_HOST", env.Emulator().URI())

	orig := newReauthHooks
	t.Cleanup(func() { newReauthHooks = orig })
	var detectCalls int
	newReauthHooks = func() *reauthHooks {
		h := orig()
		h.detect = func(context.Context) (*auth.Credentials, error) {
			detectCalls++
			return nil, errors.New("DetectDefault must not run when client options are injected")
		}
		return h
	}

	var executeSQL int
	dialOptions := grpctest.Inspect(func(_ string, req any) error {
		if _, ok := req.(*sppb.ExecuteSqlRequest); ok {
			executeSQL++
		}
		return nil
	})

	args := []string{
		env.DatabaseID, "--project", env.ProjectID, "--instance", env.InstanceID,
		"--sql", "SELECT 1", "--timeout", "30s", "--reauth", "auto",
	}
	out, err := captureStdout(t, func() error {
		return runMain(t, args, option.WithGRPCDialOption(dialOptions[0]), option.WithGRPCDialOption(dialOptions[1]))
	})
	if err != nil {
		t.Fatal(err)
	}
	if detectCalls != 0 {
		t.Fatalf("detect calls = %d, want 0", detectCalls)
	}
	if executeSQL != 1 {
		t.Fatalf("ExecuteSql count = %d, want 1 (unchanged from a single SELECT)", executeSQL)
	}
	if !strings.Contains(out, "1") {
		t.Fatalf("stdout = %q, want query result", out)
	}
}
