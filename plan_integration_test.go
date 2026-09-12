package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

func TestPreparedPlanUsesFrozenSQLFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.sql")
	const sql = "SELECT frozen_query_marker"
	if err := os.WriteFile(path, []byte(sql), 0600); err != nil {
		t.Fatal(err)
	}
	o, err := processFlags([]string{"db", "--project", "p", "--instance", "i", "--sql-file", path,
		"--query-mode", "PROFILE", "--plan-output", "plan.dot", "--plan-format", "dot", "--plan-show-query"})
	if err != nil {
		t.Fatal(err)
	}
	command, err := prepareCommand(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	var plan bytes.Buffer
	sinks := &outputSinks{plan: &plan, hasPlan: true}
	if err := command.writeResultSet(t.Context(), profileResultSetForSplitTest(), sinks); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), sql) {
		t.Fatalf("plan does not contain frozen SQL: %s", &plan)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plan.Reset()
	if err := command.writeResultSet(ctx, profileResultSetForSplitTest(), sinks); !errors.Is(err, context.Canceled) {
		t.Fatalf("render error = %v, want cancellation", err)
	}
}

func TestDMLRenderFailurePreservesCommittedOutput(t *testing.T) {
	for _, format := range []string{"json", "yaml", "experimental_csv"} {
		t.Run(format, func(t *testing.T) {
			server := &executionServer{queryPlan: &sppb.QueryPlan{PlanNodes: []*sppb.PlanNode{{
				Index: 0, Kind: sppb.PlanNode_RELATIONAL, DisplayName: "Apply",
				ChildLinks: []*sppb.PlanNode_ChildLink{{ChildIndex: 0}},
			}}}}
			startQueryStatsModeServer(t, server)
			dir := t.TempDir()
			rows, plan := filepath.Join(dir, "rows"), filepath.Join(dir, "plan")
			if err := os.WriteFile(plan, []byte("previous plan"), 0600); err != nil {
				t.Fatal(err)
			}
			err := runCLI(t.Context(), []string{"db", "--project", "p", "--instance", "i",
				"--sql", "UPDATE T SET V=1 THEN RETURN V", "--query-mode", "PROFILE",
				"--format", format, "--output", rows, "--plan-output", plan, "--plan-format", "text", "--timeout", "5s"})
			for _, want := range []string{"statement was committed", "not a rollback", "plan rendering failed", "cycle"} {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %v, want %q", err, want)
				}
			}
			if server.executes.Load() != 1 || server.commits.Load() != 1 {
				t.Fatalf("SQL replayed: executions=%d commits=%d", server.executes.Load(), server.commits.Load())
			}
			data, err := os.ReadFile(rows)
			if err != nil || !strings.Contains(string(data), "attempt-1") {
				t.Fatalf("committed primary output=%q, error=%v", data, err)
			}
			data, err = os.ReadFile(plan)
			if err != nil || string(data) != "previous plan" {
				t.Fatalf("previous plan replaced: %q, error=%v", data, err)
			}
		})
	}
}
