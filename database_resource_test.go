package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/apstndb/spanemuboost"
)

func TestDatabaseResourceName(t *testing.T) {
	t.Parallel()
	const full = "projects/project/instances/instance/databases/database"
	for _, tt := range []struct {
		name, project, instance, database, want, err string
	}{
		{name: "IDs", project: "project", instance: "instance", database: "database", want: full},
		{name: "full_name", database: full, want: full},
		{name: "full_name_overrides_defaults", project: "other-project", instance: "other-instance", database: full, want: full},
		{name: "missing_database", err: "database ID is required"},
		{name: "missing_project", database: "database", instance: "instance", err: "--project is required"},
		{name: "missing_instance", database: "database", project: "project", err: "--instance is required"},
		{name: "project_path", project: "projects/project", instance: "instance", database: "database", err: "must be IDs"},
		{name: "instance_path", project: "project", instance: "instances/instance", database: "database", err: "must be IDs"},
		{name: "partial_name", database: "instances/instance/databases/database", err: "invalid database resource name"},
		{name: "empty_project", database: "projects//instances/instance/databases/database", err: "invalid database resource name"},
		{name: "empty_instance", database: "projects/project/instances//databases/database", err: "invalid database resource name"},
		{name: "empty_database", database: "projects/project/instances/instance/databases/", err: "invalid database resource name"},
		{name: "wrong_collection", database: "projects/project/instances/instance/tables/database", err: "invalid database resource name"},
		{name: "extra_segment", database: full + "/table", err: "invalid database resource name"},
		{name: "leading_slash", database: "/" + full, err: "invalid database resource name"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := databaseResourceName(tt.project, tt.instance, tt.database)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("got %q, %v; want error containing %q", got, err, tt.err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestDatabaseResourceIntegration(t *testing.T) {
	env, err := spanemuboost.RunEmulatorWithClients(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close() //nolint:errcheck
	t.Setenv("SPANNER_EMULATOR_HOST", env.Emulator().URI())
	t.Setenv("CLOUDSDK_CORE_PROJECT", "other-project")
	t.Setenv("CLOUDSDK_SPANNER_INSTANCE", "other-instance")
	full, err := databaseResourceName(env.ProjectID, env.InstanceID, env.DatabaseID)
	if err != nil {
		t.Fatal(err)
	}
	for _, flags := range [][]string{nil, {"--project=another-project", "--instance=another-instance"}} {
		output, err := captureStdout(t, func() error {
			return runMain(t, append([]string{full, "--sql=SELECT 12345 AS n"}, flags...))
		})
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Rows [][]string `json:"rows"`
		}
		if err := json.Unmarshal([]byte(output), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != "12345" {
			t.Fatalf("unexpected query result: %s", output)
		}
	}
}

func TestDatabaseResourceFlags(t *testing.T) {
	const full = "projects/project/instances/instance/databases/database"
	for _, tt := range []struct {
		name, projectEnv, instanceEnv string
		args                          []string
		want, err                     string
	}{
		{name: "full_name_only", args: []string{full}, want: full},
		{name: "full_name_overrides_flags", args: []string{full, "--project=other", "--instance=other"}, want: full},
		{name: "full_name_overrides_env", projectEnv: "other", instanceEnv: "other", args: []string{full}, want: full},
		{name: "short_name_with_flags", args: []string{"database", "-p", "project", "-i", "instance"}, want: full},
		{name: "short_name_with_env", projectEnv: "project", instanceEnv: "instance", args: []string{"database"}, want: full},
		{name: "flags_override_env", projectEnv: "other", instanceEnv: "other", args: []string{"database", "-p", "project", "-i", "instance"}, want: full},
		{name: "missing_project", args: []string{"database", "-i", "instance"}, err: "--project is required"},
		{name: "missing_instance", args: []string{"database", "-p", "project"}, err: "--instance is required"},
		{name: "malformed_name", args: []string{"projects/project/databases/database"}, err: "invalid database resource name"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CLOUDSDK_CORE_PROJECT", tt.projectEnv)
			t.Setenv("CLOUDSDK_SPANNER_INSTANCE", tt.instanceEnv)
			var o opts
			parser, err := kong.New(&o)
			if err != nil {
				t.Fatal(err)
			}
			_, err = parser.Parse(append(append([]string{}, tt.args...), "--sql=SELECT 1"))
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("Parse() error = %v; want %q", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := databaseResourceName(o.Project, o.Instance, o.Database)
			if err != nil || got != tt.want {
				t.Fatalf("parsed resource = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}
