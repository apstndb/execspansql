package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/jqresult"
	"go.uber.org/zap"
)

func TestValidateJqOutputOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		o    opts
		mode jqresult.InputMode
		err  string
	}{
		{name: "json_defaults", o: opts{}, mode: jqresult.InputEager},
		{name: "json_raw_with_json", o: opts{Format: "json", JqRawOutput: true}, mode: jqresult.InputEager},
		{
			name: "yaml_raw_output_not_allowed",
			o:    opts{Format: "yaml", JqRawOutput: true},
			mode: jqresult.InputEager,
			err:  "--raw-output and --compact-output are only supported with --format=json",
		},
		{
			name: "yaml_compact_output_not_allowed",
			o:    opts{Format: "yaml", CompactOutput: true},
			mode: jqresult.InputEager,
			err:  "--raw-output and --compact-output are only supported with --format=json",
		},
		{
			name: "experimental_csv_filter_not_allowed",
			o:    opts{Format: "experimental_csv", JqFilter: "."},
			mode: jqresult.InputEager,
			err:  "--format=experimental_csv does not support jq filtering options",
		},
		{
			name: "experimental_csv_lazy_not_allowed",
			o:    opts{Format: "experimental_csv", JqInputMode: "lazy"},
			mode: jqresult.InputLazy,
			err:  "--format=experimental_csv does not support jq filtering options",
		},
		{
			name: "try_partition_filter_not_allowed",
			o:    opts{TryPartitionQuery: true, JqFilter: "."},
			mode: jqresult.InputEager,
			err:  "--try-partition-query does not support jq filtering options",
		},
		{
			name: "try_partition_jq_output_mode_lazy_not_allowed",
			o:    opts{TryPartitionQuery: true, JqInputMode: "lazy"},
			mode: jqresult.InputLazy,
			err:  "--try-partition-query does not support jq filtering options",
		},
		{
			name: "try_partition_compact_output_not_allowed",
			o:    opts{Format: "yaml", TryPartitionQuery: true, CompactOutput: true},
			mode: jqresult.InputEager,
			err:  "--try-partition-query does not support jq filtering options",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateJqOutputOptions(tt.o, tt.mode)
			if tt.err == "" {
				if err != nil {
					t.Fatalf("validateJqOutputOptions() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateJqOutputOptions() expected error containing %q", tt.err)
			}
			if !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("validateJqOutputOptions() error = %q, want %q", err, tt.err)
			}
		})
	}
}

func TestIsReadWriteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantDML bool
	}{
		{name: "plain_update", query: "UPDATE T SET X=1", wantDML: true},
		{name: "commented_update", query: "/* comment */ UPDATE T SET X=1", wantDML: true},
		{name: "line_comment_update", query: "-- comment\nUPDATE T SET X=1", wantDML: true},
		{name: "hash_comment_update", query: "# comment\nINSERT T(a) VALUES(1)", wantDML: true},
		{name: "plain_select", query: "SELECT 1", wantDML: false},
		{name: "commented_select", query: "-- comment\nSELECT 1", wantDML: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isReadWriteStatement(tt.query); got != tt.wantDML {
				t.Fatalf("isReadWriteStatement(%q) = %v, want %v", tt.query, got, tt.wantDML)
			}
		})
	}
}

func TestParseTimestampBound(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, 7, 4, 12, 34, 56, 0, time.UTC)
	tests := []struct {
		name string
		raw  string
		want string
		err  bool
	}{
		{
			name: "default_to_strong",
			raw:  "",
			want: spanner.StrongRead().String(),
		},
		{
			name: "read_timestamp",
			raw:  timestamp.Format(time.RFC3339Nano),
			want: spanner.ReadTimestamp(timestamp).String(),
		},
		{
			name: "invalid_timestamp",
			raw:  "bad",
			err:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimestampBound(tt.raw)
			if tt.err {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTimestampBound(%q) error = %v", tt.raw, err)
			}
			if got.String() != tt.want {
				t.Fatalf("parseTimestampBound(%q) = %q, want %q", tt.raw, got.String(), tt.want)
			}
		})
	}
}

func TestLogGrpcClientOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
		want int
	}{
		{name: "off", mode: logGrpcModeOff, want: 0},
		{name: "metadata", mode: logGrpcModeMetadata, want: 2},
		{name: "payload", mode: logGrpcModePayload, want: 2},
		{name: "unknown", mode: "unexpected", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := len(logGrpcClientOptions(tt.mode)); got != tt.want {
				t.Fatalf("len(logGrpcClientOptions(%q)) = %d, want %d", tt.mode, got, tt.want)
			}
		})
	}
}

func TestBuildGrpcZapLoggerFallback(t *testing.T) {
	t.Parallel()

	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{"unknown-sink://stderr"}

	if got := buildGrpcZapLogger(cfg); got == nil {
		t.Fatal("buildGrpcZapLogger() returned nil")
	}
}

func optsWithReadTimestamp(ts string) opts {
	var o opts
	o.TimestampBound.ReadTimestamp = ts
	return o
}

func optsWithStrong() opts {
	var o opts
	o.TimestampBound.Strong = true
	return o
}

func TestValidateExecutionOptions(t *testing.T) {
	t.Parallel()

	queryTimestamp := spanner.ReadTimestamp(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC))

	tests := []struct {
		name string
		o    opts
		mode queryMode
		err  string
	}{
		{
			name: "try_partition_allows_single_query",
			o:    opts{TryPartitionQuery: true},
			mode: single{spanner.StrongRead()},
		},
		{
			name: "try_partition_rejects_priority",
			o:    opts{TryPartitionQuery: true, Priority: "high"},
			mode: single{spanner.StrongRead()},
			err:  "--priority cannot be used with --try-partition-query",
		},
		{
			name: "try_partition_rejects_dml",
			o:    opts{TryPartitionQuery: true},
			mode: readWrite{},
			err:  "--try-partition-query cannot be used with DML statements",
		},
		{
			name: "try_partition_rejects_partitioned_dml",
			o:    opts{TryPartitionQuery: true, EnablePartitionedDML: true},
			mode: partitionedDML{},
			err:  "--try-partition-query cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "read_timestamp_allows_single_query",
			o:    optsWithReadTimestamp("2025-01-01T00:00:00Z"),
			mode: single{queryTimestamp},
		},
		{
			name: "read_timestamp_rejects_dml",
			o:    optsWithReadTimestamp("2025-01-01T00:00:00Z"),
			mode: readWrite{},
			err:  "--read-timestamp cannot be used with DML statements",
		},
		{
			name: "read_timestamp_rejects_partitioned_dml",
			o: func() opts {
				o := optsWithReadTimestamp("2025-01-01T00:00:00Z")
				o.EnablePartitionedDML = true
				return o
			}(),
			mode: partitionedDML{},
			err:  "--read-timestamp cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "strong_rejects_dml",
			o:    optsWithStrong(),
			mode: readWrite{},
			err:  "--strong cannot be used with DML statements",
		},
		{
			name: "strong_rejects_partitioned_dml",
			o: func() opts {
				o := optsWithStrong()
				o.EnablePartitionedDML = true
				return o
			}(),
			mode: partitionedDML{},
			err:  "--strong cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "partitioned_dml_rejects_non_dml",
			o:    opts{EnablePartitionedDML: true},
			mode: single{spanner.StrongRead()},
			err:  "--enable-partitioned-dml can only be used with DML statements",
		},
		{
			name: "jq_lazy_allows_read_write_dml",
			o:    opts{JqInputMode: "lazy"},
			mode: readWrite{},
		},
		{
			name: "partitioned_dml_allows_eager",
			o:    opts{EnablePartitionedDML: true, JqInputMode: "eager"},
			mode: partitionedDML{},
		},
		{
			name: "partitioned_dml_rejects_lazy",
			o:    opts{EnablePartitionedDML: true, JqInputMode: "lazy"},
			mode: partitionedDML{},
			err:  "--jq-input-mode=lazy is not supported for partitioned DML",
		},
		{
			name: "plan_allows_read_write_dml",
			o:    opts{QueryMode: "PLAN"},
			mode: readWrite{},
		},
		{
			name: "profile_allows_read_write_dml",
			o:    opts{QueryMode: "PROFILE"},
			mode: readWrite{},
		},
		{
			name: "with_plan_and_stats_allows_read_write_dml",
			o:    opts{QueryMode: "WITH_PLAN_AND_STATS"},
			mode: readWrite{},
		},
		{
			name: "with_stats_allows_read_write_dml",
			o:    opts{QueryMode: "WITH_STATS"},
			mode: readWrite{},
		},
		{
			name: "plan_rejects_partitioned_dml",
			o:    opts{EnablePartitionedDML: true, QueryMode: "PLAN"},
			mode: partitionedDML{},
			err:  "--query-mode=PLAN cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "profile_rejects_partitioned_dml",
			o:    opts{EnablePartitionedDML: true, QueryMode: "PROFILE"},
			mode: partitionedDML{},
			err:  "--query-mode=PROFILE cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "with_plan_and_stats_rejects_partitioned_dml",
			o:    opts{EnablePartitionedDML: true, QueryMode: "WITH_PLAN_AND_STATS"},
			mode: partitionedDML{},
			err:  "--query-mode=WITH_PLAN_AND_STATS cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "with_stats_rejects_partitioned_dml",
			o:    opts{EnablePartitionedDML: true, QueryMode: "WITH_STATS"},
			mode: partitionedDML{},
			err:  "--query-mode=WITH_STATS cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "plan_rejects_partitioned_dml_json",
			o:    opts{EnablePartitionedDML: true, QueryMode: "PLAN", Format: "json"},
			mode: partitionedDML{},
			err:  "--query-mode=PLAN cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "profile_rejects_partitioned_dml_yaml",
			o:    opts{EnablePartitionedDML: true, QueryMode: "PROFILE", Format: "yaml"},
			mode: partitionedDML{},
			err:  "--query-mode=PROFILE cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "plan_rejects_partitioned_dml_csv",
			o:    opts{EnablePartitionedDML: true, QueryMode: "PLAN", Format: "experimental_csv"},
			mode: partitionedDML{},
			err:  "--query-mode=PLAN cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "normal_allows_partitioned_dml_json",
			o:    opts{EnablePartitionedDML: true, QueryMode: "NORMAL", Format: "json"},
			mode: partitionedDML{},
		},
		{
			name: "normal_allows_partitioned_dml_yaml",
			o:    opts{EnablePartitionedDML: true, QueryMode: "NORMAL", Format: "yaml"},
			mode: partitionedDML{},
		},
		{
			name: "normal_allows_partitioned_dml_csv",
			o:    opts{EnablePartitionedDML: true, QueryMode: "NORMAL", Format: "experimental_csv"},
			mode: partitionedDML{},
		},
		{
			name: "plan_output_allows_profile",
			o:    opts{PlanOutput: "plan.json", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
		},
		{
			name: "plan_output_allows_plan_mode",
			o:    opts{PlanOutput: "plan.json", QueryMode: "PLAN"},
			mode: single{spanner.StrongRead()},
		},
		{
			name: "plan_output_allows_with_plan_and_stats",
			o:    opts{PlanOutput: "plan.json", QueryMode: "WITH_PLAN_AND_STATS"},
			mode: single{spanner.StrongRead()},
		},
		{
			name: "plan_output_rejects_normal",
			o:    opts{PlanOutput: "plan.json", QueryMode: "NORMAL"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-output requires --query-mode=PLAN, PROFILE, or WITH_PLAN_AND_STATS",
		},
		{
			name: "plan_output_rejects_with_stats",
			o:    opts{PlanOutput: "plan.json", QueryMode: "WITH_STATS"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-output requires --query-mode=PLAN, PROFILE, or WITH_PLAN_AND_STATS",
		},
		{
			name: "plan_format_requires_plan_output",
			o:    opts{PlanFormat: "json", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-format requires --plan-output",
		},
		{
			name: "discard_results_requires_plan_output",
			o:    opts{DiscardResults: true, QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--discard-results requires --plan-output",
		},
		{
			name: "plan_output_rejects_try_partition_query",
			o:    opts{PlanOutput: "plan.json", QueryMode: "PROFILE", TryPartitionQuery: true},
			mode: single{spanner.StrongRead()},
			err:  "--plan-output cannot be combined with --try-partition-query",
		},
		{
			name: "plan_output_rejects_partitioned_dml",
			o:    opts{PlanOutput: "plan.json", QueryMode: "PROFILE", EnablePartitionedDML: true},
			mode: partitionedDML{},
			err:  "--plan-output cannot be combined with --enable-partitioned-dml",
		},
		{
			name: "plan_format_allows_text",
			o:    opts{PlanOutput: "plan.txt", PlanFormat: "text", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
		},
		{
			name: "plan_format_allows_svg",
			o:    opts{PlanOutput: "plan.svg", PlanFormat: "svg", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
		},
		{
			name: "plan_format_rejects_unknown",
			o:    opts{PlanOutput: "plan.json", PlanFormat: "html", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-format must be json, yaml, text, dot, mermaid, d2, svg, or png",
		},
		{
			name: "plan_text_style_requires_plan_output",
			o:    opts{PlanTextStyle: "compact", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-text-style requires --plan-output",
		},
		{
			name: "plan_text_style_rejects_json",
			o:    opts{PlanOutput: "plan.json", PlanFormat: "json", PlanTextStyle: "compact", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-text-style cannot be used with --plan-format=json",
		},
		{
			name: "plan_full_rejects_text",
			o:    opts{PlanOutput: "plan.txt", PlanFormat: "text", PlanFull: true, QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-full cannot be used with --plan-format=text",
		},
		{
			name: "plan_show_query_rejects_yaml",
			o:    opts{PlanOutput: "plan.yaml", PlanFormat: "yaml", PlanShowQuery: true, QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-show-query cannot be used with --plan-format=yaml",
		},
		{
			name: "plan_wrap_width_rejects_dot",
			o:    opts{PlanOutput: "plan.dot", PlanFormat: "dot", PlanWrapWidth: 80, QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
			err:  "--plan-wrap-width cannot be used with --plan-format=dot",
		},
		{
			name: "plan_text_style_allows_text",
			o:    opts{PlanOutput: "plan.txt", PlanFormat: "text", PlanTextStyle: "compact", PlanPrint: "enhanced", QueryMode: "PROFILE"},
			mode: single{spanner.StrongRead()},
		},
		{
			name: "plan_full_allows_mermaid",
			o:    opts{PlanOutput: "plan.mmd", PlanFormat: "mermaid", PlanFull: true, PlanShowQuery: true, QueryMode: "PROFILE", Sql: "SELECT 1"},
			mode: single{spanner.StrongRead()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExecutionOptions(tt.o, tt.mode)
			if tt.err == "" {
				if err != nil {
					t.Fatalf("validateExecutionOptions() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateExecutionOptions() expected error containing %q", tt.err)
			}
			if !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("validateExecutionOptions() error = %q, want %q", err, tt.err)
			}
		})
	}
}

func TestQueryModeForQuery(t *testing.T) {
	t.Parallel()

	tb := spanner.StrongRead()
	candidateTimestamp := spanner.ReadTimestamp(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC))

	tests := []struct {
		name               string
		query              string
		partitionedEnabled bool
		tb                 spanner.TimestampBound
		wantMode           string
	}{
		{name: "dml_with_comment", query: "-- c\nUPDATE T SET X=1", partitionedEnabled: false, tb: tb, wantMode: "readWrite"},
		{name: "partitioned_dml", query: "UPDATE T SET X=1", partitionedEnabled: true, tb: tb, wantMode: "partitionedDML"},
		{name: "normal_query", query: "SELECT 1", partitionedEnabled: false, tb: candidateTimestamp, wantMode: "single"},
		{
			name:               "partitioned_flag_with_select",
			query:              "SELECT 1",
			partitionedEnabled: true,
			tb:                 candidateTimestamp,
			wantMode:           "single",
		},
		{
			name:               "partitioned_flag_with_ddl",
			query:              "CREATE TABLE T (K INT64) PRIMARY KEY (K)",
			partitionedEnabled: true,
			tb:                 tb,
			wantMode:           "single",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryModeForQuery(tt.query, tt.partitionedEnabled, tt.tb)
			switch tt.wantMode {
			case "readWrite":
				if _, ok := got.(readWrite); !ok {
					t.Fatalf("queryModeForQuery() = %T, want readWrite", got)
				}
			case "partitionedDML":
				if _, ok := got.(partitionedDML); !ok {
					t.Fatalf("queryModeForQuery() = %T, want partitionedDML", got)
				}
			case "single":
				s, ok := got.(single)
				if !ok {
					t.Fatalf("queryModeForQuery() = %T, want single", got)
				}
				if s.String() != tt.tb.String() {
					t.Fatalf("queryModeForQuery() tb = %q, want %q", s.String(), tt.tb.String())
				}
			}
		})
	}
}

func TestQueryOptionsForPriority(t *testing.T) {
	t.Parallel()

	mode := sppb.ExecuteSqlRequest_PROFILE
	tests := []struct {
		name     string
		priority string
		want     sppb.RequestOptions_Priority
	}{
		{name: "high", priority: "high", want: sppb.RequestOptions_PRIORITY_HIGH},
		{name: "low", priority: "low", want: sppb.RequestOptions_PRIORITY_LOW},
		{name: "medium", priority: "medium", want: sppb.RequestOptions_PRIORITY_MEDIUM},
		{name: "unspecified", priority: "unspecified", want: sppb.RequestOptions_PRIORITY_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryOptionsFor(mode, tt.priority)
			if got.Mode == nil || *got.Mode != mode {
				t.Fatalf("query options mode = %v, want %v", got.Mode, mode)
			}
			if got.Priority != tt.want {
				t.Fatalf("query options priority = %v, want %v", got.Priority, tt.want)
			}
		})
	}
}

func TestProcessFlagsPriority(t *testing.T) {
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })

	baseArgs := []string{"execspansql", "database", "--project", "project", "--instance", "instance", "--sql", "SELECT 1"}
	tests := []struct {
		name    string
		args    []string
		want    string
		wantErr string
	}{
		{name: "default", args: baseArgs, want: "unspecified"},
		{name: "high", args: append(baseArgs, "--priority", "high"), want: "high"},
		{name: "low", args: append(baseArgs, "--priority", "low"), want: "low"},
		{name: "medium", args: append(baseArgs, "--priority", "medium"), want: "medium"},
		{name: "unspecified", args: append(baseArgs, "--priority", "unspecified"), want: "unspecified"},
		{name: "invalid", args: append(baseArgs, "--priority", "urgent"), wantErr: "--priority must be one of"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = tt.args
			got, err := processFlags()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("processFlags() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Priority != tt.want {
				t.Fatalf("priority = %q, want %q", got.Priority, tt.want)
			}
		})
	}
}
