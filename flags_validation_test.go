package main

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
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
