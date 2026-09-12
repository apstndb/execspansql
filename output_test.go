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
	"github.com/apstndb/execspansql/jqresult"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestResolveDestinationStdoutSpellings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw  string
		kind destKind
	}{
		{raw: "-", kind: destKindStdout},
		{raw: "/dev/stdout", kind: destKindStdout},
		{raw: "/dev/stderr", kind: destKindStderr},
		{raw: "plan.json", kind: destKindFile},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got := resolveDestination(tt.raw)
			if got.kind != tt.kind {
				t.Fatalf("resolveDestination(%q).kind = %v, want %v", tt.raw, got.kind, tt.kind)
			}
		})
	}
}

func TestEffectivePlanFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		o    opts
		want string
	}{
		{name: "explicit_yaml", o: opts{PlanFormat: "yaml", Format: "json"}, want: "yaml"},
		{name: "explicit_text_case", o: opts{PlanFormat: "TEXT", Format: "json"}, want: "text"},
		{name: "follows_json", o: opts{Format: "json"}, want: "json"},
		{name: "follows_yaml", o: opts{Format: "yaml"}, want: "yaml"},
		{name: "csv_defaults_to_json", o: opts{Format: "experimental_csv"}, want: "json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectivePlanFormat(tt.o); got != tt.want {
				t.Fatalf("effectivePlanFormat() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateDestinationsStdoutCollision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		o    opts
		err  string
	}{
		{
			name: "dash_and_dash",
			o:    opts{Output: "-", PlanOutput: "-"},
			err:  "cannot both write to stdout",
		},
		{
			name: "dash_and_dev_stdout",
			o:    opts{Output: "-", PlanOutput: "/dev/stdout"},
			err:  "cannot both write to stdout",
		},
		{
			name: "dev_stdout_and_dash",
			o:    opts{Output: "/dev/stdout", PlanOutput: "-"},
			err:  "cannot both write to stdout",
		},
		{
			name: "both_stderr",
			o:    opts{Output: "/dev/stderr", PlanOutput: "/dev/stderr"},
			err:  "cannot both write to stderr",
		},
		{
			name: "stdout_and_stderr_ok",
			o:    opts{Output: "-", PlanOutput: "/dev/stderr"},
		},
		{
			name: "discard_allows_plan_on_stdout",
			o:    opts{Output: "-", PlanOutput: "-", DiscardResults: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDestinations(tt.o)
			if tt.err == "" {
				if err != nil {
					t.Fatalf("validateDestinations() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("validateDestinations() error = %v, want %q", err, tt.err)
			}
		})
	}
}

func TestValidateDestinationsSameFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.json")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := validateDestinations(opts{Output: path, PlanOutput: path})
	if err == nil || !strings.Contains(err.Error(), "same file") {
		t.Fatalf("same path: error = %v, want same file", err)
	}

	rel := filepath.Join(dir, ".", "out.json")
	err = validateDestinations(opts{Output: path, PlanOutput: rel})
	if err == nil || !strings.Contains(err.Error(), "same file") {
		t.Fatalf("cleaned path: error = %v, want same file", err)
	}
}

func TestValidateDestinationsSymlinkAndHardLink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "plan.json")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join(dir, "plan-link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	err := validateDestinations(opts{Output: target, PlanOutput: link})
	if err == nil || !strings.Contains(err.Error(), "same file") {
		t.Fatalf("symlink: error = %v, want same file", err)
	}

	hard := filepath.Join(dir, "plan-hard.json")
	if err := os.Link(target, hard); err != nil {
		t.Skipf("hard link not supported: %v", err)
	}
	err = validateDestinations(opts{Output: target, PlanOutput: hard})
	if err == nil || !strings.Contains(err.Error(), "same file") {
		t.Fatalf("hard link: error = %v, want same file", err)
	}
}

func TestValidateDestinationsInputAlias(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sqlFile := filepath.Join(dir, "query.sql")
	paramFile := filepath.Join(dir, "params.yaml")
	filterFile := filepath.Join(dir, "filter.jq")
	for _, p := range []string{sqlFile, paramFile, filterFile} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name string
		o    opts
		err  string
	}{
		{
			name: "output_aliases_sql_file",
			o:    opts{Output: sqlFile, SqlFile: sqlFile},
			err:  "--output cannot alias --sql-file",
		},
		{
			name: "plan_aliases_param_file",
			o:    opts{Output: filepath.Join(dir, "rows.json"), PlanOutput: paramFile, ParamFile: paramFile},
			err:  "--plan-output cannot alias --param-file",
		},
		{
			name: "output_aliases_filter_file",
			o:    opts{Output: filterFile, JqFromFile: filterFile},
			err:  "--output cannot alias --filter-file",
		},
		{
			name: "discard_skips_primary_alias",
			o: opts{
				Output:         sqlFile,
				PlanOutput:     filepath.Join(dir, "plan.json"),
				SqlFile:        sqlFile,
				DiscardResults: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDestinations(tt.o)
			if tt.err == "" {
				if err != nil {
					t.Fatalf("validateDestinations() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("validateDestinations() error = %v, want %q", err, tt.err)
			}
		})
	}
}

func TestStripQueryPlanForPrimaryDoesNotMutateOriginal(t *testing.T) {
	t.Parallel()

	rs := profileResultSetForSplitTest()
	orig := rs.Stats
	planStats, md := stripQueryPlanForPrimary(rs)
	if orig.GetQueryPlan() == nil || len(orig.GetQueryPlan().GetPlanNodes()) == 0 {
		t.Fatal("original stats.QueryPlan was mutated")
	}
	if rs.Stats == orig {
		t.Fatal("primary stats still shares the original stats pointer")
	}
	if rs.Stats.GetQueryPlan() != nil {
		t.Fatal("primary stats still has QueryPlan")
	}
	if planStats.GetQueryPlan() == nil || planStats.GetQueryPlan().GetPlanNodes()[0].GetDisplayName() != "Scan" {
		t.Fatal("plan clone missing QueryPlan")
	}
	if md == nil || md.GetRowType() == nil {
		t.Fatal("metadata missing")
	}

	m, err := jqresult.ResultSetMap(rs)
	if err != nil {
		t.Fatal(err)
	}
	stats, _ := m["stats"].(map[string]any)
	if stats == nil {
		t.Fatal("primary stats missing")
	}
	if _, ok := stats["queryPlan"]; ok {
		t.Fatalf("primary stats still has queryPlan: %#v", stats)
	}
	if _, ok := stats["queryStats"]; !ok {
		t.Fatalf("primary stats missing queryStats: %#v", stats)
	}
}

func TestWritePlanEnvelopeOmitsRows(t *testing.T) {
	t.Parallel()

	rs := profileResultSetForSplitTest()
	planStats, md := stripQueryPlanForPrimary(rs)

	var buf bytes.Buffer
	if err := writePlan(context.Background(), &buf, "json", md, planStats, opts{}); err != nil {
		t.Fatal(err)
	}
	m, err := jqresult.ProtoToMap(&sppb.ResultSet{Metadata: md, Stats: planStats})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m["rows"]; ok {
		t.Fatalf("plan envelope has rows: %#v", m)
	}
	stats, _ := m["stats"].(map[string]any)
	if _, ok := stats["queryPlan"]; !ok {
		t.Fatal("plan envelope missing queryPlan")
	}

	var yamlBuf bytes.Buffer
	if err := writePlan(context.Background(), &yamlBuf, "yaml", md, planStats, opts{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(yamlBuf.String(), "queryPlan") {
		t.Fatalf("yaml plan missing queryPlan: %s", yamlBuf.String())
	}
}

func TestWritePlanRejectsEmptyPlan(t *testing.T) {
	t.Parallel()

	err := writePlan(context.Background(), ioDiscardWriter{}, "json", nil, &sppb.ResultSetStats{
		QueryPlan: &sppb.QueryPlan{},
	}, opts{})
	if !errors.Is(err, errNoQueryPlan) {
		t.Fatalf("error = %v, want errNoQueryPlan", err)
	}
}

func TestOutputSinksPublishAndAbort(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	primary := filepath.Join(dir, "rows.json")
	if err := os.WriteFile(primary, []byte("old-primary"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("abort_leaves_existing", func(t *testing.T) {
		s, err := newOutputSinks(opts{Output: primary})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.primary.Write([]byte("new-primary")); err != nil {
			t.Fatal(err)
		}
		s.Abort()
		got, err := os.ReadFile(primary)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "old-primary" {
			t.Fatalf("after abort: %q, want old-primary", got)
		}
	})

	t.Run("finish_error_before_primary_ready_aborts", func(t *testing.T) {
		s, err := newOutputSinks(opts{Output: primary})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.primary.Write([]byte("partial")); err != nil {
			t.Fatal(err)
		}
		err = s.Finish(errors.New("query failed"))
		if err == nil || !strings.Contains(err.Error(), "query failed") {
			t.Fatalf("Finish() error = %v", err)
		}
		got, err := os.ReadFile(primary)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "old-primary" {
			t.Fatalf("after failed query: %q, want old-primary", got)
		}
	})

	t.Run("publish_overwrites", func(t *testing.T) {
		s, err := newOutputSinks(opts{Output: primary})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.primary.Write([]byte("new-primary")); err != nil {
			t.Fatal(err)
		}
		s.MarkPrimaryComplete()
		if err := s.Finish(nil); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(primary)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "new-primary" {
			t.Fatalf("after publish: %q, want new-primary", got)
		}
	})

	t.Run("plan_error_publishes_primary", func(t *testing.T) {
		plan := filepath.Join(dir, "plan.json")
		if err := os.WriteFile(plan, []byte("old-plan"), 0o644); err != nil {
			t.Fatal(err)
		}
		s, err := newOutputSinks(opts{Output: primary, PlanOutput: plan})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.primary.Write([]byte("rows-ok")); err != nil {
			t.Fatal(err)
		}
		s.MarkPrimaryComplete()
		err = s.Finish(errNoQueryPlan)
		if err == nil || !strings.Contains(err.Error(), "primary output written") {
			t.Fatalf("Finish() error = %v, want primary written", err)
		}
		got, err := os.ReadFile(primary)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "rows-ok" {
			t.Fatalf("primary = %q, want rows-ok", got)
		}
		gotPlan, err := os.ReadFile(plan)
		if err != nil {
			t.Fatal(err)
		}
		if string(gotPlan) != "old-plan" {
			t.Fatalf("plan = %q, want old-plan", gotPlan)
		}
	})
}

func TestDiscardResultsProducesNoPrimaryBytes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	primary := filepath.Join(dir, "rows.csv")
	plan := filepath.Join(dir, "plan.json")
	s, err := newOutputSinks(opts{
		Output:         primary,
		PlanOutput:     plan,
		DiscardResults: true,
		QueryMode:      "PROFILE",
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.primary != nil {
		t.Fatal("primary writer should be nil when discarding results")
	}
	if _, err := s.plan.Write([]byte("plan-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(primary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("primary file exists: %v", err)
	}
	got, err := os.ReadFile(plan)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "plan-bytes" {
		t.Fatalf("plan = %q", got)
	}
}

func TestMaterializeWithoutRows(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		o    opts
		want bool
	}{
		{"default keeps rows", opts{}, false},
		{"redact drops rows", opts{RedactRows: true}, true},
		{"discard drops rows", opts{DiscardResults: true}, true},
		{"both drop rows", opts{RedactRows: true, DiscardResults: true}, true},
	}
	for _, tc := range cases {
		if got := materializeWithoutRows(tc.o); got != tc.want {
			t.Errorf("%s: materializeWithoutRows = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestFinishPublishFailureAfterCommitIsWrapped(t *testing.T) {
	t.Parallel()

	// Make the plan rename fail by turning the target path into a directory
	// after the sinks opened their temp files.
	dir := t.TempDir()
	plan := filepath.Join(dir, "plan.json")
	s, err := newOutputSinks(opts{
		Output:     "-",
		PlanOutput: plan,
		QueryMode:  "PROFILE",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(plan, 0o755); err != nil {
		t.Fatal(err)
	}
	s.MarkPrimaryComplete()
	finishErr := s.Finish(nil)
	if finishErr == nil {
		t.Fatal("expected publish failure")
	}
	// runCLI wraps this for committed modes; the wrapper must carry the
	// no-rollback wording so a caller does not replay the DML.
	got := wrapCommittedOutputError(finishErr).Error()
	for _, want := range []string{"not a rollback", "not replayed", "plan output"} {
		if !strings.Contains(got, want) {
			t.Fatalf("wrapped error %q lacks %q", got, want)
		}
	}
}

func TestProcessFlagsOutputDefaults(t *testing.T) {
	args := []string{"database", "--project", "p", "--instance", "i", "--sql", "SELECT 1"}
	got, err := processFlags(args)
	if err != nil {
		t.Fatal(err)
	}
	if got.Output != "-" {
		t.Fatalf("Output = %q, want -", got.Output)
	}
	if got.PlanOutput != "" || got.PlanFormat != "" || got.DiscardResults {
		t.Fatalf("unexpected plan flags: %+v", got)
	}

	args = append([]string{"database", "--project", "p", "--instance", "i", "--sql", "SELECT 1"},
		"-o", "rows.json", "--plan-output", "plan.json", "--plan-format", "yaml", "--discard-results")
	got, err = processFlags(args)
	if err != nil {
		t.Fatal(err)
	}
	if got.Output != "rows.json" || got.PlanOutput != "plan.json" || got.PlanFormat != "yaml" || !got.DiscardResults {
		t.Fatalf("got %+v", got)
	}

	args = []string{"database", "--project", "p", "--instance", "i", "--sql", "SELECT 1",
		"--plan-output", "plan.txt", "--plan-format", "text", "--plan-text-style", "compact",
		"--plan-wrap-width", "80", "--plan-print", "enhanced"}
	got, err = processFlags(args)
	if err != nil {
		t.Fatal(err)
	}
	if got.PlanFormat != "text" || got.PlanTextStyle != "compact" || got.PlanWrapWidth != 80 || got.PlanPrint != "enhanced" {
		t.Fatalf("renderer flags: %+v", got)
	}
}

func TestSplitModeValidationBeforeClient(t *testing.T) {
	err := runMain(t, []string{
		"database", "--project", "p", "--instance", "i", "--sql", "SELECT 1",
		"--query-mode", "PROFILE", "--plan-output", "-",
	})
	if err == nil || !strings.Contains(err.Error(), "stdout") {
		t.Fatalf("both on stdout: error = %v", err)
	}

	err = runMain(t, []string{
		"database", "--project", "p", "--instance", "i", "--sql", "SELECT 1",
		"--plan-output", "plan.json",
	})
	if err == nil || !strings.Contains(err.Error(), "PLAN, PROFILE, or WITH_PLAN_AND_STATS") {
		t.Fatalf("NORMAL plan-output: error = %v", err)
	}

	orig := planDestIsTerminal
	planDestIsTerminal = func(kind destKind) bool { return kind == destKindStdout }
	defer func() { planDestIsTerminal = orig }()
	err = runMain(t, []string{
		"database", "--project", "p", "--instance", "i", "--sql", "SELECT 1",
		"--query-mode", "PROFILE", "--discard-results", "--plan-output", "-",
		"--plan-format", "png",
	})
	if err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("png to TTY: error = %v", err)
	}
}

type ioDiscardWriter struct{}

func (ioDiscardWriter) Write([]byte) (int, error) { return 0, nil }

func TestWritePlanTextContainsOperator(t *testing.T) {
	t.Parallel()

	rs := profileResultSetForSplitTest()
	planStats, md := stripQueryPlanForPrimary(rs)
	var buf bytes.Buffer
	if err := writePlan(context.Background(), &buf, "text", md, planStats, opts{}); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, "Scan") {
		t.Fatalf("text plan = %q, want Scan", got)
	}
	if strings.Contains(got, "queryPlan") {
		t.Fatalf("text plan still looks like JSON: %s", got)
	}
}

func TestWritePlanGraphSmoke(t *testing.T) {
	t.Parallel()

	rs := profileResultSetForSplitTest()
	planStats, md := stripQueryPlanForPrimary(rs)
	for _, format := range []string{"dot", "mermaid", "d2"} {
		var buf bytes.Buffer
		if err := writePlan(context.Background(), &buf, format, md, planStats, opts{}); err != nil {
			t.Fatalf("%s: %v", format, err)
		}
		if !strings.Contains(buf.String(), "Scan") {
			t.Fatalf("%s plan = %q, want Scan", format, buf.String())
		}
	}
}

func TestWritePlanPNGMagic(t *testing.T) {
	rs := profileResultSetForSplitTest()
	planStats, md := stripQueryPlanForPrimary(rs)
	var buf bytes.Buffer
	if err := writePlan(context.Background(), &buf, "png", md, planStats, opts{}); err != nil {
		t.Fatal(err)
	}
	got := buf.Bytes()
	if len(got) < 8 || !bytes.Equal(got[:8], []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}) {
		t.Fatalf("png magic = %x", got[:min(8, len(got))])
	}
}

func TestValidatePNGOnTerminal(t *testing.T) {
	orig := planDestIsTerminal
	t.Cleanup(func() { planDestIsTerminal = orig })

	planDestIsTerminal = func(kind destKind) bool {
		return kind == destKindStdout || kind == destKindStderr
	}
	err := validatePlanOutputOptions(opts{
		PlanOutput: "-", PlanFormat: "png", QueryMode: "PROFILE", DiscardResults: true,
	})
	if err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("error = %v, want terminal refusal", err)
	}

	planDestIsTerminal = func(destKind) bool { return false }
	if err := validatePlanOutputOptions(opts{
		PlanOutput: "-", PlanFormat: "png", QueryMode: "PROFILE", DiscardResults: true,
	}); err != nil {
		t.Fatalf("redirected stdout png: %v", err)
	}
	if err := validatePlanOutputOptions(opts{
		PlanOutput: "plan.png", PlanFormat: "png", QueryMode: "PROFILE",
	}); err != nil {
		t.Fatalf("file png: %v", err)
	}
}

func profileResultSetForSplitTest() *sppb.ResultSet {
	return &sppb.ResultSet{
		Metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{Fields: []*sppb.StructType_Field{
				{Name: "id", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
			}},
		},
		Rows: []*structpb.ListValue{{Values: []*structpb.Value{structpb.NewStringValue("1")}}},
		Stats: &sppb.ResultSetStats{
			QueryPlan: &sppb.QueryPlan{PlanNodes: []*sppb.PlanNode{{
				Index:       0,
				Kind:        sppb.PlanNode_RELATIONAL,
				DisplayName: "Scan",
			}}},
			QueryStats: &structpb.Struct{Fields: map[string]*structpb.Value{
				"elapsed_time": structpb.NewStringValue("1 msecs"),
			}},
		},
	}
}
