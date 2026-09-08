package planrender

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/*.golden")

func TestParseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    Format
		wantErr bool
	}{
		{in: "text", want: FormatText},
		{in: "TEXT", want: FormatText},
		{in: " dot ", want: FormatDOT},
		{in: "mermaid", want: FormatMermaid},
		{in: "d2", want: FormatD2},
		{in: "svg", want: FormatSVG},
		{in: "png", want: FormatPNG},
		{in: "json", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := ParseFormat(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseFormat(%q) error = nil, want error", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFormat(%q) error = %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("ParseFormat(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestFormatFlags(t *testing.T) {
	t.Parallel()

	if FormatPNG.IsBinary() != true {
		t.Fatal("FormatPNG.IsBinary() = false, want true")
	}
	if FormatSVG.IsBinary() {
		t.Fatal("FormatSVG.IsBinary() = true, want false")
	}
	if FormatText.IsBinary() {
		t.Fatal("FormatText.IsBinary() = true, want false")
	}
	if !FormatSVG.NeedsGraphviz() || !FormatPNG.NeedsGraphviz() {
		t.Fatal("SVG and PNG should need Graphviz")
	}
	if FormatDOT.NeedsGraphviz() || FormatText.NeedsGraphviz() || FormatMermaid.NeedsGraphviz() || FormatD2.NeedsGraphviz() {
		t.Fatal("text and source formats should not need Graphviz")
	}
}

func TestRenderTextGolden(t *testing.T) {
	rs := loadSingersFixture(t)
	stats := rs.GetStats()

	t.Run("profile", func(t *testing.T) {
		assertTextGolden(t, "singers_limit3_profile_text.golden", rs.GetMetadata().GetRowType(), stats)
	})
	t.Run("plan", func(t *testing.T) {
		assertTextGolden(t, "singers_limit3_plan_text.golden", rs.GetMetadata().GetRowType(), stripNodeStats(stats))
	})
}

func TestGraphSourceSmoke(t *testing.T) {
	t.Parallel()

	rs := loadSingersFixture(t)
	rowType := rs.GetMetadata().GetRowType()
	stats := rs.GetStats()
	const root = "Limit"

	for _, format := range []Format{FormatDOT, FormatMermaid, FormatD2} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := Render(context.Background(), &buf, format, rowType, stats, Options{}); err != nil {
				t.Fatalf("Render(%s) error = %v", format, err)
			}
			got := buf.String()
			if got == "" {
				t.Fatalf("Render(%s) produced empty output", format)
			}
			if !strings.Contains(got, root) {
				t.Fatalf("Render(%s) output does not contain root operator %q:\n%s", format, root, got)
			}
		})
	}
}

func TestRenderSVG(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Graphviz WASM render in short mode")
	}

	var buf bytes.Buffer
	if err := Render(context.Background(), &buf, FormatSVG, nil, smallScanStats(), Options{}); err != nil {
		t.Fatalf("Render(svg) error = %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "<svg") && !strings.HasPrefix(strings.TrimSpace(got), "<svg") {
		t.Fatalf("Render(svg) output does not contain <svg; got prefix %q", truncate(got, 80))
	}
}

func TestRenderPNG(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Graphviz WASM render in short mode")
	}

	var buf bytes.Buffer
	if err := Render(context.Background(), &buf, FormatPNG, nil, smallScanStats(), Options{}); err != nil {
		t.Fatalf("Render(png) error = %v", err)
	}
	got := buf.Bytes()
	magic := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}
	if len(got) < len(magic) || !bytes.Equal(got[:len(magic)], magic) {
		t.Fatalf("Render(png) missing PNG magic bytes; got prefix %q", truncate(string(got), 32))
	}
}

func TestRenderMissingPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stats *sppb.ResultSetStats
	}{
		{name: "nil stats"},
		{name: "nil queryPlan", stats: &sppb.ResultSetStats{}},
		{name: "empty planNodes", stats: &sppb.ResultSetStats{QueryPlan: &sppb.QueryPlan{}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := Render(context.Background(), io.Discard, FormatText, nil, tc.stats, Options{})
			if !errors.Is(err, ErrNoQueryPlan) {
				t.Fatalf("Render() error = %v, want ErrNoQueryPlan", err)
			}
		})
	}
}

func TestRenderDanglingChildIndex(t *testing.T) {
	t.Parallel()

	stats := &sppb.ResultSetStats{
		QueryPlan: &sppb.QueryPlan{
			PlanNodes: []*sppb.PlanNode{
				{
					Index:       0,
					Kind:        sppb.PlanNode_RELATIONAL,
					DisplayName: "Scan",
					ChildLinks: []*sppb.PlanNode_ChildLink{
						{ChildIndex: 99},
					},
				},
			},
		},
	}
	err := Render(context.Background(), io.Discard, FormatText, nil, stats, Options{})
	if err == nil {
		t.Fatal("Render() error = nil, want dangling childIndex error")
	}
	if errors.Is(err, ErrNoQueryPlan) {
		t.Fatalf("Render() error = %v, want library validation error not ErrNoQueryPlan", err)
	}
}

func TestRenderRepeatedChildDAG(t *testing.T) {
	t.Parallel()

	stats := &sppb.ResultSetStats{
		QueryPlan: &sppb.QueryPlan{
			PlanNodes: []*sppb.PlanNode{
				{
					Index:       0,
					Kind:        sppb.PlanNode_RELATIONAL,
					DisplayName: "Apply",
					ChildLinks: []*sppb.PlanNode_ChildLink{
						{ChildIndex: 1},
						{ChildIndex: 1},
					},
				},
				{
					Index:       1,
					Kind:        sppb.PlanNode_RELATIONAL,
					DisplayName: "Scan",
				},
			},
		},
	}
	for _, format := range []Format{FormatText, FormatDOT, FormatMermaid, FormatD2} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := Render(context.Background(), &buf, format, nil, stats, Options{}); err != nil {
				t.Fatalf("Render(%s) error = %v", format, err)
			}
			if buf.Len() == 0 {
				t.Fatalf("Render(%s) produced empty output", format)
			}
			if !strings.Contains(buf.String(), "Apply") {
				t.Fatalf("Render(%s) output does not contain Apply:\n%s", format, buf.String())
			}
		})
	}
}

func TestRenderUnicodeAndControlChars(t *testing.T) {
	t.Parallel()

	desc := "日本語\nline2\r\n\tctrl:\x01\x1b"
	stats := filterStatsWithPredicate(desc)
	for _, format := range []Format{FormatText, FormatDOT, FormatMermaid, FormatD2} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := Render(context.Background(), &buf, format, nil, stats, Options{}); err != nil {
				t.Fatalf("Render(%s) error = %v (adapter must not panic or fail on predicate escaping)", format, err)
			}
			if buf.Len() == 0 {
				t.Fatalf("Render(%s) produced empty output", format)
			}
		})
	}
}

func TestRenderCycle(t *testing.T) {
	t.Parallel()

	stats := &sppb.ResultSetStats{
		QueryPlan: &sppb.QueryPlan{
			PlanNodes: []*sppb.PlanNode{
				{
					Index:       0,
					Kind:        sppb.PlanNode_RELATIONAL,
					DisplayName: "Apply",
					ChildLinks: []*sppb.PlanNode_ChildLink{
						{ChildIndex: 0},
					},
				},
			},
		},
	}
	err := Render(context.Background(), io.Discard, FormatText, nil, stats, Options{})
	if err == nil {
		t.Fatal("Render() error = nil, want cycle error")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Render() error = %v, want cycle", err)
	}
}

func TestOptionValidation(t *testing.T) {
	t.Parallel()

	stats := smallScanStats()
	tests := []struct {
		name    string
		format  Format
		opts    Options
		wantErr string
	}{
		{name: "text defaults", format: FormatText, opts: Options{}},
		{name: "text explicit current", format: FormatText, opts: Options{TextStyle: "current"}},
		{name: "text compact", format: FormatText, opts: Options{TextStyle: "compact"}},
		{name: "text wrap", format: FormatText, opts: Options{WrapWidth: 40}},
		{name: "text none sections", format: FormatText, opts: Options{PrintSections: "none"}},
		{name: "graph defaults", format: FormatDOT, opts: Options{}},
		{name: "graph full", format: FormatDOT, opts: Options{Full: true}},
		{name: "graph show query", format: FormatDOT, opts: Options{ShowQuery: true, Query: "SELECT 1"}},
		{name: "graph show query stats", format: FormatDOT, opts: Options{ShowQueryStats: true}},
		{
			name:    "text with Full",
			format:  FormatText,
			opts:    Options{Full: true},
			wantErr: "Full",
		},
		{
			name:    "text with ShowQuery",
			format:  FormatText,
			opts:    Options{ShowQuery: true},
			wantErr: "ShowQuery",
		},
		{
			name:    "text with ShowQueryStats",
			format:  FormatText,
			opts:    Options{ShowQueryStats: true},
			wantErr: "ShowQueryStats",
		},
		{
			name:    "graph with TextStyle",
			format:  FormatDOT,
			opts:    Options{TextStyle: "compact"},
			wantErr: "TextStyle",
		},
		{
			name:    "graph with WrapWidth",
			format:  FormatMermaid,
			opts:    Options{WrapWidth: 80},
			wantErr: "WrapWidth",
		},
		{
			name:    "graph with PrintSections",
			format:  FormatD2,
			opts:    Options{PrintSections: "full"},
			wantErr: "PrintSections",
		},
		{
			name:    "svg with TextStyle",
			format:  FormatSVG,
			opts:    Options{TextStyle: "traditional"},
			wantErr: "TextStyle",
		},
		{
			name:    "Query without ShowQuery",
			format:  FormatDOT,
			opts:    Options{Query: "SELECT 1"},
			wantErr: "option Query is only used when ShowQuery is true",
		},
		{
			name:    "invalid TextStyle",
			format:  FormatText,
			opts:    Options{TextStyle: "wide"},
			wantErr: "invalid TextStyle",
		},
		{
			name:    "invalid PrintSections",
			format:  FormatText,
			opts:    Options{PrintSections: "not-a-section"},
			wantErr: "invalid PrintSections",
		},
		{
			name:    "negative WrapWidth",
			format:  FormatText,
			opts:    Options{WrapWidth: -1},
			wantErr: "WrapWidth cannot be negative",
		},
		{
			name:    "unknown format",
			format:  Format("json"),
			opts:    Options{},
			wantErr: "unknown plan format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := Render(context.Background(), io.Discard, tc.format, nil, stats, tc.opts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Render() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Render() error = nil, want substring %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Render() error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestRenderCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := Render(ctx, io.Discard, FormatDOT, nil, smallScanStats(), Options{})
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Render() error = %v, want context.Canceled", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("cancelled graph render took %s, want prompt return", elapsed)
	}
}

func TestRenderCancelledSVG(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Graphviz cancellation check in short mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := Render(ctx, io.Discard, FormatSVG, nil, smallScanStats(), Options{})
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Render() error = %v, want context.Canceled", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("cancelled SVG render took %s (WASM should not start)", elapsed)
	}
}

func TestRenderWriterError(t *testing.T) {
	t.Parallel()

	want := errors.New("sink closed")
	err := Render(context.Background(), errWriter{err: want}, FormatText, nil, smallScanStats(), Options{})
	if !errors.Is(err, want) {
		t.Fatalf("Render() error = %v, want wrapped sink error", err)
	}
}

func TestRenderNilWriter(t *testing.T) {
	t.Parallel()

	err := Render(context.Background(), nil, FormatText, nil, smallScanStats(), Options{})
	if err == nil || !strings.Contains(err.Error(), "writer is nil") {
		t.Fatalf("Render() error = %v, want nil writer error", err)
	}
}

func TestRenderNilRowType(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	if err := Render(context.Background(), &buf, FormatDOT, nil, smallScanStats(), Options{}); err != nil {
		t.Fatalf("Render() with nil rowType error = %v", err)
	}
	if !strings.Contains(buf.String(), "Scan") {
		t.Fatalf("Render() output missing Scan:\n%s", buf.String())
	}
}

func TestShowQueryInjectsQueryText(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	err := Render(context.Background(), &buf, FormatDOT, nil, smallScanStats(), Options{
		ShowQuery: true,
		Query:     "SELECT custom_query_text",
	})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	if !strings.Contains(buf.String(), "SELECT custom_query_text") {
		t.Fatalf("Render() output missing injected query:\n%s", buf.String())
	}
}

func assertTextGolden(t *testing.T, name string, rowType *sppb.StructType, stats *sppb.ResultSetStats) {
	t.Helper()
	goldenPath := filepath.Join("testdata", name)

	var buf bytes.Buffer
	if err := Render(context.Background(), &buf, FormatText, rowType, stats, Options{}); err != nil {
		t.Fatalf("Render(text) error = %v", err)
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
		t.Fatalf("ReadFile(%q) error = %v (run: go test -update-golden -run TestRenderTextGolden ./internal/planrender)", goldenPath, err)
	}
	if string(got) != string(want) {
		t.Fatalf("text output mismatch for %s\n\ngot:\n%s\n\nwant:\n%s", name, got, want)
	}
}

func loadSingersFixture(t *testing.T) *sppb.ResultSet {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "profile", "singers_limit3.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	var rs sppb.ResultSet
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, &rs); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
	return &rs
}

func stripNodeStats(stats *sppb.ResultSetStats) *sppb.ResultSetStats {
	cloned := proto.CloneOf(stats)
	for _, n := range cloned.GetQueryPlan().GetPlanNodes() {
		n.ExecutionStats = nil
	}
	return cloned
}

func smallScanStats() *sppb.ResultSetStats {
	return &sppb.ResultSetStats{
		QueryPlan: &sppb.QueryPlan{
			PlanNodes: []*sppb.PlanNode{
				{
					Index:       0,
					Kind:        sppb.PlanNode_RELATIONAL,
					DisplayName: "Scan",
				},
			},
		},
	}
}

func filterStatsWithPredicate(desc string) *sppb.ResultSetStats {
	return &sppb.ResultSetStats{
		QueryPlan: &sppb.QueryPlan{
			PlanNodes: []*sppb.PlanNode{
				{
					Index:       0,
					Kind:        sppb.PlanNode_RELATIONAL,
					DisplayName: "Filter",
					ChildLinks: []*sppb.PlanNode_ChildLink{
						{ChildIndex: 1},
						{ChildIndex: 2, Type: "Residual Condition"},
					},
				},
				{
					Index:       1,
					Kind:        sppb.PlanNode_RELATIONAL,
					DisplayName: "Scan",
				},
				{
					Index:       2,
					Kind:        sppb.PlanNode_SCALAR,
					DisplayName: "Function",
					ShortRepresentation: &sppb.PlanNode_ShortRepresentation{
						Description: desc,
					},
				},
			},
		},
	}
}

type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
