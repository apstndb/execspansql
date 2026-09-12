package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/internal/planrender"
	"github.com/apstndb/execspansql/jqresult"
	svwriter "github.com/apstndb/spanvalue/writer"
	"golang.org/x/term"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	destStdoutDash = "-"
	destDevStdout  = "/dev/stdout"
	destDevStderr  = "/dev/stderr"

	planModesHelp  = "PLAN, PROFILE, or WITH_PLAN_AND_STATS"
	planFormatHelp = "json, yaml, text, dot, mermaid, d2, svg, or png"
)

var errNoQueryPlan = errors.New("query returned no query plan (nil or empty planNodes); not retrying the statement")

// planDestIsTerminal reports whether a stdout/stderr plan destination is a TTY.
// Tests replace this to avoid depending on the process's real descriptors.
var planDestIsTerminal = func(kind destKind) bool {
	switch kind {
	case destKindStdout:
		return term.IsTerminal(int(os.Stdout.Fd()))
	case destKindStderr:
		return term.IsTerminal(int(os.Stderr.Fd()))
	default:
		return false
	}
}

type destKind int

const (
	destKindStdout destKind = iota
	destKindStderr
	destKindFile
)

type resolvedDest struct {
	kind destKind
	raw  string
	abs  string
}

func defaultPrimaryOutput(raw string) string {
	if raw == "" {
		return destStdoutDash
	}
	return raw
}

func resolveDestination(raw string) resolvedDest {
	switch raw {
	case destStdoutDash, destDevStdout:
		return resolvedDest{kind: destKindStdout, raw: raw}
	case destDevStderr:
		return resolvedDest{kind: destKindStderr, raw: raw}
	default:
		abs, err := filepath.Abs(filepath.Clean(raw))
		if err != nil {
			abs = filepath.Clean(raw)
		}
		return resolvedDest{kind: destKindFile, raw: raw, abs: abs}
	}
}

func destLabel(d resolvedDest) string {
	switch d.kind {
	case destKindStdout:
		return "stdout"
	case destKindStderr:
		return "stderr"
	default:
		if d.abs != "" {
			return d.abs
		}
		return d.raw
	}
}

func isPlanProducingQueryMode(mode string) bool {
	switch mode {
	case "PLAN", "PROFILE", "WITH_PLAN_AND_STATS":
		return true
	default:
		return false
	}
}

func effectivePlanFormat(o opts) string {
	if o.PlanFormat != "" {
		return strings.ToLower(o.PlanFormat)
	}
	switch o.Format {
	case "json", "yaml":
		return o.Format
	default:
		return "json"
	}
}

func planRenderFlagNames(o opts) []string {
	var names []string
	if o.PlanTextStyle != "" {
		names = append(names, "--plan-text-style")
	}
	if o.PlanWrapWidth != 0 {
		names = append(names, "--plan-wrap-width")
	}
	if o.PlanPrint != "" {
		names = append(names, "--plan-print")
	}
	if o.PlanFull {
		names = append(names, "--plan-full")
	}
	if o.PlanShowQuery {
		names = append(names, "--plan-show-query")
	}
	if o.PlanShowQueryStats {
		names = append(names, "--plan-show-query-stats")
	}
	return names
}

func planRenderOptions(o opts) planrender.Options {
	ro := planrender.Options{
		TextStyle:      o.PlanTextStyle,
		WrapWidth:      o.PlanWrapWidth,
		PrintSections:  o.PlanPrint,
		Full:           o.PlanFull,
		ShowQuery:      o.PlanShowQuery,
		ShowQueryStats: o.PlanShowQueryStats,
	}
	if o.PlanShowQuery {
		ro.Query = o.Sql
	}
	return ro
}

func validatePlanFormatValue(format string) error {
	switch strings.ToLower(format) {
	case "json", "yaml", "text", "dot", "mermaid", "d2", "svg", "png":
		return nil
	default:
		return fmt.Errorf("--plan-format must be %s", planFormatHelp)
	}
}

func flagsCannotApply(names []string, format string) error {
	if len(names) == 0 {
		return nil
	}
	verb := "cannot"
	if len(names) == 1 {
		return fmt.Errorf("%s cannot be used with --plan-format=%s", names[0], format)
	}
	return fmt.Errorf("%s %s be used with --plan-format=%s", strings.Join(names, ", "), verb, format)
}

func validatePlanRenderOptions(o opts) error {
	format := effectivePlanFormat(o)
	var textFlags, graphFlags []string
	if o.PlanTextStyle != "" {
		textFlags = append(textFlags, "--plan-text-style")
	}
	if o.PlanWrapWidth != 0 {
		textFlags = append(textFlags, "--plan-wrap-width")
	}
	if o.PlanPrint != "" {
		textFlags = append(textFlags, "--plan-print")
	}
	if o.PlanFull {
		graphFlags = append(graphFlags, "--plan-full")
	}
	if o.PlanShowQuery {
		graphFlags = append(graphFlags, "--plan-show-query")
	}
	if o.PlanShowQueryStats {
		graphFlags = append(graphFlags, "--plan-show-query-stats")
	}

	switch format {
	case "json", "yaml":
		return flagsCannotApply(append(append([]string{}, textFlags...), graphFlags...), format)
	case "text":
		if err := flagsCannotApply(graphFlags, format); err != nil {
			return err
		}
	default:
		if err := flagsCannotApply(textFlags, format); err != nil {
			return err
		}
	}

	if format == "json" || format == "yaml" {
		return nil
	}
	pf, err := planrender.ParseFormat(format)
	if err != nil {
		return fmt.Errorf("--plan-format must be %s", planFormatHelp)
	}
	if err := planRenderOptions(o).Validate(pf); err != nil {
		return err
	}
	if pf.IsBinary() && planDestIsTerminal(resolveDestination(o.PlanOutput).kind) {
		return fmt.Errorf("--plan-format=png cannot write to a terminal; use a file or redirect")
	}
	return nil
}

func validatePlanOutputOptions(o opts) error {
	hasPlan := o.PlanOutput != ""
	if o.PlanFormat != "" && !hasPlan {
		return fmt.Errorf("--plan-format requires --plan-output")
	}
	if o.DiscardResults && !hasPlan {
		return fmt.Errorf("--discard-results requires --plan-output")
	}
	if names := planRenderFlagNames(o); len(names) > 0 && !hasPlan {
		return fmt.Errorf("%s requires --plan-output", strings.Join(names, ", "))
	}
	if o.PlanFormat != "" {
		if err := validatePlanFormatValue(o.PlanFormat); err != nil {
			return err
		}
	}
	if !hasPlan {
		return nil
	}
	mode := o.QueryMode
	if mode == "" {
		mode = "NORMAL"
	}
	if !isPlanProducingQueryMode(mode) {
		return fmt.Errorf("--plan-output requires --query-mode=%s", planModesHelp)
	}
	if o.TryPartitionQuery {
		return fmt.Errorf("--plan-output cannot be combined with --try-partition-query")
	}
	if o.EnablePartitionedDML {
		return fmt.Errorf("--plan-output cannot be combined with --enable-partitioned-dml")
	}
	return validatePlanRenderOptions(o)
}

func validateDestinations(o opts) error {
	primaryRaw := defaultPrimaryOutput(o.Output)
	primary := resolveDestination(primaryRaw)
	var plan *resolvedDest
	if o.PlanOutput != "" {
		d := resolveDestination(o.PlanOutput)
		plan = &d
	}

	if plan != nil && !o.DiscardResults {
		if primary.kind == destKindStdout && plan.kind == destKindStdout {
			return fmt.Errorf("in split mode --output and --plan-output cannot both write to stdout")
		}
		if primary.kind == destKindStderr && plan.kind == destKindStderr {
			return fmt.Errorf("in split mode --output and --plan-output cannot both write to stderr")
		}
		if primary.kind == destKindFile && plan.kind == destKindFile {
			same, err := sameOutputFile(primary.raw, plan.raw)
			if err != nil {
				return err
			}
			if same {
				return fmt.Errorf("--output and --plan-output cannot target the same file")
			}
		}
	}

	inputs := []struct {
		flag, path string
	}{
		{"--sql-file", o.SqlFile},
		{"--param-file", o.ParamFile},
		{"--filter-file", o.JqFromFile},
	}

	checkAlias := func(flagName, destRaw string, d resolvedDest) error {
		if d.kind != destKindFile {
			return nil
		}
		for _, in := range inputs {
			if in.path == "" {
				continue
			}
			same, err := sameOutputFile(destRaw, in.path)
			if err != nil {
				return err
			}
			if same {
				return fmt.Errorf("%s cannot alias %s", flagName, in.flag)
			}
		}
		return nil
	}

	if !o.DiscardResults {
		if err := checkAlias("--output", primaryRaw, primary); err != nil {
			return err
		}
	}
	if plan != nil {
		if err := checkAlias("--plan-output", o.PlanOutput, *plan); err != nil {
			return err
		}
	}
	return nil
}

func fileIdentity(path string) (abs string, info os.FileInfo, exists bool, err error) {
	abs, err = filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", nil, false, err
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return abs, nil, false, nil
		}
		return abs, nil, false, statErr
	}
	if eval, evalErr := filepath.EvalSymlinks(path); evalErr == nil {
		if evalAbs, absErr := filepath.Abs(eval); absErr == nil {
			abs = evalAbs
		}
	}
	return abs, info, true, nil
}

func sameOutputFile(a, b string) (bool, error) {
	absA, infoA, existsA, err := fileIdentity(a)
	if err != nil {
		return false, err
	}
	absB, infoB, existsB, err := fileIdentity(b)
	if err != nil {
		return false, err
	}
	if absA == absB {
		return true, nil
	}
	if existsA && existsB {
		return os.SameFile(infoA, infoB), nil
	}
	return false, nil
}

// fileSink is a regular-file destination written via a sibling temp file.
type fileSink struct {
	file  *os.File
	final string
}

type outputSinks struct {
	primary io.Writer
	plan    io.Writer

	primaryDest resolvedDest
	planDest    resolvedDest
	hasPlan     bool
	discard     bool

	primaryFile *fileSink
	planFile    *fileSink

	primaryReady bool
	done         bool
}

func newOutputSinks(o opts) (*outputSinks, error) {
	if err := validateDestinations(o); err != nil {
		return nil, err
	}
	s := &outputSinks{discard: o.DiscardResults}
	if o.DiscardResults {
		s.primaryReady = true
	} else {
		d := resolveDestination(defaultPrimaryOutput(o.Output))
		w, file, err := openDestination(d)
		if err != nil {
			return nil, fmt.Errorf("--output: %w", err)
		}
		s.primary = w
		s.primaryDest = d
		s.primaryFile = file
	}
	if o.PlanOutput != "" {
		d := resolveDestination(o.PlanOutput)
		w, file, err := openDestination(d)
		if err != nil {
			s.Abort()
			return nil, fmt.Errorf("--plan-output: %w", err)
		}
		s.plan = w
		s.planDest = d
		s.planFile = file
		s.hasPlan = true
	}
	return s, nil
}

func openDestination(d resolvedDest) (io.Writer, *fileSink, error) {
	switch d.kind {
	case destKindStdout:
		return os.Stdout, nil, nil
	case destKindStderr:
		return os.Stderr, nil, nil
	case destKindFile:
		dir := filepath.Dir(d.abs)
		tmp, err := os.CreateTemp(dir, ".execspansql-*.tmp")
		if err != nil {
			return nil, nil, err
		}
		if err := tmp.Chmod(0o600); err != nil {
			name := tmp.Name()
			_ = tmp.Close()
			_ = os.Remove(name)
			return nil, nil, err
		}
		return tmp, &fileSink{file: tmp, final: d.abs}, nil
	default:
		return nil, nil, fmt.Errorf("unknown destination kind")
	}
}

func (s *outputSinks) MarkPrimaryComplete() {
	if s == nil {
		return
	}
	s.primaryReady = true
}

func (s *outputSinks) primaryLabel() string {
	if s == nil || s.discard {
		return ""
	}
	return destLabel(s.primaryDest)
}

func abortFileSink(fs *fileSink) {
	if fs == nil || fs.file == nil {
		return
	}
	name := fs.file.Name()
	_ = fs.file.Close()
	_ = os.Remove(name)
	fs.file = nil
}

func publishFileSink(fs *fileSink) error {
	if fs == nil || fs.file == nil {
		return nil
	}
	name := fs.file.Name()
	if err := fs.file.Close(); err != nil {
		_ = os.Remove(name)
		fs.file = nil
		return err
	}
	fs.file = nil
	if err := os.Rename(name, fs.final); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

func (s *outputSinks) Abort() {
	if s == nil || s.done {
		return
	}
	s.done = true
	abortFileSink(s.primaryFile)
	abortFileSink(s.planFile)
}

func (s *outputSinks) Finish(workErr error) error {
	if s == nil {
		return workErr
	}
	if workErr != nil && !s.primaryReady {
		s.Abort()
		return workErr
	}
	if workErr != nil {
		pubErr := publishFileSink(s.primaryFile)
		abortFileSink(s.planFile)
		s.done = true
		if pubErr != nil {
			return fmt.Errorf("%v; also failed to publish primary output: %w", workErr, pubErr)
		}
		if label := s.primaryLabel(); label != "" {
			return fmt.Errorf("primary output written to %s; %w", label, workErr)
		}
		return workErr
	}
	if err := publishFileSink(s.primaryFile); err != nil {
		abortFileSink(s.planFile)
		s.done = true
		return err
	}
	if err := publishFileSink(s.planFile); err != nil {
		s.done = true
		if label := s.primaryLabel(); label != "" {
			return fmt.Errorf("primary output written to %s; failed to publish plan output: %w", label, err)
		}
		return fmt.Errorf("failed to publish plan output: %w", err)
	}
	s.done = true
	return nil
}

func wrapCommittedOutputError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("output failed after the statement was committed; this is not a rollback and the SQL is not replayed: %w", err)
}

func hasUsableQueryPlan(stats *sppb.ResultSetStats) bool {
	if stats == nil || stats.GetQueryPlan() == nil {
		return false
	}
	return len(stats.GetQueryPlan().GetPlanNodes()) > 0
}

// stripQueryPlanForPrimary clones stats for the plan artifact and clears QueryPlan
// on a separate clone assigned to rs. The original stats message is not mutated.
func stripQueryPlanForPrimary(rs *sppb.ResultSet) (planStats *sppb.ResultSetStats, metadata *sppb.ResultSetMetadata) {
	if rs == nil {
		return nil, nil
	}
	metadata = rs.Metadata
	if rs.Stats == nil {
		return nil, metadata
	}
	planStats, ok := proto.Clone(rs.Stats).(*sppb.ResultSetStats)
	if !ok || planStats == nil {
		return nil, metadata
	}
	primaryStats, ok := proto.Clone(rs.Stats).(*sppb.ResultSetStats)
	if !ok || primaryStats == nil {
		return planStats, metadata
	}
	primaryStats.QueryPlan = nil
	rs.Stats = primaryStats
	return planStats, metadata
}

func writePlan(ctx context.Context, w io.Writer, format string, metadata *sppb.ResultSetMetadata, stats *sppb.ResultSetStats, o opts) error {
	if w == nil {
		return nil
	}
	if !hasUsableQueryPlan(stats) {
		return errNoQueryPlan
	}
	format = strings.ToLower(format)
	switch format {
	case "json", "yaml":
		return writePlanEnvelope(w, format, metadata, stats)
	default:
		return renderPlan(ctx, w, format, metadata, stats, o)
	}
}

func writePlanEnvelope(w io.Writer, format string, metadata *sppb.ResultSetMetadata, stats *sppb.ResultSetStats) error {
	envelope := &sppb.ResultSet{
		Metadata: metadata,
		Stats:    stats,
	}
	m, err := jqresult.ProtoToMap(envelope)
	if err != nil {
		return err
	}
	enc, err := newEncoder(w, format, false, false)
	if err != nil {
		return err
	}
	if err := enc.Encode(m); err != nil {
		_ = closeEncoder(enc)
		return err
	}
	return closeEncoder(enc)
}

func renderPlan(ctx context.Context, w io.Writer, format string, metadata *sppb.ResultSetMetadata, stats *sppb.ResultSetStats, o opts) error {
	pf, err := planrender.ParseFormat(format)
	if err != nil {
		return err
	}
	var rowType *sppb.StructType
	if metadata != nil {
		rowType = metadata.GetRowType()
	}
	err = planrender.Render(ctx, w, pf, rowType, stats, planRenderOptions(o))
	if err == nil {
		return nil
	}
	if errors.Is(err, planrender.ErrNoQueryPlan) {
		return errNoQueryPlan
	}
	return fmt.Errorf("query succeeded, plan rendering failed: %w", err)
}

func statsFromWriterResult(r *svwriter.RowIteratorResult) (*sppb.ResultSetStats, error) {
	if r == nil {
		return nil, nil
	}
	stats := &sppb.ResultSetStats{
		QueryPlan: r.Stats.QueryPlan,
	}
	if r.Stats.QueryStats != nil {
		qs, err := structpb.NewStruct(r.Stats.QueryStats)
		if err != nil {
			return nil, fmt.Errorf("encode query stats: %w", err)
		}
		stats.QueryStats = qs
	}
	if stats.QueryPlan == nil && stats.QueryStats == nil && stats.RowCount == nil {
		return nil, nil
	}
	return stats, nil
}
